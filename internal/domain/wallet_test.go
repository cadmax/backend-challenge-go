package domain_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/domain"
)

var epoch = time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)

func wallet(t testing.TB, balance string) *domain.Wallet {
	t.Helper()
	w, err := domain.NewWallet("wallet-1", "player-1", money(t, balance), epoch)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestWalletMutationsPreserveBalanceAndVersion(t *testing.T) {
	w := wallet(t, "100")
	if err := w.Debit(money(t, "80"), epoch.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	beforeRejected := w.Snapshot()
	if err := w.Debit(money(t, "80"), epoch.Add(2*time.Second)); !errors.Is(err, domain.ErrInsufficientFunds) {
		t.Fatal(err)
	}
	if w.Snapshot() != beforeRejected || beforeRejected.Balance.MinorUnits() != 2000 || beforeRejected.Version != 2 {
		t.Fatalf("invalid rejected debit state: %+v", w.Snapshot())
	}
	if err := w.Credit(money(t, "25"), epoch.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	s := w.Snapshot()
	if s.Balance.MinorUnits() != 4500 || s.Version != 3 || !s.CreatedAt.Equal(epoch) || !s.UpdatedAt.Equal(epoch.Add(3*time.Second)) {
		t.Fatalf("invalid credited state: %+v", s)
	}
	for _, operation := range []func(domain.Money, time.Time) error{w.Debit, w.Credit} {
		if err := operation(money(t, "0"), epoch.Add(4*time.Second)); err != nil || w.Snapshot() != s {
			t.Fatal("zero movement changed version or balance", err)
		}
	}
	if err := w.Debit(money(t, "45"), epoch.Add(5*time.Second)); err != nil || w.Snapshot().Balance.MinorUnits() != 0 {
		t.Fatal("exact balance debit failed", err)
	}
}

func TestWalletRejectsInvalidStateAndMovements(t *testing.T) {
	base := wallet(t, "10").Snapshot()
	for name, mutate := range map[string]func(*domain.WalletSnapshot){
		"empty ID":         func(s *domain.WalletSnapshot) { s.ID = "" },
		"padded ID":        func(s *domain.WalletSnapshot) { s.ID = " wallet" },
		"control ID":       func(s *domain.WalletSnapshot) { s.ID = "wal\nlet" },
		"empty player":     func(s *domain.WalletSnapshot) { s.PlayerID = "" },
		"invalid money":    func(s *domain.WalletSnapshot) { s.Balance = domain.Money{} },
		"negative balance": func(s *domain.WalletSnapshot) { s.Balance = minor(t, -1, "BRL") },
		"zero version":     func(s *domain.WalletSnapshot) { s.Version = 0 },
		"negative version": func(s *domain.WalletSnapshot) { s.Version = -1 },
		"zero created":     func(s *domain.WalletSnapshot) { s.CreatedAt = time.Time{} },
		"earlier update":   func(s *domain.WalletSnapshot) { s.UpdatedAt = epoch.Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			s := base
			mutate(&s)
			if _, err := domain.RestoreWallet(s); !errors.Is(err, domain.ErrInvalidWallet) {
				t.Fatalf("invalid restoration: %v", err)
			}
		})
	}
	w := wallet(t, "10")
	for _, operation := range []func(domain.Money, time.Time) error{w.Debit, w.Credit} {
		for _, invalid := range []domain.Money{domain.Money{}, minor(t, -1, "BRL"), minor(t, 1, "USD")} {
			if err := operation(invalid, epoch.Add(time.Second)); err == nil || w.Snapshot() != base {
				t.Fatalf("invalid money mutation: %v", err)
			}
		}
		for _, invalid := range []time.Time{{}, epoch.Add(-time.Second)} {
			if err := operation(money(t, "1"), invalid); !errors.Is(err, domain.ErrInvalidWallet) || w.Snapshot() != base {
				t.Fatalf("invalid timestamp mutation: %v", err)
			}
		}
	}
	var uninitialized domain.Wallet
	if err := uninitialized.Credit(money(t, "1"), epoch); !errors.Is(err, domain.ErrInvalidWallet) {
		t.Fatal(err)
	}
	var nilWallet *domain.Wallet
	if err := nilWallet.Debit(money(t, "1"), epoch); !errors.Is(err, domain.ErrInvalidWallet) {
		t.Fatal(err)
	}
}

func TestWalletRestorationAndOverflowAreAtomic(t *testing.T) {
	s := wallet(t, "0").Snapshot()
	s.Balance = minor(t, math.MaxInt64, "BRL")
	s.Version = 18
	w, err := domain.RestoreWallet(s)
	if err != nil || w.Snapshot() != s {
		t.Fatal("restoration changed stored values", err)
	}
	if err := w.Credit(money(t, "0.01"), epoch); !errors.Is(err, domain.ErrOverflow) || w.Snapshot() != s {
		t.Fatal("overflow mutation", err)
	}
	s.Version = math.MaxInt64
	w, err = domain.RestoreWallet(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Debit(money(t, "0.01"), epoch); !errors.Is(err, domain.ErrOverflow) || w.Snapshot() != s {
		t.Fatal("version overflow mutation", err)
	}
	s.Balance = money(t, "0")
	if w.Snapshot().Balance.MinorUnits() != math.MaxInt64 {
		t.Fatal("snapshot mutation changed aggregate")
	}
}
