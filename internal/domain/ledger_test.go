package domain_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/domain"
)

func TestLedgerValidatesArithmeticAndKeepsImmutableValues(t *testing.T) {
	for _, tc := range []struct {
		direction             domain.Direction
		amount, before, after string
	}{
		{domain.Credit, "100", "0", "100"},
		{domain.Credit, "25", "100", "125"},
		{domain.Debit, "25", "100", "75"},
		{domain.Debit, "100", "100", "0"},
	} {
		t.Run(string(tc.direction)+tc.before+tc.after, func(t *testing.T) {
			entry, err := domain.NewLedgerEntry("entry-1", "wallet-1", "transaction-1", tc.direction,
				money(t, tc.amount), money(t, tc.before), money(t, tc.after), epoch)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := entry.Snapshot()
			snapshot.BalanceAfter = money(t, "999")
			if entry.Snapshot().BalanceAfter != money(t, tc.after) {
				t.Fatal("ledger snapshot mutated original")
			}
		})
	}
}

func TestLedgerRejectsInvalidSnapshots(t *testing.T) {
	entry, err := domain.NewLedgerEntry("entry-1", "wallet-1", "transaction-1", domain.Debit,
		money(t, "25"), money(t, "100"), money(t, "75"), epoch)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*domain.LedgerSnapshot){
		"empty ID":                 func(s *domain.LedgerSnapshot) { s.ID = "" },
		"empty wallet":             func(s *domain.LedgerSnapshot) { s.WalletID = "" },
		"empty transaction":        func(s *domain.LedgerSnapshot) { s.TransactionID = "" },
		"invalid direction":        func(s *domain.LedgerSnapshot) { s.Direction = "MOVE" },
		"wrong arithmetic":         func(s *domain.LedgerSnapshot) { s.BalanceAfter = money(t, "80") },
		"zero amount":              func(s *domain.LedgerSnapshot) { s.Money = money(t, "0") },
		"negative amount":          func(s *domain.LedgerSnapshot) { s.Money = minor(t, -2500, "BRL") },
		"negative before":          func(s *domain.LedgerSnapshot) { s.BalanceBefore = minor(t, -1, "BRL") },
		"negative after":           func(s *domain.LedgerSnapshot) { s.BalanceAfter = minor(t, -1, "BRL") },
		"uninitialized money":      func(s *domain.LedgerSnapshot) { s.Money = domain.Money{} },
		"uninitialized before":     func(s *domain.LedgerSnapshot) { s.BalanceBefore = domain.Money{} },
		"uninitialized after":      func(s *domain.LedgerSnapshot) { s.BalanceAfter = domain.Money{} },
		"currency mismatch amount": func(s *domain.LedgerSnapshot) { s.Money = minor(t, 2500, "USD") },
		"currency mismatch after":  func(s *domain.LedgerSnapshot) { s.BalanceAfter = minor(t, 7500, "USD") },
		"missing time":             func(s *domain.LedgerSnapshot) { s.CreatedAt = time.Time{} },
		"overflow": func(s *domain.LedgerSnapshot) {
			s.Direction = domain.Credit
			s.BalanceBefore = minor(t, math.MaxInt64, "BRL")
		},
	} {
		t.Run(name, func(t *testing.T) {
			invalid := entry.Snapshot()
			mutate(&invalid)
			if err := invalid.Validate(); !errors.Is(err, domain.ErrInvalidLedger) {
				t.Fatal("invalid ledger accepted", err)
			}
			if _, err := domain.NewLedgerEntry(invalid.ID, invalid.WalletID, invalid.TransactionID, invalid.Direction,
				invalid.Money, invalid.BalanceBefore, invalid.BalanceAfter, invalid.CreatedAt); !errors.Is(err, domain.ErrInvalidLedger) {
				t.Fatal("constructor bypasses invariant", err)
			}
		})
	}
	if err := (domain.WalletLedgerEntry{}).Snapshot().Validate(); !errors.Is(err, domain.ErrInvalidLedger) {
		t.Fatal("zero ledger accepted", err)
	}
}
