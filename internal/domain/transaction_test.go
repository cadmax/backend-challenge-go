package domain_test

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/domain"
)

func wagerInput(t testing.TB, kind domain.Kind, amount string) domain.WagerInput {
	t.Helper()
	input := domain.WagerInput{
		ID: "transaction-1", ProviderID: "provider-a", ExternalTransactionID: "external-1",
		IdempotencyKey: "provider-a:external-1", PayloadHash: strings.Repeat("a", 64),
		WalletID: "wallet-1", PlayerID: "player-1", RoundID: "round-1", GameID: "game-1",
		Kind: kind, Money: money(t, amount),
	}
	if kind == domain.Refund || kind == domain.Rollback {
		input.ReferenceExternalTransactionID = "external-original"
	}
	return input
}

func wager(t testing.TB, kind domain.Kind, amount string) *domain.WagerTransaction {
	t.Helper()
	tx, err := domain.NewWagerTransaction(wagerInput(t, kind, amount), epoch)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func reference(t testing.TB, kind domain.Kind, amount string, status domain.Status) domain.TransactionSnapshot {
	t.Helper()
	input := wagerInput(t, kind, amount)
	input.ID, input.ExternalTransactionID, input.IdempotencyKey = "transaction-original", "external-original", "original-key"
	if kind == domain.Refund || kind == domain.Rollback {
		input.ReferenceExternalTransactionID = "external-underlying"
	}
	tx, err := domain.NewWagerTransaction(input, epoch)
	if err != nil {
		t.Fatal(err)
	}
	switch status {
	case domain.Processed:
		refID := ""
		if input.ReferenceExternalTransactionID != "" {
			refID = "transaction-underlying"
		}
		err = tx.Process(money(t, "100"), refID, epoch)
	case domain.Rejected:
		err = tx.Reject("TEST_REJECTION", nil, epoch)
	case domain.Failed:
		err = tx.Fail("PERMANENT_FAILURE", epoch)
	case domain.PendingReference:
		err = tx.AwaitReference(epoch)
	}
	if err != nil {
		t.Fatal(err)
	}
	return tx.Snapshot()
}

func TestExternalTransactionConstructionAndZeroPolicy(t *testing.T) {
	for _, kind := range []domain.Kind{domain.Bet, domain.Win, domain.Refund, domain.Rollback} {
		if _, err := domain.NewWagerTransaction(wagerInput(t, kind, "0"), epoch); !errors.Is(err, domain.ErrInvalidTransaction) {
			t.Fatalf("%s accepted zero: %v", kind, err)
		}
		tx := wager(t, kind, "0.01")
		if tx.Snapshot().Status != domain.Pending || tx.Snapshot().ResultBalance != nil {
			t.Fatalf("%s did not start pending", kind)
		}
	}
	if _, err := domain.NewWagerTransaction(wagerInput(t, domain.Loss, "0.01"), epoch); !errors.Is(err, domain.ErrInvalidTransaction) {
		t.Fatal("LOSS accepted nonzero", err)
	}
	if tx := wager(t, domain.Loss, "0"); tx.Snapshot().Money.MinorUnits() != 0 {
		t.Fatal("LOSS amount")
	}
	for name, mutate := range map[string]func(*domain.WagerInput){
		"empty internal ID":          func(s *domain.WagerInput) { s.ID = "" },
		"empty provider":             func(s *domain.WagerInput) { s.ProviderID = "" },
		"empty external ID":          func(s *domain.WagerInput) { s.ExternalTransactionID = "" },
		"empty key":                  func(s *domain.WagerInput) { s.IdempotencyKey = "" },
		"invalid hash":               func(s *domain.WagerInput) { s.PayloadHash = "not-a-hash" },
		"nonhex hash":                func(s *domain.WagerInput) { s.PayloadHash = strings.Repeat("z", 64) },
		"empty wallet":               func(s *domain.WagerInput) { s.WalletID = "" },
		"empty player":               func(s *domain.WagerInput) { s.PlayerID = "" },
		"empty round":                func(s *domain.WagerInput) { s.RoundID = "" },
		"empty game":                 func(s *domain.WagerInput) { s.GameID = "" },
		"invalid money":              func(s *domain.WagerInput) { s.Money = domain.Money{} },
		"negative money":             func(s *domain.WagerInput) { s.Money = minor(t, -1, "BRL") },
		"unknown kind":               func(s *domain.WagerInput) { s.Kind = "CREDIT" },
		"external opening":           func(s *domain.WagerInput) { s.Kind = domain.Opening },
		"reference on bet":           func(s *domain.WagerInput) { s.ReferenceExternalTransactionID = "another" },
		"missing refund reference":   func(s *domain.WagerInput) { s.Kind = domain.Refund },
		"missing rollback reference": func(s *domain.WagerInput) { s.Kind = domain.Rollback },
		"self reference": func(s *domain.WagerInput) {
			s.Kind = domain.Refund
			s.ReferenceExternalTransactionID = s.ExternalTransactionID
		},
		"oversized key": func(s *domain.WagerInput) { s.IdempotencyKey = strings.Repeat("x", 256) },
	} {
		t.Run(name, func(t *testing.T) {
			input := wagerInput(t, domain.Bet, "25")
			mutate(&input)
			if _, err := domain.NewWagerTransaction(input, epoch); !errors.Is(err, domain.ErrInvalidTransaction) {
				t.Fatalf("invalid input accepted: %v", err)
			}
		})
	}
	if _, err := domain.NewWagerTransaction(wagerInput(t, domain.Bet, "1"), time.Time{}); !errors.Is(err, domain.ErrInvalidTransaction) {
		t.Fatal(err)
	}
}

func TestOpeningHasOnlyInternalMetadata(t *testing.T) {
	tx, err := domain.NewOpeningTransaction("opening-1", "wallet-1", "player-1", money(t, "100"), epoch)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Process(money(t, "100"), "", epoch); err != nil {
		t.Fatal(err)
	}
	s := tx.Snapshot()
	if s.Kind != domain.Opening || s.Status != domain.Processed || s.ProviderID != "" ||
		s.ExternalTransactionID != "" || s.IdempotencyKey != "" || s.PayloadHash != "" || s.RoundID != "" || s.GameID != "" {
		t.Fatalf("invalid internal opening: %+v", s)
	}
	for name, mutate := range map[string]func(*domain.TransactionSnapshot){
		"provider":                     func(s *domain.TransactionSnapshot) { s.ProviderID = "provider-a" },
		"external ID":                  func(s *domain.TransactionSnapshot) { s.ExternalTransactionID = "external" },
		"key":                          func(s *domain.TransactionSnapshot) { s.IdempotencyKey = "key" },
		"hash":                         func(s *domain.TransactionSnapshot) { s.PayloadHash = strings.Repeat("a", 64) },
		"round":                        func(s *domain.TransactionSnapshot) { s.RoundID = "round" },
		"game":                         func(s *domain.TransactionSnapshot) { s.GameID = "game" },
		"reference":                    func(s *domain.TransactionSnapshot) { s.ReferenceExternalTransactionID = "ref" },
		"balance differs from opening": func(s *domain.TransactionSnapshot) { b := money(t, "101"); s.ResultBalance = &b },
	} {
		t.Run(name, func(t *testing.T) {
			bad := s
			mutate(&bad)
			if _, err := domain.RestoreWagerTransaction(bad); !errors.Is(err, domain.ErrInvalidTransaction) {
				t.Fatal("accepted invalid internal metadata", err)
			}
		})
	}
	for _, invalid := range []domain.Money{money(t, "0"), minor(t, -1, "BRL"), {}} {
		if _, err := domain.NewOpeningTransaction("opening-1", "wallet-1", "player-1", invalid, epoch); !errors.Is(err, domain.ErrInvalidTransaction) {
			t.Fatal("invalid opening amount accepted", err)
		}
	}
}

func TestFinancialRulesForAllExternalKinds(t *testing.T) {
	for _, tc := range []struct {
		name      string
		kind      domain.Kind
		amount    string
		refKind   domain.Kind
		refAmount string
		direction domain.Direction
	}{
		{"bet", domain.Bet, "25", "", "", domain.Debit},
		{"win without reference", domain.Win, "25", "", "", domain.Credit},
		{"win with bet", domain.Win, "50", domain.Bet, "25", domain.Credit},
		{"loss", domain.Loss, "0", "", "", ""},
		{"refund bet", domain.Refund, "25", domain.Bet, "25", domain.Credit},
		{"rollback bet", domain.Rollback, "25", domain.Bet, "25", domain.Credit},
		{"rollback win", domain.Rollback, "25", domain.Win, "25", domain.Debit},
		{"rollback refund", domain.Rollback, "25", domain.Refund, "25", domain.Debit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := wagerInput(t, tc.kind, tc.amount)
			var ref *domain.TransactionSnapshot
			if tc.refKind != "" {
				input.ReferenceExternalTransactionID = "external-original"
				s := reference(t, tc.refKind, tc.refAmount, domain.Processed)
				ref = &s
			}
			tx, err := domain.NewWagerTransaction(input, epoch)
			if err != nil {
				t.Fatal(err)
			}
			w := wallet(t, "100")
			before, transactionBefore := w.Snapshot(), tx.Snapshot()
			movement, err := tx.Evaluate(before, ref, false)
			if err != nil || movement.Direction != tc.direction || movement.Money != input.Money {
				t.Fatalf("movement: %+v, %v", movement, err)
			}
			if w.Snapshot() != before || !reflect.DeepEqual(tx.Snapshot(), transactionBefore) {
				t.Fatal("evaluation mutated state")
			}
		})
	}
}

func TestRuleRejectionsHaveStableSpecificCodes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		kind     domain.Kind
		amount   string
		balance  string
		refKind  domain.Kind
		refAmt   string
		reversed bool
		code     string
	}{
		{"bet funds", domain.Bet, "80", "20", "", "", false, domain.CodeInsufficientFunds},
		{"rollback funds", domain.Rollback, "25", "20", domain.Win, "25", false, domain.CodeReversalInsufficientFunds},
		{"refund partial", domain.Refund, "20", "100", domain.Bet, "25", false, domain.CodeReferenceMismatch},
		{"rollback partial", domain.Rollback, "20", "100", domain.Win, "25", false, domain.CodeReferenceMismatch},
		{"refund win", domain.Refund, "25", "100", domain.Win, "25", false, domain.CodeInvalidReferenceKind},
		{"win references win", domain.Win, "25", "100", domain.Win, "25", false, domain.CodeInvalidReferenceKind},
		{"rollback rollback", domain.Rollback, "25", "100", domain.Rollback, "25", false, domain.CodeInvalidReferenceKind},
		{"duplicate refund", domain.Refund, "25", "100", domain.Bet, "25", true, domain.CodeAlreadyReversed},
		{"rollback after refund", domain.Rollback, "25", "100", domain.Bet, "25", true, domain.CodeAlreadyReversed},
		{"duplicate rollback win", domain.Rollback, "25", "100", domain.Win, "25", true, domain.CodeAlreadyReversed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := wagerInput(t, tc.kind, tc.amount)
			var ref *domain.TransactionSnapshot
			if tc.refKind != "" {
				input.ReferenceExternalTransactionID = "external-original"
				s := reference(t, tc.refKind, tc.refAmt, domain.Processed)
				ref = &s
			}
			tx, err := domain.NewWagerTransaction(input, epoch)
			if err != nil {
				t.Fatal(err)
			}
			_, err = tx.Evaluate(wallet(t, tc.balance).Snapshot(), ref, tc.reversed)
			assertRuleCode(t, err, tc.code)
		})
	}
	w := wallet(t, "100").Snapshot()
	for _, tc := range []struct {
		mutate func(*domain.WalletSnapshot)
		code   string
	}{
		{func(s *domain.WalletSnapshot) { s.PlayerID = "other" }, domain.CodeWalletMismatch},
		{func(s *domain.WalletSnapshot) { s.ID = "other" }, domain.CodeWalletMismatch},
		{func(s *domain.WalletSnapshot) { s.Balance = minor(t, 10000, "USD") }, domain.CodeCurrencyMismatch},
	} {
		other := w
		tc.mutate(&other)
		_, err := wager(t, domain.Bet, "25").Evaluate(other, nil, false)
		assertRuleCode(t, err, tc.code)
	}
	w.Balance = minor(t, math.MaxInt64, "BRL")
	_, err := wager(t, domain.Win, "0.01").Evaluate(w, nil, false)
	assertRuleCode(t, err, domain.CodeBalanceOverflow)
}

func assertRuleCode(t testing.TB, err error, code string) {
	t.Helper()
	var rule *domain.RuleError
	if !errors.As(err, &rule) || rule.Code != code || rule.Error() != code {
		t.Fatalf("expected rule %s, got %v", code, err)
	}
}

func TestRuleErrorsMatchOnlyTheirSentinel(t *testing.T) {
	for _, test := range []struct {
		code string
		want error
	}{
		{domain.CodeInsufficientFunds, domain.ErrInsufficientFunds},
		{domain.CodeReversalInsufficientFunds, domain.ErrInsufficientFunds},
		{domain.CodeCurrencyMismatch, domain.ErrCurrencyMismatch},
		{domain.CodeBalanceOverflow, domain.ErrOverflow},
		{domain.CodeReferenceMismatch, nil},
	} {
		t.Run(test.code, func(t *testing.T) {
			err := &domain.RuleError{Code: test.code}
			for _, target := range []error{domain.ErrInsufficientFunds, domain.ErrCurrencyMismatch, domain.ErrOverflow, domain.ErrInvalidTransaction} {
				if got := errors.Is(err, target); got != (target == test.want) {
					t.Fatalf("errors.Is(%s, %v) = %v", test.code, target, got)
				}
			}
		})
	}
}

func TestReferenceIdentityAndStateRules(t *testing.T) {
	tx, w := wager(t, domain.Refund, "25"), wallet(t, "100").Snapshot()
	if _, err := tx.Evaluate(w, nil, false); !errors.Is(err, domain.ErrReferencePending) {
		t.Fatal("missing reference should wait", err)
	}
	for _, status := range []domain.Status{domain.Pending, domain.PendingReference, domain.Rejected, domain.Failed} {
		kind := domain.Bet
		if status == domain.PendingReference {
			kind = domain.Refund
		}
		ref := reference(t, kind, "25", status)
		_, err := tx.Evaluate(w, &ref, false)
		if status == domain.Pending || status == domain.PendingReference {
			if !errors.Is(err, domain.ErrReferencePending) {
				t.Fatal("pending reference should wait", err)
			}
		} else {
			assertRuleCode(t, err, domain.CodeReferenceNotProcessed)
		}
	}
	for name, mutate := range map[string]func(*domain.TransactionSnapshot){
		"provider":    func(s *domain.TransactionSnapshot) { s.ProviderID = "provider-b" },
		"external ID": func(s *domain.TransactionSnapshot) { s.ExternalTransactionID = "other" },
		"player":      func(s *domain.TransactionSnapshot) { s.PlayerID = "other" },
		"wallet":      func(s *domain.TransactionSnapshot) { s.WalletID = "other" },
		"round":       func(s *domain.TransactionSnapshot) { s.RoundID = "other" },
		"currency": func(s *domain.TransactionSnapshot) {
			s.Money = minor(t, 2500, "USD")
			b := minor(t, 10000, "USD")
			s.ResultBalance = &b
		},
	} {
		t.Run(name, func(t *testing.T) {
			ref := reference(t, domain.Bet, "25", domain.Processed)
			mutate(&ref)
			_, err := tx.Evaluate(w, &ref, false)
			assertRuleCode(t, err, domain.CodeReferenceMismatch)
		})
	}
	ref := reference(t, domain.Bet, "25", domain.Processed)
	ref.GameID = "another-game"
	if _, err := tx.Evaluate(w, &ref, false); err != nil {
		t.Fatal("challenge reference identity intentionally does not require game ID equality", err)
	}
	if _, err := wager(t, domain.Bet, "25").Evaluate(w, &ref, false); !errors.Is(err, domain.ErrInvalidTransaction) {
		t.Fatal("unexpected reference accepted", err)
	}
	winInput := wagerInput(t, domain.Win, "50")
	winInput.ReferenceExternalTransactionID = ref.ExternalTransactionID
	win, err := domain.NewWagerTransaction(winInput, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := win.Evaluate(w, &ref, true); err != nil {
		t.Fatal("WIN does not itself reverse the reference", err)
	}
}

func TestTransactionTransitionsAndTerminalImmutability(t *testing.T) {
	for _, pendingReference := range []bool{false, true} {
		for _, terminal := range []domain.Status{domain.Processed, domain.Rejected, domain.Failed} {
			tx := wager(t, domain.Refund, "25")
			if pendingReference {
				if err := tx.AwaitReference(epoch.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				if err := tx.AwaitReference(epoch.Add(2 * time.Second)); !errors.Is(err, domain.ErrInvalidTransition) {
					t.Fatal("repeated transition should not create another event", err)
				}
			}
			at, balance := epoch.Add(3*time.Second), money(t, "100")
			var err error
			switch terminal {
			case domain.Processed:
				err = tx.Process(balance, "transaction-original", at)
			case domain.Rejected:
				err = tx.Reject(domain.CodeReferenceNotFound, &balance, at)
			case domain.Failed:
				err = tx.Fail("PERMANENT_STORAGE_FAILURE", at)
			}
			if err != nil || tx.Snapshot().Status != terminal || !tx.Snapshot().UpdatedAt.Equal(at) {
				t.Fatalf("transition to %s: %v", terminal, err)
			}
			before := tx.Snapshot()
			for _, transition := range []func() error{
				func() error { return tx.Process(balance, "transaction-original", at) },
				func() error { return tx.Reject("REJECTED_AGAIN", &balance, at) },
				func() error { return tx.Fail("FAILED_AGAIN", at) },
				func() error { return tx.AwaitReference(at) },
			} {
				if err := transition(); !errors.Is(err, domain.ErrInvalidTransition) || !reflect.DeepEqual(before, tx.Snapshot()) {
					t.Fatalf("terminal state changed: %v", err)
				}
			}
			if _, err := tx.Evaluate(wallet(t, "100").Snapshot(), nil, false); !errors.Is(err, domain.ErrInvalidTransition) {
				t.Fatal("terminal transaction evaluated", err)
			}
		}
	}
}

func TestInvalidTransitionsNeverChangeState(t *testing.T) {
	tx := wager(t, domain.Bet, "25")
	before := tx.Snapshot()
	for _, transition := range []func() error{
		func() error { return tx.AwaitReference(epoch) },
		func() error { return tx.Process(domain.Money{}, "", epoch) },
		func() error { return tx.Process(minor(t, -1, "BRL"), "", epoch) },
		func() error { return tx.Process(minor(t, 100, "USD"), "", epoch) },
		func() error { return tx.Process(money(t, "75"), "unexpected-reference", epoch) },
		func() error { return tx.Process(money(t, "75"), "", time.Time{}) },
		func() error { return tx.Process(money(t, "75"), "", epoch.Add(-time.Second)) },
		func() error { return tx.Reject("", nil, epoch) },
		func() error { return tx.Reject("unclassified text", nil, epoch) },
		func() error { return tx.Fail("", epoch) },
	} {
		if err := transition(); err == nil || !reflect.DeepEqual(before, tx.Snapshot()) {
			t.Fatal("invalid transition changed state", err)
		}
	}
	refTx := wager(t, domain.Refund, "25")
	if err := refTx.Process(money(t, "100"), "", epoch); !errors.Is(err, domain.ErrInvalidTransaction) {
		t.Fatal("processed unresolved reference", err)
	}
	var zero domain.WagerTransaction
	if err := zero.Fail("ERROR", epoch); !errors.Is(err, domain.ErrInvalidTransaction) {
		t.Fatal(err)
	}
	var nilTx *domain.WagerTransaction
	if err := nilTx.AwaitReference(epoch); !errors.Is(err, domain.ErrInvalidTransaction) {
		t.Fatal(err)
	}
}

func TestRehydrationAndResultSnapshotsDoNotSharePointers(t *testing.T) {
	tx := wager(t, domain.Bet, "25")
	if err := tx.Process(money(t, "75"), "", epoch); err != nil {
		t.Fatal(err)
	}
	snapshot := tx.Snapshot()
	restored, err := domain.RestoreWagerTransaction(snapshot)
	if err != nil || !reflect.DeepEqual(restored.Snapshot(), snapshot) {
		t.Fatal("restore changed transaction", err)
	}
	*snapshot.ResultBalance = money(t, "99")
	if tx.Snapshot().ResultBalance.MinorUnits() != 7500 || restored.Snapshot().ResultBalance.MinorUnits() != 7500 {
		t.Fatal("snapshot exposes mutable result")
	}
	for name, mutate := range map[string]func(*domain.TransactionSnapshot){
		"unknown status":                      func(s *domain.TransactionSnapshot) { s.Status = "PROCESSING" },
		"missing result":                      func(s *domain.TransactionSnapshot) { s.ResultBalance = nil },
		"successful failure":                  func(s *domain.TransactionSnapshot) { s.FailureCode = "FAILED" },
		"pending result":                      func(s *domain.TransactionSnapshot) { s.Status = domain.Pending },
		"pending reference without reference": func(s *domain.TransactionSnapshot) { s.Status = domain.PendingReference; s.ResultBalance = nil },
		"rejection without code":              func(s *domain.TransactionSnapshot) { s.Status = domain.Rejected },
		"backward timestamp":                  func(s *domain.TransactionSnapshot) { s.UpdatedAt = epoch.Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := tx.Snapshot()
			mutate(&invalid)
			if _, err := domain.RestoreWagerTransaction(invalid); !errors.Is(err, domain.ErrInvalidTransaction) {
				t.Fatal("invalid state restored", err)
			}
		})
	}
	// Currency mismatch rejections may return the actual wallet's currency.
	rejected := wager(t, domain.Bet, "25")
	actual := minor(t, 10000, "USD")
	if err := rejected.Reject(domain.CodeCurrencyMismatch, &actual, epoch); err != nil {
		t.Fatal(err)
	}
	actual = money(t, "999")
	if rejected.Snapshot().ResultBalance.Currency() != "USD" {
		t.Fatal("rejection retained mutable caller pointer")
	}
}
