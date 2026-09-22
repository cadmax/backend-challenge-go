package domain

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Kind string

const (
	Opening  Kind = "OPENING"
	Bet      Kind = "BET"
	Win      Kind = "WIN"
	Loss     Kind = "LOSS"
	Refund   Kind = "REFUND"
	Rollback Kind = "ROLLBACK"
)

type Status string

const (
	Pending          Status = "PENDING"
	PendingReference Status = "PENDING_REFERENCE"
	Processed        Status = "PROCESSED"
	Rejected         Status = "REJECTED"
	Failed           Status = "FAILED"
)

type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

var (
	ErrInvalidTransaction = errors.New("invalid transaction")
	ErrInvalidTransition  = errors.New("invalid transaction transition")
	ErrReferencePending   = errors.New("reference unavailable or pending")
)

const (
	CodeInsufficientFunds         = "INSUFFICIENT_FUNDS"
	CodeReversalInsufficientFunds = "REVERSAL_INSUFFICIENT_FUNDS"
	CodeWalletMismatch            = "WALLET_MISMATCH"
	CodeCurrencyMismatch          = "CURRENCY_MISMATCH"
	CodeReferenceNotFound         = "REFERENCE_NOT_FOUND"
	CodeReferenceNotProcessed     = "REFERENCE_NOT_PROCESSED"
	CodeReferenceMismatch         = "REFERENCE_MISMATCH"
	CodeInvalidReferenceKind      = "INVALID_REFERENCE_KIND"
	CodeAlreadyReversed           = "ALREADY_REVERSED"
	CodeBalanceOverflow           = "BALANCE_OVERFLOW"
)

// RuleError is a terminal business rejection, distinct from a reference that
// may become available later or infrastructure errors outside this package.
type RuleError struct{ Code string }

func (e *RuleError) Error() string { return e.Code }

func (e *RuleError) Is(target error) bool {
	return (target == ErrInsufficientFunds &&
		(e.Code == CodeInsufficientFunds || e.Code == CodeReversalInsufficientFunds)) ||
		(target == ErrCurrencyMismatch && e.Code == CodeCurrencyMismatch) ||
		(target == ErrOverflow && e.Code == CodeBalanceOverflow)
}

type WagerInput struct {
	ID                             string
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	WalletID                       string
	PlayerID                       string
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Money                          Money
	ReferenceExternalTransactionID string
}

type TransactionSnapshot struct {
	WagerInput
	Status                 Status
	ReferenceTransactionID string
	FailureCode            string
	ResultBalance          *Money
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

type WagerTransaction struct{ state TransactionSnapshot }

func NewWagerTransaction(input WagerInput, at time.Time) (*WagerTransaction, error) {
	if input.Kind == Opening {
		return nil, fmt.Errorf("%w: OPENING is internal", ErrInvalidTransaction)
	}
	return RestoreWagerTransaction(TransactionSnapshot{
		WagerInput: input, Status: Pending, CreatedAt: at.UTC(), UpdatedAt: at.UTC(),
	})
}

// NewOpeningTransaction creates the internal operation for a positive opening.
// Zero-balance wallets deliberately have no financial opening transaction.
func NewOpeningTransaction(id, walletID, playerID string, money Money, at time.Time) (*WagerTransaction, error) {
	return RestoreWagerTransaction(TransactionSnapshot{
		WagerInput: WagerInput{ID: id, WalletID: walletID, PlayerID: playerID, Kind: Opening, Money: money},
		Status:     Pending, CreatedAt: at.UTC(), UpdatedAt: at.UTC(),
	})
}

// RestoreWagerTransaction validates stored state without transitioning or emitting
// anything. Copies prevent callers from modifying the stored result through a pointer.
func RestoreWagerTransaction(state TransactionSnapshot) (*WagerTransaction, error) {
	if err := state.Validate(); err != nil {
		return nil, err
	}
	state = cloneTransactionSnapshot(state)
	state.CreatedAt, state.UpdatedAt = state.CreatedAt.UTC(), state.UpdatedAt.UTC()
	return &WagerTransaction{state: state}, nil
}

func (s TransactionSnapshot) Validate() error {
	if err := validateWagerInput(s.WagerInput); err != nil {
		return err
	}
	if s.CreatedAt.IsZero() || s.UpdatedAt.Before(s.CreatedAt) {
		return ErrInvalidTransaction
	}
	if s.ReferenceTransactionID != "" && (!validIdentifier(s.ReferenceTransactionID) ||
		s.ReferenceExternalTransactionID == "" || s.ReferenceTransactionID == s.ID) {
		return ErrInvalidTransaction
	}
	if s.ResultBalance != nil && (s.ResultBalance.Validate() != nil || s.ResultBalance.MinorUnits() < 0) {
		return ErrInvalidTransaction
	}
	switch s.Status {
	case Pending, PendingReference:
		if s.FailureCode != "" || s.ResultBalance != nil || s.ReferenceTransactionID != "" {
			return ErrInvalidTransaction
		}
		if s.Status == PendingReference && s.ReferenceExternalTransactionID == "" {
			return ErrInvalidTransaction
		}
	case Processed:
		if s.FailureCode != "" || s.ResultBalance == nil ||
			(s.ReferenceExternalTransactionID != "" && s.ReferenceTransactionID == "") {
			return ErrInvalidTransaction
		}
		if s.ResultBalance.Currency() != s.Money.Currency() ||
			(s.Kind == Opening && s.ResultBalance.MinorUnits() != s.Money.MinorUnits()) {
			return ErrInvalidTransaction
		}
	case Rejected, Failed:
		if !validFailureCode(s.FailureCode) {
			return ErrInvalidTransaction
		}
	default:
		return ErrInvalidTransaction
	}
	return nil
}

func validateWagerInput(input WagerInput) error {
	if !validIdentifier(input.ID) || !validIdentifier(input.WalletID) || !validIdentifier(input.PlayerID) ||
		input.Money.Validate() != nil || input.Money.MinorUnits() < 0 {
		return ErrInvalidTransaction
	}
	if input.Kind == Opening {
		if input.Money.MinorUnits() == 0 || input.ProviderID != "" || input.ExternalTransactionID != "" ||
			input.IdempotencyKey != "" || input.PayloadHash != "" || input.RoundID != "" ||
			input.GameID != "" || input.ReferenceExternalTransactionID != "" {
			return ErrInvalidTransaction
		}
		return nil
	}
	if !validIdentifier(input.ProviderID) || !validIdentifier(input.ExternalTransactionID) ||
		!validIdentifier(input.IdempotencyKey) || !validIdentifier(input.RoundID) || !validIdentifier(input.GameID) ||
		!validHash(input.PayloadHash) {
		return ErrInvalidTransaction
	}
	if input.ReferenceExternalTransactionID != "" &&
		(!validIdentifier(input.ReferenceExternalTransactionID) || input.ReferenceExternalTransactionID == input.ExternalTransactionID) {
		return ErrInvalidTransaction
	}
	switch input.Kind {
	case Bet:
		if input.Money.MinorUnits() == 0 || input.ReferenceExternalTransactionID != "" {
			return ErrInvalidTransaction
		}
	case Win:
		if input.Money.MinorUnits() == 0 {
			return ErrInvalidTransaction
		}
	case Loss:
		if input.Money.MinorUnits() != 0 || input.ReferenceExternalTransactionID != "" {
			return ErrInvalidTransaction
		}
	case Refund, Rollback:
		if input.Money.MinorUnits() == 0 || input.ReferenceExternalTransactionID == "" {
			return ErrInvalidTransaction
		}
	default:
		return ErrInvalidTransaction
	}
	return nil
}

func validHash(hash string) bool {
	if len(hash) != 64 || strings.ToLower(hash) != hash {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil
}

func validFailureCode(code string) bool {
	if code == "" || len(code) > 64 {
		return false
	}
	for _, c := range code {
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}

func cloneTransactionSnapshot(state TransactionSnapshot) TransactionSnapshot {
	if state.ResultBalance != nil {
		balance := *state.ResultBalance
		state.ResultBalance = &balance
	}
	return state
}

func (t *WagerTransaction) Snapshot() TransactionSnapshot {
	if t == nil {
		return TransactionSnapshot{}
	}
	return cloneTransactionSnapshot(t.state)
}

func (t *WagerTransaction) canTransition(at time.Time) error {
	if t == nil || t.state.Validate() != nil {
		return ErrInvalidTransaction
	}
	if (t.state.Status != Pending && t.state.Status != PendingReference) || at.IsZero() || at.Before(t.state.UpdatedAt) {
		return ErrInvalidTransition
	}
	return nil
}

func (t *WagerTransaction) transition(next TransactionSnapshot, at time.Time) error {
	if err := t.canTransition(at); err != nil {
		return err
	}
	next.UpdatedAt = at.UTC()
	if err := next.Validate(); err != nil {
		return err
	}
	t.state = cloneTransactionSnapshot(next)
	return nil
}

func (t *WagerTransaction) AwaitReference(at time.Time) error {
	if err := t.canTransition(at); err != nil {
		return err
	}
	if t.state.Status != Pending || t.state.ReferenceExternalTransactionID == "" {
		return ErrInvalidTransition
	}
	next := t.state
	next.Status = PendingReference
	return t.transition(next, at)
}

func (t *WagerTransaction) Process(balance Money, referenceID string, at time.Time) error {
	if err := t.canTransition(at); err != nil {
		return err
	}
	next := t.state
	next.Status, next.ResultBalance, next.ReferenceTransactionID = Processed, &balance, referenceID
	return t.transition(next, at)
}

func (t *WagerTransaction) Reject(failureCode string, balance *Money, at time.Time) error {
	if err := t.canTransition(at); err != nil {
		return err
	}
	next := t.state
	next.Status, next.FailureCode, next.ResultBalance = Rejected, failureCode, balance
	return t.transition(next, at)
}

func (t *WagerTransaction) Fail(failureCode string, at time.Time) error {
	if err := t.canTransition(at); err != nil {
		return err
	}
	next := t.state
	next.Status, next.FailureCode = Failed, failureCode
	return t.transition(next, at)
}

type Movement struct {
	Direction Direction
	Money     Money
}

// Evaluate returns a proposed movement without mutating the transaction or wallet.
// alreadyReversed must be checked persistently while holding the wallet lock.
// A successful REFUND or ROLLBACK consumes the reference permanently; reversing
// a refund does not make the original BET refundable again.
func (t *WagerTransaction) Evaluate(wallet WalletSnapshot, reference *TransactionSnapshot, alreadyReversed bool) (Movement, error) {
	if t == nil || t.state.Validate() != nil {
		return Movement{}, ErrInvalidTransaction
	}
	if t.state.Status != Pending && t.state.Status != PendingReference {
		return Movement{}, ErrInvalidTransition
	}
	if err := wallet.Validate(); err != nil {
		return Movement{}, err
	}
	s := t.state
	if s.WalletID != wallet.ID || s.PlayerID != wallet.PlayerID {
		return Movement{}, &RuleError{Code: CodeWalletMismatch}
	}
	if s.Money.Currency() != wallet.Balance.Currency() {
		return Movement{}, &RuleError{Code: CodeCurrencyMismatch}
	}
	if s.ReferenceExternalTransactionID != "" {
		if reference == nil {
			return Movement{}, ErrReferencePending
		}
		if err := reference.Validate(); err != nil {
			return Movement{}, err
		}
		if reference.ProviderID != s.ProviderID || reference.ExternalTransactionID != s.ReferenceExternalTransactionID ||
			reference.PlayerID != s.PlayerID || reference.WalletID != s.WalletID ||
			reference.RoundID != s.RoundID || reference.Money.Currency() != s.Money.Currency() {
			return Movement{}, &RuleError{Code: CodeReferenceMismatch}
		}
		if reference.Status == Pending || reference.Status == PendingReference {
			return Movement{}, ErrReferencePending
		}
		if reference.Status != Processed {
			return Movement{}, &RuleError{Code: CodeReferenceNotProcessed}
		}
		if (s.Kind == Win || s.Kind == Refund) && reference.Kind != Bet {
			return Movement{}, &RuleError{Code: CodeInvalidReferenceKind}
		}
		if s.Kind == Rollback && reference.Kind != Bet && reference.Kind != Win && reference.Kind != Refund {
			return Movement{}, &RuleError{Code: CodeInvalidReferenceKind}
		}
		if s.Kind == Refund || s.Kind == Rollback {
			if reference.Money.MinorUnits() != s.Money.MinorUnits() {
				return Movement{}, &RuleError{Code: CodeReferenceMismatch}
			}
			if alreadyReversed {
				return Movement{}, &RuleError{Code: CodeAlreadyReversed}
			}
		}
	} else if reference != nil {
		return Movement{}, ErrInvalidTransaction
	}

	movement := Movement{Money: s.Money}
	switch s.Kind {
	case Bet:
		movement.Direction = Debit
	case Opening, Win, Refund:
		movement.Direction = Credit
	case Rollback:
		movement.Direction = Debit
		if reference.Kind == Bet {
			movement.Direction = Credit
		}
	case Loss:
		return movement, nil
	}
	if movement.Direction == Debit && s.Money.MinorUnits() > wallet.Balance.MinorUnits() {
		code := CodeInsufficientFunds
		if s.Kind == Rollback {
			code = CodeReversalInsufficientFunds
		}
		return Movement{}, &RuleError{Code: code}
	}
	if movement.Direction == Credit {
		if _, err := wallet.Balance.Add(s.Money); err != nil {
			return Movement{}, &RuleError{Code: CodeBalanceOverflow}
		}
	}
	return movement, nil
}
