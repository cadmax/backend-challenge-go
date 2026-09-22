package domain

import (
	"errors"
	"math"
	"strings"
	"time"
	"unicode"
)

var (
	ErrInvalidWallet     = errors.New("invalid wallet")
	ErrInsufficientFunds = errors.New("insufficient funds")
)

type Wallet struct{ state WalletSnapshot }

// WalletSnapshot contains persistence values, never a mutable view of an entity.
type WalletSnapshot struct {
	ID        string
	PlayerID  string
	Balance   Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewWallet(id, playerID string, initial Money, at time.Time) (*Wallet, error) {
	return RestoreWallet(WalletSnapshot{
		ID: id, PlayerID: playerID, Balance: initial, Version: 1,
		CreatedAt: at.UTC(), UpdatedAt: at.UTC(),
	})
}

func RestoreWallet(state WalletSnapshot) (*Wallet, error) {
	if err := state.Validate(); err != nil {
		return nil, err
	}
	state.CreatedAt, state.UpdatedAt = state.CreatedAt.UTC(), state.UpdatedAt.UTC()
	return &Wallet{state: state}, nil
}

func (s WalletSnapshot) Validate() error {
	if !validIdentifier(s.ID) || !validIdentifier(s.PlayerID) ||
		s.Balance.Validate() != nil || s.Balance.MinorUnits() < 0 || s.Version < 1 ||
		s.CreatedAt.IsZero() || s.UpdatedAt.Before(s.CreatedAt) {
		return ErrInvalidWallet
	}
	return nil
}

func (w *Wallet) Snapshot() WalletSnapshot {
	if w == nil {
		return WalletSnapshot{}
	}
	return w.state
}

func (w *Wallet) Debit(money Money, at time.Time) error {
	return w.change(money, at, true)
}

func (w *Wallet) Credit(money Money, at time.Time) error {
	return w.change(money, at, false)
}

func (w *Wallet) change(money Money, at time.Time, debit bool) error {
	if w == nil || w.state.Validate() != nil || at.IsZero() || at.Before(w.state.UpdatedAt) {
		return ErrInvalidWallet
	}
	if err := money.Validate(); err != nil {
		return err
	}
	if money.MinorUnits() < 0 {
		return ErrInvalidMoney
	}
	if err := w.state.Balance.compatible(money); err != nil {
		return err
	}
	if money.MinorUnits() == 0 {
		return nil
	}
	if w.state.Version == math.MaxInt64 {
		return ErrOverflow
	}
	var balance Money
	var err error
	if debit {
		balance, err = w.state.Balance.Sub(money)
	} else {
		balance, err = w.state.Balance.Add(money)
	}
	if err != nil {
		return err
	}
	if balance.MinorUnits() < 0 {
		return ErrInsufficientFunds
	}
	w.state.Balance = balance
	w.state.Version++
	w.state.UpdatedAt = at.UTC()
	return nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 255 || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
