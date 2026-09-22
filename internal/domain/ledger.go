package domain

import (
	"errors"
	"time"
)

var ErrInvalidLedger = errors.New("invalid ledger entry")

type WalletLedgerEntry struct{ state LedgerSnapshot }

type LedgerSnapshot struct {
	ID            string
	WalletID      string
	TransactionID string
	Direction     Direction
	Money         Money
	BalanceBefore Money
	BalanceAfter  Money
	CreatedAt     time.Time
}

func NewLedgerEntry(id, walletID, transactionID string, direction Direction, money, balanceBefore, balanceAfter Money, at time.Time) (WalletLedgerEntry, error) {
	state := LedgerSnapshot{
		ID: id, WalletID: walletID, TransactionID: transactionID, Direction: direction,
		Money: money, BalanceBefore: balanceBefore, BalanceAfter: balanceAfter, CreatedAt: at.UTC(),
	}
	if err := state.Validate(); err != nil {
		return WalletLedgerEntry{}, err
	}
	return WalletLedgerEntry{state: state}, nil
}

func (e WalletLedgerEntry) Snapshot() LedgerSnapshot { return e.state }

func (s LedgerSnapshot) Validate() error {
	if !validIdentifier(s.ID) || !validIdentifier(s.WalletID) || !validIdentifier(s.TransactionID) ||
		s.Money.Validate() != nil || s.Money.MinorUnits() <= 0 || s.BalanceBefore.Validate() != nil ||
		s.BalanceAfter.Validate() != nil || s.BalanceBefore.MinorUnits() < 0 || s.BalanceAfter.MinorUnits() < 0 ||
		s.CreatedAt.IsZero() {
		return ErrInvalidLedger
	}
	var expected Money
	var err error
	switch s.Direction {
	case Debit:
		expected, err = s.BalanceBefore.Sub(s.Money)
	case Credit:
		expected, err = s.BalanceBefore.Add(s.Money)
	default:
		return ErrInvalidLedger
	}
	if err != nil {
		return errors.Join(ErrInvalidLedger, err)
	}
	comparison, err := expected.Compare(s.BalanceAfter)
	if err != nil {
		return errors.Join(ErrInvalidLedger, err)
	}
	if comparison != 0 {
		return ErrInvalidLedger
	}
	return nil
}
