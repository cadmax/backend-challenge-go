package domain

import (
	"encoding/json"
	"errors"
	"time"
)

var ErrInvalidEvent = errors.New("invalid domain event")

type EventMetadata struct {
	EventID       string
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
}

type Event interface {
	ID() string
	AggregateID() string
	Type() string
	OccurredAt() time.Time
	MarshalJSON() ([]byte, error)
}

// Concrete event types expose no writable fields. Each holds an independent
// snapshot; subsequent aggregate changes cannot alter an outbox payload.
type WagerTransactionProcessed struct{ event[TransactionEventData] }
type WagerTransactionRejected struct{ event[TransactionEventData] }
type WagerTransactionPendingReference struct{ event[TransactionEventData] }
type WalletBalanceChanged struct {
	event[WalletBalanceChangedData]
}

type event[T any] struct {
	metadata    EventMetadata
	aggregateID string
	eventType   string
	data        T
}

type TransactionEventData struct {
	TransactionID                  string `json:"transactionId"`
	WalletID                       string `json:"walletId"`
	PlayerID                       string `json:"playerId"`
	ProviderID                     string `json:"providerId,omitempty"`
	ExternalTransactionID          string `json:"externalTransactionId,omitempty"`
	RoundID                        string `json:"roundId,omitempty"`
	GameID                         string `json:"gameId,omitempty"`
	Kind                           Kind   `json:"kind"`
	Money                          Money  `json:"money"`
	Status                         Status `json:"status"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         string `json:"referenceTransactionId,omitempty"`
	FailureCode                    string `json:"failureCode,omitempty"`
	Balance                        *Money `json:"balance,omitempty"`
}

type WalletBalanceChangedData struct {
	WalletID      string    `json:"walletId"`
	TransactionID string    `json:"transactionId"`
	Direction     Direction `json:"direction"`
	Money         Money     `json:"money"`
	BalanceBefore Money     `json:"balanceBefore"`
	BalanceAfter  Money     `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}

func (e event[T]) ID() string            { return e.metadata.EventID }
func (e event[T]) AggregateID() string   { return e.aggregateID }
func (e event[T]) Type() string          { return e.eventType }
func (e event[T]) OccurredAt() time.Time { return e.metadata.OccurredAt }

func (e event[T]) MarshalJSON() ([]byte, error) {
	if err := validateEventMetadata(e.metadata, e.aggregateID); err != nil || e.eventType == "" {
		return nil, ErrInvalidEvent
	}
	return json.Marshal(struct {
		EventID       string    `json:"eventId"`
		EventType     string    `json:"eventType"`
		AggregateID   string    `json:"aggregateId"`
		CorrelationID string    `json:"correlationId"`
		CausationID   string    `json:"causationId,omitempty"`
		OccurredAt    time.Time `json:"occurredAt"`
		Version       int       `json:"version"`
		Data          T         `json:"data"`
	}{e.metadata.EventID, e.eventType, e.aggregateID, e.metadata.CorrelationID,
		e.metadata.CausationID, e.metadata.OccurredAt.UTC(), 1, e.data})
}

func validateEventMetadata(meta EventMetadata, aggregateID string) error {
	if !validIdentifier(meta.EventID) || !validIdentifier(meta.CorrelationID) ||
		!validIdentifier(aggregateID) || meta.OccurredAt.IsZero() ||
		(meta.CausationID != "" && !validIdentifier(meta.CausationID)) {
		return ErrInvalidEvent
	}
	return nil
}

func newTransactionEvent(meta EventMetadata, state TransactionSnapshot, status Status, eventType string) (event[TransactionEventData], error) {
	if state.Validate() != nil || state.Status != status || meta.OccurredAt.Before(state.UpdatedAt) {
		return event[TransactionEventData]{}, ErrInvalidEvent
	}
	if err := validateEventMetadata(meta, state.WalletID); err != nil {
		return event[TransactionEventData]{}, err
	}
	state = cloneTransactionSnapshot(state)
	meta.OccurredAt = meta.OccurredAt.UTC()
	data := TransactionEventData{
		TransactionID: state.ID, WalletID: state.WalletID, PlayerID: state.PlayerID,
		ProviderID: state.ProviderID, ExternalTransactionID: state.ExternalTransactionID,
		RoundID: state.RoundID, GameID: state.GameID, Kind: state.Kind, Money: state.Money,
		Status: state.Status, ReferenceExternalTransactionID: state.ReferenceExternalTransactionID,
		ReferenceTransactionID: state.ReferenceTransactionID, FailureCode: state.FailureCode,
		Balance: state.ResultBalance,
	}
	return event[TransactionEventData]{metadata: meta, aggregateID: state.WalletID, eventType: eventType, data: data}, nil
}

func NewWagerTransactionProcessed(meta EventMetadata, state TransactionSnapshot) (WagerTransactionProcessed, error) {
	e, err := newTransactionEvent(meta, state, Processed, "WagerTransactionProcessed")
	return WagerTransactionProcessed{event: e}, err
}

func NewWagerTransactionRejected(meta EventMetadata, state TransactionSnapshot) (WagerTransactionRejected, error) {
	e, err := newTransactionEvent(meta, state, Rejected, "WagerTransactionRejected")
	return WagerTransactionRejected{event: e}, err
}

func NewWagerTransactionPendingReference(meta EventMetadata, state TransactionSnapshot) (WagerTransactionPendingReference, error) {
	e, err := newTransactionEvent(meta, state, PendingReference, "WagerTransactionPendingReference")
	return WagerTransactionPendingReference{event: e}, err
}

func NewWalletBalanceChanged(meta EventMetadata, ledger LedgerSnapshot, walletVersion int64) (WalletBalanceChanged, error) {
	if ledger.Validate() != nil || walletVersion < 1 || meta.OccurredAt.Before(ledger.CreatedAt) {
		return WalletBalanceChanged{}, ErrInvalidEvent
	}
	if err := validateEventMetadata(meta, ledger.WalletID); err != nil {
		return WalletBalanceChanged{}, err
	}
	meta.OccurredAt = meta.OccurredAt.UTC()
	return WalletBalanceChanged{event: event[WalletBalanceChangedData]{
		metadata: meta, aggregateID: ledger.WalletID, eventType: "WalletBalanceChanged",
		data: WalletBalanceChangedData{
			WalletID: ledger.WalletID, TransactionID: ledger.TransactionID, Direction: ledger.Direction,
			Money: ledger.Money, BalanceBefore: ledger.BalanceBefore, BalanceAfter: ledger.BalanceAfter,
			WalletVersion: walletVersion,
		},
	}}, nil
}

// Data returns a copy, including a separate result pointer for transaction events.
func (e WagerTransactionProcessed) Data() TransactionEventData {
	return cloneEventData(e.event.data)
}

func (e WagerTransactionRejected) Data() TransactionEventData {
	return cloneEventData(e.event.data)
}

func (e WagerTransactionPendingReference) Data() TransactionEventData {
	return cloneEventData(e.event.data)
}

func (e WalletBalanceChanged) Data() WalletBalanceChangedData { return e.event.data }

func cloneEventData(data TransactionEventData) TransactionEventData {
	if data.Balance != nil {
		balance := *data.Balance
		data.Balance = &balance
	}
	return data
}
