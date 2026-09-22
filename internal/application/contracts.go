package application

import (
	"context"
	"errors"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/domain"
)

var (
	ErrInvalidInput = errors.New("invalid input")
	ErrConflict     = errors.New("conflicting identity or payload")
	ErrNotFound     = errors.New("resource not found")
	ErrUnavailable  = errors.New("storage unavailable")
)

type Command struct {
	ProviderID                     string       `json:"providerId"`
	ExternalTransactionID          string       `json:"externalTransactionId"`
	IdempotencyKey                 string       `json:"idempotencyKey,omitempty"`
	PlayerID                       string       `json:"playerId"`
	WalletID                       string       `json:"walletId"`
	RoundID                        string       `json:"roundId"`
	GameID                         string       `json:"gameId"`
	Kind                           string       `json:"kind"`
	Money                          domain.Money `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
}

type Metadata struct{ CorrelationID, CausationID string }

type Envelope struct {
	MessageID  string    `json:"messageId"`
	Type       string    `json:"type"`
	OccurredAt time.Time `json:"occurredAt"`
	Data       Command   `json:"data"`
}

type Result struct {
	TransactionID    string        `json:"transactionId"`
	Status           string        `json:"status"`
	Balance          *domain.Money `json:"balance,omitempty"`
	FailureCode      string        `json:"failureCode,omitempty"`
	IdempotentReplay bool          `json:"idempotentReplay"`
}

type WalletView struct {
	ID       string       `json:"id"`
	PlayerID string       `json:"playerId"`
	Balance  domain.Money `json:"balance"`
	Version  int64        `json:"version"`
}

type LedgerView struct {
	ID            string       `json:"id"`
	WalletID      string       `json:"walletId"`
	TransactionID string       `json:"transactionId"`
	Direction     string       `json:"direction"`
	Money         domain.Money `json:"money"`
	BalanceBefore domain.Money `json:"balanceBefore"`
	BalanceAfter  domain.Money `json:"balanceAfter"`
	CreatedAt     time.Time    `json:"createdAt"`
}

type LedgerPage struct {
	Entries    []LedgerView `json:"entries"`
	NextCursor string       `json:"nextCursor,omitempty"`
}

type Reconciliation struct {
	WalletID          string       `json:"walletId"`
	StoredBalance     domain.Money `json:"storedBalance"`
	CalculatedBalance domain.Money `json:"calculatedBalance"`
	Difference        domain.Money `json:"difference"`
	Consistent        bool         `json:"consistent"`
	CheckedEntries    int64        `json:"checkedEntries"`
}

// Record holds persistence scheduling, separately from the financial aggregate.
type Record struct {
	Transaction       domain.TransactionSnapshot
	ReferenceAttempts int
	NextAttemptAt     time.Time
	ReferenceDeadline time.Time
	Metadata          Metadata
}

type InboxRecord struct {
	MessageID, Hash, TransactionID string
}

type Event struct {
	ID, AggregateID, Type string
	Payload               []byte
	OccurredAt            time.Time
}

// UnitOfWork is one SQL transaction. Implementations coordinate by wallet and
// durable request identity; no process-local mutex is part of correctness.
type UnitOfWork interface {
	LockIdentity(context.Context, string, string, string) error
	FindKey(context.Context, string, string) (*Record, string, error)
	FindExternal(context.Context, string, string) (*Record, error)
	GetTransaction(context.Context, string) (*Record, error)
	GetPendingTransaction(context.Context, string) (*Record, error)
	BindKey(context.Context, string, string, string, string) error
	GetWallet(context.Context, string) (*domain.Wallet, error)
	GetAvailableWallet(context.Context, string) (*domain.Wallet, error)
	InsertWallet(context.Context, domain.WalletSnapshot) error
	SaveWallet(context.Context, domain.WalletSnapshot) error
	InsertTransaction(context.Context, Record) error
	SaveTransaction(context.Context, Record) error
	IsReversed(context.Context, string) (bool, error)
	InsertLedger(context.Context, LedgerView, int64) error
	InsertEvent(context.Context, Event) error
	ClaimInbox(context.Context, string, string) (*InboxRecord, error)
	CompleteInbox(context.Context, string, string) error
}

type Repository interface {
	Within(context.Context, func(UnitOfWork) error) error
	Wallet(context.Context, string) (WalletView, error)
	Ledger(context.Context, string, string, int) (LedgerPage, error)
	Reconcile(context.Context, string) (Reconciliation, error)
	Transaction(context.Context, string, string, bool) (Result, error)
	Pending(context.Context, int) ([]string, error)
}
