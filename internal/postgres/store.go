package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/application"
	"github.com/cadmax/backend-challenge-go/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func storageError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ErrNotFound
	}
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) && pgerr.Code == "23505" {
		return fmt.Errorf("%w: database uniqueness", application.ErrConflict)
	}
	if errors.Is(err, application.ErrInvalidInput) || errors.Is(err, application.ErrConflict) || errors.Is(err, application.ErrNotFound) {
		return err
	}
	return fmt.Errorf("%w: %w", application.ErrUnavailable, err)
}

func (s *Store) Within(ctx context.Context, fn func(application.UnitOfWork) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return storageError(err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	if err := fn(&unit{tx: tx}); err != nil {
		return storageError(err)
	}
	return storageError(tx.Commit(ctx))
}

type unit struct{ tx pgx.Tx }

func (u *unit) LockIdentity(ctx context.Context, provider, key, external string) error {
	// These are per-request identities, never a global mutex. Hash collisions
	// only serialize unrelated identities; uniqueness remains schema-enforced.
	_, err := u.tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "key\x1f"+provider+"\x1f"+key)
	if err != nil {
		return err
	}
	_, err = u.tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,1))`, "external\x1f"+provider+"\x1f"+external)
	return err
}

const transactionColumns = `t.id::text,coalesce(t.provider_id,''),coalesce(t.external_transaction_id,''),coalesce(t.idempotency_key,''),coalesce(t.payload_hash,''),
 t.wallet_id::text,t.player_id::text,coalesce(t.round_id,''),coalesce(t.game_id,''),t.kind,t.amount,t.currency,
 coalesce(t.reference_external_transaction_id,''),coalesce(t.reference_transaction_id::text,''),t.status,coalesce(t.failure_code,''),
 t.result_balance,t.result_currency,t.created_at,t.updated_at,t.reference_attempts,t.next_attempt_at,t.reference_deadline,t.correlation_id,coalesce(t.causation_id,'')`

func scanRecord(row pgx.Row) (*application.Record, error) {
	var r application.Record
	t := &r.Transaction
	var amount int64
	var currency string
	var balance *int64
	var resultCurrency *string
	err := row.Scan(&t.ID, &t.ProviderID, &t.ExternalTransactionID, &t.IdempotencyKey, &t.PayloadHash, &t.WalletID, &t.PlayerID, &t.RoundID, &t.GameID,
		&t.Kind, &amount, &currency, &t.ReferenceExternalTransactionID, &t.ReferenceTransactionID, &t.Status, &t.FailureCode, &balance, &resultCurrency,
		&t.CreatedAt, &t.UpdatedAt, &r.ReferenceAttempts, &r.NextAttemptAt, &r.ReferenceDeadline, &r.Metadata.CorrelationID, &r.Metadata.CausationID)
	if err != nil {
		return nil, err
	}
	t.Money, err = domain.MoneyFromMinor(amount, currency)
	if err != nil {
		return nil, err
	}
	if balance != nil && resultCurrency != nil {
		money, err := domain.MoneyFromMinor(*balance, *resultCurrency)
		if err != nil {
			return nil, err
		}
		t.ResultBalance = &money
	}
	return &r, nil
}

func (u *unit) FindKey(ctx context.Context, provider, key string) (*application.Record, string, error) {
	r, err := scanRecord(u.tx.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions t JOIN idempotency_keys k ON k.transaction_id=t.id WHERE k.provider_id=$1 AND k.idempotency_key=$2`, provider, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	return r, r.Transaction.PayloadHash, nil
}

func (u *unit) FindExternal(ctx context.Context, provider, external string) (*application.Record, error) {
	r, err := scanRecord(u.tx.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions t WHERE provider_id=$1 AND external_transaction_id=$2`, provider, external))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return r, err
}

func (u *unit) GetTransaction(ctx context.Context, id string) (*application.Record, error) {
	return scanRecord(u.tx.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions t WHERE id=$1 FOR UPDATE`, id))
}

func (u *unit) GetPendingTransaction(ctx context.Context, id string) (*application.Record, error) {
	r, err := scanRecord(u.tx.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions t WHERE id=$1 AND status IN ('PENDING','PENDING_REFERENCE') AND next_attempt_at<=now() FOR UPDATE SKIP LOCKED`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return r, err
}

func (u *unit) BindKey(ctx context.Context, provider, key, id, hash string) error {
	_, err := u.tx.Exec(ctx, `INSERT INTO idempotency_keys(provider_id,idempotency_key,transaction_id,payload_hash) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, provider, key, id, hash)
	return err
}

func scanWallet(row pgx.Row) (*domain.Wallet, error) {
	var w domain.WalletSnapshot
	var balance int64
	var currency string
	err := row.Scan(&w.ID, &w.PlayerID, &currency, &balance, &w.Version, &w.CreatedAt, &w.UpdatedAt)
	if err != nil {
		return nil, err
	}
	w.Balance, err = domain.MoneyFromMinor(balance, currency)
	if err != nil {
		return nil, err
	}
	return domain.RestoreWallet(w)
}

const walletColumns = `id::text,player_id::text,currency,balance,version,created_at,updated_at`

func (u *unit) GetWallet(ctx context.Context, id string) (*domain.Wallet, error) {
	return scanWallet(u.tx.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id=$1 FOR UPDATE`, id))
}

func (u *unit) GetAvailableWallet(ctx context.Context, id string) (*domain.Wallet, error) {
	w, err := scanWallet(u.tx.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id=$1 FOR UPDATE SKIP LOCKED`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return w, err
}

func (u *unit) InsertWallet(ctx context.Context, w domain.WalletSnapshot) error {
	_, err := u.tx.Exec(ctx, `INSERT INTO wallets(id,player_id,currency,balance,version,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7)`, w.ID, w.PlayerID, w.Balance.Currency(), w.Balance.MinorUnits(), w.Version, w.CreatedAt, w.UpdatedAt)
	return err
}

func (u *unit) SaveWallet(ctx context.Context, w domain.WalletSnapshot) error {
	_, err := u.tx.Exec(ctx, `UPDATE wallets SET balance=$2,version=$3,updated_at=$4 WHERE id=$1`, w.ID, w.Balance.MinorUnits(), w.Version, w.UpdatedAt)
	return err
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}
func balanceColumns(balance *domain.Money) (any, any) {
	if balance == nil {
		return nil, nil
	}
	return balance.MinorUnits(), balance.Currency()
}

func (u *unit) InsertTransaction(ctx context.Context, r application.Record) error {
	t := r.Transaction
	balance, currency := balanceColumns(t.ResultBalance)
	_, err := u.tx.Exec(ctx, `INSERT INTO wager_transactions(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,
 kind,amount,currency,reference_external_transaction_id,reference_transaction_id,status,failure_code,result_balance,result_currency,
 created_at,updated_at,reference_attempts,next_attempt_at,reference_deadline,correlation_id,causation_id)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)`,
		t.ID, nullable(t.ProviderID), nullable(t.ExternalTransactionID), nullable(t.IdempotencyKey), nullable(t.PayloadHash), t.WalletID, t.PlayerID, nullable(t.RoundID), nullable(t.GameID),
		t.Kind, t.Money.MinorUnits(), t.Money.Currency(), nullable(t.ReferenceExternalTransactionID), nullable(t.ReferenceTransactionID), t.Status, nullable(t.FailureCode), balance, currency,
		t.CreatedAt, t.UpdatedAt, r.ReferenceAttempts, r.NextAttemptAt, r.ReferenceDeadline, r.Metadata.CorrelationID, nullable(r.Metadata.CausationID))
	return err
}

func (u *unit) SaveTransaction(ctx context.Context, r application.Record) error {
	t := r.Transaction
	balance, currency := balanceColumns(t.ResultBalance)
	_, err := u.tx.Exec(ctx, `UPDATE wager_transactions SET status=$2,reference_transaction_id=$3,failure_code=$4,result_balance=$5,result_currency=$6,
 updated_at=$7,reference_attempts=$8,next_attempt_at=$9 WHERE id=$1`, t.ID, t.Status, nullable(t.ReferenceTransactionID), nullable(t.FailureCode), balance, currency, t.UpdatedAt, r.ReferenceAttempts, r.NextAttemptAt)
	return err
}

func (u *unit) IsReversed(ctx context.Context, id string) (bool, error) {
	var exists bool
	err := u.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM wager_transactions WHERE reference_transaction_id=$1 AND kind IN ('REFUND','ROLLBACK') AND status='PROCESSED')`, id).Scan(&exists)
	return exists, err
}

func (u *unit) InsertLedger(ctx context.Context, e application.LedgerView, version int64) error {
	_, err := u.tx.Exec(ctx, `INSERT INTO wallet_ledger(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,wallet_version,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		e.ID, e.WalletID, e.TransactionID, e.Direction, e.Money.MinorUnits(), e.Money.Currency(), e.BalanceBefore.MinorUnits(), e.BalanceAfter.MinorUnits(), version, e.CreatedAt)
	return err
}

func (u *unit) InsertEvent(ctx context.Context, e application.Event) error {
	_, err := u.tx.Exec(ctx, `INSERT INTO outbox(event_id,aggregate_id,event_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5)`, e.ID, e.AggregateID, e.Type, e.Payload, e.OccurredAt)
	return err
}

func (u *unit) ClaimInbox(ctx context.Context, id, hash string) (*application.InboxRecord, error) {
	_, err := u.tx.Exec(ctx, `INSERT INTO inbox(consumer_name,message_id,payload_hash,received_at) VALUES('wager-transactions',$1,$2,now()) ON CONFLICT DO NOTHING`, id, hash)
	if err != nil {
		return nil, err
	}
	var r application.InboxRecord
	err = u.tx.QueryRow(ctx, `SELECT message_id,payload_hash,coalesce(transaction_id::text,'') FROM inbox WHERE consumer_name='wager-transactions' AND message_id=$1 FOR UPDATE`, id).Scan(&r.MessageID, &r.Hash, &r.TransactionID)
	if err != nil {
		return nil, err
	}
	if r.Hash != hash {
		return nil, application.ErrConflict
	}
	return &r, nil
}

func (u *unit) CompleteInbox(ctx context.Context, id, transactionID string) error {
	_, err := u.tx.Exec(ctx, `UPDATE inbox SET transaction_id=$2,completed_at=now() WHERE consumer_name='wager-transactions' AND message_id=$1`, id, transactionID)
	return err
}
