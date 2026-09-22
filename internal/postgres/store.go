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

const transactionColumns = `
	t.id::text,
	coalesce(t.provider_id, ''),
	coalesce(t.external_transaction_id, ''),
	coalesce(t.idempotency_key, ''),
	coalesce(t.payload_hash, ''),
	t.wallet_id::text,
	t.player_id::text,
	coalesce(t.round_id, ''),
	coalesce(t.game_id, ''),
	t.kind,
	t.amount,
	t.currency,
	coalesce(t.reference_external_transaction_id, ''),
	coalesce(t.reference_transaction_id::text, ''),
	t.status,
	coalesce(t.failure_code, ''),
	t.result_balance,
	t.result_currency,
	t.created_at,
	t.updated_at,
	t.reference_attempts,
	t.next_attempt_at,
	t.reference_deadline,
	t.correlation_id,
	coalesce(t.causation_id, '')`

func scanRecord(row pgx.Row) (*application.Record, error) {
	var record application.Record
	transaction := &record.Transaction
	var amount int64
	var currency string
	var balance *int64
	var resultCurrency *string
	err := row.Scan(
		&transaction.ID,
		&transaction.ProviderID,
		&transaction.ExternalTransactionID,
		&transaction.IdempotencyKey,
		&transaction.PayloadHash,
		&transaction.WalletID,
		&transaction.PlayerID,
		&transaction.RoundID,
		&transaction.GameID,
		&transaction.Kind,
		&amount,
		&currency,
		&transaction.ReferenceExternalTransactionID,
		&transaction.ReferenceTransactionID,
		&transaction.Status,
		&transaction.FailureCode,
		&balance,
		&resultCurrency,
		&transaction.CreatedAt,
		&transaction.UpdatedAt,
		&record.ReferenceAttempts,
		&record.NextAttemptAt,
		&record.ReferenceDeadline,
		&record.Metadata.CorrelationID,
		&record.Metadata.CausationID,
	)
	if err != nil {
		return nil, err
	}
	transaction.Money, err = domain.MoneyFromMinor(amount, currency)
	if err != nil {
		return nil, err
	}
	if balance != nil && resultCurrency != nil {
		money, err := domain.MoneyFromMinor(*balance, *resultCurrency)
		if err != nil {
			return nil, err
		}
		transaction.ResultBalance = &money
	}
	return &record, nil
}

func (u *unit) FindKey(ctx context.Context, provider, key string) (*application.Record, string, error) {
	query := `SELECT ` + transactionColumns + `
		FROM wager_transactions t
		JOIN idempotency_keys k ON k.transaction_id = t.id
		WHERE k.provider_id = $1 AND k.idempotency_key = $2`
	record, err := scanRecord(u.tx.QueryRow(ctx, query, provider, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	return record, record.Transaction.PayloadHash, nil
}

func (u *unit) FindExternal(ctx context.Context, provider, external string) (*application.Record, error) {
	query := `SELECT ` + transactionColumns + `
		FROM wager_transactions t
		WHERE provider_id = $1 AND external_transaction_id = $2`
	record, err := scanRecord(u.tx.QueryRow(ctx, query, provider, external))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return record, err
}

func (u *unit) GetTransaction(ctx context.Context, id string) (*application.Record, error) {
	query := `SELECT ` + transactionColumns + `
		FROM wager_transactions t
		WHERE id = $1
		FOR UPDATE`
	return scanRecord(u.tx.QueryRow(ctx, query, id))
}

func (u *unit) GetPendingTransaction(ctx context.Context, id string) (*application.Record, error) {
	query := `SELECT ` + transactionColumns + `
		FROM wager_transactions t
		WHERE id = $1
		  AND status IN ('PENDING', 'PENDING_REFERENCE')
		  AND next_attempt_at <= now()
		FOR UPDATE SKIP LOCKED`
	record, err := scanRecord(u.tx.QueryRow(ctx, query, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return record, err
}

func (u *unit) BindKey(ctx context.Context, provider, key, id, hash string) error {
	const query = `
		INSERT INTO idempotency_keys (
			provider_id, idempotency_key, transaction_id, payload_hash
		) VALUES ($1, $2, $3, $4)
		ON CONFLICT DO NOTHING`
	_, err := u.tx.Exec(ctx, query, provider, key, id, hash)
	return err
}

func scanWallet(row pgx.Row) (*domain.Wallet, error) {
	var wallet domain.WalletSnapshot
	var balance int64
	var currency string
	err := row.Scan(&wallet.ID, &wallet.PlayerID, &currency, &balance, &wallet.Version, &wallet.CreatedAt, &wallet.UpdatedAt)
	if err != nil {
		return nil, err
	}
	wallet.Balance, err = domain.MoneyFromMinor(balance, currency)
	if err != nil {
		return nil, err
	}
	return domain.RestoreWallet(wallet)
}

const walletColumns = `id::text, player_id::text, currency, balance, version, created_at, updated_at`

func (u *unit) GetWallet(ctx context.Context, id string) (*domain.Wallet, error) {
	query := `SELECT ` + walletColumns + `
		FROM wallets
		WHERE id = $1
		FOR UPDATE`
	return scanWallet(u.tx.QueryRow(ctx, query, id))
}

func (u *unit) GetAvailableWallet(ctx context.Context, id string) (*domain.Wallet, error) {
	query := `SELECT ` + walletColumns + `
		FROM wallets
		WHERE id = $1
		FOR UPDATE SKIP LOCKED`
	wallet, err := scanWallet(u.tx.QueryRow(ctx, query, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return wallet, err
}

func (u *unit) InsertWallet(ctx context.Context, wallet domain.WalletSnapshot) error {
	const query = `
		INSERT INTO wallets (
			id, player_id, currency, balance, version, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`
	_, err := u.tx.Exec(ctx, query,
		wallet.ID,
		wallet.PlayerID,
		wallet.Balance.Currency(),
		wallet.Balance.MinorUnits(),
		wallet.Version,
		wallet.CreatedAt,
		wallet.UpdatedAt,
	)
	return err
}

func (u *unit) SaveWallet(ctx context.Context, wallet domain.WalletSnapshot) error {
	const query = `
		UPDATE wallets
		SET balance = $2, version = $3, updated_at = $4
		WHERE id = $1`
	_, err := u.tx.Exec(ctx, query,
		wallet.ID, wallet.Balance.MinorUnits(), wallet.Version, wallet.UpdatedAt,
	)
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

func (u *unit) InsertTransaction(ctx context.Context, record application.Record) error {
	transaction := record.Transaction
	balance, currency := balanceColumns(transaction.ResultBalance)
	const query = `
		INSERT INTO wager_transactions (
			id, provider_id, external_transaction_id, idempotency_key, payload_hash,
			wallet_id, player_id, round_id, game_id, kind, amount, currency,
			reference_external_transaction_id, reference_transaction_id,
			status, failure_code, result_balance, result_currency,
			created_at, updated_at, reference_attempts, next_attempt_at,
			reference_deadline, correlation_id, causation_id
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
			$14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25
		)`
	_, err := u.tx.Exec(ctx, query,
		transaction.ID,
		nullable(transaction.ProviderID),
		nullable(transaction.ExternalTransactionID),
		nullable(transaction.IdempotencyKey),
		nullable(transaction.PayloadHash),
		transaction.WalletID,
		transaction.PlayerID,
		nullable(transaction.RoundID),
		nullable(transaction.GameID),
		transaction.Kind,
		transaction.Money.MinorUnits(),
		transaction.Money.Currency(),
		nullable(transaction.ReferenceExternalTransactionID),
		nullable(transaction.ReferenceTransactionID),
		transaction.Status,
		nullable(transaction.FailureCode),
		balance,
		currency,
		transaction.CreatedAt,
		transaction.UpdatedAt,
		record.ReferenceAttempts,
		record.NextAttemptAt,
		record.ReferenceDeadline,
		record.Metadata.CorrelationID,
		nullable(record.Metadata.CausationID),
	)
	return err
}

func (u *unit) SaveTransaction(ctx context.Context, record application.Record) error {
	transaction := record.Transaction
	balance, currency := balanceColumns(transaction.ResultBalance)
	const query = `
		UPDATE wager_transactions
		SET status = $2,
			reference_transaction_id = $3,
			failure_code = $4,
			result_balance = $5,
			result_currency = $6,
			updated_at = $7,
			reference_attempts = $8,
			next_attempt_at = $9
		WHERE id = $1`
	_, err := u.tx.Exec(ctx, query,
		transaction.ID,
		transaction.Status,
		nullable(transaction.ReferenceTransactionID),
		nullable(transaction.FailureCode),
		balance,
		currency,
		transaction.UpdatedAt,
		record.ReferenceAttempts,
		record.NextAttemptAt,
	)
	return err
}

func (u *unit) IsReversed(ctx context.Context, id string) (bool, error) {
	var exists bool
	const query = `
		SELECT EXISTS (
			SELECT 1 FROM wager_transactions
			WHERE reference_transaction_id = $1
			  AND kind IN ('REFUND', 'ROLLBACK')
			  AND status = 'PROCESSED'
		)`
	err := u.tx.QueryRow(ctx, query, id).Scan(&exists)
	return exists, err
}

func (u *unit) InsertLedger(ctx context.Context, entry application.LedgerView, version int64) error {
	const query = `
		INSERT INTO wallet_ledger (
			id, wallet_id, transaction_id, direction, amount, currency,
			balance_before, balance_after, wallet_version, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	_, err := u.tx.Exec(ctx, query,
		entry.ID,
		entry.WalletID,
		entry.TransactionID,
		entry.Direction,
		entry.Money.MinorUnits(),
		entry.Money.Currency(),
		entry.BalanceBefore.MinorUnits(),
		entry.BalanceAfter.MinorUnits(),
		version,
		entry.CreatedAt,
	)
	return err
}

func (u *unit) InsertEvent(ctx context.Context, event application.Event) error {
	const query = `
		INSERT INTO outbox (event_id, aggregate_id, event_type, payload, occurred_at)
		VALUES ($1, $2, $3, $4, $5)`
	_, err := u.tx.Exec(ctx, query,
		event.ID, event.AggregateID, event.Type, event.Payload, event.OccurredAt,
	)
	return err
}

func (u *unit) ClaimInbox(ctx context.Context, id, hash string) (*application.InboxRecord, error) {
	const insert = `
		INSERT INTO inbox (consumer_name, message_id, payload_hash, received_at)
		VALUES ('wager-transactions', $1, $2, now())
		ON CONFLICT DO NOTHING`
	_, err := u.tx.Exec(ctx, insert, id, hash)
	if err != nil {
		return nil, err
	}
	var record application.InboxRecord
	const selectInbox = `
		SELECT message_id, payload_hash, coalesce(transaction_id::text, '')
		FROM inbox
		WHERE consumer_name = 'wager-transactions' AND message_id = $1
		FOR UPDATE`
	err = u.tx.QueryRow(ctx, selectInbox, id).Scan(&record.MessageID, &record.Hash, &record.TransactionID)
	if err != nil {
		return nil, err
	}
	if record.Hash != hash {
		return nil, application.ErrConflict
	}
	return &record, nil
}

func (u *unit) CompleteInbox(ctx context.Context, id, transactionID string) error {
	const query = `
		UPDATE inbox
		SET transaction_id = $2, completed_at = now()
		WHERE consumer_name = 'wager-transactions' AND message_id = $1`
	_, err := u.tx.Exec(ctx, query, id, transactionID)
	return err
}
