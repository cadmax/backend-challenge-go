package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/application"
	"github.com/cadmax/backend-challenge-go/internal/domain"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) Wallet(ctx context.Context, id string) (application.WalletView, error) {
	query := `SELECT ` + walletColumns + `
		FROM wallets
		WHERE id = $1`
	wallet, err := scanWallet(s.pool.QueryRow(ctx, query, id))
	if err != nil {
		return application.WalletView{}, storageError(err)
	}
	snapshot := wallet.Snapshot()
	return application.WalletView{
		ID:       snapshot.ID,
		PlayerID: snapshot.PlayerID,
		Balance:  snapshot.Balance,
		Version:  snapshot.Version,
	}, nil
}

func (s *Store) Transaction(ctx context.Context, provider, id string, external bool) (application.Result, error) {
	query := `SELECT ` + transactionColumns + `
		FROM wager_transactions t
		WHERE provider_id = $1 AND id = $2`
	if external {
		query = `SELECT ` + transactionColumns + `
			FROM wager_transactions t
			WHERE provider_id = $1 AND external_transaction_id = $2`
	}
	record, err := scanRecord(s.pool.QueryRow(ctx, query, provider, id))
	if err != nil {
		return application.Result{}, storageError(err)
	}
	return application.Result{
		TransactionID: record.Transaction.ID,
		Status:        string(record.Transaction.Status),
		Balance:       record.Transaction.ResultBalance,
		FailureCode:   record.Transaction.FailureCode,
	}, nil
}

type ledgerCursor struct {
	WalletID  string    `json:"walletId"`
	CreatedAt time.Time `json:"createdAt"`
	ID        string    `json:"id"`
}

func (s *Store) Ledger(ctx context.Context, id, encoded string, limit int) (application.LedgerPage, error) {
	page := application.LedgerPage{Entries: make([]application.LedgerView, 0, limit)}
	if _, err := s.Wallet(ctx, id); err != nil {
		return page, err
	}
	cursor := ledgerCursor{
		WalletID:  id,
		CreatedAt: time.Unix(0, 0).UTC(),
		ID:        "00000000-0000-0000-0000-000000000000",
	}
	if encoded != "" {
		cursorJSON, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil {
			return page, application.ErrInvalidInput
		}
		if err := json.Unmarshal(cursorJSON, &cursor); err != nil {
			return page, application.ErrInvalidInput
		}
		if cursor.WalletID != id || cursor.CreatedAt.IsZero() || len(cursor.ID) != 36 {
			return page, application.ErrInvalidInput
		}
		var uuid pgtype.UUID
		if err := uuid.Scan(cursor.ID); err != nil {
			return page, application.ErrInvalidInput
		}
	}
	const query = `
		SELECT id::text, wallet_id::text, transaction_id::text,
			direction, amount, currency, balance_before, balance_after, created_at
		FROM wallet_ledger
		WHERE wallet_id = $1 AND (created_at, id) > ($2, $3::uuid)
		ORDER BY created_at, id
		LIMIT $4`
	rows, err := s.pool.Query(ctx, query, id, cursor.CreatedAt, cursor.ID, limit+1)
	if err != nil {
		return page, storageError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var entry application.LedgerView
		var amount, before, after int64
		var currency string
		if err := rows.Scan(
			&entry.ID, &entry.WalletID, &entry.TransactionID,
			&entry.Direction, &amount, &currency, &before, &after, &entry.CreatedAt,
		); err != nil {
			return page, storageError(err)
		}
		entry.Money, err = domain.MoneyFromMinor(amount, currency)
		if err != nil {
			return page, err
		}
		entry.BalanceBefore, err = domain.MoneyFromMinor(before, currency)
		if err != nil {
			return page, err
		}
		entry.BalanceAfter, err = domain.MoneyFromMinor(after, currency)
		if err != nil {
			return page, err
		}
		page.Entries = append(page.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return page, storageError(err)
	}
	if len(page.Entries) > limit {
		last := page.Entries[limit-1]
		cursorJSON, err := json.Marshal(ledgerCursor{
			WalletID:  id,
			CreatedAt: last.CreatedAt,
			ID:        last.ID,
		})
		if err != nil {
			return page, err
		}
		page.NextCursor = base64.RawURLEncoding.EncodeToString(cursorJSON)
		page.Entries = page.Entries[:limit]
	}
	return page, nil
}

func (s *Store) Reconcile(ctx context.Context, id string) (application.Reconciliation, error) {
	var reconciliation application.Reconciliation
	var currency, calculatedMinorUnits string
	var storedMinorUnits int64
	// One statement uses one MVCC snapshot even at READ COMMITTED.
	const query = `
		SELECT w.id::text, w.currency, w.balance,
			coalesce(sum(
				CASE l.direction
					WHEN 'CREDIT' THEN l.amount::numeric
					ELSE -l.amount::numeric
				END
			), 0)::text,
			count(l.id)
		FROM wallets w
		LEFT JOIN wallet_ledger l ON l.wallet_id = w.id
		WHERE w.id = $1
		GROUP BY w.id`
	err := s.pool.QueryRow(ctx, query, id).Scan(
		&reconciliation.WalletID,
		&currency,
		&storedMinorUnits,
		&calculatedMinorUnits,
		&reconciliation.CheckedEntries,
	)
	if err != nil {
		return reconciliation, storageError(err)
	}
	reconciliation.StoredBalance, err = domain.MoneyFromMinor(storedMinorUnits, currency)
	if err != nil {
		return reconciliation, err
	}
	// PostgreSQL SUM(BIGINT) is numeric, so detect out-of-range corrupt data
	// rather than overflowing an int64 accumulator during reconciliation.
	var calculatedMinor int64
	if _, err = fmt.Sscan(calculatedMinorUnits, &calculatedMinor); err != nil {
		return reconciliation, fmt.Errorf("ledger total is outside int64: %w", err)
	}
	reconciliation.CalculatedBalance, err = domain.MoneyFromMinor(calculatedMinor, currency)
	if err != nil {
		return reconciliation, err
	}
	reconciliation.Difference, err = reconciliation.StoredBalance.Sub(reconciliation.CalculatedBalance)
	if err != nil {
		return reconciliation, err
	}
	reconciliation.Consistent = reconciliation.Difference.MinorUnits() == 0
	return reconciliation, nil
}

func (s *Store) Pending(ctx context.Context, limit int) ([]string, error) {
	const query = `
		SELECT id::text
		FROM wager_transactions
		WHERE status IN ('PENDING', 'PENDING_REFERENCE')
		  AND next_attempt_at <= now()
		ORDER BY next_attempt_at, id
		LIMIT $1`
	rows, err := s.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, storageError(err)
	}
	defer rows.Close()
	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, storageError(err)
		}
		ids = append(ids, id)
	}
	return ids, storageError(rows.Err())
}
