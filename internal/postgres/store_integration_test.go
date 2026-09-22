//go:build integration

package postgres_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/application"
	"github.com/cadmax/backend-challenge-go/internal/domain"
	"github.com/cadmax/backend-challenge-go/internal/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func setup(t *testing.T) (context.Context, *pgxpool.Pool, *application.Service) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to a real PostgreSQL owner connection")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_" + strings.ReplaceAll(application.NewID(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanup, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_, _ = admin.Exec(cleanup, `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		admin.Close()
	})
	if err := postgres.Migrate(ctx, pool, "up"); err != nil {
		t.Fatal(err)
	}
	if err := postgres.Migrate(ctx, pool, "up"); err != nil {
		t.Fatal("repeat migration", err)
	}
	return ctx, pool, application.NewService(postgres.New(pool))
}

func money(t *testing.T, amount string) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func open(t *testing.T, ctx context.Context, s *application.Service, amount string) application.WalletView {
	t.Helper()
	w, err := s.OpenWallet(ctx, application.NewID(), money(t, amount), application.Metadata{})
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func command(t *testing.T, w application.WalletView, external, kind, amount string) application.Command {
	t.Helper()
	return application.Command{ProviderID: "provider-a", ExternalTransactionID: external, IdempotencyKey: "key-" + external, PlayerID: w.PlayerID, WalletID: w.ID, RoundID: "round", GameID: "game", Kind: kind, Money: money(t, amount)}
}
func process(t *testing.T, ctx context.Context, s *application.Service, c application.Command) application.Result {
	t.Helper()
	r, err := s.Process(ctx, c, application.Metadata{})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func balance(t *testing.T, ctx context.Context, s *application.Service, wallet string, want int64) {
	t.Helper()
	r, err := s.Reconcile(ctx, wallet)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Consistent || r.StoredBalance.MinorUnits() != want {
		t.Fatalf("unexpected reconciliation %+v", r)
	}
}

func TestOpeningLedgerAndDatabaseGuards(t *testing.T) {
	ctx, pool, s := setup(t)
	w := open(t, ctx, s, "100.00")
	if w.Version != 1 {
		t.Fatal("opening changed initial version")
	}
	balance(t, ctx, s, w.ID, 10000)
	var opening, events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wager_transactions WHERE wallet_id=$1 AND kind='OPENING' AND status='PROCESSED' AND provider_id IS NULL`, w.ID).Scan(&opening); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE aggregate_id=$1`, w.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if opening != 1 || events != 2 {
		t.Fatalf("opening=%d events=%d", opening, events)
	}
	if _, err := s.OpenWallet(ctx, w.PlayerID, money(t, "0"), application.Metadata{}); !errors.Is(err, application.ErrConflict) {
		t.Fatalf("duplicate wallet: %v", err)
	}
	for _, query := range []string{
		`UPDATE wallet_ledger SET amount=amount+1 WHERE wallet_id=$1`,
		`DELETE FROM wallet_ledger WHERE wallet_id=$1`,
		`UPDATE wallets SET balance=balance+1,version=version+1 WHERE id=$1`,
		`UPDATE wallets SET balance=-1,version=version+1 WHERE id=$1`,
		`UPDATE wager_transactions SET status='PENDING' WHERE wallet_id=$1`,
		`DELETE FROM wager_transactions WHERE wallet_id=$1`,
		`UPDATE outbox SET payload='{}' WHERE aggregate_id=$1`,
	} {
		if _, err := pool.Exec(ctx, query, w.ID); err == nil {
			t.Fatalf("database allowed %s", query)
		}
	}
	zero := open(t, ctx, s, "0")
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger WHERE wallet_id=$1`, zero.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("zero opening created ledger")
	}
	balance(t, ctx, s, w.ID, 10000)
}

func TestFiftyConcurrentDuplicatesAndDurableReplay(t *testing.T) {
	ctx, pool, s := setup(t)
	w := open(t, ctx, s, "100")
	cmd := command(t, w, "duplicate", "BET", "25")
	var wg sync.WaitGroup
	results := make(chan application.Result, 50)
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.Process(ctx, cmd, application.Metadata{})
			if err != nil {
				errs <- err
				return
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	first := ""
	original := 0
	for r := range results {
		if first == "" {
			first = r.TransactionID
		}
		if r.TransactionID != first || r.Status != "PROCESSED" {
			t.Errorf("inconsistent duplicate %+v", r)
		}
		if !r.IdempotentReplay {
			original++
		}
	}
	if original != 1 {
		t.Fatalf("processed originals=%d", original)
	}
	process(t, ctx, s, command(t, w, "later", "WIN", "10"))
	restarted := application.NewService(postgres.New(pool))
	r := process(t, ctx, restarted, cmd)
	if !r.IdempotentReplay || r.Balance.MinorUnits() != 7500 {
		t.Fatalf("historical replay %+v", r)
	}
	cmd.IdempotencyKey = "alias"
	if !process(t, ctx, s, cmd).IdempotentReplay {
		t.Fatal("alternate key reapplied operation")
	}
	cmd.ExternalTransactionID = "new-external"
	if _, err := s.Process(ctx, cmd, application.Metadata{}); !errors.Is(err, application.ErrConflict) {
		t.Fatalf("alias reused for other operation: %v", err)
	}
	balance(t, ctx, s, w.ID, 8500)
}

func TestTwoEightyBetsAgainstOneHundred(t *testing.T) {
	ctx, _, s := setup(t)
	w := open(t, ctx, s, "100")
	results := make(chan application.Result, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := s.Process(ctx, command(t, w, fmt.Sprint(i), "BET", "80"), application.Metadata{})
			results <- r
			errs <- err
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	processed, rejected := 0, 0
	for r := range results {
		if r.Status == "PROCESSED" {
			processed++
		}
		if r.Status == "REJECTED" && r.FailureCode == "INSUFFICIENT_FUNDS" {
			rejected++
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("processed=%d rejected=%d", processed, rejected)
	}
	balance(t, ctx, s, w.ID, 2000)
}

func TestInboxSharesCommitAndChecksEnvelopeHash(t *testing.T) {
	ctx, pool, s := setup(t)
	w := open(t, ctx, s, "100")
	cmd := command(t, w, "mixed", "BET", "25")
	http := process(t, ctx, s, cmd)
	envelope := application.Envelope{MessageID: "message", Type: "WagerTransactionRequested", OccurredAt: time.Now().UTC(), Data: cmd}
	r, err := s.ProcessInbox(ctx, envelope, application.Metadata{})
	if err != nil || !r.IdempotentReplay || r.TransactionID != http.TransactionID {
		t.Fatalf("cross transport replay %+v %v", r, err)
	}
	r, err = s.ProcessInbox(ctx, envelope, application.Metadata{})
	if err != nil || !r.IdempotentReplay {
		t.Fatalf("inbox replay %+v %v", r, err)
	}
	envelope.Data.Money = money(t, "30")
	if _, err := s.ProcessInbox(ctx, envelope, application.Metadata{}); !errors.Is(err, application.ErrConflict) {
		t.Fatalf("changed message accepted %v", err)
	}
	var completed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM inbox WHERE completed_at IS NOT NULL`).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if completed != 1 {
		t.Fatalf("completed=%d", completed)
	}
	// A failure after claiming inbox must not leave acceptance outside the
	// financial commit. The wallet does not exist, so the whole unit aborts.
	envelope.MessageID = "invalid-wallet"
	envelope.Data.WalletID = application.NewID()
	envelope.Data.ExternalTransactionID = "missing"
	envelope.Data.IdempotencyKey = "missing"
	if _, err := s.ProcessInbox(ctx, envelope, application.Metadata{}); !errors.Is(err, application.ErrNotFound) {
		t.Fatalf("missing wallet: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM inbox WHERE message_id='invalid-wallet'`).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if completed != 0 {
		t.Fatal("inbox acceptance survived financial rollback")
	}
	balance(t, ctx, s, w.ID, 7500)
}

func TestPendingReferenceRecoveryAndExpiry(t *testing.T) {
	ctx, pool, s := setup(t)
	w := open(t, ctx, s, "100")
	refund := command(t, w, "refund", "REFUND", "25")
	refund.ReferenceExternalTransactionID = "bet"
	r := process(t, ctx, s, refund)
	if r.Status != "PENDING_REFERENCE" {
		t.Fatalf("expected pending %+v", r)
	}
	process(t, ctx, s, command(t, w, "bet", "BET", "25"))
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET next_attempt_at=now() WHERE id=$1`, r.TransactionID); err != nil {
		t.Fatal(err)
	}
	observed := 0
	restarted := application.NewService(postgres.New(pool), application.WithPendingObserver(func(outcome application.Result, elapsed time.Duration) {
		observed++
		committed, err := s.GetExternalTransaction(ctx, refund.ProviderID, refund.ExternalTransactionID)
		if err != nil || committed.Status != outcome.Status || outcome.Status != "PROCESSED" || elapsed <= 0 {
			t.Errorf("observer ran before successful commit: %+v %v", outcome, err)
		}
	}))
	if n, err := restarted.ResumePending(ctx); err != nil || n != 1 {
		t.Fatalf("resumed=%d err=%v", n, err)
	}
	if observed != 1 {
		t.Fatalf("observer calls=%d", observed)
	}
	r = process(t, ctx, s, refund)
	if r.Status != "PROCESSED" || !r.IdempotentReplay {
		t.Fatalf("pending recovery %+v", r)
	}
	missing := command(t, w, "expire", "ROLLBACK", "10")
	missing.ReferenceExternalTransactionID = "never-arrives"
	expiring := application.NewService(postgres.New(pool), application.WithReferencePolicy(1, time.Hour))
	r = process(t, ctx, expiring, missing)
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET next_attempt_at=now() WHERE id=$1`, r.TransactionID); err != nil {
		t.Fatal(err)
	}
	if _, err := expiring.ResumePending(ctx); err != nil {
		t.Fatal(err)
	}
	r = process(t, ctx, expiring, missing)
	if r.Status != "REJECTED" || r.FailureCode != "REFERENCE_NOT_FOUND" {
		t.Fatalf("expiry %+v", r)
	}
	balance(t, ctx, s, w.ID, 10000)
}

func TestReversalsAreExclusiveAndLossDoesNotMoveMoney(t *testing.T) {
	ctx, _, s := setup(t)
	w := open(t, ctx, s, "100")
	bet := command(t, w, "bet", "BET", "25")
	process(t, ctx, s, bet)
	refund := command(t, w, "refund", "REFUND", "25")
	refund.ReferenceExternalTransactionID = "bet"
	process(t, ctx, s, refund)
	rollback := command(t, w, "rollback", "ROLLBACK", "25")
	rollback.ReferenceExternalTransactionID = "bet"
	if r := process(t, ctx, s, rollback); r.Status != "REJECTED" || r.FailureCode != "ALREADY_REVERSED" {
		t.Fatalf("duplicate reversal %+v", r)
	}
	rollbackRefund := command(t, w, "rollback-refund", "ROLLBACK", "25")
	rollbackRefund.ReferenceExternalTransactionID = "refund"
	if r := process(t, ctx, s, rollbackRefund); r.Status != "PROCESSED" {
		t.Fatalf("rollback refund %+v", r)
	}
	refund.ExternalTransactionID = "second-refund"
	refund.IdempotencyKey = "second-refund"
	if r := process(t, ctx, s, refund); r.FailureCode != "ALREADY_REVERSED" {
		t.Fatalf("original bet reopened %+v", r)
	}
	before, err := s.GetWallet(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if r := process(t, ctx, s, command(t, w, "loss", "LOSS", "0")); r.Status != "PROCESSED" {
		t.Fatalf("loss %+v", r)
	}
	after, err := s.GetWallet(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if before.Version != after.Version {
		t.Fatal("loss changed wallet version")
	}
	balance(t, ctx, s, w.ID, 7500)
}

func TestMigrationCanBeReversedAndReapplied(t *testing.T) {
	ctx, pool, _ := setup(t)
	if err := postgres.Migrate(ctx, pool, "down"); err != nil {
		t.Fatal(err)
	}
	if err := postgres.Migrate(ctx, pool, "down"); err != nil {
		t.Fatal(err)
	}
	if err := postgres.Migrate(ctx, pool, "up"); err != nil {
		t.Fatal(err)
	}
}

func TestFinancialCommitRollsBackWhenOutboxInsertFails(t *testing.T) {
	ctx, pool, s := setup(t)
	w := open(t, ctx, s, "100")
	_, err := pool.Exec(ctx, `CREATE FUNCTION fail_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected storage failure'; END $$;
CREATE TRIGGER fail_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION fail_outbox()`)
	if err != nil {
		t.Fatal(err)
	}
	envelope := application.Envelope{MessageID: "atomic-rollback", Type: "WagerTransactionRequested", OccurredAt: time.Now().UTC(), Data: command(t, w, "rollback-all", "BET", "25")}
	if _, err := s.ProcessInbox(ctx, envelope, application.Metadata{}); !errors.Is(err, application.ErrUnavailable) {
		t.Fatalf("expected storage failure, got %v", err)
	}
	var ledger, transactions, inbox, outbox int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM wallet_ledger),(SELECT count(*) FROM wager_transactions),(SELECT count(*) FROM inbox),(SELECT count(*) FROM outbox)`).Scan(&ledger, &transactions, &inbox, &outbox); err != nil {
		t.Fatal(err)
	}
	if ledger != 1 || transactions != 1 || inbox != 0 || outbox != 2 {
		t.Fatalf("partial financial commit ledger=%d transactions=%d inbox=%d outbox=%d", ledger, transactions, inbox, outbox)
	}
	balance(t, ctx, s, w.ID, 10000)
	if _, err := pool.Exec(ctx, `DROP TRIGGER fail_outbox ON outbox; DROP FUNCTION fail_outbox()`); err != nil {
		t.Fatal(err)
	}
	if r, err := s.ProcessInbox(ctx, envelope, application.Metadata{}); err != nil || r.Status != "PROCESSED" {
		t.Fatalf("recovery after failure %+v %v", r, err)
	}
	balance(t, ctx, s, w.ID, 7500)
}

func TestPendingWorkerSkipsBusyWallet(t *testing.T) {
	ctx, pool, s := setup(t)
	busy := open(t, ctx, s, "100")
	free := open(t, ctx, s, "100")
	for i, w := range []application.WalletView{busy, free} {
		refund := command(t, w, fmt.Sprintf("refund-%d", i), "REFUND", "25")
		refund.ReferenceExternalTransactionID = fmt.Sprintf("bet-%d", i)
		process(t, ctx, s, refund)
		process(t, ctx, s, command(t, w, fmt.Sprintf("bet-%d", i), "BET", "25"))
	}
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET next_attempt_at=now() WHERE status='PENDING_REFERENCE'`); err != nil {
		t.Fatal(err)
	}
	lock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err := lock.Exec(ctx, `SELECT id FROM wallets WHERE id=$1 FOR UPDATE`, busy.ID); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if n, err := s.ResumePending(short); err != nil || n != 1 {
		t.Fatalf("busy wallet blocked other wallet, resumed=%d err=%v", n, err)
	}
	balance(t, ctx, s, free.ID, 10000)
	if err := lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ResumePending(ctx); err != nil || n != 1 {
		t.Fatalf("busy wallet did not resume n=%d err=%v", n, err)
	}
	balance(t, ctx, s, busy.ID, 10000)
}

func TestAuditableBusinessRejectionsAndStablePagination(t *testing.T) {
	ctx, _, s := setup(t)
	w := open(t, ctx, s, "0")
	win := command(t, w, "win", "WIN", "10")
	process(t, ctx, s, win)
	process(t, ctx, s, command(t, w, "spent", "BET", "10"))
	rollback := command(t, w, "rollback-win", "ROLLBACK", "10")
	rollback.ReferenceExternalTransactionID = "win"
	if r := process(t, ctx, s, rollback); r.FailureCode != "REVERSAL_INSUFFICIENT_FUNDS" {
		t.Fatalf("reversal funds %+v", r)
	}
	wrongCurrency := command(t, w, "currency", "LOSS", "0")
	wrongCurrency.Money, _ = domain.NewMoney("0", "USD")
	if r := process(t, ctx, s, wrongCurrency); r.FailureCode != "CURRENCY_MISMATCH" || r.Balance.Currency() != "BRL" {
		t.Fatalf("currency audit %+v", r)
	}
	page, err := s.Ledger(ctx, w.ID, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.NextCursor == "" {
		t.Fatalf("first page %+v", page)
	}
	next, err := s.Ledger(ctx, w.ID, page.NextCursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Entries) != 1 || next.NextCursor != "" || next.Entries[0].ID == page.Entries[0].ID {
		t.Fatalf("next page %+v", next)
	}
	invalid := fmt.Sprintf(`{"walletId":%q,"createdAt":"2026-09-22T00:00:00Z","id":"zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz"}`, w.ID)
	if _, err := s.Ledger(ctx, w.ID, base64.RawURLEncoding.EncodeToString([]byte(invalid)), 1); !errors.Is(err, application.ErrInvalidInput) {
		t.Fatalf("invalid cursor mapped to %v", err)
	}
	balance(t, ctx, s, w.ID, 0)
}

func TestUUIDCaseNormalizesBeforeIdentityHash(t *testing.T) {
	ctx, _, s := setup(t)
	player := strings.ToUpper(application.NewID())
	w, err := s.OpenWallet(ctx, player, money(t, "100"), application.Metadata{})
	if err != nil {
		t.Fatal(err)
	}
	if w.PlayerID != strings.ToLower(player) {
		t.Fatal("wallet player not canonical")
	}
	cmd := command(t, w, "case-insensitive-uuid", "BET", "25")
	cmd.PlayerID, cmd.WalletID = strings.ToUpper(cmd.PlayerID), strings.ToUpper(cmd.WalletID)
	first := process(t, ctx, s, cmd)
	if first.Status != "PROCESSED" {
		t.Fatalf("uppercase UUID rejected %+v", first)
	}
	cmd.PlayerID, cmd.WalletID = strings.ToLower(cmd.PlayerID), strings.ToLower(cmd.WalletID)
	replay := process(t, ctx, s, cmd)
	if !replay.IdempotentReplay || first.TransactionID != replay.TransactionID {
		t.Fatalf("UUID representation conflicted %+v", replay)
	}
	balance(t, ctx, s, w.ID, 7500)
}
