//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/postgres"
)

func TestMigrationPreservesExistingFinancialData(t *testing.T) {
	ctx, pool, service := setup(t)
	wallet := open(t, ctx, service, "100.00")
	process(t, ctx, service, command(t, wallet, "migration-preservation", "BET", "25.00"))
	// Recreate the metadata format written by the original runner. Only this
	// isolated schema is changed; all financial records remain in place.
	if _, err := pool.Exec(ctx, `
		ALTER TABLE schema_migrations DROP COLUMN IF EXISTS dirty;
		ALTER TABLE schema_migrations ALTER COLUMN version TYPE integer;
		ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS applied_at timestamptz NOT NULL DEFAULT now();
	`); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		if err := postgres.Migrate(ctx, pool, "up"); err != nil {
			t.Fatal(err)
		}
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("migration closed caller pool: %v", err)
	}
	balance(t, ctx, service, wallet.ID, 7500)
	var entries int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger WHERE wallet_id = $1`, wallet.ID).Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if entries != 2 {
		t.Fatalf("ledger entries = %d, want 2", entries)
	}
}

func TestMigrationConcurrentDeployments(t *testing.T) {
	ctx, pool, _ := setup(t)
	if err := postgres.Migrate(ctx, pool, "down"); err != nil {
		t.Fatal(err)
	}

	var group sync.WaitGroup
	results := make(chan error, 3)
	for range 3 {
		group.Go(func() { results <- postgres.Migrate(ctx, pool, "up") })
	}
	group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent migration: %v", err)
		}
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationCanceledContext(t *testing.T) {
	ctx, pool, _ := setup(t)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := postgres.Migrate(canceled, pool, "up"); !errors.Is(err, context.Canceled) {
		t.Fatalf("migration error = %v, want context.Canceled", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationDeadlineWhileMetadataIsLocked(t *testing.T) {
	ctx, pool, _ := setup(t)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `LOCK TABLE schema_migrations IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	deadline, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	err = postgres.Migrate(deadline, pool, "up")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("migration error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("migration ignored deadline for %s", elapsed)
	}
}

func TestMigrationRejectsInvalidDirection(t *testing.T) {
	ctx, pool, _ := setup(t)
	err := postgres.Migrate(ctx, pool, "sideways")
	if err == nil || !strings.Contains(err.Error(), "up or down") {
		t.Fatalf("migration error = %v, want invalid direction", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
