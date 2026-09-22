package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/cadmax/backend-challenge-go/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Migrate applies or reverses the schema as its dedicated owner. Application
// traffic never takes this deployment lock and runtime credentials cannot DDL.
func Migrate(ctx context.Context, pool *pgxpool.Pool, direction string) error {
	if direction != "up" && direction != "down" {
		return fmt.Errorf("migration direction must be up or down")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(827364019)`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations(version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	var applied bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=1)`).Scan(&applied); err != nil {
		return err
	}
	if applied == (direction == "up") {
		return tx.Commit(ctx)
	}
	script, err := migrations.Files.ReadFile("001_financial." + direction + ".sql")
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, string(script)); err != nil {
		return fmt.Errorf("migration 001 %s: %w", direction, err)
	}
	if direction == "up" {
		_, err = tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES(1)`)
	} else {
		_, err = tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version=1`)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
