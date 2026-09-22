package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"

	"github.com/cadmax/backend-challenge-go/migrations"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// Migrate applies or reverses all embedded migrations as the dedicated owner.
// golang-migrate owns versioning, dirty-state detection and deployment locking.
func Migrate(ctx context.Context, pool *pgxpool.Pool, direction string) error {
	return migrateFiles(ctx, pool, direction, migrations.Files)
}

func migrateFiles(ctx context.Context, pool *pgxpool.Pool, direction string, files fs.FS) error {
	if direction != "up" && direction != "down" {
		return fmt.Errorf("migration direction must be up or down")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	source, err := iofs.New(files, ".")
	if err != nil {
		return fmt.Errorf("open migration files: %w", err)
	}
	defer func() { _ = source.Close() }()

	db, closeDB := migrationDatabase(ctx, pool)
	var driver database.Driver
	defer func() { closeDB(driver) }()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect migration owner: %w", migrationError(ctx, err))
	}
	if err := adoptLegacyMigrationTable(ctx, db); err != nil {
		return fmt.Errorf("adopt legacy migration metadata: %w", migrationError(ctx, err))
	}

	// Keep multi-statement mode disabled: PostgreSQL executes each whole SQL file
	// in one implicit transaction, including the deferred financial constraints.
	driver, err = migratepgx.WithInstance(db, &migratepgx.Config{})
	if err != nil {
		return migrationError(ctx, err)
	}
	runner, err := migrate.NewWithInstance("iofs", source, "pgx5", driver)
	if err != nil {
		return err
	}
	if direction == "up" {
		err = runner.Up()
	} else {
		err = runner.Down()
	}
	if errors.Is(err, migrate.ErrNoChange) {
		return nil
	}
	return migrationError(ctx, err)
}

// golang-migrate does not accept context.Context. Give it an owned connection,
// and interrupt its queries (including advisory-lock waits) by closing the
// network connection on cancellation. Never close the caller's pgx pool.
func migrationDatabase(ctx context.Context, pool *pgxpool.Pool) (*sql.DB, func(database.Driver)) {
	var closeConnections []func()
	db := stdlib.OpenDB(*pool.Config().ConnConfig,
		stdlib.OptionBeforeConnect(func(context.Context, *pgx.ConnConfig) error { return ctx.Err() }),
		stdlib.OptionAfterConnect(func(_ context.Context, conn *pgx.Conn) error {
			socket := conn.PgConn().Conn()
			stop := context.AfterFunc(ctx, func() { _ = socket.Close() })
			closeConnections = append(closeConnections, func() {
				stop()
				_ = socket.Close()
			})
			return nil
		}),
	)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, func(driver database.Driver) {
		// A library lock timeout can leave its Lock goroutine blocked in SQL.
		// Interrupt that query before driver.Close waits for its connection.
		for _, closeConnection := range closeConnections {
			closeConnection()
		}
		if driver != nil {
			_ = driver.Close()
		}
		_ = db.Close()
	}
}

func migrationError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// The original runner recorded only version=1 and applied_at. Adopt that marker
// without rerunning financial DDL or deleting existing wallets and ledger rows.
// Retain the original lock during this one-time conversion for old deployments.
func adoptLegacyMigrationTable(ctx context.Context, db *sql.DB) error {
	const query = `
        DO $adoption$
        BEGIN
            PERFORM pg_advisory_xact_lock(827364019);
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                    AND table_name = 'schema_migrations' AND column_name = 'applied_at'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                    AND table_name = 'schema_migrations' AND column_name = 'dirty'
            ) THEN
                IF EXISTS (SELECT 1 FROM schema_migrations WHERE version <> 1) THEN
                    RAISE EXCEPTION 'unsupported legacy migration version; inspect metadata before upgrading';
                END IF;
                ALTER TABLE schema_migrations ALTER COLUMN version TYPE bigint;
                ALTER TABLE schema_migrations ADD COLUMN dirty boolean NOT NULL DEFAULT false;
            END IF;
        END
        $adoption$;`
	_, err := db.ExecContext(ctx, query)
	return err
}
