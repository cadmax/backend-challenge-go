//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMigrationFilesApplyInOrderAndReverse(t *testing.T) {
	ctx, pool := migrationTestPool(t)
	files := fstest.MapFS{
		"001_example.up.sql":   {Data: []byte(`CREATE TABLE example (value integer PRIMARY KEY); INSERT INTO example VALUES (1);`)},
		"001_example.down.sql": {Data: []byte(`DROP TABLE example;`)},
		"002_example.up.sql":   {Data: []byte(`ALTER TABLE example ADD COLUMN name text; UPDATE example SET name = 'kept';`)},
		"002_example.down.sql": {Data: []byte(`ALTER TABLE example DROP COLUMN name;`)},
	}
	for range 2 {
		if err := migrateFiles(ctx, pool, "up", files); err != nil {
			t.Fatal(err)
		}
	}
	var name string
	if err := pool.QueryRow(ctx, `SELECT name FROM example WHERE value = 1`).Scan(&name); err != nil || name != "kept" {
		t.Fatalf("ordered migration data = %q, err %v", name, err)
	}
	assertMigrationVersion(t, ctx, pool, 2, false)
	for range 2 {
		if err := migrateFiles(ctx, pool, "down", files); err != nil {
			t.Fatal(err)
		}
	}
	var empty bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('example') IS NULL`).Scan(&empty); err != nil || !empty {
		t.Fatalf("down did not reverse all migrations: table absent = %v, err %v", empty, err)
	}
	if err := migrateFiles(ctx, pool, "up", files); err != nil {
		t.Fatal("up after down:", err)
	}
}

func TestMigrationFailureRollsBackSQLAndRequiresDirtyStateRecovery(t *testing.T) {
	ctx, pool := migrationTestPool(t)
	files := fstest.MapFS{
		"001_example.up.sql":   {Data: []byte(`CREATE TABLE example (value integer PRIMARY KEY); INSERT INTO example VALUES (1);`)},
		"001_example.down.sql": {Data: []byte(`DROP TABLE example;`)},
		"002_example.up.sql":   {Data: []byte(`CREATE TABLE partial_change (value integer); INSERT INTO example VALUES (1);`)},
		"002_example.down.sql": {Data: []byte(`DROP TABLE partial_change;`)},
	}
	if err := migrateFiles(ctx, pool, "up", files); err == nil {
		t.Fatal("expected duplicate-key migration failure")
	}
	assertMigrationVersion(t, ctx, pool, 2, true)
	var absent bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('partial_change') IS NULL`).Scan(&absent); err != nil || !absent {
		t.Fatalf("failed SQL file was not rolled back: table absent = %v, err %v", absent, err)
	}
	for _, direction := range []string{"up", "down"} {
		var dirty migrate.ErrDirty
		if err := migrateFiles(ctx, pool, direction, files); !errors.As(err, &dirty) || dirty.Version != 2 {
			t.Fatalf("%s error = %v, want dirty version 2", direction, err)
		}
	}
}

func TestMigrationRejectsUnknownLegacyVersion(t *testing.T) {
	ctx, pool := migrationTestPool(t)
	if _, err := pool.Exec(ctx, `
		CREATE TABLE schema_migrations (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());
		INSERT INTO schema_migrations(version) VALUES (7);
	`); err != nil {
		t.Fatal(err)
	}
	err := Migrate(ctx, pool, "up")
	if err == nil || !strings.Contains(err.Error(), "unsupported legacy migration version") {
		t.Fatalf("migration error = %v, want unsupported legacy version", err)
	}
	var version int
	if err := pool.QueryRow(ctx, `SELECT version FROM schema_migrations`).Scan(&version); err != nil || version != 7 {
		t.Fatalf("unknown metadata changed: version = %d, err %v", version, err)
	}
}

func TestMigrationAdoptsEmptyLegacyMetadata(t *testing.T) {
	ctx, pool := migrationTestPool(t)
	if _, err := pool.Exec(ctx, `CREATE TABLE schema_migrations (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool, "up"); err != nil {
		t.Fatal(err)
	}
	assertMigrationVersion(t, ctx, pool, 1, false)
	var columnType string
	if err := pool.QueryRow(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'schema_migrations' AND column_name = 'version'
	`).Scan(&columnType); err != nil || columnType != "bigint" {
		t.Fatalf("version column = %q, err %v", columnType, err)
	}
}

func TestMigrationDeadlineInterruptsSQL(t *testing.T) {
	ctx, pool := migrationTestPool(t)
	files := fstest.MapFS{
		"001_slow.up.sql":   {Data: []byte(`CREATE TABLE partial_change (value integer); SELECT pg_sleep(10);`)},
		"001_slow.down.sql": {Data: []byte(`DROP TABLE partial_change;`)},
	}
	deadline, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := migrateFiles(deadline, pool, "up", files); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("migration error = %v, want context deadline", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("migration ignored deadline for %s", elapsed)
	}
	assertMigrationVersion(t, ctx, pool, 1, true)
	var absent bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('partial_change') IS NULL`).Scan(&absent); err != nil || !absent {
		t.Fatalf("interrupted SQL was not rolled back: table absent = %v, err %v", absent, err)
	}
}

func TestMigrationDeadlineInterruptsDeploymentLock(t *testing.T) {
	ctx, pool := migrationTestPool(t)
	if err := Migrate(ctx, pool, "up"); err != nil {
		t.Fatal(err)
	}
	var dbName, schemaName string
	if err := pool.QueryRow(ctx, `SELECT current_database(), current_schema()`).Scan(&dbName, &schemaName); err != nil {
		t.Fatal(err)
	}
	lockID, err := database.GenerateAdvisoryLockId(dbName, schemaName, "schema_migrations")
	if err != nil {
		t.Fatal(err)
	}
	connection, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Release()
	if _, err := connection.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = connection.Exec(ctx, `SELECT pg_advisory_unlock($1)`, lockID) }()
	deadline, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := Migrate(deadline, pool, "up"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("migration error = %v, want context deadline", err)
	}
}

func TestMigrationCleanupInterruptsTimedOutLibraryLock(t *testing.T) {
	ctx, pool := migrationTestPool(t)
	// The migration context deliberately has no deadline: only the library's
	// LockTimeout returns while its advisory-lock query remains blocked.
	db, cleanup := migrationDatabase(context.Background(), pool)
	var driver database.Driver
	defer func() { cleanup(driver) }()
	var err error
	driver, err = migratepgx.WithInstance(db, &migratepgx.Config{})
	if err != nil {
		t.Fatal(err)
	}
	source, err := iofs.New(fstest.MapFS{
		"001_example.up.sql":   {Data: []byte(`CREATE TABLE example (value integer);`)},
		"001_example.down.sql": {Data: []byte(`DROP TABLE example;`)},
	}, ".")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	runner, err := migrate.NewWithInstance("iofs", source, "pgx5", driver)
	if err != nil {
		t.Fatal(err)
	}
	runner.LockTimeout = 25 * time.Millisecond

	var dbName, schemaName string
	if err := pool.QueryRow(ctx, `SELECT current_database(), current_schema()`).Scan(&dbName, &schemaName); err != nil {
		t.Fatal(err)
	}
	lockID, err := database.GenerateAdvisoryLockId(dbName, schemaName, "schema_migrations")
	if err != nil {
		t.Fatal(err)
	}
	locker, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Release()
	if _, err := locker.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = locker.Exec(ctx, `SELECT pg_advisory_unlock($1)`, lockID) }()
	if err := runner.Up(); !errors.Is(err, migrate.ErrLockTimeout) {
		t.Fatalf("migration error = %v, want library lock timeout", err)
	}

	done := make(chan struct{})
	go func() {
		cleanup(driver)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		// Release the fixture lock before failing so a regression cannot hang
		// the test process during deferred cleanup.
		_, _ = locker.Exec(ctx, `SELECT pg_advisory_unlock($1)`, lockID)
		<-done
		t.Fatal("migration cleanup waited for the blocked advisory lock")
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("cleanup closed caller pool: %v", err)
	}
}

func assertMigrationVersion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want int, wantDirty bool) {
	t.Helper()
	var version int
	var dirty bool
	if err := pool.QueryRow(ctx, `SELECT version, dirty FROM schema_migrations`).Scan(&version, &dirty); err != nil {
		t.Fatal(err)
	}
	if version != want || dirty != wantDirty {
		t.Fatalf("migration version = %d, dirty = %v; want %d, %v", version, dirty, want, wantDirty)
	}
}

func migrationTestPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to a real PostgreSQL owner connection")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("migration_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanupCtx, `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`); err != nil {
			t.Errorf("drop isolated migration schema: %v", err)
		}
		admin.Close()
	})
	return ctx, pool
}
