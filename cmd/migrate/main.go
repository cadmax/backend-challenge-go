package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) != 2 || (os.Args[1] != "up" && os.Args[1] != "down") {
		return fmt.Errorf("usage: migrate up|down")
	}
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return fmt.Errorf("invalid database configuration")
	}
	defer pool.Close()
	if err := postgres.Migrate(ctx, pool, os.Args[1]); err != nil {
		return fmt.Errorf("migration %s: %w", os.Args[1], err)
	}
	fmt.Printf("migration %s completed\n", os.Args[1])
	return nil
}
