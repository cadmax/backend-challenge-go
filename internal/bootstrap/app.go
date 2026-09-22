// Package bootstrap is the composition root. Domain and use cases do not know Fx.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/application"
	"github.com/cadmax/backend-challenge-go/internal/auth"
	"github.com/cadmax/backend-challenge-go/internal/config"
	"github.com/cadmax/backend-challenge-go/internal/httpapi"
	"github.com/cadmax/backend-challenge-go/internal/messaging"
	"github.com/cadmax/backend-challenge-go/internal/observability"
	"github.com/cadmax/backend-challenge-go/internal/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

func Module() fx.Option {
	return fx.Options(
		fx.Module("configuration", fx.Provide(config.Load, newLogger)),
		fx.Module("persistence", fx.Provide(newPool, postgres.New)),
		fx.Module("application", fx.Provide(newService, observability.New)),
		fx.Module("authentication", fx.Provide(newVerifier)),
		fx.Module("messaging", fx.Provide(newQueue, newWorkers), fx.Invoke(registerWorkers)),
		fx.Module("http", fx.Provide(newHandler), fx.Invoke(registerServer)),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger { return &fxevent.SlogLogger{Logger: log} }),
	)
}

func New() *fx.App {
	// Read first to use the configured shutdown deadline also for Fx itself.
	c, err := config.Load()
	if err != nil {
		return fx.New(fx.Error(err))
	}
	return fx.New(Module(), fx.StopTimeout(c.ShutdownTimeout), fx.StartTimeout(30*time.Second))
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func newPool(c config.Config, lc fx.Lifecycle) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(c.DatabaseURL)
	if err != nil {
		return nil, errors.New("DATABASE_URL is invalid")
	}
	pc.MaxConns = 12
	pc.MinConns = 1
	pc.ConnConfig.ConnectTimeout = 5 * time.Second
	pc.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	pc.ConnConfig.RuntimeParams["lock_timeout"] = "5000"
	pc.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "15000"
	pool, err := pgxpool.NewWithConfig(context.Background(), pc)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	lc.Append(fx.Hook{OnStart: pool.Ping, OnStop: func(context.Context) error { pool.Close(); return nil }})
	return pool, nil
}

func newService(store *postgres.Store, c config.Config, metrics *observability.Metrics) *application.Service {
	return application.NewService(store, application.WithReferencePolicy(c.ReferenceMaxAttempts, c.ReferenceTTL),
		application.WithPendingObserver(func(result application.Result, duration time.Duration) {
			metrics.Result(result.Status, result.IdempotentReplay)
			metrics.ObserveProcessing(duration)
		}))
}

func newQueue(c config.Config, lc fx.Lifecycle) (*messaging.Queue, error) {
	q, err := messaging.NewQueue(c)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStart: q.Initialize})
	return q, nil
}

func newVerifier(c config.Config, lc fx.Lifecycle) (*auth.Verifier, error) {
	jwks := c.OIDCJWKSURL
	if jwks == "" {
		jwks = c.OIDCIssuerURL + "/protocol/openid-connect/certs"
	}
	verifier, err := auth.New(context.Background(), c.OIDCIssuerURL, c.OIDCAudience, jwks)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStart: verifier.Ready})
	return verifier, nil
}

func newWorkers(c config.Config, q *messaging.Queue, pool *pgxpool.Pool, service *application.Service, log *slog.Logger, m *observability.Metrics) *messaging.Workers {
	return messaging.NewWorkers(c, q, pool, service, log, m)
}
func registerWorkers(lc fx.Lifecycle, w *messaging.Workers) {
	lc.Append(fx.Hook{OnStart: w.Start, OnStop: w.Stop})
}

func newHandler(c config.Config, service *application.Service, verifier *auth.Verifier, pool *pgxpool.Pool, q *messaging.Queue, log *slog.Logger, m *observability.Metrics) http.Handler {
	return httpapi.New(&observedService{Service: service, metrics: m, log: log}, verifier, httpapi.Options{
		Logger: log, ReadinessChecks: map[string]func(context.Context) error{"postgres": pool.Ping, "sqs": q.Ready},
		Metrics: m, RequestTimeout: c.ProcessTimeout,
	})
}

func registerServer(lc fx.Lifecycle, c config.Config, handler http.Handler, log *slog.Logger, shutdown fx.Shutdowner, workers *messaging.Workers) {
	server := &http.Server{Addr: c.HTTPAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: c.ProcessTimeout + 5*time.Second, WriteTimeout: c.ProcessTimeout + 5*time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 * 1024}
	done := make(chan struct{})
	lc.Append(fx.Hook{OnStart: func(context.Context) error {
		listener, err := net.Listen("tcp", c.HTTPAddr)
		if err != nil {
			return err
		}
		go func() {
			defer close(done)
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("HTTP server stopped unexpectedly", "error", err)
				_ = shutdown.Shutdown(fx.ExitCode(1))
			}
		}()
		log.Info("HTTP server started", "address", listener.Addr().String())
		return nil
	}, OnStop: func(ctx context.Context) error {
		workers.Quiesce()
		err := server.Shutdown(ctx)
		if err != nil {
			_ = server.Close()
		}
		<-done
		log.Info("HTTP server stopped")
		return err
	}})
}

type observedService struct {
	*application.Service
	metrics *observability.Metrics
	log     *slog.Logger
}

func (s *observedService) Process(ctx context.Context, command application.Command, meta application.Metadata) (application.Result, error) {
	start := time.Now()
	defer func() { s.metrics.ObserveProcessing(time.Since(start)) }()
	result, err := s.Service.Process(ctx, command, meta)
	s.metrics.StorageError(err)
	if err == nil {
		s.metrics.Result(result.Status, result.IdempotentReplay)
		s.log.Info("transaction completed", "correlationId", meta.CorrelationID, "transactionId", result.TransactionID, "providerId", command.ProviderID, "walletId", command.WalletID, "status", result.Status, "replay", result.IdempotentReplay)
	}
	if errors.Is(err, application.ErrConflict) {
		s.metrics.Inc("conflicts_total{kind=\"identity\"}")
	}
	if errors.Is(err, application.ErrUnavailable) {
		s.metrics.Inc("storage_failures_total")
	}
	return result, err
}
func (s *observedService) Reconcile(ctx context.Context, id string) (application.Reconciliation, error) {
	result, err := s.Service.Reconcile(ctx, id)
	if err == nil && !result.Consistent {
		s.metrics.Inc("reconciliation_divergences_total")
	}
	return result, err
}
