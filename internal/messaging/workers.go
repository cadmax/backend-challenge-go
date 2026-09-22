package messaging

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/application"
	"github.com/cadmax/backend-challenge-go/internal/config"
	"github.com/cadmax/backend-challenge-go/internal/observability"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Processor interface {
	ProcessInbox(context.Context, application.Envelope, application.Metadata) (application.Result, error)
	ResumePending(context.Context) (int, error)
}

type Workers struct {
	cfg                   config.Config
	queue                 *Queue
	pool                  *pgxpool.Pool
	service               Processor
	log                   *slog.Logger
	metrics               *observability.Metrics
	stopPolling, stopWork context.CancelFunc
	done                  chan struct{}
}

func NewWorkers(cfg config.Config, queue *Queue, pool *pgxpool.Pool, service Processor, log *slog.Logger, metrics *observability.Metrics) *Workers {
	return &Workers{
		cfg:     cfg,
		queue:   queue,
		pool:    pool,
		service: service,
		log:     log,
		metrics: metrics,
	}
}

func (w *Workers) Start(context.Context) error {
	pollCtx, stopPolling := context.WithCancel(context.Background())
	workCtx, stopWork := context.WithCancel(context.Background())
	w.stopPolling, w.stopWork, w.done = stopPolling, stopWork, make(chan struct{})
	var wg sync.WaitGroup
	start := func(name string, fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.log.Info("worker started", "worker", name)
			defer w.log.Info("worker stopped", "worker", name)
			fn()
		}()
	}
	if w.cfg.WorkersEnabled {
		if w.cfg.ConsumerEnabled {
			start("consumer", func() { w.consume(pollCtx, workCtx) })
		}
		if w.cfg.PublisherEnabled {
			start("outbox", func() { w.publish(pollCtx, workCtx) })
		}
		if w.cfg.ReferenceWorkerEnabled {
			start("references", func() { w.references(pollCtx, workCtx) })
		}
		start("queue-metrics", func() { w.queueMetrics(pollCtx) })
	}
	go func() {
		wg.Wait()
		close(w.done)
	}()
	return nil
}

func (w *Workers) Stop(ctx context.Context) error {
	if w.stopPolling == nil {
		return nil
	}
	w.Quiesce()
	select {
	case <-w.done:
		w.stopWork()
		return nil
	case <-ctx.Done():
		w.stopWork()
		// All I/O has its own deadline. Join cleanup before allowing lifecycle
		// hooks to close the database used by rollback/visibility release.
		<-w.done
		return ctx.Err()
	}
}

// Quiesce stops accepting new messages while allowing in-flight work to finish.
// The HTTP shutdown hook calls this before it begins draining requests.
func (w *Workers) Quiesce() {
	if w.stopPolling != nil {
		w.stopPolling()
	}
}

func (w *Workers) failpoint(name string) {
	if w.cfg.EnableTestFailpoints && w.cfg.TestFailpoint == name {
		w.log.Warn("test failpoint reached", "failpoint", name)
		os.Exit(86)
	}
}

func backoff(attempt int) time.Duration {
	return time.Second * time.Duration(1<<min(6, max(0, attempt-1)))
}

func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
