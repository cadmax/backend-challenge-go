// Package observability exposes application metrics through the Prometheus client.
package observability

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	transactionStatuses = []string{"PROCESSED", "REJECTED", "PENDING_REFERENCE", "FAILED", "PENDING"}
	retryWorkers        = []string{"consumer_poll", "consumer", "ack", "outbox", "references"}
	conflictKinds       = []string{"identity", "inbox"}
)

type Metrics struct {
	transactions              *prometheus.CounterVec
	retries                   *prometheus.CounterVec
	conflicts                 *prometheus.CounterVec
	idempotentReplays         prometheus.Counter
	outboxPublished           prometheus.Counter
	reconciliationDivergences prometheus.Counter
	storageFailures           prometheus.Counter
	concurrencyConflicts      prometheus.Counter
	outboxLag                 prometheus.Gauge
	dlqDepth                  prometheus.Gauge
	processing                prometheus.Histogram
	handler                   http.Handler
}

func New() *Metrics {
	// Each application owns its registry, including applications created in tests.
	registry := prometheus.NewRegistry()
	factory := promauto.With(registry)
	m := &Metrics{
		transactions: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_transactions_total",
			Help: "Transaction results served, including idempotent replays.",
		}, []string{"status"}),
		retries: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_retries_total",
			Help: "Retry attempts by worker.",
		}, []string{"worker"}),
		conflicts: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_conflicts_total",
			Help: "Payload conflicts by identity boundary.",
		}, []string{"kind"}),
		idempotentReplays: factory.NewCounter(prometheus.CounterOpts{
			Name: "wager_idempotent_replays_total",
			Help: "Previously persisted transaction results served again.",
		}),
		outboxPublished: factory.NewCounter(prometheus.CounterOpts{
			Name: "wager_outbox_published_total",
			Help: "Outbox events successfully published and marked as sent.",
		}),
		reconciliationDivergences: factory.NewCounter(prometheus.CounterOpts{
			Name: "wager_reconciliation_divergences_total",
			Help: "Reconciliations where the wallet balance differs from the ledger.",
		}),
		storageFailures: factory.NewCounter(prometheus.CounterOpts{
			Name: "wager_storage_failures_total",
			Help: "HTTP operations that failed because storage was unavailable.",
		}),
		concurrencyConflicts: factory.NewCounter(prometheus.CounterOpts{
			Name: "wager_concurrency_conflicts_total",
			Help: "PostgreSQL serialization failures, deadlocks and unavailable locks.",
		}),
		outboxLag: factory.NewGauge(prometheus.GaugeOpts{
			Name: "wager_outbox_oldest_pending_seconds",
			Help: "Age in seconds of the oldest unpublished outbox event.",
		}),
		dlqDepth: factory.NewGauge(prometheus.GaugeOpts{
			Name: "wager_dlq_messages",
			Help: "Approximate number of messages available in the dead-letter queue.",
		}),
		processing: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "wager_processing_seconds",
			Help:    "Time spent processing HTTP, SQS and pending-reference operations.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 1, 10},
		}),
		handler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	}
	// Expose every bounded label value from startup, even before the first event.
	for _, status := range transactionStatuses {
		m.transactions.WithLabelValues(strings.ToLower(status))
	}
	for _, worker := range retryWorkers {
		m.retries.WithLabelValues(worker)
	}
	for _, kind := range conflictKinds {
		m.conflicts.WithLabelValues(kind)
	}
	return m
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.handler.ServeHTTP(w, r)
}

// SQLState is exposed by PostgreSQL errors even through wrapped storage errors.
func (m *Metrics) StorageError(err error) {
	var state interface{ SQLState() string }
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "40001", "40P01", "55P03":
			m.concurrencyConflicts.Inc()
		}
	}
}

func (m *Metrics) Result(status string, replay bool) {
	if slices.Contains(transactionStatuses, status) {
		m.transactions.WithLabelValues(strings.ToLower(status)).Inc()
	}
	if replay {
		m.idempotentReplays.Inc()
	}
}

func (m *Metrics) Retry(worker string) {
	if slices.Contains(retryWorkers, worker) {
		m.retries.WithLabelValues(worker).Inc()
	}
}

func (m *Metrics) Conflict(kind string) {
	if slices.Contains(conflictKinds, kind) {
		m.conflicts.WithLabelValues(kind).Inc()
	}
}

func (m *Metrics) OutboxPublished() {
	m.outboxPublished.Inc()
}

func (m *Metrics) ReconciliationDivergence() {
	m.reconciliationDivergences.Inc()
}

func (m *Metrics) StorageFailure() {
	m.storageFailures.Inc()
}

func (m *Metrics) OutboxLag(seconds int64) {
	m.outboxLag.Set(float64(seconds))
}

func (m *Metrics) DLQDepth(messages int64) {
	m.dlqDepth.Set(float64(messages))
}

func (m *Metrics) ObserveProcessing(duration time.Duration) {
	m.processing.Observe(max(0, duration).Seconds())
}
