// Package observability exposes a small, bounded-cardinality Prometheus registry.
package observability

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type Metrics struct {
	mu                          sync.Mutex
	counters                    map[string]uint64
	gauges                      map[string]int64
	latencyCount, latencyMicros uint64
	buckets                     [7]uint64
}

func New() *Metrics {
	m := &Metrics{counters: map[string]uint64{}, gauges: map[string]int64{}}
	for _, key := range []string{"idempotent_replays_total", "outbox_published_total", "reconciliation_divergences_total", "storage_failures_total", "concurrency_conflicts_total", `conflicts_total{kind="identity"}`, `conflicts_total{kind="inbox"}`} {
		m.counters[key] = 0
	}
	for _, status := range []string{"processed", "rejected", "pending_reference", "failed", "pending"} {
		m.counters[`transactions_total{status="`+status+`"}`] = 0
	}
	for _, worker := range []string{"consumer_poll", "consumer", "ack", "outbox", "references"} {
		m.counters[`retries_total{worker="`+worker+`"}`] = 0
	}
	m.gauges["outbox_oldest_pending_seconds"], m.gauges["dlq_messages"] = 0, 0
	return m
}

// SQLState is exposed by PostgreSQL errors even through wrapped storage errors.
func (m *Metrics) StorageError(err error) {
	var state interface{ SQLState() string }
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "40001", "40P01", "55P03":
			m.Inc("concurrency_conflicts_total")
		}
	}
}

// Keys are selected by application code, never interpolated from provider input.
func (m *Metrics) Inc(key string) { m.mu.Lock(); defer m.mu.Unlock(); m.counters[key]++ }
func (m *Metrics) Gauge(key string, value int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[key] = value
}
func (m *Metrics) ObserveProcessing(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latencyCount++
	m.latencyMicros += uint64(max(0, d.Microseconds()))
	for i, upper := range []time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, time.Second, 10 * time.Second} {
		if d <= upper {
			m.buckets[i]++
		}
	}
}
func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	keys := make([]string, 0, len(m.counters)+len(m.gauges))
	for k := range m.counters {
		keys = append(keys, k)
	}
	for k := range m.gauges {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if v, ok := m.counters[k]; ok {
			fmt.Fprintf(w, "wager_%s %d\n", k, v)
		} else {
			fmt.Fprintf(w, "wager_%s %d\n", k, m.gauges[k])
		}
	}
	fmt.Fprintln(w, "# TYPE wager_processing_seconds histogram")
	for i, upper := range []string{"0.001", "0.005", "0.01", "0.05", "0.1", "1", "10"} {
		fmt.Fprintf(w, "wager_processing_seconds_bucket{le=%q} %d\n", upper, m.buckets[i])
	}
	fmt.Fprintf(w, "wager_processing_seconds_bucket{le=\"+Inf\"} %d\nwager_processing_seconds_count %d\nwager_processing_seconds_sum %d.%06d\n", m.latencyCount, m.latencyCount, m.latencyMicros/1_000_000, m.latencyMicros%1_000_000)
}

func (m *Metrics) Result(status string, replay bool) {
	switch status {
	case "PROCESSED", "REJECTED", "PENDING_REFERENCE", "FAILED", "PENDING":
		m.Inc("transactions_total{status=\"" + strings.ToLower(status) + "\"}")
	}
	if replay {
		m.Inc("idempotent_replays_total")
	}
}
