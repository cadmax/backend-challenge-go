package observability

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

func TestInitialMetricsExposeAllSeries(t *testing.T) {
	families := scrape(t, New())
	if len(families) != 11 {
		t.Fatalf("metric families = %d, want 11", len(families))
	}
	for _, name := range []string{
		"idempotent_replays_total", "outbox_published_total", "reconciliation_divergences_total",
		"storage_failures_total", "concurrency_conflicts_total", "outbox_oldest_pending_seconds", "dlq_messages",
	} {
		assertValue(t, families, name, "", "", 0)
	}
	for _, status := range []string{"processed", "rejected", "pending_reference", "failed", "pending"} {
		assertValue(t, families, "transactions_total", "status", status, 0)
	}
	for _, worker := range []string{"consumer_poll", "consumer", "ack", "outbox", "references"} {
		assertValue(t, families, "retries_total", "worker", worker, 0)
	}
	for _, kind := range []string{"identity", "inbox"} {
		assertValue(t, families, "conflicts_total", "kind", kind, 0)
	}
	histogram := metric(t, families, "processing_seconds", "", "").GetHistogram()
	if histogram == nil || histogram.GetSampleCount() != 0 || histogram.GetSampleSum() != 0 {
		t.Fatalf("initial histogram = %v", histogram)
	}
}

func TestResultsAndStorageErrors(t *testing.T) {
	m := New()
	for _, status := range []string{"PROCESSED", "REJECTED", "PENDING_REFERENCE", "FAILED", "PENDING"} {
		m.Result(status, false)
	}
	m.Result("PROCESSED", true)
	m.Result("provider-input", false)
	for _, state := range []string{"40001", "40P01", "55P03", "23505"} {
		m.StorageError(fmt.Errorf("persist operation: %w", databaseError{state: state}))
	}
	m.StorageError(nil)
	m.StorageError(errors.New("unavailable"))
	families := scrape(t, m)
	assertValue(t, families, "transactions_total", "status", "processed", 2)
	for _, status := range []string{"rejected", "pending_reference", "failed", "pending"} {
		assertValue(t, families, "transactions_total", "status", status, 1)
	}
	if got := len(families["wager_transactions_total"].Metric); got != 5 {
		t.Fatalf("transaction series = %d, want bounded set of 5", got)
	}
	assertValue(t, families, "idempotent_replays_total", "", "", 1)
	assertValue(t, families, "concurrency_conflicts_total", "", "", 3)
}

func TestWorkerCountersAndGauges(t *testing.T) {
	m := New()
	m.Retry("consumer")
	m.Conflict("inbox")
	m.OutboxPublished()
	m.ReconciliationDivergence()
	m.StorageFailure()
	m.OutboxLag(42)
	m.DLQDepth(7)
	m.DLQDepth(2)
	families := scrape(t, m)
	assertValue(t, families, "retries_total", "worker", "consumer", 1)
	assertValue(t, families, "conflicts_total", "kind", "inbox", 1)
	for _, name := range []string{"outbox_published_total", "reconciliation_divergences_total", "storage_failures_total"} {
		assertValue(t, families, name, "", "", 1)
	}
	assertValue(t, families, "outbox_oldest_pending_seconds", "", "", 42)
	assertValue(t, families, "dlq_messages", "", "", 2)
}

func TestMetricTypesAndBoundedWorkerLabels(t *testing.T) {
	m := New()
	m.Retry("provider-supplied-worker")
	m.Conflict("provider-supplied-conflict")
	families := scrape(t, m)
	for _, name := range []string{
		"transactions_total", "retries_total", "conflicts_total", "idempotent_replays_total",
		"outbox_published_total", "reconciliation_divergences_total", "storage_failures_total", "concurrency_conflicts_total",
	} {
		if got := families["wager_"+name].GetType(); got != dto.MetricType_COUNTER {
			t.Errorf("%s type = %s, want COUNTER", name, got)
		}
	}
	for _, name := range []string{"outbox_oldest_pending_seconds", "dlq_messages"} {
		if got := families["wager_"+name].GetType(); got != dto.MetricType_GAUGE {
			t.Errorf("%s type = %s, want GAUGE", name, got)
		}
	}
	if got := families["wager_processing_seconds"].GetType(); got != dto.MetricType_HISTOGRAM {
		t.Errorf("processing_seconds type = %s, want HISTOGRAM", got)
	}
	if got := len(families["wager_retries_total"].Metric); got != 5 {
		t.Errorf("retry series = %d, want 5", got)
	}
	if got := len(families["wager_conflicts_total"].Metric); got != 2 {
		t.Errorf("conflict series = %d, want 2", got)
	}
}

func TestProcessingHistogram(t *testing.T) {
	m := New()
	for _, duration := range []time.Duration{-time.Millisecond, time.Millisecond, 5 * time.Millisecond, 25 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second, 11 * time.Second} {
		m.ObserveProcessing(duration)
	}
	histogram := metric(t, scrape(t, m), "processing_seconds", "", "").GetHistogram()
	if histogram.GetSampleCount() != 7 || math.Abs(histogram.GetSampleSum()-13.531) > 1e-9 {
		t.Fatalf("histogram count = %d, sum = %v", histogram.GetSampleCount(), histogram.GetSampleSum())
	}
	expected := map[float64]uint64{0.001: 2, 0.005: 3, 0.01: 3, 0.05: 4, 0.1: 4, 1: 5, 10: 6, math.Inf(1): 7}
	for _, bucket := range histogram.Bucket {
		want, ok := expected[bucket.GetUpperBound()]
		if !ok || bucket.GetCumulativeCount() != want {
			t.Errorf("bucket %v = %d, want %d", bucket.GetUpperBound(), bucket.GetCumulativeCount(), want)
		}
		delete(expected, bucket.GetUpperBound())
	}
	if len(expected) != 0 {
		t.Errorf("missing buckets: %v", expected)
	}
}

func TestInstancesAreIndependentAndUpdatesAreConcurrent(t *testing.T) {
	first, second := New(), New()
	var workers sync.WaitGroup
	for range 20 {
		workers.Go(func() {
			for range 100 {
				first.Result("PROCESSED", true)
				first.ObserveProcessing(time.Millisecond)
			}
		})
	}
	workers.Wait()
	families := scrape(t, first)
	assertValue(t, families, "transactions_total", "status", "processed", 2000)
	assertValue(t, families, "idempotent_replays_total", "", "", 2000)
	if count := metric(t, families, "processing_seconds", "", "").GetHistogram().GetSampleCount(); count != 2000 {
		t.Fatalf("histogram count = %d, want 2000", count)
	}
	assertValue(t, scrape(t, second), "transactions_total", "status", "processed", 0)
}

type databaseError struct{ state string }

func (e databaseError) Error() string    { return "database error" }
func (e databaseError) SQLState() string { return e.state }

func scrape(t *testing.T, m *Metrics) map[string]*dto.MetricFamily {
	t.Helper()
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	request.Header.Set("Accept", "text/plain; version=0.0.4")
	m.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("scrape status = %d: %s", response.Code, response.Body.String())
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(response.Body)
	if err != nil {
		t.Fatalf("parse Prometheus exposition: %v", err)
	}
	return families
}

func metric(t *testing.T, families map[string]*dto.MetricFamily, name, label, value string) *dto.Metric {
	t.Helper()
	family := families["wager_"+name]
	if family == nil {
		t.Fatalf("missing metric family %s", name)
	}
	for _, sample := range family.Metric {
		if label == "" && len(sample.Label) == 0 {
			return sample
		}
		for _, pair := range sample.Label {
			if pair.GetName() == label && pair.GetValue() == value {
				return sample
			}
		}
	}
	t.Fatalf("missing metric %s{%s=%q}", name, label, value)
	return nil
}

func assertValue(t *testing.T, families map[string]*dto.MetricFamily, name, label, value string, want float64) {
	t.Helper()
	sample := metric(t, families, name, label, value)
	var got float64
	switch {
	case sample.Counter != nil:
		got = sample.Counter.GetValue()
	case sample.Gauge != nil:
		got = sample.Gauge.GetValue()
	default:
		t.Fatalf("metric %s has no numeric sample", name)
	}
	if got != want {
		t.Errorf("%s{%s=%q} = %v, want %v", name, label, value, got, want)
	}
}
