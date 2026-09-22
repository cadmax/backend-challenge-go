//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5"
)

func TestRuntimeOutages(t *testing.T) {
	s := newSuite(t)
	t.Run("PostgreSQL_disconnect_rolls_back_and_same_key_recovers", func(t *testing.T) {
		s.t = t
		s.testDatabaseOutage(t)
	})
	s.t = t
	t.Run("SQS_outage_preserves_outbox_and_unconsumed_requests", func(t *testing.T) {
		s.t = t
		s.testBrokerOutage(t)
	})
	s.t = t
}

func (s *suite) testDatabaseOutage(t *testing.T) {
	w := s.open("100.00")
	c := operation(w, "BET", "25.00")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// A runtime-role connection can terminate other sessions of its own role.
	// Both the privilege change and the termination are limited to this suite's
	// disposable database; no shared service or unrelated database is stopped.
	runtimeConnection, err := pgx.Connect(ctx, s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer runtimeConnection.Close(context.Background())
	var databaseName string
	if err := s.pool.QueryRow(ctx, "SELECT current_database()").Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	runtimeRole := pgx.Identifier{envOr("INTEGRATION_RUNTIME_DB_USER", "wager")}.Sanitize()
	database := pgx.Identifier{databaseName}.Sanitize()
	restored := false
	restore := func() {
		if restored {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := s.pool.Exec(ctx, "GRANT CONNECT ON DATABASE "+database+" TO PUBLIC, "+runtimeRole); err != nil {
			t.Errorf("restore isolated database connectivity: %v", err)
			return
		}
		restored = true
	}
	defer restore()
	if _, err := s.pool.Exec(ctx, "REVOKE CONNECT ON DATABASE "+database+" FROM PUBLIC, "+runtimeRole); err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeConnection.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE datname=current_database() AND usename=current_user AND pid<>pg_backend_pid()`); err != nil {
		t.Fatal(err)
	}
	if err := runtimeConnection.Close(ctx); err != nil {
		t.Fatal(err)
	}
	s.call(s.processes[0], "GET", "/health/ready", "", "", nil, http.StatusServiceUnavailable, nil)
	s.call(s.processes[0], "GET", "/health/live", "", "", nil, http.StatusOK, nil)
	s.submit(s.processes[1], c, c.ExternalTransactionID, http.StatusServiceUnavailable)
	if count := s.sqlCount("SELECT count(*) FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2", c.ProviderID, c.ExternalTransactionID); count != 0 {
		t.Fatalf("unavailable database retained %d partially accepted operations", count)
	}
	if count := s.sqlCount("SELECT count(*) FROM wallets WHERE id=$1 AND balance=10000 AND version=1", w.ID); count != 1 {
		t.Fatal("database outage changed the financial state")
	}
	restore()
	for _, p := range s.processes {
		eventually(t, 15*time.Second, "application reconnects after PostgreSQL recovery", func() bool {
			r, err := s.request(p, "GET", "/health/ready", "", "", nil)
			return err == nil && r.Code == http.StatusOK
		})
	}
	processed := s.submit(s.processes[1], c, c.ExternalTransactionID, http.StatusOK)
	replay := s.submit(s.processes[2], c, c.ExternalTransactionID, http.StatusOK)
	if !replay.Replay || replay.TransactionID != processed.TransactionID {
		t.Fatalf("retry after PostgreSQL recovery changed identity: %+v", replay)
	}
	s.balance(w, "75.00", 2)
}

func (s *suite) testBrokerOutage(t *testing.T) {
	s.stopAll()
	target, err := url.Parse(envOr("INTEGRATION_SQS_ENDPOINT", "http://localhost:4567"))
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	var unavailable atomic.Bool
	var sent, received, blockedSends atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		operation := r.Header.Get("X-Amz-Target")
		if unavailable.Load() {
			if strings.HasSuffix(operation, ".SendMessage") {
				blockedSends.Add(1)
			}
			w.Header().Set("Content-Type", "application/x-amz-json-1.0")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"__type":"ServiceUnavailable","message":"temporary integration network outage"}`))
			return
		}
		if strings.HasSuffix(operation, ".SendMessage") {
			sent.Add(1)
		}
		if strings.HasSuffix(operation, ".ReceiveMessage") {
			received.Add(1)
		}
		forward.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	defer s.stopAll()
	p := s.start(map[string]string{"SQS_ENDPOINT": proxy.URL, "REFERENCE_WORKER_ENABLED": "false"})
	s.processes = []*process{p}
	w := s.open("100.00")
	eventually(t, 10*time.Second, "real broker traffic passes through the outage proxy", func() bool {
		return sent.Load() >= 2 && received.Load() >= 1 && s.sqlCount("SELECT count(*) FROM outbox WHERE aggregate_id=$1 AND published_at IS NULL", w.ID) == 0
	})
	unavailable.Store(true)
	s.call(p, "GET", "/health/ready", "", "", nil, http.StatusServiceUnavailable, nil)
	s.call(p, "GET", "/health/live", "", "", nil, http.StatusOK, nil)
	bet := operation(w, "BET", "25.00")
	s.submit(p, bet, bet.ExternalTransactionID, http.StatusOK)
	s.balance(w, "75.00", 2)
	eventually(t, 10*time.Second, "failed sends retain events and durable retry metadata", func() bool {
		return blockedSends.Load() > 0 && s.sqlCount("SELECT count(*) FROM outbox WHERE aggregate_id=$1 AND published_at IS NULL AND attempts>0", w.ID) == 2
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := s.pool.Query(ctx, "SELECT event_id::text FROM outbox WHERE aggregate_id=$1 AND published_at IS NULL", w.ID)
	if err != nil {
		t.Fatal(err)
	}
	eventIDs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	// The producer still reaches the real broker directly. The application's
	// failed network path must leave this request available for later delivery.
	win := operation(w, "WIN", "5.00")
	win.IdempotencyKey = win.ExternalTransactionID
	messageID := uniqueID()
	s.enqueue(win, messageID)
	if count := s.sqlCount("SELECT count(*) FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2", win.ProviderID, win.ExternalTransactionID); count != 0 {
		t.Fatal("consumer bypassed the failing broker endpoint")
	}
	unavailable.Store(false)
	s.awaitStatus(win, "PROCESSED")
	eventually(t, 15*time.Second, "outbox retry publishes each original event after recovery", func() bool {
		return s.sqlCount("SELECT count(*) FROM outbox WHERE event_id::text=ANY($1) AND published_at IS NOT NULL AND attempts>=2", eventIDs) == len(eventIDs)
	})
	eventually(t, 10*time.Second, "queued request is durably acknowledged after recovery", s.emptyQueue)
	if count := s.sqlCount("SELECT count(*) FROM inbox WHERE message_id=$1 AND completed_at IS NOT NULL", messageID); count != 1 {
		t.Fatal("recovered consumer did not complete the durable inbox")
	}
	s.call(p, "GET", "/health/ready", "", "", nil, http.StatusOK, nil)
	if replay := s.submit(p, bet, bet.ExternalTransactionID, 200); !replay.Replay || replay.Balance.Amount != "75.00" {
		t.Fatalf("broker recovery lost historical idempotency: %+v", replay)
	}
	s.balance(w, "80.00", 3)
	wanted, delivered := map[string]bool{}, map[string]bool{}
	for _, id := range eventIDs {
		wanted[id] = true
	}
	eventually(t, 15*time.Second, "original event IDs are observable in real SQS", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		out, err := s.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(s.eventURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		if err != nil {
			return false
		}
		for _, message := range out.Messages {
			var event struct {
				ID string `json:"eventId"`
			}
			if err := json.Unmarshal([]byte(aws.ToString(message.Body)), &event); err != nil {
				t.Fatal(err)
			}
			if wanted[event.ID] {
				delivered[event.ID] = true
			}
			if _, err := s.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(s.eventURL), ReceiptHandle: message.ReceiptHandle}); err != nil {
				t.Fatal(err)
			}
		}
		return len(delivered) == len(wanted)
	})
}
