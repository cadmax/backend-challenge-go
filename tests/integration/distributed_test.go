//go:build integration

package integration_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Each subtest goes through real HTTP, Keycloak, PostgreSQL and SQS. Three
// separately exec'd, race-instrumented application processes share only durable
// infrastructure. Nothing in the test coordinates their financial operations.
func TestDistributedSystem(t *testing.T) {
	s := newSuite(t)
	run := func(name string, test func(*testing.T)) {
		t.Run(name, func(t *testing.T) { s.t = t; test(t) })
		s.t = t
	}

	run("real_OIDC_and_provider_isolation", func(t *testing.T) {
		w := s.open("100.00")
		c := operation(w, "BET", "10.00")
		for _, token := range []string{"", "not-a-jwt"} {
			s.call(s.processes[0], "POST", "/wagering/transactions", token, c.ExternalTransactionID, c, http.StatusUnauthorized, nil)
		}
		expired := s.token("provider-expired")
		parts := strings.Split(expired, ".")
		claims, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			t.Fatal(err)
		}
		var expiry struct {
			Exp int64 `json:"exp"`
		}
		if err := json.Unmarshal(claims, &expiry); err != nil {
			t.Fatal(err)
		}
		if delay := time.Until(time.Unix(expiry.Exp, 0)); delay > 10*time.Second {
			t.Fatalf("provider-expired must issue short-lived real tokens, expires in %s", delay)
		}
		eventually(t, 12*time.Second, "real token expires", func() bool { return time.Now().Unix() > expiry.Exp })
		s.call(s.processes[1], "POST", "/wagering/transactions", expired, c.ExternalTransactionID, c, http.StatusUnauthorized, nil)
		s.call(s.processes[0], "POST", "/wagering/transactions", s.token("provider-b"), c.ExternalTransactionID, c, http.StatusForbidden, nil)
		s.call(s.processes[0], "POST", "/wallets", s.token("provider-a"), "", map[string]any{"playerId": uniqueID(), "initialBalance": money{"100.00", "BRL"}}, http.StatusForbidden, nil)
		for _, path := range []string{"/wallets/" + w.ID, "/wallets/" + w.ID + "/ledger"} {
			s.call(s.processes[1], "GET", path, s.token("provider-a"), "", nil, http.StatusForbidden, nil)
		}
		s.call(s.processes[2], "POST", "/wallets/"+w.ID+"/reconciliation", s.token("provider-a"), "", nil, http.StatusForbidden, nil)
		s.balance(w, "100.00", 1)
		first := s.submit(s.processes[0], c, c.ExternalTransactionID, http.StatusOK)
		s.call(s.processes[1], "GET", "/wagering/transactions/"+first.TransactionID, s.token("provider-b"), "", nil, http.StatusNotFound, nil)
		s.call(s.processes[2], "GET", "/providers/provider-a/wagering/transactions/"+c.ExternalTransactionID, s.token("provider-b"), "", nil, http.StatusForbidden, nil)
		s.call(s.processes[1], "POST", "/wagering/transactions", s.token("provider-b"), c.ExternalTransactionID, c, http.StatusForbidden, nil)
		s.balance(w, "90.00", 2)
	})

	run("50_concurrent_replays_have_one_debit_and_historical_balance", func(t *testing.T) {
		w := s.open("100.00")
		c := operation(w, "BET", "25.00")
		responses := concurrentRequests(s, 50, func(int) command { return c }, func(int) string { return "duplicate-" + c.ExternalTransactionID })
		var transactionID string
		fresh := 0
		for _, item := range responses {
			if item.err != nil || item.response.Code != http.StatusOK {
				t.Fatalf("duplicate request: %v %s", item.err, item.response)
			}
			var out result
			if err := json.Unmarshal(item.response.Body, &out); err != nil {
				t.Fatal(err)
			}
			if out.Status != "PROCESSED" || out.Balance == nil || out.Balance.Amount != "75.00" {
				t.Fatalf("unexpected duplicate result: %+v", out)
			}
			if transactionID == "" {
				transactionID = out.TransactionID
			}
			if out.TransactionID != transactionID {
				t.Fatal("duplicate changed transaction identity")
			}
			if !out.Replay {
				fresh++
			}
		}
		if fresh != 1 {
			t.Fatalf("non-replay responses=%d, want 1", fresh)
		}
		s.balance(w, "75.00", 2)
		win := operation(w, "WIN", "10.00")
		s.submit(s.processes[1], win, win.ExternalTransactionID, http.StatusOK)
		replay := s.submit(s.processes[2], c, "another-key-"+c.ExternalTransactionID, http.StatusOK)
		if !replay.Replay || replay.Balance.Amount != "75.00" {
			t.Fatalf("replay must retain historical balance: %+v", replay)
		}
		changed := c
		changed.Money.Amount = "26.00"
		s.submit(s.processes[0], changed, "duplicate-"+c.ExternalTransactionID, http.StatusConflict)
		s.submit(s.processes[1], changed, "third-key-"+c.ExternalTransactionID, http.StatusConflict)
		changed.ExternalTransactionID = uniqueID()
		s.submit(s.processes[2], changed, "another-key-"+c.ExternalTransactionID, http.StatusConflict)
		s.balance(w, "85.00", 3)
	})

	run("two_80_bets_against_100_across_processes", func(t *testing.T) {
		w := s.open("100.00")
		commands := []command{operation(w, "BET", "80.00"), operation(w, "BET", "80.00")}
		responses := concurrentRequests(s, 2, func(i int) command { return commands[i] }, func(i int) string { return commands[i].ExternalTransactionID })
		processed, rejected := 0, 0
		for _, item := range responses {
			if item.err != nil {
				t.Fatal(item.err)
			}
			var out result
			if err := json.Unmarshal(item.response.Body, &out); err != nil {
				t.Fatal(err)
			}
			switch out.Status {
			case "PROCESSED":
				processed++
				if item.response.Code != 200 {
					t.Fatal(item.response)
				}
			case "REJECTED":
				rejected++
				if item.response.Code != 422 || out.FailureCode == "" {
					t.Fatal(item.response)
				}
			default:
				t.Fatal(item.response)
			}
		}
		if processed != 1 || rejected != 1 {
			t.Fatalf("processed=%d rejected=%d", processed, rejected)
		}
		for _, c := range commands {
			r, err := s.request(s.processes[2], "POST", "/wagering/transactions", s.token("provider-a"), c.ExternalTransactionID, c)
			if err != nil || (r.Code != 200 && r.Code != 422) {
				t.Fatalf("replay: %v %s", err, r)
			}
			var out result
			_ = json.Unmarshal(r.Body, &out)
			if !out.Replay {
				t.Fatal("terminal replay was reprocessed")
			}
		}
		s.balance(w, "20.00", 2)
		if count := s.sqlCount("SELECT count(*) FROM wallet_ledger WHERE wallet_id=$1 AND direction='DEBIT'", w.ID); count != 1 {
			t.Fatalf("debits=%d", count)
		}
	})

	run("locked_wallet_does_not_block_independent_wallet", func(t *testing.T) {
		blocked, free := s.open("100.00"), s.open("100.00")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		if _, err := tx.Exec(ctx, "SELECT id FROM wallets WHERE id=$1 FOR UPDATE", blocked.ID); err != nil {
			t.Fatal(err)
		}
		c := operation(blocked, "BET", "10.00")
		done := make(chan requestResult, 1)
		go func() {
			response, err := s.request(s.processes[0], "POST", "/wagering/transactions", s.tokens["provider-a"], c.ExternalTransactionID, c)
			done <- requestResult{response, err}
		}()
		eventually(t, 2*time.Second, "first operation waits on its wallet row lock", func() bool {
			return s.sqlCount("SELECT count(*) FROM pg_stat_activity WHERE $1::integer=ANY(pg_blocking_pids(pid))", int(tx.Conn().PgConn().PID())) > 0
		})
		// The lock is held by a separate database connection; the other wallet
		// must commit before we release it, without relying on timing thresholds.
		other := operation(free, "BET", "10.00")
		s.submit(s.processes[1], other, other.ExternalTransactionID, http.StatusOK)
		select {
		case early := <-done:
			t.Fatalf("blocked wallet finished before lock release: %v %s", early.err, early.response)
		default:
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		finished := <-done
		if finished.err != nil || finished.response.Code != 200 {
			t.Fatalf("released wallet: %v %s", finished.err, finished.response)
		}
		s.balance(blocked, "90.00", 2)
		s.balance(free, "90.00", 2)
	})

	run("five_kinds_references_reversals_and_pagination", func(t *testing.T) {
		w := s.open("100.00")
		bet := operation(w, "BET", "20.00")
		s.submit(s.processes[0], bet, bet.ExternalTransactionID, 200)
		win := operation(w, "WIN", "10.00")
		win.Reference = bet.ExternalTransactionID
		s.submit(s.processes[1], win, win.ExternalTransactionID, 200)
		loss := operation(w, "LOSS", "0.00")
		s.submit(s.processes[2], loss, loss.ExternalTransactionID, 200)
		refund := operation(w, "REFUND", "20.00")
		refund.Reference = bet.ExternalTransactionID
		s.submit(s.processes[0], refund, refund.ExternalTransactionID, 200)
		duplicate := operation(w, "ROLLBACK", "20.00")
		duplicate.Reference = bet.ExternalTransactionID
		rejected := s.submit(s.processes[1], duplicate, duplicate.ExternalTransactionID, 422)
		if rejected.FailureCode == "" {
			t.Fatal("duplicate reversal must have stable reason")
		}
		rollbackWin := operation(w, "ROLLBACK", "10.00")
		rollbackWin.Reference = win.ExternalTransactionID
		s.submit(s.processes[2], rollbackWin, rollbackWin.ExternalTransactionID, 200)
		rollbackRefund := operation(w, "ROLLBACK", "20.00")
		rollbackRefund.Reference = refund.ExternalTransactionID
		s.submit(s.processes[0], rollbackRefund, rollbackRefund.ExternalTransactionID, 200)
		secondRefund := operation(w, "REFUND", "20.00")
		secondRefund.Reference = bet.ExternalTransactionID
		s.submit(s.processes[1], secondRefund, secondRefund.ExternalTransactionID, 422)
		wrongRound := operation(w, "WIN", "1.00")
		wrongRound.Reference, wrongRound.RoundID = bet.ExternalTransactionID, "other-round"
		s.submit(s.processes[2], wrongRound, wrongRound.ExternalTransactionID, 422)
		s.balance(w, "80.00", 6)
		seen := map[string]bool{}
		cursor := ""
		for {
			var page struct {
				Entries []ledgerEntry `json:"entries"`
				Next    string        `json:"nextCursor"`
			}
			s.call(s.processes[0], "GET", "/wallets/"+w.ID+"/ledger?limit=2&cursor="+cursor, s.token("internal-service"), "", nil, 200, &page)
			if len(page.Entries) > 2 {
				t.Fatal("ledger page limit ignored")
			}
			for _, entry := range page.Entries {
				if seen[entry.ID] {
					t.Fatal("ledger pagination duplicated entry")
				}
				seen[entry.ID] = true
			}
			if page.Next == "" {
				break
			}
			if page.Next == cursor {
				t.Fatal("ledger cursor did not advance")
			}
			cursor = page.Next
		}
		if len(seen) != 6 {
			t.Fatalf("paged %d entries, want 6", len(seen))
		}
		var latest wallet
		s.call(s.processes[0], "GET", "/wallets/"+w.ID, s.token("internal-service"), "", nil, 200, &latest)
		if latest.Version != 6 {
			t.Fatalf("LOSS or rejection changed wallet version: %d", latest.Version)
		}
	})

	run("HTTP_SQS_replays_reach_durable_inbox", func(t *testing.T) {
		w := s.open("100.00")
		c := operation(w, "BET", "25.00")
		key := c.ExternalTransactionID
		first := s.submit(s.processes[0], c, key, 200)
		c.IdempotencyKey = key
		ids := []string{uniqueID(), uniqueID(), uniqueID()}
		for _, id := range ids {
			s.enqueue(c, id)
		}
		// Same durable envelope ID, new SQS transport dedup ID: broker FIFO
		// deduplication cannot hide an application-level duplicate here.
		s.enqueue(c, ids[0])
		eventually(t, 20*time.Second, "three distinct deliveries durably completed", func() bool {
			return s.sqlCount("SELECT count(*) FROM inbox WHERE message_id=ANY($1) AND completed_at IS NOT NULL", ids) == len(ids)
		})
		eventually(t, 20*time.Second, "replayed SQS messages removed", s.emptyQueue)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		dlq, err := s.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(s.dlqURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages}})
		if err != nil {
			t.Fatal(err)
		}
		if dlq.Attributes["ApproximateNumberOfMessages"] != "0" {
			t.Fatal("valid application replays were discarded to the DLQ")
		}
		replay := s.submit(s.processes[2], c, key, 200)
		if !replay.Replay || replay.TransactionID != first.TransactionID {
			t.Fatalf("cross transport changed identity: %+v", replay)
		}
		s.balance(w, "75.00", 2)
		fromQueue := operation(w, "WIN", "5.00")
		fromQueue.IdempotencyKey = fromQueue.ExternalTransactionID
		s.enqueue(fromQueue, uniqueID())
		s.awaitStatus(fromQueue, "PROCESSED")
		if replay := s.submit(s.processes[1], fromQueue, fromQueue.IdempotencyKey, 200); !replay.Replay {
			t.Fatal("SQS to HTTP replay not recognized")
		}
		s.balance(w, "80.00", 3)
	})

	run("zero_opening_and_rejections_preserve_financial_state", func(t *testing.T) {
		w := s.open("0.00")
		s.balance(w, "0.00", 0)
		if count := s.sqlCount("SELECT count(*) FROM wager_transactions WHERE wallet_id=$1 AND kind='OPENING'", w.ID); count != 0 {
			t.Fatalf("zero opening created %d financial transactions", count)
		}
		s.call(s.processes[0], "POST", "/wallets", s.token("internal-service"), "", map[string]any{"playerId": w.PlayerID, "initialBalance": money{"10.00", "BRL"}}, 409, nil)
		win := operation(w, "WIN", "20.00")
		s.submit(s.processes[0], win, win.ExternalTransactionID, 200)
		bet := operation(w, "BET", "20.00")
		s.submit(s.processes[1], bet, bet.ExternalTransactionID, 200)
		rollback := operation(w, "ROLLBACK", "20.00")
		rollback.Reference = win.ExternalTransactionID
		reversalRejected := s.submit(s.processes[2], rollback, rollback.ExternalTransactionID, 422)
		tooLarge := operation(w, "BET", "1.00")
		betRejected := s.submit(s.processes[0], tooLarge, tooLarge.ExternalTransactionID, 422)
		if reversalRejected.FailureCode == "" || reversalRejected.FailureCode == betRejected.FailureCode {
			t.Fatalf("reversal and bet insufficient funds must differ: %q versus %q", reversalRejected.FailureCode, betRejected.FailureCode)
		}
		partial := operation(w, "REFUND", "1.00")
		partial.Reference = bet.ExternalTransactionID
		s.submit(s.processes[1], partial, partial.ExternalTransactionID, 422)
		wrongCurrency := operation(w, "LOSS", "0.00")
		wrongCurrency.Money.Currency = "USD"
		s.submit(s.processes[2], wrongCurrency, wrongCurrency.ExternalTransactionID, 422)
		for _, kind := range []string{"OPENING", "LOSS"} {
			invalid := operation(w, kind, "1.00")
			s.submit(s.processes[0], invalid, invalid.ExternalTransactionID, 400)
		}
		other := s.open("0.00")
		crossWallet := operation(other, "REFUND", "20.00")
		crossWallet.Reference = bet.ExternalTransactionID
		s.submit(s.processes[1], crossWallet, crossWallet.ExternalTransactionID, 422)
		s.balance(w, "0.00", 2)
		s.balance(other, "0.00", 0)
	})

	run("references_survive_all_processes_restarting_and_expire", func(t *testing.T) {
		w := s.open("100.00")
		bet := operation(w, "BET", "20.00")
		refund := operation(w, "REFUND", "20.00")
		refund.Reference = bet.ExternalTransactionID
		pending := s.submit(s.processes[0], refund, refund.ExternalTransactionID, 202)
		if pending.Status != "PENDING_REFERENCE" {
			t.Fatalf("status=%s", pending.Status)
		}
		s.stopAll()
		s.startThree()
		s.submit(s.processes[1], bet, bet.ExternalTransactionID, 200)
		resolved := s.awaitStatus(refund, "PROCESSED")
		if resolved.TransactionID != pending.TransactionID {
			t.Fatal("restart lost pending identity")
		}
		s.balance(w, "100.00", 3)
		replay := s.submit(s.processes[2], bet, bet.ExternalTransactionID, 200)
		if !replay.Replay || replay.Balance.Amount != "80.00" {
			t.Fatalf("restart lost original result: %+v", replay)
		}
		missing := operation(w, "ROLLBACK", "3.00")
		missing.Reference = uniqueID()
		s.submit(s.processes[0], missing, missing.ExternalTransactionID, 202)
		rejected := s.awaitStatus(missing, "REJECTED")
		if rejected.FailureCode == "" {
			t.Fatal("expired reference has no rejection code")
		}
		s.balance(w, "100.00", 3)
	})

	run("poison_message_retries_then_reaches_real_DLQ", func(t *testing.T) {
		poison := `{"messageId":"` + uniqueID() + `","type":"UnsupportedMessageType"}`
		s.send(poison, uniqueID())
		eventually(t, 35*time.Second, "poison body in DLQ after broker redrive", func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			out, err := s.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(s.dlqURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
			if err != nil {
				return false
			}
			for _, message := range out.Messages {
				if aws.ToString(message.Body) == poison {
					return true
				}
			}
			return false
		})
	})

	run("committed_inbox_is_safe_after_consumer_crash", func(t *testing.T) { s.testConsumerCrash(t) })
	run("outbox_recovers_both_crash_windows_with_stable_event_ID", func(t *testing.T) { s.testOutboxCrash(t) })
}

type requestResult struct {
	response response
	err      error
}

func concurrentRequests(s *suite, count int, commandAt func(int) command, keyAt func(int) string) []requestResult {
	start := make(chan struct{})
	responses := make([]requestResult, count)
	var wg sync.WaitGroup
	token := s.token("provider-a")
	for i := range count {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			responses[i].response, responses[i].err = s.request(s.processes[i%len(s.processes)], "POST", "/wagering/transactions", token, keyAt(i), commandAt(i))
		}(i)
	}
	close(start)
	wg.Wait()
	return responses
}

func (s *suite) testConsumerCrash(t *testing.T) {
	eventually(t, 20*time.Second, "clean input queue before crash", s.emptyQueue)
	w := s.open("100.00")
	c := operation(w, "BET", "25.00")
	c.IdempotencyKey = c.ExternalTransactionID
	messageID := uniqueID()
	s.stopAll()
	p := s.start(map[string]string{"ENABLE_TEST_FAILPOINTS": "true", "TEST_FAILPOINT": "after_inbox_commit", "PUBLISHER_ENABLED": "false", "REFERENCE_WORKER_ENABLED": "false"})
	s.enqueue(c, messageID)
	select {
	case <-p.done:
		if p.cmd.ProcessState.ExitCode() != 86 {
			t.Fatalf("consumer exited outside the injected failure: %v", p.err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer did not reach commit-before-delete crash window")
	}
	if count := s.sqlCount("SELECT count(*) FROM inbox WHERE message_id=$1 AND completed_at IS NOT NULL", messageID); count != 1 {
		t.Fatalf("crash must happen after durable inbox commit, count=%d", count)
	}
	s.startThree()
	eventually(t, 20*time.Second, "post-crash redelivery is acknowledged", s.emptyQueue)
	if count := s.sqlCount("SELECT count(*) FROM inbox WHERE message_id=$1", messageID); count != 1 {
		t.Fatalf("inbox duplicated after crash, count=%d", count)
	}
	if replay := s.submit(s.processes[2], c, c.IdempotencyKey, 200); !replay.Replay {
		t.Fatal("crashed consumer lost committed result")
	}
	s.balance(w, "75.00", 2)
}

func (s *suite) testOutboxCrash(t *testing.T) {
	eventually(t, 20*time.Second, "existing outbox fully published", func() bool { return s.sqlCount("SELECT count(*) FROM outbox WHERE published_at IS NULL") == 0 })
	s.stopAll()
	api := s.start(map[string]string{"CONSUMER_ENABLED": "false", "PUBLISHER_ENABLED": "false", "REFERENCE_WORKER_ENABLED": "false"})
	s.processes = []*process{api}
	w := s.open("100.00")
	if count := s.sqlCount("SELECT count(*) FROM outbox WHERE aggregate_id=$1 AND published_at IS NULL", w.ID); count != 2 {
		t.Fatalf("financial commit must leave two unpublished events, count=%d", count)
	}
	// Kill the only API after commit: neither event has had a publisher yet.
	api.expectCrash = true
	_ = api.cmd.Process.Kill()
	<-api.done
	publisher := s.start(map[string]string{"CONSUMER_ENABLED": "false", "REFERENCE_WORKER_ENABLED": "false", "ENABLE_TEST_FAILPOINTS": "true", "TEST_FAILPOINT": "after_outbox_send"})
	select {
	case <-publisher.done:
		if publisher.cmd.ProcessState.ExitCode() != 86 {
			t.Fatalf("publisher exited outside the injected failure: %v", publisher.err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("publisher did not reach send-before-mark crash window")
	}
	if count := s.sqlCount("SELECT count(*) FROM outbox WHERE aggregate_id=$1 AND published_at IS NULL", w.ID); count != 2 {
		t.Fatalf("send must not mark an uncommitted publication, pending=%d", count)
	}
	s.startThree()
	eventually(t, 20*time.Second, "other publishers assume abandoned events", func() bool {
		return s.sqlCount("SELECT count(*) FROM outbox WHERE aggregate_id=$1 AND published_at IS NULL", w.ID) == 0
	})
	s.balance(w, "100.00", 1)
	seen := map[string]string{}
	eventually(t, 25*time.Second, "committed event envelopes reach SQS", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		out, err := s.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(s.eventURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		if err != nil {
			return false
		}
		for _, message := range out.Messages {
			var event struct {
				EventID       string          `json:"eventId"`
				EventType     string          `json:"eventType"`
				AggregateID   string          `json:"aggregateId"`
				CorrelationID string          `json:"correlationId"`
				Version       int             `json:"version"`
				OccurredAt    time.Time       `json:"occurredAt"`
				Data          json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal([]byte(aws.ToString(message.Body)), &event); err != nil {
				t.Fatalf("invalid event envelope: %v", err)
			}
			if event.AggregateID == w.ID {
				if event.EventID == "" || event.CorrelationID == "" || event.Version != 1 || event.OccurredAt.IsZero() || len(event.Data) == 0 {
					t.Fatalf("incomplete event: %+v", event)
				}
				if count := s.sqlCount("SELECT count(*) FROM outbox WHERE event_id=$1 AND aggregate_id=$2", event.EventID, w.ID); count != 1 {
					t.Fatal("publisher changed durable event ID")
				}
				seen[event.EventID] = event.EventType
			}
			_, err := s.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(s.eventURL), ReceiptHandle: message.ReceiptHandle})
			if err != nil {
				t.Fatal(err)
			}
		}
		return len(seen) == 2
	})
	types := map[string]int{}
	for _, kind := range seen {
		types[kind]++
	}
	if types["WagerTransactionProcessed"] != 1 || types["WalletBalanceChanged"] != 1 {
		t.Fatal(fmt.Sprintf("unexpected opening events: %v", types))
	}
}
