package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/cadmax/backend-challenge-go/internal/application"
	"github.com/cadmax/backend-challenge-go/internal/config"
	"github.com/cadmax/backend-challenge-go/internal/observability"
	"github.com/jackc/pgx/v5"
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

func NewWorkers(c config.Config, q *Queue, pool *pgxpool.Pool, service Processor, log *slog.Logger, m *observability.Metrics) *Workers {
	return &Workers{cfg: c, queue: q, pool: pool, service: service, log: log, metrics: m}
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
	go func() { wg.Wait(); close(w.done) }()
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

func (w *Workers) consume(pollCtx, workCtx context.Context) {
	for pollCtx.Err() == nil {
		out, err := w.queue.Client.ReceiveMessage(pollCtx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(w.queue.InputURL), MaxNumberOfMessages: 1, WaitTimeSeconds: w.cfg.SQSWaitSeconds,
			VisibilityTimeout: w.cfg.SQSVisibilitySeconds, MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
		})
		if err != nil {
			if pollCtx.Err() != nil {
				return
			}
			w.metrics.Inc("retries_total{worker=\"consumer_poll\"}")
			w.log.Warn("SQS receive failed", "error", err)
			if !wait(pollCtx, time.Second) {
				return
			}
			continue
		}
		for _, message := range out.Messages {
			if pollCtx.Err() != nil {
				w.release(message)
				continue
			}
			w.handleMessage(workCtx, message)
		}
	}
}

func (w *Workers) handleMessage(workCtx context.Context, message types.Message) {
	start := time.Now()
	defer func() { w.metrics.ObserveProcessing(time.Since(start)) }()
	envelope, err := decodeEnvelope(aws.ToString(message.Body))
	var result application.Result
	if err == nil {
		ctx, cancel := context.WithTimeout(workCtx, w.cfg.ProcessTimeout)
		result, err = w.service.ProcessInbox(ctx, envelope, application.Metadata{CorrelationID: envelope.MessageID, CausationID: envelope.MessageID})
		cancel()
	}
	if err != nil {
		attempt, _ := strconv.Atoi(message.Attributes["ApproximateReceiveCount"])
		w.metrics.StorageError(err)
		w.metrics.Inc("retries_total{worker=\"consumer\"}")
		if errors.Is(err, application.ErrConflict) {
			w.metrics.Inc("conflicts_total{kind=\"inbox\"}")
		}
		// Invalid envelopes are never acknowledged: the broker's redrive policy
		// retains the original bytes for diagnosis in the DLQ.
		w.log.Warn("message deferred", "messageId", envelope.MessageID, "providerId", envelope.Data.ProviderID, "walletId", envelope.Data.WalletID, "attempt", attempt, "error", err)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		delay := int32(backoff(attempt) / time.Second)
		if workCtx.Err() != nil {
			delay = 0
		}
		_, visibilityErr := w.queue.Client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(w.queue.InputURL), ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: delay})
		if visibilityErr != nil {
			w.log.Warn("cannot change message visibility", "messageId", envelope.MessageID, "error", visibilityErr)
		}
		return
	}
	w.metrics.Result(result.Status, result.IdempotentReplay)
	w.log.Info("message committed", "messageId", envelope.MessageID, "correlationId", envelope.MessageID, "transactionId", result.TransactionID, "providerId", envelope.Data.ProviderID, "walletId", envelope.Data.WalletID, "status", result.Status, "replay", result.IdempotentReplay)
	w.failpoint("after_inbox_commit")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err = w.queue.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(w.queue.InputURL), ReceiptHandle: message.ReceiptHandle}); err != nil {
		w.metrics.Inc("retries_total{worker=\"ack\"}")
		w.log.Warn("message committed but acknowledgement failed", "messageId", envelope.MessageID, "error", err)
	}
}

func (w *Workers) release(message types.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := w.queue.Client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(w.queue.InputURL), ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: 0})
	if err != nil {
		w.log.Warn("cannot release message", "error", err)
	}
}

func decodeEnvelope(body string) (application.Envelope, error) {
	var envelope application.Envelope
	if len(body) > 256*1024 {
		return envelope, errors.New("message exceeds 256 KiB")
	}
	decoder := json.NewDecoder(bytes.NewBufferString(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return envelope, fmt.Errorf("invalid message envelope: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return envelope, errors.New("message must contain one JSON object")
	}
	if envelope.MessageID == "" || len(envelope.MessageID) > 200 || envelope.OccurredAt.IsZero() || envelope.Type != "WagerTransactionRequested" || envelope.Data.IdempotencyKey == "" {
		return envelope, errors.New("missing or invalid message metadata")
	}
	return envelope, nil
}

func (w *Workers) publish(pollCtx, workCtx context.Context) {
	for pollCtx.Err() == nil {
		ctx, cancel := context.WithTimeout(workCtx, w.cfg.ProcessTimeout)
		found, err := w.publishOne(ctx)
		cancel()
		if err != nil {
			w.log.Warn("outbox publication deferred", "error", err)
			w.metrics.Inc("retries_total{worker=\"outbox\"}")
		}
		if err != nil || !found {
			if !wait(pollCtx, w.cfg.OutboxPoll) {
				return
			}
		}
	}
}

// The lock is held only by this publisher, never by a financial transaction.
// A bounded send keeps the connection/lock cost predictable. Process death or
// database disconnection releases the claim without a lease-recovery job.
func (w *Workers) publishOne(ctx context.Context) (bool, error) {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	var eventID, aggregateID string
	var payload []byte
	var attempts int
	err = tx.QueryRow(ctx, `SELECT event_id::text,aggregate_id::text,payload,attempts FROM outbox WHERE published_at IS NULL AND next_attempt_at<=now() ORDER BY occurred_at,event_id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&eventID, &aggregateID, &payload, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, sendErr := w.queue.Client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(w.queue.EventURL), MessageBody: aws.String(string(payload)), MessageGroupId: aws.String(aggregateID), MessageDeduplicationId: aws.String(eventID)})
	if sendErr != nil {
		// Reserve time to persist retry metadata even when the network request
		// consumed its deadline. Failure to commit still leaves a retryable row.
		retryCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err = tx.Exec(retryCtx, `UPDATE outbox SET attempts=attempts+1,next_attempt_at=clock_timestamp()+$2::interval WHERE event_id=$1`, eventID, backoff(attempts+1).String())
		if err != nil {
			return true, err
		}
		if err = tx.Commit(retryCtx); err != nil {
			return true, err
		}
		return true, fmt.Errorf("send event %s: %w", eventID, sendErr)
	}
	w.failpoint("after_outbox_send")
	_, err = tx.Exec(ctx, `UPDATE outbox SET attempts=attempts+1,published_at=clock_timestamp() WHERE event_id=$1`, eventID)
	if err != nil {
		return true, err
	}
	if err = tx.Commit(ctx); err != nil {
		return true, err
	}
	w.metrics.Inc("outbox_published_total")
	w.log.Info("event published", "eventId", eventID, "aggregateId", aggregateID)
	return true, nil
}

func (w *Workers) references(pollCtx, workCtx context.Context) {
	for pollCtx.Err() == nil {
		ctx, cancel := context.WithTimeout(workCtx, w.cfg.ProcessTimeout)
		count, err := w.service.ResumePending(ctx)
		cancel()
		if err != nil {
			w.metrics.Inc("retries_total{worker=\"references\"}")
			w.metrics.StorageError(err)
			w.log.Warn("reference retry deferred", "error", err)
		} else if count > 0 {
			w.log.Info("pending references revisited", "count", count)
		}
		if !wait(pollCtx, w.cfg.ReferencePoll) {
			return
		}
	}
}

func (w *Workers) queueMetrics(ctx context.Context) {
	for ctx.Err() == nil {
		queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		var lag int64
		if err := w.pool.QueryRow(queryCtx, `SELECT COALESCE(EXTRACT(EPOCH FROM (clock_timestamp()-MIN(occurred_at)))::bigint,0) FROM outbox WHERE published_at IS NULL`).Scan(&lag); err == nil {
			w.metrics.Gauge("outbox_oldest_pending_seconds", max(0, lag))
		}
		out, err := w.queue.Client.GetQueueAttributes(queryCtx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(w.queue.DLQURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages}})
		if err == nil {
			count, _ := strconv.ParseInt(out.Attributes["ApproximateNumberOfMessages"], 10, 64)
			w.metrics.Gauge("dlq_messages", count)
		}
		cancel()
		if !wait(ctx, 5*time.Second) {
			return
		}
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
