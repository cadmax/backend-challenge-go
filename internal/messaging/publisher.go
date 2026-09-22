package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5"
)

func (w *Workers) publish(pollCtx, workCtx context.Context) {
	for pollCtx.Err() == nil {
		ctx, cancel := context.WithTimeout(workCtx, w.cfg.ProcessTimeout)
		found, err := w.publishOne(ctx)
		cancel()
		if err != nil {
			w.log.Warn("outbox publication deferred", "error", err)
			w.metrics.Retry("outbox")
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
	err = tx.QueryRow(ctx, `
		SELECT event_id::text, aggregate_id::text, payload, attempts
		FROM outbox
		WHERE published_at IS NULL AND next_attempt_at <= now()
		ORDER BY occurred_at, event_id
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`).Scan(&eventID, &aggregateID, &payload, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, sendErr := w.queue.Client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(w.queue.EventURL),
		MessageBody:            aws.String(string(payload)),
		MessageGroupId:         aws.String(aggregateID),
		MessageDeduplicationId: aws.String(eventID),
	})
	if sendErr != nil {
		// Reserve time to persist retry metadata even when the network request
		// consumed its deadline. Failure to commit still leaves a retryable row.
		retryCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err = tx.Exec(retryCtx, `
			UPDATE outbox
			SET attempts = attempts + 1,
			    next_attempt_at = clock_timestamp() + $2::interval
			WHERE event_id = $1
		`, eventID, backoff(attempts+1).String())
		if err != nil {
			return true, err
		}
		if err = tx.Commit(retryCtx); err != nil {
			return true, err
		}
		return true, fmt.Errorf("send event %s: %w", eventID, sendErr)
	}
	w.failpoint("after_outbox_send")
	_, err = tx.Exec(ctx, `
		UPDATE outbox
		SET attempts = attempts + 1, published_at = clock_timestamp()
		WHERE event_id = $1
	`, eventID)
	if err != nil {
		return true, err
	}
	if err = tx.Commit(ctx); err != nil {
		return true, err
	}
	w.metrics.OutboxPublished()
	w.log.Info("event published", "eventId", eventID, "aggregateId", aggregateID)
	return true, nil
}
