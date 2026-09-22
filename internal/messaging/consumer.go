package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/cadmax/backend-challenge-go/internal/application"
)

func (w *Workers) consume(pollCtx, workCtx context.Context) {
	for pollCtx.Err() == nil {
		out, err := w.queue.Client.ReceiveMessage(pollCtx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(w.queue.InputURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     w.cfg.SQSWaitSeconds,
			VisibilityTimeout:   w.cfg.SQSVisibilitySeconds,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
			},
		})
		if err != nil {
			if pollCtx.Err() != nil {
				return
			}
			w.metrics.Retry("consumer_poll")
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
		result, err = w.service.ProcessInbox(ctx, envelope, application.Metadata{
			CorrelationID: envelope.MessageID,
			CausationID:   envelope.MessageID,
		})
		cancel()
	}
	if err != nil {
		attempt, _ := strconv.Atoi(message.Attributes["ApproximateReceiveCount"])
		w.metrics.StorageError(err)
		w.metrics.Retry("consumer")
		if errors.Is(err, application.ErrConflict) {
			w.metrics.Conflict("inbox")
		}
		// Invalid envelopes are never acknowledged: the broker's redrive policy
		// retains the original bytes for diagnosis in the DLQ.
		w.log.Warn("message deferred",
			"messageId", envelope.MessageID,
			"providerId", envelope.Data.ProviderID,
			"walletId", envelope.Data.WalletID,
			"attempt", attempt,
			"error", err,
		)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		delay := int32(backoff(attempt) / time.Second)
		if workCtx.Err() != nil {
			delay = 0
		}
		_, visibilityErr := w.queue.Client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(w.queue.InputURL),
			ReceiptHandle:     message.ReceiptHandle,
			VisibilityTimeout: delay,
		})
		if visibilityErr != nil {
			w.log.Warn("cannot change message visibility", "messageId", envelope.MessageID, "error", visibilityErr)
		}
		return
	}
	w.metrics.Result(result.Status, result.IdempotentReplay)
	w.log.Info("message committed",
		"messageId", envelope.MessageID,
		"correlationId", envelope.MessageID,
		"transactionId", result.TransactionID,
		"providerId", envelope.Data.ProviderID,
		"walletId", envelope.Data.WalletID,
		"status", result.Status,
		"replay", result.IdempotentReplay,
	)
	w.failpoint("after_inbox_commit")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err = w.queue.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(w.queue.InputURL),
		ReceiptHandle: message.ReceiptHandle,
	}); err != nil {
		w.metrics.Retry("ack")
		w.log.Warn("message committed but acknowledgement failed", "messageId", envelope.MessageID, "error", err)
	}
}

func (w *Workers) release(message types.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := w.queue.Client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(w.queue.InputURL),
		ReceiptHandle:     message.ReceiptHandle,
		VisibilityTimeout: 0,
	})
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
	if envelope.MessageID == "" || len(envelope.MessageID) > 200 {
		return envelope, errors.New("missing or invalid message metadata")
	}
	if envelope.OccurredAt.IsZero() || envelope.Type != "WagerTransactionRequested" {
		return envelope, errors.New("missing or invalid message metadata")
	}
	if envelope.Data.IdempotencyKey == "" {
		return envelope, errors.New("missing or invalid message metadata")
	}
	return envelope, nil
}
