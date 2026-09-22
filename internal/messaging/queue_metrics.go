package messaging

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func (w *Workers) queueMetrics(ctx context.Context) {
	for ctx.Err() == nil {
		queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		var lag int64
		err := w.pool.QueryRow(queryCtx, `
			SELECT COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - MIN(occurred_at)))::bigint, 0)
			FROM outbox
			WHERE published_at IS NULL
		`).Scan(&lag)
		if err == nil {
			w.metrics.Gauge("outbox_oldest_pending_seconds", max(0, lag))
		}
		out, err := w.queue.Client.GetQueueAttributes(queryCtx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(w.queue.DLQURL),
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameApproximateNumberOfMessages,
			},
		})
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
