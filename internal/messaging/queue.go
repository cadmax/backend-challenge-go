// Package messaging adapts SQS and runs durable inbox/outbox workers.
package messaging

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/cadmax/backend-challenge-go/internal/config"
)

type Queue struct {
	Client                     *sqs.Client
	InputURL, EventURL, DLQURL string
	cfg                        config.Config
}

func NewQueue(cfg config.Config) (*Queue, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.AWSRegion), awsconfig.WithRetryMaxAttempts(2),
		awsconfig.WithHTTPClient(&http.Client{Timeout: time.Duration(cfg.SQSWaitSeconds)*time.Second + 5*time.Second}))
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	client := sqs.NewFromConfig(c, func(o *sqs.Options) {
		if cfg.SQSEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.SQSEndpoint)
		}
	})
	return &Queue{Client: client, cfg: cfg}, nil
}

func (q *Queue) Initialize(ctx context.Context) error {
	for _, item := range []struct {
		name   string
		target *string
	}{
		{q.cfg.QueueName, &q.InputURL}, {q.cfg.EventQueueName, &q.EventURL}, {q.cfg.DLQName, &q.DLQURL},
	} {
		out, err := q.Client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(item.name)})
		if err != nil {
			return fmt.Errorf("resolve queue %s: %w", item.name, err)
		}
		*item.target = aws.ToString(out.QueueUrl)
	}
	return q.Ready(ctx)
}

func (q *Queue) Ready(ctx context.Context) error {
	for _, url := range []string{q.InputURL, q.EventURL, q.DLQURL} {
		if url == "" {
			return fmt.Errorf("queue not initialized")
		}
		if _, err := q.Client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(url), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}}); err != nil {
			return err
		}
	}
	return nil
}
