// Package config loads and validates the process configuration at startup.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"time"

	"github.com/caarlos0/env/v11"
)

var supportedTestFailpoints = []string{"after_inbox_commit", "after_outbox_send"}

type Config struct {
	HTTPAddr               string        `env:"HTTP_ADDR" envDefault:":8080"`
	DatabaseURL            string        `env:"DATABASE_URL"`
	OIDCIssuerURL          string        `env:"OIDC_ISSUER_URL"`
	OIDCJWKSURL            string        `env:"OIDC_JWKS_URL"`
	OIDCAudience           string        `env:"OIDC_AUDIENCE" envDefault:"wagering-api"`
	AWSRegion              string        `env:"AWS_REGION" envDefault:"us-east-1"`
	SQSEndpoint            string        `env:"SQS_ENDPOINT"`
	QueueName              string        `env:"SQS_QUEUE_NAME" envDefault:"wager-transactions.fifo"`
	EventQueueName         string        `env:"SQS_EVENT_QUEUE_NAME" envDefault:"wager-events.fifo"`
	DLQName                string        `env:"SQS_DLQ_NAME" envDefault:"wager-transactions-dlq.fifo"`
	WorkersEnabled         bool          `env:"WORKERS_ENABLED" envDefault:"true"`
	ConsumerEnabled        bool          `env:"CONSUMER_ENABLED" envDefault:"true"`
	PublisherEnabled       bool          `env:"PUBLISHER_ENABLED" envDefault:"true"`
	ReferenceWorkerEnabled bool          `env:"REFERENCE_WORKER_ENABLED" envDefault:"true"`
	ReferenceMaxAttempts   int           `env:"REFERENCE_MAX_ATTEMPTS" envDefault:"8"`
	ReferenceTTL           time.Duration `env:"REFERENCE_TTL" envDefault:"24h"`
	ReferencePoll          time.Duration `env:"REFERENCE_POLL_INTERVAL" envDefault:"1s"`
	OutboxPoll             time.Duration `env:"OUTBOX_POLL_INTERVAL" envDefault:"500ms"`
	ProcessTimeout         time.Duration `env:"PROCESS_TIMEOUT" envDefault:"10s"`
	ShutdownTimeout        time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"20s"`
	SQSWaitSeconds         int32         `env:"SQS_WAIT_SECONDS" envDefault:"10"`
	SQSVisibilitySeconds   int32         `env:"SQS_VISIBILITY_SECONDS" envDefault:"30"`
	EnableTestFailpoints   bool          `env:"ENABLE_TEST_FAILPOINTS" envDefault:"false"`
	TestFailpoint          string        `env:"TEST_FAILPOINT"`
}

func Load() (Config, error) {
	c, err := env.ParseAs[Config]()
	if err != nil {
		return c, err
	}
	return c, c.validate()
}

// Parsing belongs to env; relationships between settings belong to this service.
func (c Config) validate() error {
	var errs []error
	for _, item := range []struct {
		name  string
		value time.Duration
	}{
		{"REFERENCE_TTL", c.ReferenceTTL}, {"REFERENCE_POLL_INTERVAL", c.ReferencePoll},
		{"OUTBOX_POLL_INTERVAL", c.OutboxPoll}, {"PROCESS_TIMEOUT", c.ProcessTimeout},
		{"SHUTDOWN_TIMEOUT", c.ShutdownTimeout},
	} {
		if item.value <= 0 {
			errs = append(errs, fmt.Errorf("%s must be a positive duration", item.name))
		}
	}
	for _, item := range []struct {
		name            string
		value, min, max int
	}{
		{"REFERENCE_MAX_ATTEMPTS", c.ReferenceMaxAttempts, 1, 100},
		{"SQS_WAIT_SECONDS", int(c.SQSWaitSeconds), 0, 20},
		{"SQS_VISIBILITY_SECONDS", int(c.SQSVisibilitySeconds), 1, 43200},
	} {
		if item.value < item.min || item.value > item.max {
			errs = append(errs, fmt.Errorf("%s must be between %d and %d", item.name, item.min, item.max))
		}
	}
	if _, _, err := net.SplitHostPort(c.HTTPAddr); err != nil {
		errs = append(errs, errors.New("HTTP_ADDR must be host:port"))
	}
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	for _, item := range []struct {
		name, value string
		optional    bool
	}{
		{"OIDC_ISSUER_URL", c.OIDCIssuerURL, false}, {"OIDC_JWKS_URL", c.OIDCJWKSURL, true}, {"SQS_ENDPOINT", c.SQSEndpoint, true},
	} {
		if item.optional && item.value == "" {
			continue
		}
		u, err := url.Parse(item.value)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s must be an HTTP(S) URL without credentials", item.name))
			continue
		}
		isHTTP := u.Scheme == "http" || u.Scheme == "https"
		if u.Host == "" || !isHTTP || u.User != nil {
			errs = append(errs, fmt.Errorf("%s must be an HTTP(S) URL without credentials", item.name))
		}
	}
	if c.ProcessTimeout+5*time.Second >= time.Duration(c.SQSVisibilitySeconds)*time.Second {
		errs = append(errs, errors.New("SQS_VISIBILITY_SECONDS must exceed PROCESS_TIMEOUT by more than 5 seconds"))
	}
	if c.ShutdownTimeout <= c.ProcessTimeout {
		errs = append(errs, errors.New("SHUTDOWN_TIMEOUT must exceed PROCESS_TIMEOUT"))
	}
	if c.TestFailpoint != "" && !c.EnableTestFailpoints {
		errs = append(errs, errors.New("TEST_FAILPOINT requires ENABLE_TEST_FAILPOINTS=true"))
	}
	if c.TestFailpoint != "" && !slices.Contains(supportedTestFailpoints, c.TestFailpoint) {
		errs = append(errs, errors.New("unknown TEST_FAILPOINT"))
	}
	return errors.Join(errs...)
}
