// Package config loads and validates the process configuration at startup.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"time"
)

var supportedTestFailpoints = []string{"after_inbox_commit", "after_outbox_send"}

type Config struct {
	HTTPAddr, DatabaseURL                    string
	OIDCIssuerURL, OIDCJWKSURL, OIDCAudience string
	AWSRegion, SQSEndpoint                   string
	QueueName, EventQueueName, DLQName       string
	WorkersEnabled, ConsumerEnabled          bool
	PublisherEnabled, ReferenceWorkerEnabled bool
	ReferenceMaxAttempts                     int
	ReferenceTTL, ReferencePoll, OutboxPoll  time.Duration
	ProcessTimeout, ShutdownTimeout          time.Duration
	SQSWaitSeconds, SQSVisibilitySeconds     int32
	EnableTestFailpoints                     bool
	TestFailpoint                            string
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr: env("HTTP_ADDR", ":8080"), DatabaseURL: os.Getenv("DATABASE_URL"),
		OIDCIssuerURL: os.Getenv("OIDC_ISSUER_URL"), OIDCJWKSURL: os.Getenv("OIDC_JWKS_URL"),
		OIDCAudience: env("OIDC_AUDIENCE", "wagering-api"), AWSRegion: env("AWS_REGION", "us-east-1"),
		SQSEndpoint: os.Getenv("SQS_ENDPOINT"), QueueName: env("SQS_QUEUE_NAME", "wager-transactions.fifo"),
		EventQueueName: env("SQS_EVENT_QUEUE_NAME", "wager-events.fifo"), DLQName: env("SQS_DLQ_NAME", "wager-transactions-dlq.fifo"),
		TestFailpoint: os.Getenv("TEST_FAILPOINT"),
	}
	var errs []error
	for _, item := range []struct {
		name     string
		target   *bool
		fallback bool
	}{
		{"WORKERS_ENABLED", &c.WorkersEnabled, true}, {"CONSUMER_ENABLED", &c.ConsumerEnabled, true},
		{"PUBLISHER_ENABLED", &c.PublisherEnabled, true}, {"REFERENCE_WORKER_ENABLED", &c.ReferenceWorkerEnabled, true},
		{"ENABLE_TEST_FAILPOINTS", &c.EnableTestFailpoints, false},
	} {
		value, err := strconv.ParseBool(env(item.name, strconv.FormatBool(item.fallback)))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s must be a boolean", item.name))
		}
		*item.target = value
	}
	for _, item := range []struct {
		name, fallback string
		target         *time.Duration
	}{
		{"REFERENCE_TTL", "24h", &c.ReferenceTTL}, {"REFERENCE_POLL_INTERVAL", "1s", &c.ReferencePoll},
		{"OUTBOX_POLL_INTERVAL", "500ms", &c.OutboxPoll}, {"PROCESS_TIMEOUT", "10s", &c.ProcessTimeout},
		{"SHUTDOWN_TIMEOUT", "20s", &c.ShutdownTimeout},
	} {
		value, err := time.ParseDuration(env(item.name, item.fallback))
		if err != nil || value <= 0 {
			errs = append(errs, fmt.Errorf("%s must be a positive duration", item.name))
		}
		*item.target = value
	}
	for _, item := range []struct {
		name               string
		fallback, min, max int
		assign             func(int)
	}{
		{"REFERENCE_MAX_ATTEMPTS", 8, 1, 100, func(v int) { c.ReferenceMaxAttempts = v }},
		{"SQS_WAIT_SECONDS", 10, 0, 20, func(v int) { c.SQSWaitSeconds = int32(v) }},
		{"SQS_VISIBILITY_SECONDS", 30, 1, 43200, func(v int) { c.SQSVisibilitySeconds = int32(v) }},
	} {
		value, err := strconv.Atoi(env(item.name, strconv.Itoa(item.fallback)))
		if err != nil || value < item.min || value > item.max {
			errs = append(errs, fmt.Errorf("%s must be between %d and %d", item.name, item.min, item.max))
		}
		item.assign(value)
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
	return c, errors.Join(errs...)
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
