package config

import (
	"testing"
	"time"
)

func TestConfigurationDefaultsAndEmptyOptionalValues(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("OIDC_ISSUER_URL", "http://localhost:8081/realms/jungle")
	for _, name := range []string{
		"HTTP_ADDR", "OIDC_AUDIENCE", "AWS_REGION", "SQS_QUEUE_NAME", "SQS_EVENT_QUEUE_NAME", "SQS_DLQ_NAME",
		"WORKERS_ENABLED", "CONSUMER_ENABLED", "PUBLISHER_ENABLED", "REFERENCE_WORKER_ENABLED",
		"REFERENCE_MAX_ATTEMPTS", "REFERENCE_TTL", "REFERENCE_POLL_INTERVAL", "OUTBOX_POLL_INTERVAL",
		"PROCESS_TIMEOUT", "SHUTDOWN_TIMEOUT", "SQS_WAIT_SECONDS", "SQS_VISIBILITY_SECONDS",
		"ENABLE_TEST_FAILPOINTS", "TEST_FAILPOINT", "OIDC_JWKS_URL", "SQS_ENDPOINT",
	} {
		t.Setenv(name, "")
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.OIDCAudience != "wagering-api" || cfg.AWSRegion != "us-east-1" {
		t.Fatalf("unexpected endpoint defaults: %+v", cfg)
	}
	if !cfg.WorkersEnabled || !cfg.ConsumerEnabled || !cfg.PublisherEnabled || !cfg.ReferenceWorkerEnabled || cfg.EnableTestFailpoints {
		t.Fatal("worker defaults changed")
	}
	if cfg.ReferenceMaxAttempts != 8 || cfg.ReferenceTTL != 24*time.Hour || cfg.ReferencePoll != time.Second || cfg.OutboxPoll != 500*time.Millisecond {
		t.Fatal("reference or publisher defaults changed")
	}
	if cfg.ProcessTimeout != 10*time.Second || cfg.ShutdownTimeout != 20*time.Second || cfg.SQSWaitSeconds != 10 || cfg.SQSVisibilitySeconds != 30 {
		t.Fatal("processing defaults changed")
	}
	if cfg.QueueName != "wager-transactions.fifo" || cfg.EventQueueName != "wager-events.fifo" || cfg.DLQName != "wager-transactions-dlq.fifo" {
		t.Fatal("queue defaults changed")
	}
}

func TestConfigurationParsesTypedOverrides(t *testing.T) {
	for name, value := range map[string]string{
		"DATABASE_URL": "postgres://localhost/test", "OIDC_ISSUER_URL": "http://localhost:8081/realms/jungle",
		"HTTP_ADDR": "127.0.0.1:9090", "WORKERS_ENABLED": "false", "REFERENCE_MAX_ATTEMPTS": "12",
		"REFERENCE_TTL": "90m", "SQS_WAIT_SECONDS": "0", "PROCESS_TIMEOUT": "2s",
		"SQS_VISIBILITY_SECONDS": "8", "SHUTDOWN_TIMEOUT": "3s",
	} {
		t.Setenv(name, value)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != "127.0.0.1:9090" || cfg.WorkersEnabled || cfg.ReferenceMaxAttempts != 12 || cfg.ReferenceTTL != 90*time.Minute || cfg.SQSWaitSeconds != 0 {
		t.Fatalf("typed overrides were not loaded: %+v", cfg)
	}
}

func TestConfigurationValidation(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("OIDC_ISSUER_URL", "http://localhost:8081/realms/jungle")
	if _, err := Load(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, value string }{
		{"PROCESS_TIMEOUT", "forever"}, {"SQS_WAIT_SECONDS", "21"}, {"SQS_VISIBILITY_SECONDS", "15"},
		{"WORKERS_ENABLED", "perhaps"}, {"REFERENCE_MAX_ATTEMPTS", "0"}, {"REFERENCE_TTL", "-1s"},
		{"OIDC_ISSUER_URL", ""}, {"OIDC_JWKS_URL", "file:///etc/passwd"}, {"SHUTDOWN_TIMEOUT", "1s"},
		{"TEST_FAILPOINT", "after_outbox_send"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.name, tc.value)
			if _, err := Load(); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestFailpointNames(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("OIDC_ISSUER_URL", "http://localhost:8081/realms/jungle")
	t.Setenv("ENABLE_TEST_FAILPOINTS", "true")
	for _, test := range []struct {
		name  string
		valid bool
	}{
		{"", true},
		{"after_inbox_commit", true},
		{"after_outbox_send", true},
		{"after_inbox", false},
		{"after_inbox_commit ", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("TEST_FAILPOINT", test.name)
			_, err := Load()
			if (err == nil) != test.valid {
				t.Fatalf("failpoint %q: %v", test.name, err)
			}
		})
	}
}
