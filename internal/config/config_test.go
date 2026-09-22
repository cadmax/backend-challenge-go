package config

import "testing"

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
