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
