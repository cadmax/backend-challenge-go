package messaging

import (
	"context"
	"testing"
	"time"
)

func TestBackoffCapsWithoutOverflow(t *testing.T) {
	for _, tc := range []struct {
		attempt int
		want    time.Duration
	}{{0, time.Second}, {1, time.Second}, {2, 2 * time.Second}, {7, 64 * time.Second}, {1000000, 64 * time.Second}} {
		if got := backoff(tc.attempt); got != tc.want {
			t.Errorf("attempt=%d got=%s want=%s", tc.attempt, got, tc.want)
		}
	}
}
func TestWaitRespondsToCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if wait(ctx, time.Hour) {
		t.Fatal("cancelled wait continued")
	}
}
func TestDecodeRejectsInvalidEnvelope(t *testing.T) {
	for _, body := range []string{"{}", "null", `{"messageId":"x","type":"Wrong"}`, `{} {}`, `{"unknown":true}`} {
		if _, err := decodeEnvelope(body); err == nil {
			t.Errorf("accepted %s", body)
		}
	}
}
