package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/application"
	"github.com/cadmax/backend-challenge-go/internal/auth"
	"github.com/cadmax/backend-challenge-go/internal/domain"
)

const operationJSON = `{"providerId":"provider-a","externalTransactionId":"tx-one","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"round-one","gameId":"game-one","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}`

type fakeVerifier struct{}

func (fakeVerifier) Verify(_ context.Context, token string) (auth.Principal, error) {
	switch token {
	case "internal":
		return auth.Principal{Subject: "internal", Roles: []string{auth.RoleInternal}}, nil
	case "provider-a", "provider-b":
		return auth.Principal{Subject: token, ProviderID: token, Roles: []string{auth.RoleProvider}}, nil
	case "missing-provider":
		return auth.Principal{Subject: "incomplete", Roles: []string{auth.RoleProvider}}, nil
	case "unprivileged":
		return auth.Principal{Subject: "unprivileged"}, nil
	default:
		return auth.Principal{}, auth.ErrInvalidToken
	}
}

type fakeService struct {
	calls                int
	result               application.Result
	err                  error
	provider, id, cursor string
	limit                int
	command              application.Command
	metadata             application.Metadata
	reconcileConsistent  bool
	processFn            func(context.Context) error
}

func (s *fakeService) OpenWallet(_ context.Context, playerID string, balance domain.Money, metadata application.Metadata) (application.WalletView, error) {
	s.calls++
	s.metadata = metadata
	return application.WalletView{ID: "wallet-created", PlayerID: playerID, Balance: balance, Version: 1}, s.err
}
func (s *fakeService) GetWallet(_ context.Context, id string) (application.WalletView, error) {
	s.calls++
	s.id = id
	balance, _ := domain.Zero("BRL")
	return application.WalletView{ID: id, PlayerID: "player", Balance: balance, Version: 1}, s.err
}
func (s *fakeService) Ledger(_ context.Context, id, cursor string, limit int) (application.LedgerPage, error) {
	s.calls++
	s.id, s.cursor, s.limit = id, cursor, limit
	return application.LedgerPage{Entries: []application.LedgerView{}, NextCursor: "opaque-next"}, s.err
}
func (s *fakeService) Reconcile(_ context.Context, id string) (application.Reconciliation, error) {
	s.calls++
	s.id = id
	zero, _ := domain.Zero("BRL")
	return application.Reconciliation{WalletID: id, StoredBalance: zero, CalculatedBalance: zero, Difference: zero, Consistent: s.reconcileConsistent}, s.err
}
func (s *fakeService) Process(ctx context.Context, command application.Command, metadata application.Metadata) (application.Result, error) {
	s.calls++
	s.command, s.metadata = command, metadata
	if s.processFn != nil {
		return application.Result{}, s.processFn(ctx)
	}
	return s.result, s.err
}
func (s *fakeService) GetTransaction(_ context.Context, provider, id string) (application.Result, error) {
	s.calls++
	s.provider, s.id = provider, id
	return s.result, s.err
}
func (s *fakeService) GetExternalTransaction(_ context.Context, provider, id string) (application.Result, error) {
	s.calls++
	s.provider, s.id = provider, id
	return s.result, s.err
}

func request(h http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	r.Header.Set("Idempotency-Key", "client-selected-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestEveryBusinessRouteRequiresAuthenticationAndRole(t *testing.T) {
	routes := []struct{ method, path, role string }{
		{"POST", "/wallets", "internal"},
		{"GET", "/wallets/wallet-one", "internal"},
		{"GET", "/wallets/wallet-one/ledger", "internal"},
		{"POST", "/wallets/wallet-one/reconciliation", "internal"},
		{"POST", "/wagering/transactions", "provider-a"},
		{"GET", "/wagering/transactions/tx-one", "provider-a"},
		{"GET", "/providers/provider-a/wagering/transactions/tx-one", "provider-a"},
	}
	for _, route := range routes {
		t.Run(route.method+route.path, func(t *testing.T) {
			for _, token := range []string{"", "invalid", "unprivileged", "missing-provider"} {
				s := &fakeService{}
				w := request(New(s, fakeVerifier{}, Options{}), route.method, route.path, token, operationJSON)
				want := http.StatusForbidden
				if token == "" || token == "invalid" {
					want = http.StatusUnauthorized
				}
				if w.Code != want || s.calls != 0 {
					t.Fatalf("token=%q status=%d calls=%d", token, w.Code, s.calls)
				}
				if want == http.StatusUnauthorized && w.Header().Get("WWW-Authenticate") == "" {
					t.Fatal("missing bearer challenge")
				}
			}
			wrongRole := "internal"
			if route.role == "internal" {
				wrongRole = "provider-a"
			}
			s := &fakeService{}
			w := request(New(s, fakeVerifier{}, Options{}), route.method, route.path, wrongRole, operationJSON)
			if w.Code != http.StatusForbidden || s.calls != 0 {
				t.Fatalf("role isolation failed: %d calls=%d", w.Code, s.calls)
			}
		})
	}
}

func TestProviderIsolationBeforeProcessingAndReplay(t *testing.T) {
	for _, replay := range []bool{false, true} {
		s := &fakeService{result: application.Result{TransactionID: "tx-existing", Status: "PROCESSED", IdempotentReplay: replay}}
		w := request(New(s, fakeVerifier{}, Options{}), "POST", "/wagering/transactions", "provider-b", operationJSON)
		if w.Code != http.StatusForbidden || s.calls != 0 {
			t.Fatalf("replay=%v status=%d calls=%d", replay, w.Code, s.calls)
		}
	}
	s := &fakeService{}
	h := New(s, fakeVerifier{}, Options{})
	w := request(h, "GET", "/providers/provider-a/wagering/transactions/tx-one", "provider-b", "")
	if w.Code != http.StatusForbidden || s.calls != 0 {
		t.Fatalf("external lookup leaked: %d", w.Code)
	}
	s.err = application.ErrNotFound
	w = request(h, "GET", "/wagering/transactions/tx-one", "provider-b", "")
	if w.Code != http.StatusNotFound || s.provider != "provider-b" || s.id != "tx-one" {
		t.Fatalf("internal lookup not scoped: code=%d provider=%s id=%s", w.Code, s.provider, s.id)
	}
}

func TestProcessStatusesAndOriginalReplayResult(t *testing.T) {
	balance, err := domain.NewMoney("75.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	for status, code := range map[string]int{"PROCESSED": 200, "PENDING": 202, "PENDING_REFERENCE": 202, "REJECTED": 422, "FAILED": 503} {
		t.Run(status, func(t *testing.T) {
			s := &fakeService{result: application.Result{TransactionID: "tx-original", Status: status, Balance: &balance, IdempotentReplay: true, FailureCode: "AUDIT_CODE"}}
			r := httptest.NewRequest("POST", "/wagering/transactions", strings.NewReader(operationJSON))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Authorization", "Bearer provider-a")
			r.Header.Set("Idempotency-Key", "do-not-replace-this")
			r.Header.Set("X-Correlation-ID", "request_123:attempt-2")
			w := httptest.NewRecorder()
			New(s, fakeVerifier{}, Options{}).ServeHTTP(w, r)
			if w.Code != code {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if s.command.IdempotencyKey != "do-not-replace-this" || s.command.ProviderID != "provider-a" || s.metadata.CorrelationID != "request_123:attempt-2" {
				t.Fatalf("command=%+v metadata=%+v", s.command, s.metadata)
			}
			var got application.Result
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got.TransactionID != "tx-original" || !got.IdempotentReplay || got.Balance.Amount() != "75.00" || got.FailureCode != "AUDIT_CODE" {
				t.Fatalf("persisted result changed: %+v", got)
			}
		})
	}
}

func TestStrictRequestParsing(t *testing.T) {
	cases := map[string]string{
		"unknown field":            strings.TrimSuffix(operationJSON, "}") + `,"unknown":true}`,
		"body idempotency key":     strings.TrimSuffix(operationJSON, "}") + `,"idempotencyKey":"ambiguous"}`,
		"trailing object":          operationJSON + ` {}`,
		"trailing garbage":         operationJSON + ` x`,
		"null":                     `null`,
		"array":                    `[]`,
		"empty":                    ``,
		"numeric money":            strings.Replace(operationJSON, `"25.00"`, `25.00`, 1),
		"unknown money field":      strings.Replace(operationJSON, `"currency":"BRL"`, `"currency":"BRL","extra":1`, 1),
		"negative money":           strings.Replace(operationJSON, `"25.00"`, `"-25.00"`, 1),
		"duplicate provider":       strings.TrimSuffix(operationJSON, "}") + `,"providerId":"provider-a"}`,
		"duplicate field spelling": strings.TrimSuffix(operationJSON, "}") + `,"PROVIDERID":"provider-a"}`,
		"duplicate money field":    strings.Replace(operationJSON, `"amount":"25.00"`, `"amount":"24.00","amount":"25.00"`, 1),
		"too large":                operationJSON + strings.Repeat(" ", maxBodyBytes),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			s := &fakeService{}
			w := request(New(s, fakeVerifier{}, Options{}), "POST", "/wagering/transactions", "provider-a", body)
			if w.Code != http.StatusBadRequest || s.calls != 0 {
				t.Fatalf("status=%d calls=%d body=%s", w.Code, s.calls, w.Body.String())
			}
		})
	}
}

func TestTransportHeaders(t *testing.T) {
	cases := []struct {
		name   string
		change func(*http.Request)
		status int
	}{
		{"missing key", func(r *http.Request) { r.Header.Del("Idempotency-Key") }, 400},
		{"empty key", func(r *http.Request) { r.Header.Set("Idempotency-Key", "") }, 400},
		{"key whitespace", func(r *http.Request) { r.Header.Set("Idempotency-Key", " key ") }, 400},
		{"key too large", func(r *http.Request) { r.Header.Set("Idempotency-Key", strings.Repeat("a", 201)) }, 400},
		{"duplicate key", func(r *http.Request) { r.Header.Add("Idempotency-Key", "second") }, 400},
		{"wrong content type", func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }, 400},
		{"missing content type", func(r *http.Request) { r.Header.Del("Content-Type") }, 400},
		{"duplicate authorization", func(r *http.Request) { r.Header.Add("Authorization", "Bearer provider-b") }, 401},
		{"wrong scheme", func(r *http.Request) { r.Header.Set("Authorization", "Basic provider-a") }, 401},
		{"token trailing words", func(r *http.Request) { r.Header.Set("Authorization", "Bearer provider-a another") }, 401},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/wagering/transactions", strings.NewReader(operationJSON))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Authorization", "Bearer provider-a")
			r.Header.Set("Idempotency-Key", "key")
			tt.change(r)
			s := &fakeService{}
			w := httptest.NewRecorder()
			New(s, fakeVerifier{}, Options{}).ServeHTTP(w, r)
			if w.Code != tt.status || s.calls != 0 {
				t.Fatalf("status=%d calls=%d", w.Code, s.calls)
			}
		})
	}
}

func TestWalletRoutes(t *testing.T) {
	s := &fakeService{reconcileConsistent: true}
	h := New(s, fakeVerifier{}, Options{})
	w := request(h, "POST", "/wallets", "internal", `{"playerId":"player-one","initialBalance":{"amount":"100.00","currency":"BRL"}}`)
	if w.Code != 201 || w.Header().Get("Location") != "/wallets/wallet-created" {
		t.Fatalf("open: %d %s", w.Code, w.Body.String())
	}
	w = request(h, "GET", "/wallets/wallet-one", "internal", "")
	if w.Code != 200 || s.id != "wallet-one" {
		t.Fatalf("get: %d", w.Code)
	}
	w = request(h, "GET", "/wallets/wallet-one/ledger?cursor=opaque&limit=20", "internal", "")
	if w.Code != 200 || s.cursor != "opaque" || s.limit != 20 {
		t.Fatalf("ledger: %d cursor=%s limit=%d", w.Code, s.cursor, s.limit)
	}
	w = request(h, "GET", "/wallets/wallet-one/ledger", "internal", "")
	if w.Code != 200 || s.limit != 50 {
		t.Fatalf("default limit=%d", s.limit)
	}
	w = request(h, "POST", "/wallets/wallet-one/reconciliation", "internal", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"consistent":true`) {
		t.Fatalf("reconciliation: %d %s", w.Code, w.Body.String())
	}
	for _, query := range []string{"limit=0", "limit=-1", "limit=101", "limit=x", "limit=10&limit=20", "cursor=a&cursor=b", "cursor=" + strings.Repeat("a", 1025)} {
		before := s.calls
		w := request(h, "GET", "/wallets/wallet-one/ledger?"+query, "internal", "")
		if w.Code != 400 || s.calls != before {
			t.Fatalf("invalid pagination %s: status=%d calls=%d", query, w.Code, s.calls)
		}
	}
}

func TestApplicationErrorsAreClassifiedAndSanitized(t *testing.T) {
	for _, tt := range []struct {
		err    error
		status int
		code   string
	}{
		{fmt.Errorf("details: %w", application.ErrInvalidInput), 400, "INVALID_INPUT"},
		{application.ErrConflict, 409, "CONFLICT"},
		{application.ErrNotFound, 404, "NOT_FOUND"},
		{application.ErrUnavailable, 503, "UNAVAILABLE"},
		{errors.New("postgres password=private-secret SQL details"), 503, "UNAVAILABLE"},
	} {
		s := &fakeService{err: tt.err}
		w := request(New(s, fakeVerifier{}, Options{}), "POST", "/wagering/transactions", "provider-a", operationJSON)
		if w.Code != tt.status || !strings.Contains(w.Body.String(), tt.code) || strings.Contains(w.Body.String(), "private-secret") {
			t.Fatalf("error=%v response=%d %s", tt.err, w.Code, w.Body.String())
		}
		if tt.status == 503 && w.Header().Get("Retry-After") == "" {
			t.Fatal("missing transient retry hint")
		}
	}
}

func TestHealthAndMetricsArePublic(t *testing.T) {
	for _, failed := range []bool{false, true} {
		checks := 0
		h := New(&fakeService{}, fakeVerifier{}, Options{
			ReadinessChecks: map[string]func(context.Context) error{
				"postgres": func(ctx context.Context) error {
					checks++
					if _, ok := ctx.Deadline(); !ok {
						t.Error("missing check deadline")
					}
					return nil
				},
				"sqs": func(context.Context) error {
					checks++
					if failed {
						return errors.New("secret connection string")
					}
					return nil
				},
			},
			Metrics: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("test_metric 1\n")) }),
		})
		if w := request(h, "GET", "/health/live", "", ""); w.Code != 200 {
			t.Fatalf("liveness=%d", w.Code)
		}
		w := request(h, "GET", "/health/ready", "", "")
		want := 200
		if failed {
			want = 503
		}
		if w.Code != want || checks != 2 || strings.Contains(w.Body.String(), "secret") {
			t.Fatalf("readiness=%d checks=%d body=%s", w.Code, checks, w.Body.String())
		}
		if w := request(h, "GET", "/metrics", "", ""); w.Code != 200 || w.Body.String() != "test_metric 1\n" {
			t.Fatalf("metrics=%d %s", w.Code, w.Body.String())
		}
	}
	if w := request(New(&fakeService{}, fakeVerifier{}, Options{}), "GET", "/health/ready", "", ""); w.Code != 503 {
		t.Fatal("unconfigured readiness must not succeed")
	}
}

func TestCorrelationAndLogsContainNoCredentialsOrMoney(t *testing.T) {
	var logs bytes.Buffer
	s := &fakeService{result: application.Result{Status: "PROCESSED", TransactionID: "transaction-one"}}
	h := New(s, fakeVerifier{}, Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	r := httptest.NewRequest("POST", "/wagering/transactions?secret=password", strings.NewReader(operationJSON))
	r.Header.Set("Authorization", "Bearer provider-a")
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", "private-key")
	r.Header.Set("X-Correlation-ID", strings.Repeat("a", 129))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	correlation := w.Header().Get("X-Correlation-ID")
	if w.Code != 200 || !validCorrelationID(correlation) || correlation != s.metadata.CorrelationID {
		t.Fatalf("correlation=%q metadata=%q code=%d", correlation, s.metadata.CorrelationID, w.Code)
	}
	for _, sensitive := range []string{"Bearer", "password", "private-key", "25.00", `"money"`} {
		if strings.Contains(logs.String(), sensitive) {
			t.Fatalf("logs contain %q: %s", sensitive, logs.String())
		}
	}
	for _, required := range []string{"transaction-one", "provider-a", "correlationId", "walletId"} {
		if !strings.Contains(logs.String(), required) {
			t.Fatalf("missing log attribute %q", required)
		}
	}
}

func TestRequestDeadlineReachesApplication(t *testing.T) {
	s := &fakeService{processFn: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	h := New(s, fakeVerifier{}, Options{RequestTimeout: time.Millisecond})
	w := request(h, "POST", "/wagering/transactions", "provider-a", operationJSON)
	if w.Code != 503 || s.calls != 1 {
		t.Fatalf("deadline: code=%d calls=%d", w.Code, s.calls)
	}
}
