// Package httpapi adapts authenticated HTTP requests to the application service.
package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/application"
	"github.com/cadmax/backend-challenge-go/internal/auth"
	"github.com/cadmax/backend-challenge-go/internal/domain"
)

const maxBodyBytes = 32 << 10

type Service interface {
	OpenWallet(context.Context, string, domain.Money, application.Metadata) (application.WalletView, error)
	GetWallet(context.Context, string) (application.WalletView, error)
	Ledger(context.Context, string, string, int) (application.LedgerPage, error)
	Reconcile(context.Context, string) (application.Reconciliation, error)
	Process(context.Context, application.Command, application.Metadata) (application.Result, error)
	GetTransaction(context.Context, string, string) (application.Result, error)
	GetExternalTransaction(context.Context, string, string) (application.Result, error)
}

type TokenVerifier interface {
	Verify(context.Context, string) (auth.Principal, error)
}

type Options struct {
	Logger          *slog.Logger
	ReadinessChecks map[string]func(context.Context) error
	Metrics         http.Handler
	RequestTimeout  time.Duration
}

type handler struct {
	service  Service
	verifier TokenVerifier
	options  Options
}

// New exposes health and metrics publicly; every business route has an explicit
// role check. A provider identity is always passed to transaction reads, including
// internal-ID lookups and idempotent replays.
func New(service Service, verifier TokenVerifier, options Options) http.Handler {
	if options.Logger == nil {
		options.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if options.RequestTimeout <= 0 {
		options.RequestTimeout = 10 * time.Second
	}
	h := &handler{service: service, verifier: verifier, options: options}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /health/ready", h.ready)
	if options.Metrics != nil {
		mux.Handle("GET /metrics", options.Metrics)
	}
	mux.HandleFunc("POST /wallets", h.authorize(auth.RoleInternal, h.openWallet))
	mux.HandleFunc("GET /wallets/{walletId}", h.authorize(auth.RoleInternal, h.getWallet))
	mux.HandleFunc("GET /wallets/{walletId}/ledger", h.authorize(auth.RoleInternal, h.ledger))
	mux.HandleFunc("POST /wallets/{walletId}/reconciliation", h.authorize(auth.RoleInternal, h.reconcile))
	mux.HandleFunc("POST /wagering/transactions", h.authorize(auth.RoleProvider, h.process))
	mux.HandleFunc("GET /wagering/transactions/{transactionId}", h.authorize(auth.RoleProvider, h.getTransaction))
	mux.HandleFunc("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", h.authorize(auth.RoleProvider, h.getExternalTransaction))
	return h.observe(mux)
}

type authorizedHandler func(http.ResponseWriter, *http.Request, auth.Principal)

func (h *handler) authorize(role string, next authorizedHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		headers := r.Header.Values("Authorization")
		if len(headers) != 1 || len(headers[0]) > 16391 {
			h.unauthorized(w)
			return
		}
		parts := strings.Fields(headers[0])
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || h.verifier == nil {
			h.unauthorized(w)
			return
		}
		principal, err := h.verifier.Verify(r.Context(), parts[1])
		if err != nil {
			h.unauthorized(w)
			return
		}
		if !principal.HasRole(role) || (role == auth.RoleProvider && principal.ProviderID == "") {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "This identity cannot perform this operation")
			return
		}
		next(w, r, principal)
	}
}

func (h *handler) unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="wagering-api"`)
	writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "A valid bearer access token is required")
}

func (h *handler) openWallet(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	var body struct {
		PlayerID       string       `json:"playerId"`
		InitialBalance domain.Money `json:"initialBalance"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	result, err := h.service.OpenWallet(r.Context(), body.PlayerID, body.InitialBalance, metadata(r))
	if err != nil {
		h.applicationError(w, err)
		return
	}
	w.Header().Set("Location", "/wallets/"+result.ID)
	writeJSON(w, http.StatusCreated, result)
}

func (h *handler) getWallet(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	result, err := h.service.GetWallet(r.Context(), r.PathValue("walletId"))
	if err != nil {
		h.applicationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *handler) ledger(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	query := r.URL.Query()
	limit := 50
	if raw, ok := query["limit"]; ok {
		var err error
		if len(raw) != 1 {
			writeError(w, http.StatusBadRequest, "INVALID_INPUT", "limit must appear once")
			return
		}
		limit, err = strconv.Atoi(raw[0])
		if err != nil || limit < 1 || limit > 100 {
			writeError(w, http.StatusBadRequest, "INVALID_INPUT", "limit must be between 1 and 100")
			return
		}
	}
	if len(query["cursor"]) > 1 || len(query.Get("cursor")) > 1024 {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "Invalid ledger cursor")
		return
	}
	result, err := h.service.Ledger(r.Context(), r.PathValue("walletId"), query.Get("cursor"), limit)
	if err != nil {
		h.applicationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *handler) reconcile(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	result, err := h.service.Reconcile(r.Context(), r.PathValue("walletId"))
	if err != nil {
		h.applicationError(w, err)
		return
	}
	if !result.Consistent {
		h.options.Logger.ErrorContext(r.Context(), "wallet reconciliation mismatch", "correlationId", correlationID(r), "walletId", r.PathValue("walletId"))
	}
	writeJSON(w, http.StatusOK, result)
}

// The HTTP DTO intentionally has no idempotencyKey field. The header is the
// single transport authority; decoding into Command would accept ambiguous keys.
type operationRequest struct {
	ProviderID                     string       `json:"providerId"`
	ExternalTransactionID          string       `json:"externalTransactionId"`
	PlayerID                       string       `json:"playerId"`
	WalletID                       string       `json:"walletId"`
	RoundID                        string       `json:"roundId"`
	GameID                         string       `json:"gameId"`
	Kind                           string       `json:"kind"`
	Money                          domain.Money `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
}

func (h *handler) process(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || !validIdempotencyKey(keys[0]) {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "One nonempty Idempotency-Key header is required (maximum 200 characters)")
		return
	}
	var body operationRequest
	if !decodeBody(w, r, &body) {
		return
	}
	if body.ProviderID != principal.ProviderID {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "The provider must match the authenticated identity")
		return
	}
	command := application.Command{
		ProviderID: principal.ProviderID, ExternalTransactionID: body.ExternalTransactionID,
		IdempotencyKey: keys[0], PlayerID: body.PlayerID, WalletID: body.WalletID,
		RoundID: body.RoundID, GameID: body.GameID, Kind: body.Kind, Money: body.Money,
		ReferenceExternalTransactionID: body.ReferenceExternalTransactionID,
	}
	result, err := h.service.Process(r.Context(), command, metadata(r))
	if err != nil {
		h.applicationError(w, err)
		return
	}
	h.options.Logger.InfoContext(r.Context(), "wager request completed",
		"correlationId", correlationID(r), "providerId", principal.ProviderID,
		"walletId", body.WalletID, "transactionId", result.TransactionID, "status", result.Status)
	writeJSON(w, operationStatus(result.Status), result)
}

func (h *handler) getTransaction(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	result, err := h.service.GetTransaction(r.Context(), principal.ProviderID, r.PathValue("transactionId"))
	if err != nil {
		h.applicationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *handler) getExternalTransaction(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	if r.PathValue("providerId") != principal.ProviderID {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "The provider must match the authenticated identity")
		return
	}
	result, err := h.service.GetExternalTransaction(r.Context(), principal.ProviderID, r.PathValue("externalTransactionId"))
	if err != nil {
		h.applicationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func operationStatus(status string) int {
	switch status {
	case "PROCESSED":
		return http.StatusOK
	case "PENDING", "PENDING_REFERENCE":
		return http.StatusAccepted
	case "REJECTED":
		return http.StatusUnprocessableEntity
	default:
		return http.StatusServiceUnavailable
	}
}

func (h *handler) applicationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, application.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "Request fields are invalid")
	case errors.Is(err, application.ErrConflict):
		writeError(w, http.StatusConflict, "CONFLICT", "The operation conflicts with an existing record")
	case errors.Is(err, application.ErrNotFound):
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Resource not found")
	default:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "The service is temporarily unavailable; retry with the same idempotency key")
	}
}

func (h *handler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	checks := make(map[string]string, len(h.options.ReadinessChecks))
	status := http.StatusOK
	if len(h.options.ReadinessChecks) == 0 {
		status = http.StatusServiceUnavailable
	}
	for name, check := range h.options.ReadinessChecks {
		if check == nil || check(ctx) != nil {
			checks[name] = "unavailable"
			status = http.StatusServiceUnavailable
		} else {
			checks[name] = "ok"
		}
	}
	writeJSON(w, status, struct {
		Ready  bool              `json:"ready"`
		Checks map[string]string `json:"checks"`
	}{Ready: status == http.StatusOK, Checks: checks})
}

func decodeBody(w http.ResponseWriter, r *http.Request, target any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "Content-Type must be application/json")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil || !uniqueObject(body) {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "Expected one JSON object without duplicate fields (maximum 32 KiB)")
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_INPUT", "Expected one JSON object with known fields (maximum 32 KiB)")
		return false
	}
	return true
}

func uniqueObject(body []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return false
	}
	seen := make(map[string]bool)
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		// encoding/json matches struct fields case-insensitively. Reject such
		// duplicate spellings too, so the payload has only one interpretation.
		key = strings.ToLower(key)
		if err != nil || !ok || seen[key] {
			return false
		}
		seen[key] = true
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return false
		}
	}
	last, err := decoder.Token()
	if err != nil || last != json.Delim('}') {
		return false
	}
	_, err = decoder.Token()
	return err == io.EOF
}

func validIdempotencyKey(value string) bool {
	if len(value) == 0 || len(value) > 200 {
		return false
	}
	for _, ch := range value {
		if ch < '!' || ch > '~' {
			return false
		}
	}
	return true
}

type correlationKey struct{}

func correlationID(r *http.Request) string {
	id, _ := r.Context().Value(correlationKey{}).(string)
	return id
}

func metadata(r *http.Request) application.Metadata {
	return application.Metadata{CorrelationID: correlationID(r)}
}

func validCorrelationID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, ch := range value {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || strings.ContainsRune("-_.:", ch)) {
			return false
		}
	}
	return true
}

func (h *handler) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		id := r.Header.Get("X-Correlation-ID")
		if !validCorrelationID(id) {
			id = rand.Text()
		}
		ctx, cancel := context.WithTimeout(r.Context(), h.options.RequestTimeout)
		defer cancel()
		r = r.WithContext(context.WithValue(ctx, correlationKey{}, id))
		w.Header().Set("X-Correlation-ID", id)
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		recorder := &responseStatus{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		// Log only a route pattern; a raw URL or body can contain sensitive data.
		h.options.Logger.InfoContext(ctx, "http request completed", "correlationId", id,
			"method", r.Method, "route", r.Pattern, "status", recorder.status,
			"durationMs", time.Since(started).Milliseconds())
	})
}

type responseStatus struct {
	http.ResponseWriter
	status  int
	written bool
}

func (w *responseStatus) WriteHeader(status int) {
	if !w.written {
		w.status = status
		w.written = true
		w.ResponseWriter.WriteHeader(status)
	}
}

func (w *responseStatus) Write(body []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func (w *responseStatus) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Code: code, Message: message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	// Encode before committing headers so an invalid internal value cannot
	// produce a successful status followed by an incomplete JSON response.
	body, err := json.Marshal(value)
	if err != nil {
		status = http.StatusServiceUnavailable
		body = []byte(`{"code":"UNAVAILABLE","message":"Response temporarily unavailable"}`)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}
