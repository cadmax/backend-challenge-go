package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"maps"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestVerifierChecksSignedClaims(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{publicJWK(key, "key-one")}})
	}))
	defer server.Close()
	const issuer = "https://issuer.example/realms/jungle"
	verifier, err := New(context.Background(), issuer, "wagering-api", server.URL)
	if err != nil {
		t.Fatal(err)
	}
	baseClaims := func() map[string]any {
		return map[string]any{
			"iss": issuer, "sub": "provider-service", "aud": []string{"account", "wagering-api"},
			"exp": time.Now().Add(time.Hour).Unix(), "nbf": time.Now().Add(-time.Minute).Unix(),
			"provider_id": "provider-a", "realm_access": map[string]any{"roles": []string{RoleProvider}},
		}
	}
	valid := signedToken(t, key, "key-one", baseClaims())
	principal, err := verifier.Verify(context.Background(), valid)
	if err != nil {
		t.Fatal(err)
	}
	if principal.Subject != "provider-service" || principal.ProviderID != "provider-a" || !principal.HasRole(RoleProvider) || principal.HasRole(RoleInternal) {
		t.Fatalf("unexpected principal: %+v", principal)
	}
	if _, err := verifier.Verify(context.Background(), valid); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("expected cached JWKS; requests=%d", hits.Load())
	}

	cases := []struct {
		name string
		edit func(map[string]any)
	}{
		{"issuer", func(c map[string]any) { c["iss"] = "https://attacker.example" }},
		{"audience", func(c map[string]any) { c["aud"] = "another-api" }},
		{"expired", func(c map[string]any) { c["exp"] = time.Now().Add(-time.Second).Unix() }},
		{"missing expiry", func(c map[string]any) { delete(c, "exp") }},
		{"activation within library leeway", func(c map[string]any) { c["nbf"] = time.Now().Add(time.Minute).Unix() }},
		{"activation far future", func(c map[string]any) { c["nbf"] = time.Now().Add(time.Hour).Unix() }},
		{"activation malformed", func(c map[string]any) { c["nbf"] = "tomorrow" }},
		{"empty subject", func(c map[string]any) { c["sub"] = "" }},
		{"malformed roles", func(c map[string]any) { c["realm_access"] = map[string]string{"roles": RoleProvider} }},
		{"malformed provider", func(c map[string]any) { c["provider_id"] = []string{"provider-a"} }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			claims := baseClaims()
			tt.edit(claims)
			_, err := verifier.Verify(context.Background(), signedToken(t, key, "key-one", claims))
			if !errors.Is(err, ErrInvalidToken) {
				t.Fatalf("got %v, want invalid token", err)
			}
		})
	}
	for name, token := range map[string]string{
		"wrong signature": signedToken(t, otherKey, "key-one", baseClaims()),
		"malformed":       "not-a-jwt", "empty": "", "oversized": strings.Repeat("a", 16385),
		"unsigned": base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`)) + "." + base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"https://issuer.example/realms/jungle","sub":"service","aud":"wagering-api","exp":9999999999}`)) + ".",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := verifier.Verify(context.Background(), token); !errors.Is(err, ErrInvalidToken) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestVerifierRefreshesKeysAfterRotation(t *testing.T) {
	first, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	second, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var rotated atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		key, kid := first, "first"
		if rotated.Load() {
			key, kid = second, "second"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{publicJWK(key, kid)}})
	}))
	defer server.Close()
	verifier, err := New(context.Background(), "https://issuer.example", "wagering-api", server.URL)
	if err != nil {
		t.Fatal(err)
	}
	claims := map[string]any{"iss": "https://issuer.example", "sub": "service", "aud": "wagering-api", "exp": time.Now().Add(time.Hour).Unix()}
	if _, err := verifier.Verify(context.Background(), signedToken(t, first, "first", claims)); err != nil {
		t.Fatal(err)
	}
	rotated.Store(true)
	if _, err := verifier.Verify(context.Background(), signedToken(t, second, "second", claims)); err != nil {
		t.Fatalf("rotated key rejected: %v", err)
	}
}

func TestVerifierConfiguration(t *testing.T) {
	for _, tt := range []struct{ issuer, audience, jwks string }{
		{"", "api", "http://localhost/certs"},
		{"https://issuer.example", "", "http://localhost/certs"},
		{"https://issuer.example", "api", "file:///tmp/certs"},
		{"https://issuer.example", "api", "https://user:secret@example.com/certs"},
		{"https://issuer.example#fragment", "api", "https://example.com/certs"},
	} {
		if _, err := New(context.Background(), tt.issuer, tt.audience, tt.jwks); err == nil {
			t.Fatalf("accepted invalid configuration: %+v", tt)
		}
	}
}

func TestReadyRequiresUsableJWKS(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	valid, err := json.Marshal(map[string]any{"keys": []any{publicJWK(key, "active")}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		status int
		body   string
		valid  bool
	}{
		{"available", 200, string(valid), true},
		{"empty keys", 200, `{"keys":[]}`, false},
		{"invalid JSON", 200, `{`, false},
		{"trailing JSON", 200, string(valid) + `{}`, false},
		{"unavailable", 503, string(valid), false},
		{"wrong key type", 200, `{"keys":[{"kty":"oct","k":"secret"}]}`, false},
		{"oversized", 200, strings.Repeat(" ", (1<<20)+1), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()
			verifier, err := New(context.Background(), "https://issuer.example", "api", server.URL)
			if err != nil {
				t.Fatal(err)
			}
			if err := verifier.Ready(context.Background()); (err == nil) != tt.valid {
				t.Fatalf("ready=%v expected valid=%v", err, tt.valid)
			}
		})
	}
	t.Run("canceled context", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(valid) }))
		defer server.Close()
		verifier, err := New(context.Background(), "https://issuer.example", "api", server.URL)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := verifier.Ready(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestReadyPreservesSigningKeyPolicyForMixedJWKS(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	valid := publicJWK(key, "active")
	changed := func(field, value string) map[string]string {
		candidate := maps.Clone(valid)
		candidate[field] = value
		return candidate
	}
	for _, tt := range []struct {
		name  string
		keys  []map[string]string
		ready bool
	}{
		{"optional algorithm", []map[string]string{changed("alg", "")}, true},
		{"optional use", []map[string]string{changed("use", "")}, true},
		{"wrong algorithm", []map[string]string{changed("alg", "RS512")}, false},
		{"encryption key", []map[string]string{changed("use", "enc")}, false},
		{"small modulus", []map[string]string{changed("n", "AQAB")}, false},
		{"missing modulus", []map[string]string{changed("n", "")}, false},
		{"missing exponent", []map[string]string{changed("e", "")}, false},
		{"invalid base64", []map[string]string{changed("n", "*")}, false},
		{"unsupported key before valid key", []map[string]string{{"kty": "future"}, valid}, true},
		{"malformed key before valid key", []map[string]string{changed("n", "*"), valid}, true},
		{"unusable key after valid key", []map[string]string{valid, changed("use", "enc")}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"keys": tt.keys})
			}))
			defer server.Close()
			verifier, err := New(context.Background(), "https://issuer.example", "api", server.URL)
			if err != nil {
				t.Fatal(err)
			}
			if err := verifier.Ready(context.Background()); (err == nil) != tt.ready {
				t.Fatalf("ready error=%v, want ready=%v", err, tt.ready)
			}
		})
	}
}

func publicJWK(key *rsa.PrivateKey, id string) map[string]string {
	return map[string]string{
		"kty": "RSA", "kid": id, "alg": "RS256", "use": "sig",
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}
}

func signedToken(t *testing.T, key *rsa.PrivateKey, id string, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": id, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
}
