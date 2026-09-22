// Package auth verifies access tokens issued by the configured external IdP.
package auth

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-jose/go-jose/v4"
)

const (
	RoleInternal = "wallet:internal"
	RoleProvider = "wager:provider"
)

var ErrInvalidToken = errors.New("invalid access token")

// Principal contains only claims from a verified, signed token. The provider
// identity must never be inferred from a request body or an idempotency key.
type Principal struct {
	Subject    string
	ProviderID string
	Roles      []string
}

func (p Principal) HasRole(role string) bool {
	return slices.Contains(p.Roles, role)
}

type Verifier struct {
	tokens  *oidc.IDTokenVerifier
	issuer  string
	jwksURL string
	client  *http.Client
}

// New deliberately separates the public issuer from the JWKS address: Docker
// can reach Keycloak through its internal hostname without relaxing iss checks.
// Key rotation and the synchronized key cache are handled by go-oidc.
func New(ctx context.Context, issuer, audience, jwksURL string) (*Verifier, error) {
	for name, value := range map[string]string{"issuer": issuer, "JWKS URL": jwksURL} {
		u, err := url.Parse(value)
		if err != nil || u == nil {
			return nil, fmt.Errorf("invalid OIDC %s", name)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, fmt.Errorf("invalid OIDC %s", name)
		}
		if u.Host == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" {
			return nil, fmt.Errorf("invalid OIDC %s", name)
		}
	}
	if strings.TrimSpace(audience) == "" || audience != strings.TrimSpace(audience) {
		return nil, errors.New("OIDC audience is required")
	}
	// RemoteKeySet keeps a background context for key refreshes. Bound the HTTP
	// client as well as individual verification contexts so refresh cannot hang.
	client := &http.Client{Timeout: 5 * time.Second}
	ctx = oidc.ClientContext(ctx, client)
	keys := oidc.NewRemoteKeySet(ctx, jwksURL)
	return &Verifier{
		tokens: oidc.NewVerifier(issuer, keys, &oidc.Config{
			ClientID: audience, SupportedSigningAlgs: []string{oidc.RS256},
		}),
		issuer:  issuer,
		jwksURL: jwksURL,
		client:  client,
	}, nil
}

// Ready verifies the configured key endpoint during startup. It does not alter
// issuer validation or preload unverified tokens, and accepts only usable RSA
// signing keys for the configured RS256 token algorithm.
func (v *Verifier) Ready(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURL, nil)
	if err != nil {
		return errors.New("invalid JWKS request")
	}
	response, err := v.client.Do(request)
	if err != nil {
		return fmt.Errorf("fetch JWKS: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("JWKS endpoint returned status %d", response.StatusCode)
	}
	const maximum = 1 << 20
	body, err := io.ReadAll(io.LimitReader(response.Body, maximum+1))
	if err != nil || len(body) > maximum {
		return errors.New("invalid JWKS response size")
	}
	var keys struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if json.Unmarshal(body, &keys) != nil {
		return errors.New("invalid JWKS response")
	}
	for _, raw := range keys.Keys {
		// A rotated key set can include algorithms this API does not support.
		// Parse each key independently so one unusable key does not hide another.
		var key jose.JSONWebKey
		if json.Unmarshal(raw, &key) != nil || !key.Valid() {
			continue
		}
		if key.Algorithm != "" && key.Algorithm != "RS256" {
			continue
		}
		if key.Use != "" && key.Use != "sig" {
			continue
		}
		public, ok := key.Key.(*rsa.PublicKey)
		if ok && public.N.BitLen() >= 2048 {
			return nil
		}
	}
	return errors.New("JWKS endpoint has no supported signing keys")
}

func (v *Verifier) Verify(ctx context.Context, rawToken string) (Principal, error) {
	if len(rawToken) == 0 || len(rawToken) > 16384 {
		return Principal{}, ErrInvalidToken
	}
	token, err := v.tokens.Verify(ctx, rawToken)
	if err != nil || token.Issuer != v.issuer || token.Subject == "" {
		return Principal{}, ErrInvalidToken
	}
	var claims struct {
		ProviderID  string      `json:"provider_id"`
		NotBefore   json.Number `json:"nbf"`
		RealmAccess struct {
			Roles []string `json:"roles"`
		} `json:"realm_access"`
	}
	if err := token.Claims(&claims); err != nil {
		return Principal{}, ErrInvalidToken
	}
	// go-oidc allows five minutes of nbf clock skew. Financial operations use
	// the exact activation time instead; hosts must keep their clocks in sync.
	if claims.NotBefore != "" {
		nbf, err := claims.NotBefore.Int64()
		if err != nil || time.Now().Before(time.Unix(nbf, 0)) {
			return Principal{}, ErrInvalidToken
		}
	}
	return Principal{
		Subject: token.Subject, ProviderID: claims.ProviderID,
		Roles: append([]string(nil), claims.RealmAccess.Roles...),
	}, nil
}
