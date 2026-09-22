package application

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/cadmax/backend-challenge-go/internal/domain"
)

func TestPayloadHashNormalizesMoneyAndExcludesTransportKey(t *testing.T) {
	first, _ := domain.NewMoney("25", "BRL")
	second, _ := domain.NewMoney("25.00", "BRL")
	a := Command{ProviderID: "provider-a", ExternalTransactionID: "external", IdempotencyKey: "first", PlayerID: "player", WalletID: "wallet", RoundID: "round", GameID: "game", Kind: "BET", Money: first}
	b := a
	b.IdempotencyKey = "second"
	b.Money = second
	hashA, err := PayloadHash(a)
	if err != nil {
		t.Fatal(err)
	}
	hashB, err := PayloadHash(b)
	if err != nil {
		t.Fatal(err)
	}
	if hashA != hashB {
		t.Fatal("equivalent monetary payload changed hash")
	}
	b.GameID = "another-game"
	hashB, err = PayloadHash(b)
	if err != nil {
		t.Fatal(err)
	}
	if hashA == hashB {
		t.Fatal("different business payload preserved hash")
	}
}

func TestPayloadHashHasExactSortedCanonicalFields(t *testing.T) {
	value, _ := domain.NewMoney("25.00", "BRL")
	c := Command{ProviderID: "provider-a", ExternalTransactionID: "external", IdempotencyKey: "excluded", PlayerID: "AAAAAAAA-0000-0000-0000-000000000000", WalletID: "BBBBBBBB-0000-0000-0000-000000000000", RoundID: "round", GameID: "game", Kind: "BET", Money: value}
	canonical := `{"externalTransactionId":"external","gameId":"game","kind":"BET","money":{"amount":"25.00","currency":"BRL"},"playerId":"aaaaaaaa-0000-0000-0000-000000000000","providerId":"provider-a","referenceExternalTransactionId":"","roundId":"round","walletId":"bbbbbbbb-0000-0000-0000-000000000000"}`
	want := sha256.Sum256([]byte(canonical))
	got, err := PayloadHash(c)
	if err != nil {
		t.Fatal(err)
	}
	if got != hex.EncodeToString(want[:]) {
		t.Fatalf("unexpected canonical digest %s", got)
	}
}

func TestPayloadHashRejectsUninitializedMoney(t *testing.T) {
	if _, err := PayloadHash(Command{}); err == nil {
		t.Fatal("uninitialized money accepted")
	}
}
