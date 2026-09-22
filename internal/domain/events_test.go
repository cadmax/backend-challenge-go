package domain_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/domain"
)

func eventMetadata() domain.EventMetadata {
	return domain.EventMetadata{
		EventID: "event-1", CorrelationID: "request-1", CausationID: "message-1", OccurredAt: epoch,
	}
}

func TestRequiredEventsHaveConcreteTypedVersionedEnvelopes(t *testing.T) {
	processed := wager(t, domain.Loss, "0")
	if err := processed.Process(money(t, "100"), "", epoch); err != nil {
		t.Fatal(err)
	}
	processedEvent, err := domain.NewWagerTransactionProcessed(eventMetadata(), processed.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	rejected := wager(t, domain.Bet, "200")
	if err := rejected.Reject(domain.CodeInsufficientFunds, nil, epoch); err != nil {
		t.Fatal(err)
	}
	rejectedEvent, err := domain.NewWagerTransactionRejected(eventMetadata(), rejected.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	pending := wager(t, domain.Refund, "25")
	if err := pending.AwaitReference(epoch); err != nil {
		t.Fatal(err)
	}
	pendingEvent, err := domain.NewWagerTransactionPendingReference(eventMetadata(), pending.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	entry, err := domain.NewLedgerEntry("entry-1", "wallet-1", "transaction-1", domain.Debit,
		money(t, "25"), money(t, "100"), money(t, "75"), epoch)
	if err != nil {
		t.Fatal(err)
	}
	balanceEvent, err := domain.NewWalletBalanceChanged(eventMetadata(), entry.Snapshot(), 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		event domain.Event
		kind  string
	}{
		{processedEvent, "WagerTransactionProcessed"},
		{rejectedEvent, "WagerTransactionRejected"},
		{pendingEvent, "WagerTransactionPendingReference"},
		{balanceEvent, "WalletBalanceChanged"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			encoded, err := json.Marshal(tc.event)
			if err != nil {
				t.Fatal(err)
			}
			var envelope struct {
				EventID, EventType, AggregateID, CorrelationID, CausationID, OccurredAt string
				Version                                                                 int
				Data                                                                    json.RawMessage
			}
			if err := json.Unmarshal(encoded, &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.EventID != "event-1" || envelope.EventType != tc.kind || envelope.AggregateID != "wallet-1" ||
				envelope.CorrelationID != "request-1" || envelope.CausationID != "message-1" || envelope.Version != 1 ||
				envelope.OccurredAt != "2026-09-22T12:00:00Z" || len(envelope.Data) == 0 {
				t.Fatalf("bad envelope %s", encoded)
			}
			if tc.event.ID() != envelope.EventID || tc.event.Type() != tc.kind || tc.event.AggregateID() != envelope.AggregateID || !tc.event.OccurredAt().Equal(epoch) {
				t.Fatal("event accessors differ from serialized identity")
			}
		})
	}
	if data := processedEvent.Data(); data.Kind != domain.Loss || data.Money.MinorUnits() != 0 || data.Status != domain.Processed {
		t.Fatal("LOSS must produce a processed event with zero money")
	}
	if data := rejectedEvent.Data(); data.FailureCode != domain.CodeInsufficientFunds {
		t.Fatal("rejected event omitted stable failure code")
	}
	if data := pendingEvent.Data(); data.Status != domain.PendingReference || data.ReferenceExternalTransactionID != "external-original" {
		t.Fatal("pending reference event omitted its dependency")
	}
	if data := balanceEvent.Data(); data.Direction != domain.Debit || data.Money.MinorUnits() != 2500 ||
		data.BalanceBefore.MinorUnits() != 10000 || data.BalanceAfter.MinorUnits() != 7500 || data.WalletVersion != 2 {
		t.Fatal("balance change event omitted ledger values")
	}
}

func TestEventSnapshotsCannotBeMutatedByCaller(t *testing.T) {
	tx := wager(t, domain.Bet, "25")
	if err := tx.Process(money(t, "75"), "", epoch); err != nil {
		t.Fatal(err)
	}
	snapshot := tx.Snapshot()
	meta := eventMetadata()
	event, err := domain.NewWagerTransactionProcessed(meta, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	before, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	*snapshot.ResultBalance = money(t, "999")
	snapshot.ProviderID = "another-provider"
	meta.EventID = "another-event"
	data := event.Data()
	data.TransactionID = "another-transaction"
	*data.Balance = money(t, "888")
	after, err := json.Marshal(event)
	if err != nil || string(before) != string(after) {
		t.Fatalf("event changed: %s -> %s (%v)", before, after, err)
	}
}

func TestOpeningEventsOmitExternalMetadataAndPreserveVersionOne(t *testing.T) {
	tx, err := domain.NewOpeningTransaction("opening-1", "wallet-1", "player-1", money(t, "100"), epoch)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Process(money(t, "100"), "", epoch); err != nil {
		t.Fatal(err)
	}
	meta := eventMetadata()
	meta.CausationID = ""
	meta.OccurredAt = epoch.In(time.FixedZone("BRT", -3*60*60))
	event, err := domain.NewWagerTransactionProcessed(meta, tx.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	for _, inapplicable := range []string{"providerId", "externalTransactionId", "idempotencyKey", "payloadHash", "roundId", "gameId", "causationId", "referenceExternalTransactionId"} {
		if strings.Contains(string(encoded), `"`+inapplicable+`"`) {
			t.Fatalf("opening contains inapplicable metadata %s: %s", inapplicable, encoded)
		}
	}
	if !strings.Contains(string(encoded), `"occurredAt":"2026-09-22T12:00:00Z"`) || event.OccurredAt().Location() != time.UTC {
		t.Fatalf("event timestamp not normalized to UTC: %s", encoded)
	}
	entry, err := domain.NewLedgerEntry("entry-1", "wallet-1", "opening-1", domain.Credit,
		money(t, "100"), money(t, "0"), money(t, "100"), epoch)
	if err != nil {
		t.Fatal(err)
	}
	balance, err := domain.NewWalletBalanceChanged(meta, entry.Snapshot(), 1)
	if err != nil || balance.Data().WalletVersion != 1 {
		t.Fatal("opening event must preserve initial wallet version", err)
	}
}

func TestEventsRejectInvalidStateAndMetadata(t *testing.T) {
	pending := wager(t, domain.Bet, "25")
	if _, err := domain.NewWagerTransactionProcessed(eventMetadata(), pending.Snapshot()); !errors.Is(err, domain.ErrInvalidEvent) {
		t.Fatal("pending transaction accepted as processed", err)
	}
	if _, err := domain.NewWagerTransactionRejected(eventMetadata(), pending.Snapshot()); !errors.Is(err, domain.ErrInvalidEvent) {
		t.Fatal("pending transaction accepted as rejected", err)
	}
	if _, err := domain.NewWagerTransactionPendingReference(eventMetadata(), pending.Snapshot()); !errors.Is(err, domain.ErrInvalidEvent) {
		t.Fatal("ordinary pending accepted as reference pending", err)
	}
	if err := pending.Process(money(t, "75"), "", epoch); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*domain.EventMetadata){
		"missing event ID":    func(m *domain.EventMetadata) { m.EventID = "" },
		"missing correlation": func(m *domain.EventMetadata) { m.CorrelationID = "" },
		"invalid causation":   func(m *domain.EventMetadata) { m.CausationID = " message" },
		"missing timestamp":   func(m *domain.EventMetadata) { m.OccurredAt = time.Time{} },
		"earlier timestamp":   func(m *domain.EventMetadata) { m.OccurredAt = epoch.Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			meta := eventMetadata()
			mutate(&meta)
			if _, err := domain.NewWagerTransactionProcessed(meta, pending.Snapshot()); !errors.Is(err, domain.ErrInvalidEvent) {
				t.Fatal("invalid metadata accepted", err)
			}
		})
	}
	for _, event := range []domain.Event{
		domain.WagerTransactionProcessed{}, domain.WagerTransactionRejected{},
		domain.WagerTransactionPendingReference{}, domain.WalletBalanceChanged{},
	} {
		if _, err := json.Marshal(event); !errors.Is(err, domain.ErrInvalidEvent) {
			t.Fatal("zero-value event serialized", err)
		}
	}
	if _, err := domain.NewWalletBalanceChanged(eventMetadata(), domain.LedgerSnapshot{}, 1); !errors.Is(err, domain.ErrInvalidEvent) {
		t.Fatal("zero-value ledger event accepted", err)
	}
	entry, err := domain.NewLedgerEntry("entry-1", "wallet-1", "transaction-1", domain.Debit,
		money(t, "25"), money(t, "100"), money(t, "75"), epoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := domain.NewWalletBalanceChanged(eventMetadata(), entry.Snapshot(), 0); !errors.Is(err, domain.ErrInvalidEvent) {
		t.Fatal("zero wallet version accepted", err)
	}
}
