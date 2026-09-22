package application

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cadmax/backend-challenge-go/internal/domain"
)

type Service struct {
	repository      Repository
	maxAttempts     int
	referenceTTL    time.Duration
	pendingObserver func(Result, time.Duration)
}

type Option func(*Service)

// WithPendingObserver reports only committed worker results. Instrumentation
// must remain quick and must not re-enter ResumePending.
func WithPendingObserver(observer func(Result, time.Duration)) Option {
	return func(s *Service) { s.pendingObserver = observer }
}

func WithReferencePolicy(maxAttempts int, ttl time.Duration) Option {
	return func(s *Service) {
		if maxAttempts > 0 {
			s.maxAttempts = maxAttempts
		}
		if ttl > 0 {
			s.referenceTTL = ttl
		}
	}
}

func NewService(repository Repository, options ...Option) *Service {
	s := &Service{
		repository:   repository,
		maxAttempts:  8,
		referenceTTL: 24 * time.Hour,
	}
	for _, option := range options {
		option(s)
	}
	return s
}

// NewID returns a random UUID without coupling the domain to a UUID library.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("operating system randomness unavailable")
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	return err == nil
}

// PayloadHash hashes lexicographically ordered JSON object keys. Money has
// already normalized exact decimal strings; transport data and keys are absent.
func PayloadHash(command Command) (string, error) {
	if err := command.Money.Validate(); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	payload := map[string]any{
		"providerId":                     command.ProviderID,
		"externalTransactionId":          command.ExternalTransactionID,
		"playerId":                       strings.ToLower(command.PlayerID),
		"walletId":                       strings.ToLower(command.WalletID),
		"roundId":                        command.RoundID,
		"gameId":                         command.GameID,
		"kind":                           command.Kind,
		"money":                          command.Money,
		"referenceExternalTransactionId": command.ReferenceExternalTransactionID,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("%w: canonical payload: %v", ErrInvalidInput, err)
	}
	hash := sha256.Sum256(b)
	return hex.EncodeToString(hash[:]), nil
}

func wagerInput(command Command, hash string) domain.WagerInput {
	return domain.WagerInput{
		ID:                             NewID(),
		ProviderID:                     command.ProviderID,
		ExternalTransactionID:          command.ExternalTransactionID,
		IdempotencyKey:                 command.IdempotencyKey,
		PayloadHash:                    hash,
		WalletID:                       command.WalletID,
		PlayerID:                       command.PlayerID,
		RoundID:                        command.RoundID,
		GameID:                         command.GameID,
		Kind:                           domain.Kind(command.Kind),
		Money:                          command.Money,
		ReferenceExternalTransactionID: command.ReferenceExternalTransactionID,
	}
}

func transactionResult(transaction domain.TransactionSnapshot, replay bool) Result {
	return Result{
		TransactionID:    transaction.ID,
		Status:           string(transaction.Status),
		Balance:          transaction.ResultBalance,
		FailureCode:      transaction.FailureCode,
		IdempotentReplay: replay,
	}
}

func walletView(wallet domain.WalletSnapshot) WalletView {
	return WalletView{
		ID:       wallet.ID,
		PlayerID: wallet.PlayerID,
		Balance:  wallet.Balance,
		Version:  wallet.Version,
	}
}

func (s *Service) OpenWallet(ctx context.Context, playerID string, initial domain.Money, meta Metadata) (WalletView, error) {
	if !validUUID(playerID) {
		return WalletView{}, fmt.Errorf("%w: playerId must be a UUID", ErrInvalidInput)
	}
	playerID = strings.ToLower(playerID)
	now := time.Now().UTC()
	wallet, err := domain.NewWallet(NewID(), playerID, initial, now)
	if err != nil {
		return WalletView{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	if meta.CorrelationID == "" {
		meta.CorrelationID = NewID()
	}
	err = s.repository.Within(ctx, func(unitOfWork UnitOfWork) error {
		if err := unitOfWork.InsertWallet(ctx, wallet.Snapshot()); err != nil {
			return err
		}
		if initial.MinorUnits() == 0 {
			return nil
		}
		transaction, err := domain.NewOpeningTransaction(NewID(), wallet.Snapshot().ID, playerID, initial, now)
		if err != nil {
			return err
		}
		if err := transaction.Process(initial, "", now); err != nil {
			return err
		}
		record := Record{
			Transaction:       transaction.Snapshot(),
			NextAttemptAt:     now,
			ReferenceDeadline: now,
			Metadata:          meta,
		}
		if err := unitOfWork.InsertTransaction(ctx, record); err != nil {
			return err
		}
		zero, _ := domain.Zero(initial.Currency())
		entry, err := domain.NewLedgerEntry(NewID(), wallet.Snapshot().ID, transaction.Snapshot().ID, domain.Credit, initial, zero, initial, now)
		if err != nil {
			return err
		}
		if err := insertLedger(ctx, unitOfWork, entry.Snapshot(), 1); err != nil {
			return err
		}
		if err := recordProcessedEvent(ctx, unitOfWork, record); err != nil {
			return err
		}
		return recordBalanceChangedEvent(ctx, unitOfWork, meta, entry.Snapshot(), 1)
	})
	return walletView(wallet.Snapshot()), err
}

func (s *Service) GetWallet(ctx context.Context, id string) (WalletView, error) {
	if !validUUID(id) {
		return WalletView{}, ErrInvalidInput
	}
	return s.repository.Wallet(ctx, id)
}

func (s *Service) Ledger(ctx context.Context, id, cursor string, limit int) (LedgerPage, error) {
	if !validUUID(id) || limit < 1 || limit > 100 {
		return LedgerPage{}, ErrInvalidInput
	}
	return s.repository.Ledger(ctx, id, cursor, limit)
}

func (s *Service) Reconcile(ctx context.Context, id string) (Reconciliation, error) {
	if !validUUID(id) {
		return Reconciliation{}, ErrInvalidInput
	}
	return s.repository.Reconcile(ctx, id)
}

func (s *Service) GetTransaction(ctx context.Context, provider, id string) (Result, error) {
	if !validUUID(id) {
		return Result{}, ErrInvalidInput
	}
	return s.repository.Transaction(ctx, provider, id, false)
}

func (s *Service) GetExternalTransaction(ctx context.Context, provider, id string) (Result, error) {
	return s.repository.Transaction(ctx, provider, id, true)
}

func (s *Service) Process(ctx context.Context, command Command, meta Metadata) (Result, error) {
	return s.processCommand(ctx, command, meta, nil)
}

func (s *Service) ProcessInbox(ctx context.Context, envelope Envelope, meta Metadata) (Result, error) {
	if strings.TrimSpace(envelope.MessageID) == "" || len(envelope.MessageID) > 255 {
		return Result{}, fmt.Errorf("%w: invalid message envelope", ErrInvalidInput)
	}
	if envelope.Type != "WagerTransactionRequested" || envelope.OccurredAt.IsZero() {
		return Result{}, fmt.Errorf("%w: invalid message envelope", ErrInvalidInput)
	}
	if meta.CausationID == "" {
		meta.CausationID = envelope.MessageID
	}
	return s.processCommand(ctx, envelope.Data, meta, &envelope)
}

func (s *Service) processCommand(ctx context.Context, command Command, meta Metadata, envelope *Envelope) (Result, error) {
	if !validUUID(command.WalletID) || !validUUID(command.PlayerID) {
		return Result{}, fmt.Errorf("%w: walletId and playerId must be UUIDs", ErrInvalidInput)
	}
	command.WalletID, command.PlayerID = strings.ToLower(command.WalletID), strings.ToLower(command.PlayerID)
	if envelope != nil {
		copy := *envelope
		copy.Data = command
		envelope = &copy
	}
	hash, err := PayloadHash(command)
	if err != nil {
		return Result{}, err
	}
	now := time.Now().UTC()
	transaction, err := domain.NewWagerTransaction(wagerInput(command, hash), now)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	if meta.CorrelationID == "" {
		meta.CorrelationID = NewID()
	}
	var output Result
	err = s.repository.Within(ctx, func(unitOfWork UnitOfWork) error {
		if envelope != nil {
			// The inbox protects the complete normalized envelope, including its
			// key and occurrence time; business idempotency deliberately does not.
			body, err := json.Marshal(envelope)
			if err != nil {
				return err
			}
			digest := sha256.Sum256(body)
			entry, err := unitOfWork.ClaimInbox(ctx, envelope.MessageID, hex.EncodeToString(digest[:]))
			if err != nil {
				return err
			}
			if entry != nil && entry.TransactionID != "" {
				record, err := unitOfWork.GetTransaction(ctx, entry.TransactionID)
				if err != nil {
					return err
				}
				output = transactionResult(record.Transaction, true)
				return nil
			}
		}
		if err := unitOfWork.LockIdentity(ctx, command.ProviderID, command.IdempotencyKey, command.ExternalTransactionID); err != nil {
			return err
		}
		record, keyHash, err := unitOfWork.FindKey(ctx, command.ProviderID, command.IdempotencyKey)
		if err != nil {
			return err
		}
		if record != nil && keyHash != hash {
			return ErrConflict
		}
		if record == nil {
			record, err = unitOfWork.FindExternal(ctx, command.ProviderID, command.ExternalTransactionID)
			if err != nil {
				return err
			}
		}
		if record != nil {
			if record.Transaction.PayloadHash != hash {
				return ErrConflict
			}
			if err := unitOfWork.BindKey(ctx, command.ProviderID, command.IdempotencyKey, record.Transaction.ID, hash); err != nil {
				return err
			}
			output = transactionResult(record.Transaction, true)
		} else {
			record = &Record{
				Transaction:       transaction.Snapshot(),
				NextAttemptAt:     now,
				ReferenceDeadline: now.Add(s.referenceTTL),
				Metadata:          meta,
			}
			// Lock the wallet before inserting its dependent transaction, so the
			// foreign-key key-share locks cannot deadlock with FOR UPDATE.
			wallet, err := unitOfWork.GetWallet(ctx, command.WalletID)
			if err != nil {
				return err
			}
			if err := unitOfWork.InsertTransaction(ctx, *record); err != nil {
				return err
			}
			if err := unitOfWork.BindKey(ctx, command.ProviderID, command.IdempotencyKey, transaction.Snapshot().ID, hash); err != nil {
				return err
			}
			if err := s.processLockedTransaction(ctx, unitOfWork, record, wallet); err != nil {
				return err
			}
			output = transactionResult(record.Transaction, false)
		}
		if envelope != nil {
			return unitOfWork.CompleteInbox(ctx, envelope.MessageID, output.TransactionID)
		}
		return nil
	})
	return output, err
}

func (s *Service) processLockedTransaction(ctx context.Context, unitOfWork UnitOfWork, record *Record, wallet *domain.Wallet) error {
	// Capture processing time after obtaining the wallet lock; a competing
	// writer may have advanced its timestamp while this request was waiting.
	now := time.Now().UTC()
	transaction, err := domain.RestoreWagerTransaction(record.Transaction)
	if err != nil {
		return err
	}
	var reference *domain.TransactionSnapshot
	alreadyReversed := false
	if record.Transaction.ReferenceExternalTransactionID != "" {
		ref, err := unitOfWork.FindExternal(ctx, record.Transaction.ProviderID, record.Transaction.ReferenceExternalTransactionID)
		if err != nil {
			return err
		}
		if ref != nil {
			reference = &ref.Transaction
			alreadyReversed, err = unitOfWork.IsReversed(ctx, ref.Transaction.ID)
			if err != nil {
				return err
			}
		}
	}
	movement, err := transaction.Evaluate(wallet.Snapshot(), reference, alreadyReversed)
	if errors.Is(err, domain.ErrReferencePending) {
		return s.waitForReference(ctx, unitOfWork, record, transaction, wallet.Snapshot().Balance, now)
	}
	if err != nil {
		var rule *domain.RuleError
		if errors.As(err, &rule) {
			return rejectTransaction(ctx, unitOfWork, record, transaction, wallet.Snapshot().Balance, rule.Code, now)
		}
		return err
	}
	before := wallet.Snapshot().Balance
	if movement.Direction == domain.Debit {
		err = wallet.Debit(movement.Money, now)
	}
	if movement.Direction == domain.Credit {
		err = wallet.Credit(movement.Money, now)
	}
	if err != nil {
		return rejectTransaction(ctx, unitOfWork, record, transaction, before, "BALANCE_LIMIT_EXCEEDED", now)
	}
	referenceID := ""
	if reference != nil {
		referenceID = reference.ID
	}
	if err := transaction.Process(wallet.Snapshot().Balance, referenceID, now); err != nil {
		return err
	}
	record.Transaction = transaction.Snapshot()
	if err := unitOfWork.SaveTransaction(ctx, *record); err != nil {
		return err
	}
	if movement.Direction != "" {
		if err := persistBalanceChange(ctx, unitOfWork, *record, wallet.Snapshot(), movement, before, now); err != nil {
			return err
		}
	}
	return recordProcessedEvent(ctx, unitOfWork, *record)
}

func (s *Service) waitForReference(ctx context.Context, unitOfWork UnitOfWork, record *Record, transaction *domain.WagerTransaction, balance domain.Money, now time.Time) error {
	if record.ReferenceAttempts >= s.maxAttempts || !now.Before(record.ReferenceDeadline) {
		return rejectTransaction(ctx, unitOfWork, record, transaction, balance, "REFERENCE_NOT_FOUND", now)
	}
	firstAttempt := record.Transaction.Status == domain.Pending
	if firstAttempt {
		if err := transaction.AwaitReference(now); err != nil {
			return err
		}
	}
	record.ReferenceAttempts++
	delay := time.Second * time.Duration(1<<min(record.ReferenceAttempts-1, 8))
	record.NextAttemptAt = now.Add(delay)
	record.Transaction = transaction.Snapshot()
	if err := unitOfWork.SaveTransaction(ctx, *record); err != nil {
		return err
	}
	if firstAttempt {
		event, err := domain.NewWagerTransactionPendingReference(eventMetadata(record.Metadata, now), record.Transaction)
		if err != nil {
			return err
		}
		return insertEvent(ctx, unitOfWork, event)
	}
	return nil
}

func persistBalanceChange(ctx context.Context, unitOfWork UnitOfWork, record Record, wallet domain.WalletSnapshot, movement domain.Movement, before domain.Money, now time.Time) error {
	if err := unitOfWork.SaveWallet(ctx, wallet); err != nil {
		return err
	}
	entry, err := domain.NewLedgerEntry(NewID(), record.Transaction.WalletID, record.Transaction.ID, movement.Direction, movement.Money, before, wallet.Balance, now)
	if err != nil {
		return err
	}
	if err := insertLedger(ctx, unitOfWork, entry.Snapshot(), wallet.Version); err != nil {
		return err
	}
	return recordBalanceChangedEvent(ctx, unitOfWork, record.Metadata, entry.Snapshot(), wallet.Version)
}

func rejectTransaction(ctx context.Context, unitOfWork UnitOfWork, record *Record, transaction *domain.WagerTransaction, balance domain.Money, code string, now time.Time) error {
	if err := transaction.Reject(code, &balance, now); err != nil {
		return err
	}
	record.Transaction = transaction.Snapshot()
	if err := unitOfWork.SaveTransaction(ctx, *record); err != nil {
		return err
	}
	event, err := domain.NewWagerTransactionRejected(eventMetadata(record.Metadata, now), record.Transaction)
	if err != nil {
		return err
	}
	return insertEvent(ctx, unitOfWork, event)
}

func (s *Service) ResumePending(ctx context.Context) (int, error) {
	ids, err := s.repository.Pending(ctx, 100)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, id := range ids {
		started := time.Now()
		handled := false
		var outcome Result
		err = s.repository.Within(ctx, func(unitOfWork UnitOfWork) error {
			record, err := unitOfWork.GetPendingTransaction(ctx, id)
			if err != nil {
				return err
			}
			if record == nil {
				return nil
			}
			now := time.Now().UTC()
			if record.Transaction.Status != domain.Pending && record.Transaction.Status != domain.PendingReference {
				return nil
			}
			if record.NextAttemptAt.After(now) {
				return nil
			}
			wallet, err := unitOfWork.GetAvailableWallet(ctx, record.Transaction.WalletID)
			if err != nil {
				return err
			}
			if wallet == nil {
				return nil
			}
			if err := s.processLockedTransaction(ctx, unitOfWork, record, wallet); err != nil {
				return err
			}
			handled = true
			outcome = transactionResult(record.Transaction, false)
			return nil
		})
		if err != nil {
			return count, err
		}
		if handled {
			count++
			if s.pendingObserver != nil {
				s.pendingObserver(outcome, time.Since(started))
			}
		}
	}
	return count, nil
}

func eventMetadata(meta Metadata, now time.Time) domain.EventMetadata {
	return domain.EventMetadata{
		EventID:       NewID(),
		CorrelationID: meta.CorrelationID,
		CausationID:   meta.CausationID,
		OccurredAt:    now,
	}
}

func insertEvent(ctx context.Context, unitOfWork UnitOfWork, event domain.Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return unitOfWork.InsertEvent(ctx, Event{
		ID:          event.ID(),
		AggregateID: event.AggregateID(),
		Type:        event.Type(),
		OccurredAt:  event.OccurredAt(),
		Payload:     body,
	})
}

func recordProcessedEvent(ctx context.Context, unitOfWork UnitOfWork, record Record) error {
	event, err := domain.NewWagerTransactionProcessed(eventMetadata(record.Metadata, record.Transaction.UpdatedAt), record.Transaction)
	if err != nil {
		return err
	}
	return insertEvent(ctx, unitOfWork, event)
}

func recordBalanceChangedEvent(ctx context.Context, unitOfWork UnitOfWork, meta Metadata, entry domain.LedgerSnapshot, version int64) error {
	event, err := domain.NewWalletBalanceChanged(eventMetadata(meta, entry.CreatedAt), entry, version)
	if err != nil {
		return err
	}
	return insertEvent(ctx, unitOfWork, event)
}

func insertLedger(ctx context.Context, unitOfWork UnitOfWork, entry domain.LedgerSnapshot, version int64) error {
	return unitOfWork.InsertLedger(ctx, LedgerView{
		ID:            entry.ID,
		WalletID:      entry.WalletID,
		TransactionID: entry.TransactionID,
		Direction:     string(entry.Direction),
		Money:         entry.Money,
		BalanceBefore: entry.BalanceBefore,
		BalanceAfter:  entry.BalanceAfter,
		CreatedAt:     entry.CreatedAt,
	}, version)
}
