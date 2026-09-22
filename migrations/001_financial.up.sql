CREATE TABLE wallets (
    id uuid PRIMARY KEY,
    player_id uuid NOT NULL,
    currency text NOT NULL CHECK (currency IN ('BRL', 'USD', 'EUR')),
    balance bigint NOT NULL CHECK (balance >= 0),
    version bigint NOT NULL CHECK (version >= 1),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (player_id, currency)
);

CREATE TABLE wager_transactions (
    id uuid PRIMARY KEY,
    provider_id text,
    external_transaction_id text,
    idempotency_key text,
    payload_hash text,
    wallet_id uuid NOT NULL REFERENCES wallets(id),
    player_id uuid NOT NULL,
    round_id text,
    game_id text,
    kind text NOT NULL CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
    amount bigint NOT NULL CHECK (amount >= 0),
    currency text NOT NULL CHECK (currency IN ('BRL', 'USD', 'EUR')),
    reference_external_transaction_id text,
    reference_transaction_id uuid REFERENCES wager_transactions(id),
    status text NOT NULL CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
    failure_code text,
    result_balance bigint CHECK (result_balance >= 0),
    result_currency text CHECK (result_currency IN ('BRL','USD','EUR')),
    reference_attempts integer NOT NULL DEFAULT 0 CHECK (reference_attempts >= 0),
    next_attempt_at timestamptz NOT NULL,
    reference_deadline timestamptz NOT NULL,
    correlation_id text NOT NULL,
    causation_id text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (provider_id, external_transaction_id),
    UNIQUE (provider_id, idempotency_key),
    CHECK ((result_balance IS NULL) = (result_currency IS NULL)),
    CHECK ((status IN ('REJECTED','FAILED')) = (failure_code IS NOT NULL)),
    CHECK (status <> 'PROCESSED' OR result_balance IS NOT NULL),
    CHECK ((kind = 'LOSS' AND amount = 0) OR (kind <> 'LOSS' AND amount > 0)),
    CHECK ((kind = 'OPENING' AND provider_id IS NULL AND external_transaction_id IS NULL
        AND idempotency_key IS NULL AND payload_hash IS NULL AND round_id IS NULL
        AND game_id IS NULL AND reference_external_transaction_id IS NULL
        AND reference_transaction_id IS NULL AND status = 'PROCESSED')
      OR (kind <> 'OPENING' AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL
        AND idempotency_key IS NOT NULL AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL
        AND length(provider_id) BETWEEN 1 AND 255
        AND length(external_transaction_id) BETWEEN 1 AND 255
        AND length(idempotency_key) BETWEEN 1 AND 255
        AND length(payload_hash) = 64 AND length(round_id) BETWEEN 1 AND 255
        AND length(game_id) BETWEEN 1 AND 255)),
    CHECK (kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
    CHECK (kind NOT IN ('BET','LOSS') OR reference_external_transaction_id IS NULL),
    CHECK (reference_external_transaction_id IS NOT NULL OR reference_transaction_id IS NULL),
    CHECK (status <> 'PROCESSED' OR reference_external_transaction_id IS NULL OR reference_transaction_id IS NOT NULL)
);
CREATE UNIQUE INDEX one_opening_per_wallet ON wager_transactions(wallet_id) WHERE kind = 'OPENING';
CREATE UNIQUE INDEX one_successful_reversal ON wager_transactions(reference_transaction_id)
    WHERE kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED';
CREATE INDEX pending_transactions_due ON wager_transactions(next_attempt_at, id)
    WHERE status IN ('PENDING','PENDING_REFERENCE');
CREATE INDEX wallet_transactions ON wager_transactions(wallet_id);

-- Aliases retain the meaning of every submitted key, including a replay using
-- another key for the same provider/external ID.
CREATE TABLE idempotency_keys (
    provider_id text NOT NULL,
    idempotency_key text NOT NULL,
    transaction_id uuid NOT NULL REFERENCES wager_transactions(id),
    payload_hash text NOT NULL CHECK (length(payload_hash) = 64),
    PRIMARY KEY (provider_id, idempotency_key)
);

CREATE TABLE wallet_ledger (
    id uuid PRIMARY KEY,
    wallet_id uuid NOT NULL REFERENCES wallets(id),
    transaction_id uuid NOT NULL REFERENCES wager_transactions(id),
    direction text NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
    amount bigint NOT NULL CHECK (amount > 0),
    currency text NOT NULL CHECK (currency IN ('BRL','USD','EUR')),
    balance_before bigint NOT NULL CHECK (balance_before >= 0),
    balance_after bigint NOT NULL CHECK (balance_after >= 0),
    wallet_version bigint NOT NULL CHECK (wallet_version >= 1),
    created_at timestamptz NOT NULL,
    UNIQUE (wallet_id, transaction_id),
    UNIQUE (wallet_id, wallet_version),
    CHECK (balance_after::numeric = balance_before::numeric +
        CASE direction WHEN 'CREDIT' THEN amount::numeric ELSE -amount::numeric END)
);
CREATE INDEX ledger_page ON wallet_ledger(wallet_id, created_at, id);

CREATE TABLE inbox (
    consumer_name text NOT NULL,
    message_id text NOT NULL,
    payload_hash text NOT NULL CHECK (length(payload_hash) = 64),
    transaction_id uuid REFERENCES wager_transactions(id),
    received_at timestamptz NOT NULL,
    completed_at timestamptz,
    PRIMARY KEY (consumer_name, message_id),
    CHECK ((completed_at IS NULL) = (transaction_id IS NULL))
);

CREATE TABLE outbox (
    event_id uuid PRIMARY KEY,
    aggregate_id uuid NOT NULL REFERENCES wallets(id),
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_due ON outbox(next_attempt_at,event_id) WHERE published_at IS NULL;

CREATE FUNCTION refuse_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER immutable_ledger BEFORE UPDATE OR DELETE ON wallet_ledger FOR EACH ROW EXECUTE FUNCTION refuse_mutation();
CREATE TRIGGER immutable_keys BEFORE UPDATE OR DELETE ON idempotency_keys FOR EACH ROW EXECUTE FUNCTION refuse_mutation();
CREATE TRIGGER no_transaction_delete BEFORE DELETE ON wager_transactions FOR EACH ROW EXECUTE FUNCTION refuse_mutation();
CREATE TRIGGER no_wallet_delete BEFORE DELETE ON wallets FOR EACH ROW EXECUTE FUNCTION refuse_mutation();
CREATE TRIGGER no_outbox_delete BEFORE DELETE ON outbox FOR EACH ROW EXECUTE FUNCTION refuse_mutation();
CREATE TRIGGER no_inbox_delete BEFORE DELETE ON inbox FOR EACH ROW EXECUTE FUNCTION refuse_mutation();

CREATE FUNCTION protect_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
        RAISE EXCEPTION 'terminal transactions are immutable' USING ERRCODE = '23514';
    END IF;
    IF (to_jsonb(NEW) - ARRAY['status','reference_transaction_id','failure_code','result_balance','result_currency','reference_attempts','next_attempt_at','updated_at'])
       IS DISTINCT FROM
       (to_jsonb(OLD) - ARRAY['status','reference_transaction_id','failure_code','result_balance','result_currency','reference_attempts','next_attempt_at','updated_at']) THEN
        RAISE EXCEPTION 'transaction identity and financial input are immutable' USING ERRCODE = '23514';
    END IF;
    IF OLD.status = 'PENDING_REFERENCE' AND NEW.status = 'PENDING' THEN
        RAISE EXCEPTION 'invalid state transition' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER protect_transaction BEFORE UPDATE ON wager_transactions FOR EACH ROW EXECUTE FUNCTION protect_transaction();

CREATE FUNCTION check_terminal_result() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.status='PROCESSED' AND NEW.kind='LOSS') OR NEW.status='REJECTED' THEN
        IF NEW.result_balance IS NULL OR NOT EXISTS (
            SELECT 1 FROM wallets WHERE id=NEW.wallet_id AND balance=NEW.result_balance AND currency=NEW.result_currency
        ) THEN
            RAISE EXCEPTION 'terminal result must preserve observed wallet balance' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER check_terminal_result BEFORE INSERT OR UPDATE ON wager_transactions FOR EACH ROW EXECUTE FUNCTION check_terminal_result();

CREATE FUNCTION protect_wallet() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.id, NEW.player_id, NEW.currency, NEW.created_at) IS DISTINCT FROM
       (OLD.id, OLD.player_id, OLD.currency, OLD.created_at) THEN
        RAISE EXCEPTION 'wallet identity is immutable' USING ERRCODE = '23514';
    END IF;
    IF (NEW.balance <> OLD.balance AND NEW.version <> OLD.version + 1)
       OR (NEW.balance = OLD.balance AND NEW.version <> OLD.version) THEN
        RAISE EXCEPTION 'wallet version must track balance changes' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER protect_wallet BEFORE UPDATE ON wallets FOR EACH ROW EXECUTE FUNCTION protect_wallet();

CREATE FUNCTION protect_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.event_id, NEW.aggregate_id, NEW.event_type, NEW.payload, NEW.occurred_at)
       IS DISTINCT FROM (OLD.event_id, OLD.aggregate_id, OLD.event_type, OLD.payload, OLD.occurred_at) THEN
        RAISE EXCEPTION 'outbox snapshot is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER protect_outbox BEFORE UPDATE ON outbox FOR EACH ROW EXECUTE FUNCTION protect_outbox();

CREATE FUNCTION protect_inbox() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.completed_at IS NOT NULL OR (NEW.consumer_name,NEW.message_id,NEW.payload_hash,NEW.received_at)
       IS DISTINCT FROM (OLD.consumer_name,OLD.message_id,OLD.payload_hash,OLD.received_at) THEN
        RAISE EXCEPTION 'inbox identity or completion is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER protect_inbox BEFORE UPDATE ON inbox FOR EACH ROW EXECUTE FUNCTION protect_inbox();

CREATE FUNCTION check_key_binding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM wager_transactions WHERE id=NEW.transaction_id
        AND provider_id=NEW.provider_id AND payload_hash=NEW.payload_hash) THEN
        RAISE EXCEPTION 'idempotency key does not match its transaction' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER check_key_binding BEFORE INSERT ON idempotency_keys FOR EACH ROW EXECUTE FUNCTION check_key_binding();

-- Deferred checks see the complete financial commit. They reject unpaired
-- balance updates and processed transactions without the corresponding ledger.
CREATE FUNCTION check_financial_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    target uuid;
    w wallets%ROWTYPE;
    total numeric;
    changes bigint;
BEGIN
    IF TG_TABLE_NAME = 'wallets' THEN target := NEW.id; ELSE target := NEW.wallet_id; END IF;
    SELECT * INTO w FROM wallets WHERE id = target FOR UPDATE;
    SELECT coalesce(sum(CASE direction WHEN 'CREDIT' THEN amount::numeric ELSE -amount::numeric END),0),
           count(*) FILTER (WHERE wallet_version > 1)
      INTO total, changes FROM wallet_ledger WHERE wallet_id = target;
    IF w.balance::numeric <> total OR w.version <> changes + 1 THEN
        RAISE EXCEPTION 'wallet balance or version differs from ledger' USING ERRCODE = '23514';
    END IF;
    IF EXISTS (
        SELECT 1 FROM wager_transactions t LEFT JOIN wallet_ledger l ON l.transaction_id=t.id AND l.wallet_id=t.wallet_id
        WHERE t.wallet_id=target AND (
          (t.status='PROCESSED' AND t.kind<>'LOSS' AND l.id IS NULL)
          OR ((t.status<>'PROCESSED' OR t.kind='LOSS') AND l.id IS NOT NULL)
          OR (t.status='PROCESSED' AND (t.currency<>w.currency OR t.player_id<>w.player_id))
        )
    ) THEN
        RAISE EXCEPTION 'transaction and ledger disagree' USING ERRCODE = '23514';
    END IF;
    IF EXISTS (
        SELECT 1 FROM wallet_ledger l JOIN wager_transactions t ON t.id=l.transaction_id
        LEFT JOIN wager_transactions r ON r.id=t.reference_transaction_id
        LEFT JOIN wallet_ledger previous ON previous.wallet_id=l.wallet_id AND previous.wallet_version=l.wallet_version-1
        WHERE l.wallet_id=target AND (
          t.wallet_id<>l.wallet_id OR t.amount<>l.amount OR t.currency<>l.currency OR l.currency<>w.currency
          OR t.result_balance<>l.balance_after OR t.result_currency<>l.currency
          OR (t.kind='OPENING' AND (l.wallet_version<>1 OR l.balance_before<>0))
          OR (t.kind<>'OPENING' AND l.wallet_version<2)
          OR l.balance_before<>coalesce(previous.balance_after,0)
          OR (l.wallet_version>2 AND previous.id IS NULL)
          OR l.direction<>CASE WHEN t.kind='BET' THEN 'DEBIT'
             WHEN t.kind IN ('OPENING','WIN','REFUND') THEN 'CREDIT'
             WHEN t.kind='ROLLBACK' AND r.kind='BET' THEN 'CREDIT' ELSE 'DEBIT' END
        )
    ) THEN
        RAISE EXCEPTION 'ledger movement or sequence is inconsistent' USING ERRCODE = '23514';
    END IF;
    IF EXISTS (
        SELECT 1 FROM wager_transactions t LEFT JOIN wager_transactions r ON r.id=t.reference_transaction_id
        WHERE t.wallet_id=target AND t.status='PROCESSED' AND t.kind IN ('REFUND','ROLLBACK') AND (
           r.id IS NULL OR r.status<>'PROCESSED' OR r.wallet_id<>t.wallet_id OR r.player_id<>t.player_id
           OR r.provider_id<>t.provider_id OR r.currency<>t.currency OR r.round_id<>t.round_id OR r.amount<>t.amount
           OR r.external_transaction_id<>t.reference_external_transaction_id
           OR (t.kind='REFUND' AND r.kind<>'BET') OR (t.kind='ROLLBACK' AND r.kind NOT IN ('BET','WIN','REFUND'))
        )
    ) THEN
        RAISE EXCEPTION 'reversal reference is inconsistent' USING ERRCODE = '23514';
    END IF;
    IF EXISTS (
        SELECT 1 FROM wager_transactions t LEFT JOIN wager_transactions r ON r.id=t.reference_transaction_id
        WHERE t.wallet_id=target AND t.status='PROCESSED' AND t.kind='WIN' AND t.reference_external_transaction_id IS NOT NULL AND (
           r.id IS NULL OR r.status<>'PROCESSED' OR r.kind<>'BET' OR r.wallet_id<>t.wallet_id
           OR r.player_id<>t.player_id OR r.provider_id<>t.provider_id OR r.currency<>t.currency
           OR r.round_id<>t.round_id OR r.external_transaction_id<>t.reference_external_transaction_id
        )
    ) THEN
        RAISE EXCEPTION 'win reference is inconsistent' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER wallet_consistency AFTER INSERT OR UPDATE ON wallets DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_financial_consistency();
CREATE CONSTRAINT TRIGGER transaction_consistency AFTER INSERT OR UPDATE ON wager_transactions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_financial_consistency();
CREATE CONSTRAINT TRIGGER ledger_consistency AFTER INSERT ON wallet_ledger DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_financial_consistency();

DO $$
BEGIN
    IF current_user <> 'wager' AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname='wager') THEN
        GRANT USAGE ON SCHEMA public TO wager;
        GRANT SELECT ON wallets,wager_transactions,idempotency_keys,wallet_ledger,inbox,outbox TO wager;
        GRANT INSERT,UPDATE ON wallets,wager_transactions,inbox TO wager;
        GRANT INSERT ON wallet_ledger,idempotency_keys,outbox TO wager;
        GRANT UPDATE(attempts,next_attempt_at,published_at) ON outbox TO wager;
    END IF;
END;
$$;
