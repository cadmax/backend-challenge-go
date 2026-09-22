DROP TABLE IF EXISTS outbox, inbox, idempotency_keys, wallet_ledger, wager_transactions, wallets CASCADE;
DROP FUNCTION IF EXISTS check_financial_consistency(), check_key_binding(), check_terminal_result(), protect_inbox(), protect_outbox(), protect_wallet(), protect_transaction(), refuse_mutation();
