#!/bin/sh
set -eu
api=${API_URL:-http://localhost:${HTTP_PORT:-8080}}
curl --fail --silent --show-error "$api/health/live"
curl --fail --silent --show-error "$api/health/ready"
token=$(./scripts/token.sh internal-service)
player=$(python3 -c 'import uuid; print(uuid.uuid4())')
wallet=$(curl --fail --silent --show-error "$api/wallets" \
    -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
    -d "{\"playerId\":\"$player\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}")
printf '%s\n' "$wallet"
wallet_id=$(printf '%s' "$wallet" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl --fail --silent --show-error -X POST "$api/wallets/$wallet_id/reconciliation" \
    -H "Authorization: Bearer $token"
printf '\n'
