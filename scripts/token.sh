#!/bin/sh
set -eu
client=${1:-provider-a}
case "$client" in
    provider-a) secret=local-provider-a-secret ;;
    provider-b) secret=local-provider-b-secret ;;
    provider-expired) secret=local-provider-expired-secret ;;
    internal-service) secret=local-internal-secret ;;
    *) echo 'Usage: token.sh [provider-a|provider-b|provider-expired|internal-service]' >&2; exit 2 ;;
esac
issuer=${OIDC_ISSUER_URL:-http://localhost:${KEYCLOAK_PORT:-8081}/realms/jungle}
curl --fail --silent --show-error "$issuer/protocol/openid-connect/token" \
    --data-urlencode grant_type=client_credentials \
    --data-urlencode "client_id=$client" \
    --data-urlencode "client_secret=$secret" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'
