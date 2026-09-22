#!/bin/sh
set -eu
export TEST_DATABASE_URL=${TEST_DATABASE_URL:-postgres://wager_owner:wager-owner-local@localhost:${POSTGRES_PORT:-55432}/wager?sslmode=disable}
export OIDC_ISSUER_URL=${OIDC_ISSUER_URL:-http://localhost:${KEYCLOAK_PORT:-8081}/realms/jungle}
export OIDC_JWKS_URL=${OIDC_JWKS_URL:-$OIDC_ISSUER_URL/protocol/openid-connect/certs}
export SQS_ENDPOINT=${SQS_ENDPOINT:-http://localhost:${LOCALSTACK_PORT:-4567}}
export AWS_REGION=us-east-1
# The test harness provisions and tears down isolated queues; application Compose
# containers use the scoped IAM identity instead of these local bootstrap credentials.
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
go test -race -tags=integration -count=1 -timeout=10m ./...
