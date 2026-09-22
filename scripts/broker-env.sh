#!/bin/sh
set -eu
case "${1:-app}" in
    app|ingress|events) identity=${1:-app} ;;
    *) echo 'Usage: broker-env.sh [app|ingress|events]' >&2; exit 2 ;;
esac
# The output is intended for eval in the caller shell and contains only local emulator credentials.
docker compose exec -T localstack cat "/run/wager-aws/$identity.env" |
    sed 's/^/export /'
