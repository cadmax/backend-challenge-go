#!/bin/sh
set -eu

# Only the local Compose stack mounts this file. Production supplies its own IAM credentials.
if [ -n "${BROKER_CREDENTIALS_FILE:-}" ]; then
    set -a
    . "$BROKER_CREDENTIALS_FILE"
    set +a
fi
exec "$@"
