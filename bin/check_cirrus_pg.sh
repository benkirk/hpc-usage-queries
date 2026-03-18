#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=../etc/config_env.sh
source "$SCRIPT_DIR/../etc/config_env.sh"

set -euo pipefail

echo "CIRRUS_PG_HOST=${CIRRUS_PG_HOST}"
echo "CIRRUS_PG_USER=${CIRRUS_PG_USER}"
#echo "CIRRUS_PG_PASSWORD=${CIRRUS_PG_PASSWORD}"

echo "==> pg_isready: checking host reachability"
pg_isready -h "$CIRRUS_PG_HOST" -U "$CIRRUS_PG_USER"

echo ""
echo "==> psql -l: listing databases (requires valid credentials)"
for sslmode in disable require prefer; do
    echo "    trying sslmode=${sslmode} ..."
    PGPASSWORD="$CIRRUS_PG_PASSWORD" psql \
        "host=$CIRRUS_PG_HOST user=$CIRRUS_PG_USER sslmode=${sslmode}" \
        -l && break
done
