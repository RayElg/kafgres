#!/usr/bin/env bash
set -euo pipefail

PRIMARY_HOST="${PRIMARY_HOST:-postgres}"
PGDATA="${PGDATA:-/var/lib/postgresql/data}"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "standby: waiting for the primary to accept connections"
    until pg_isready -h "$PRIMARY_HOST" -U postgres -q; do sleep 1; done

    echo "standby: base backup from $PRIMARY_HOST"
    rm -rf "${PGDATA:?}"/*
    PGPASSWORD=postgres pg_basebackup \
        -h "$PRIMARY_HOST" -U postgres -D "$PGDATA" \
        -Fp -Xs -R -c fast -P

    cat >> "$PGDATA/postgresql.auto.conf" <<EOF
hot_standby = on
kafgres.advertised_host = '127.0.0.1'
kafgres.advertised_port = 9192
kafgres.port = 9192
kafgres.replicate_from = '${PRIMARY_HOST}:9092'
kafgres.storage_engine = '${KAFGRES_ENGINE:-table}'
EOF
    chmod 700 "$PGDATA"
fi

exec docker-entrypoint.sh postgres
