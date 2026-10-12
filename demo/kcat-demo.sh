#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

BROKER=${BROKER:-127.0.0.1:9092}
TOPIC=${TOPIC:-payments}
GROUP=$TOPIC-demo

bold=$'\e[1m'; dim=$'\e[2m'; cyan=$'\e[36m'; off=$'\e[0m'
[ -t 1 ] || { bold=; dim=; cyan=; off=; }

psql() {
    docker compose exec -T -e PGOPTIONS='-c client_min_messages=warning' \
        postgres psql -U postgres -d postgres "$@"
}
show() {
    printf '\n%s$ %s%s\n' "$cyan" "$1" "$off"
    eval "$1" 2>&1 | sed 's/^/  /'
}
note() { printf '\n%s%s%s\n' "$dim" "$1" "$off"; }

command -v docker >/dev/null 2>&1 || {
    echo "kcat-demo: docker is required — it runs the broker, and kcat when no local kcat exists" >&2
    exit 1
}
# Decided before any fallback `kcat()` exists: `command -v` finds functions too,
# so a check made after the definition would never see the binary and never build.
if ! command -v kcat >/dev/null 2>&1; then
    if ! docker image inspect kafgres-clients >/dev/null 2>&1; then
        note "no local kcat and no kafgres-clients image — building tests/clients once:"
        docker build -t kafgres-clients tests/clients
    fi
    kcat() { docker run --rm -i --network host kafgres-clients kcat "$@"; }
fi

docker compose up -d >/dev/null
ready=0
for _ in $(seq 1 60); do
    if docker compose exec -T postgres pg_isready -U postgres >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 2
done
[ "$ready" = 1 ] || {
    echo "kcat-demo: the postgres container never came up — 'docker compose logs postgres' says why" >&2
    exit 1
}
psql -tAc "SELECT kafgres_drop_topic('$TOPIC')" >/dev/null 2>&1 || true
trap 'psql -tAc "SELECT kafgres_drop_topic('"'"'$TOPIC'"'"')" >/dev/null 2>&1 || true' EXIT

printf '%skafgres: a Kafka broker inside PostgreSQL%s\n' "$bold" "$off"

note "There is no broker process. Port 9092 is served by a Postgres background worker:"
show "psql -tAc \"SELECT 'PostgreSQL ' || current_setting('server_version')\""
show "psql -tAc \"SELECT backend_type FROM pg_stat_activity
                   WHERE backend_type = 'kafgres_broker'\""

note "Topics are created in SQL, because a topic is a row:"
show "psql -tAc \"SELECT kafgres_create_topic('$TOPIC', 3)\""

note "Everything from here is plain kcat, pointed at $BROKER."

show "kcat -b $BROKER -L -t $TOPIC"

note "Produce three keyed records. The client hashes the key to a partition, so this
is also a check that we honour the partition it chose:"
show "printf 'erin:{\"amt\":120}\nken:{\"amt\":80}\nalice:{\"amt\":210}\n' \\
    | kcat -b $BROKER -t $TOPIC -K: -P"

note "Read them back with partition, offset and key:"
show "kcat -b $BROKER -t $TOPIC -C -e -q -o beginning -f '%p:%o  %k -> %s\n'"

note "Consumer groups work. The second run starts from the committed offset and
finds nothing left:"
show "kcat -b $BROKER -G $GROUP $TOPIC -e -q -o beginning -c 3 -f '%p:%o  %k -> %s\n'"
show "kcat -b $BROKER -G $GROUP $TOPIC -e -q -f 'unexpected: %p:%o\n'; echo '(no output)'"

note "The log is part of the same database as the application tables that produced
them — here are its offsets, straight from SQL:"
show "psql -c \"
    SELECT partition, COALESCE(high_watermark, 0) AS log_end_offset, log_start_offset
      FROM kafgres_partition_offsets('$TOPIC')
     ORDER BY partition\""

cat <<CLOSING
${dim}The log, the offsets, the consumer group and the topic metadata are one Postgres
instance: backed up, replicated and point-in-time restored by whatever already
does that for your data. See docs/architecture.md.${off}
CLOSING
