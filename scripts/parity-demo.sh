#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."

KAFGRES=${KAFGRES:-127.0.0.1:9092}
KAFKA=${KAFKA:-127.0.0.1:9292}
IMAGE=apache/kafka:4.3.1     # Matched to the version the codec is generated against.
CLIENTS=kafgres-clients
TOPIC=parity-demo
VERBOSE=${1:-}

docker image inspect "$CLIENTS" >/dev/null 2>&1 || {
    printf 'parity-demo: image %s not found — build it first: docker build -t %s tests/clients\n' \
        "$CLIENTS" "$CLIENTS" >&2
    exit 1
}
pass=0; fail=0
bold=$'\e[1m'; dim=$'\e[2m'; green=$'\e[32m'; red=$'\e[31m'; off=$'\e[0m'
[ -t 1 ] || { bold=; dim=; green=; red=; off=; }

k()   { docker run --rm --network host "$IMAGE" "/opt/kafka/bin/$@" 2>&1; }
cli() { docker run --rm --network host "$CLIENTS" "$@" 2>&1; }
cli_in() { docker run --rm -i --network host "$CLIENTS" "$@" 2>&1; }
psql_() { docker compose exec -T postgres psql -U postgres -d postgres "$@" 2>/dev/null; }

normalize() {
    sed -E -e 's/TopicId: [A-Za-z0-9_-]+/TopicId: <id>/g' \
           -e 's/(9092|9292)/<port>/g' \
           -e 's/min\.insync\.replicas=1//' \
           -e 's#(Elr|LastKnownElr): (N/A)?#\1: <elr>#g' \
           -e 's/[[:space:]]+$//' | grep -v '^$'
}

FAILED='Error:|Exception|^ERROR |does not exist|not found|Failed|refused|timed out'

compare() {
    local label=$1 snippet=$2 ours theirs
    ours=$(B=$KAFGRES eval "$snippet" | normalize)
    theirs=$(B=$KAFKA eval "$snippet" | normalize)
    if printf '%s\n%s\n' "$ours" "$theirs" | grep -v '^OK ' | grep -qE "$FAILED"; then
        printf '  %s✗%s %-46s %sboth failed%s\n' "$red" "$off" "$label" "$red" "$off"
        fail=$((fail + 1))
        printf '%s\n' "$ours" | head -3 | sed 's/^/      /'
        return 0
    fi
    if [ "$ours" = "$theirs" ]; then
        printf '  %s✓%s %-46s %sidentical%s\n' "$green" "$off" "$label" "$dim" "$off"
        pass=$((pass + 1))
        [ -n "$VERBOSE" ] && printf '%s\n' "$ours" | sed 's/^/      /'
    else
        printf '  %s✗%s %-46s %sdiffers%s\n' "$red" "$off" "$label" "$red" "$off"
        fail=$((fail + 1))
        diff <(printf '%s\n' "$ours") <(printf '%s\n' "$theirs") | sed 's/^/      /'
    fi
    return 0
}

section() { printf '\n%s%s%s\n' "$bold" "$1" "$off"; }

printf '%sBringing up kafgres and a reference Kafka…%s\n' "$dim" "$off"
docker compose up -d >/dev/null 2>&1
docker compose --profile conformance up -d kafka >/dev/null 2>&1

for _ in $(seq 1 60); do
    docker compose exec -T postgres pg_isready -U postgres >/dev/null 2>&1 && break
    sleep 2
done
for _ in $(seq 1 60); do
    k kafka-topics.sh --bootstrap-server "$KAFKA" --list >/dev/null 2>&1 && break
    sleep 2
done

MULTI="$TOPIC"
declare -A SCENARIO_TOPIC=(
    [sarama-pc]="$TOPIC-sarama-pc"   [sarama-grp]="$TOPIC-sarama-grp"
    [kpy-pc]="$TOPIC-kpy-pc"         [kpy-grp]="$TOPIC-kpy-grp"
)

make_topic() {  # name, partitions
    # An empty result means the create failed (e.g. the name survived a previous
    # run's cleanup): say so and stop, rather than diffing against a stale log.
    [ -n "$(psql_ -tAc "SELECT kafgres_create_topic('$1', $2)")" ] || {
        printf 'parity-demo: kafgres_create_topic(%s) failed\n' "$1" >&2
        exit 1
    }
    k kafka-topics.sh --bootstrap-server "$KAFKA" --create --topic "$1" --partitions "$2" >/dev/null
}

all_topics() { printf '%s\n' "$MULTI" "${SCENARIO_TOPIC[@]}"; }

drop_all() {
    # Read on fd 3: `docker compose exec` forwards the caller's stdin even with -T,
    # so a bare `read` loop loses every topic after the first to the first psql call.
    while read -r t <&3; do
        psql_ -tAc "SELECT kafgres_drop_topic('$t')" >/dev/null
        k kafka-topics.sh --bootstrap-server "$KAFKA" --delete --topic "$t" --if-exists >/dev/null
        for b in "$KAFGRES" "$KAFKA"; do
            for g in "$t-sarama-group" "$t-kpy-group"; do
                k kafka-consumer-groups.sh --bootstrap-server "$b" --delete \
                    --group "$g" >/dev/null 2>&1
            done
        done
    done 3< <(all_topics)
}
trap drop_all EXIT

drop_all
make_topic "$MULTI" 3
for t in "${SCENARIO_TOPIC[@]}"; do make_topic "$t" 1; done

cat <<BANNER

${bold}kafgres parity demo${off}
${dim}Left:  kafgres: a Kafka broker inside PostgreSQL, on $KAFGRES
Right: apache/kafka: a reference Kafka broker, on $KAFKA
Every check runs the identical command against both and diffs the output.${off}
BANNER

section "Admin tooling (the Java client, unmodified)"

compare "kafka-topics.sh --describe" \
    'k kafka-topics.sh --bootstrap-server $B --describe --topic '"$TOPIC"
compare "kafka-topics.sh --list" \
    'k kafka-topics.sh --bootstrap-server $B --list | grep '"$TOPIC"
compare "kafka-configs.sh --describe" \
    'k kafka-configs.sh --bootstrap-server $B --describe --entity-type topics --entity-name '"$TOPIC"
compare "kafka-get-offsets.sh" \
    'k kafka-get-offsets.sh --bootstrap-server $B --topic '"$TOPIC"

section "Produce and consume (librdkafka / kcat)"

compare "kcat -P then -C, 6 keyed records over 3 partitions" '
    for i in 1 2 3 4 5 6; do echo "key-$i:record-$i"; done \
      | cli_in kcat -b $B -t '"$MULTI"' -K: -P
    cli kcat -b $B -t '"$MULTI"' -C -e -q -o beginning -f "%p:%o:%k:%s\n" | sort'

compare "kcat -L metadata" \
    'cli kcat -b $B -L -t '"$MULTI"' | grep -E "partition [0-9]+" | sort'

section "Independent clients (no shared code with librdkafka or Java)"

compare "Sarama (Go) - produce, consume, offsets" \
    'cli sarama-conformance $B produce-consume '"${SCENARIO_TOPIC[sarama-pc]}"
compare "Sarama (Go) - consumer group, commit" \
    'cli sarama-conformance $B group-consume '"${SCENARIO_TOPIC[sarama-grp]}"
compare "kafka-python - produce, consume, offsets" \
    'cli kafka-python-conformance $B produce-consume '"${SCENARIO_TOPIC[kpy-pc]}"
compare "kafka-python - consumer group, commit" \
    'cli kafka-python-conformance $B group-consume '"${SCENARIO_TOPIC[kpy-grp]}"
compare "unknown topic returns the same error code" \
    'cli kafka-python-conformance $B unknown-topic unused'

section "Consumer groups"

compare "kafka-consumer-groups.sh --describe (lag, offsets)" '
    k kafka-consumer-groups.sh --bootstrap-server $B --describe \
        --group '"${SCENARIO_TOPIC[sarama-grp]}"'-sarama-group \
      | awk "NF { print \$1, \$2, \$3, \$4, \$5, \$6 }" | sort'
compare "kafka-consumer-groups.sh --list" '
    k kafka-consumer-groups.sh --bootstrap-server $B --list | grep '"$TOPIC"' | sort'

section "The difference"

cat <<EXPLAIN
${dim}Everything above matched. This does not:${off}
EXPLAIN

printf '\n  %s$ psql -c "SELECT partition, high_watermark AS log_end_offset, log_start_offset FROM kafgres_partition_offsets('"'"'%s'"'"')"%s\n' "$dim" "$MULTI" "$off"
psql_ -c "
    SELECT partition, COALESCE(high_watermark, 0) AS log_end_offset, log_start_offset
      FROM kafgres_partition_offsets('$MULTI')
     ORDER BY partition;" | sed 's/^/  /'

cat <<'CLOSING'
  A Kafka broker cannot be queried, joined against, backed up, or point-in-time
  restored with your application data, because it is a different system holding a
  different copy of the truth. Here the log is in the same database, and a
  produce can happen inside the same transaction as the business write that caused
  it: no outbox table, no CDC pipeline, no dual-write window. See
  docs/architecture.md.
CLOSING

printf '\n%s%d/%d checks identical%s' "$bold" "$pass" "$((pass + fail))" "$off"
if [ "$fail" -eq 0 ]; then
    printf '  %s✓%s\n\n' "$green" "$off"
else
    printf '  %s%d differ%s\n' "$red" "$fail" "$off"
    printf '%sAn uncatalogued difference is a bug. See docs/conformance.md.%s\n\n' "$dim" "$off"
fi
exit "$fail"
