#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."

BROKER=127.0.0.1:9092
CLIENTS=kafgres-clients
PIDS=/tmp/kafgres-demo-pids

psql() { docker compose exec -T postgres psql -U postgres "$@"; }
svc()  { docker run --rm --network host -v "$PWD/demo/services:/svc" "$CLIENTS" python3 "/svc/$1" "$BROKER"; }

case "${1:-}" in
setup)
    psql -q -v ON_ERROR_STOP=1 -f - < demo/setup.sql
    echo "demo: schema, topics, CDC mapping and slot ready"
    ;;

services)
    : > "$PIDS"
    for s in fulfilment inventory notifier; do
        nohup docker run --rm --name "demo-$s" --network host \
            -v "$PWD/demo/services:/svc" "$CLIENTS" python3 "/svc/$s.py" "$BROKER" \
            > "/tmp/kafgres-demo-$s.log" 2>&1 &
        echo $! >> "$PIDS"
    done
    sleep 6
    echo "demo: services up, logs in /tmp/kafgres-demo-*.log"
    ;;

traffic)
    n="${2:-6}"
    for i in $(seq 1 "$n"); do
        cust="cust-$(( (i % 3) + 1 ))"
        sku="SKU-$(( (i % 3) + 1 ))"
        qty=$(( (i % 7) + 1 ))
        psql -q -c "INSERT INTO orders (customer, sku, qty, total)
                    VALUES ('$cust', '$sku', $qty, $qty * 9.99)" >/dev/null
        sleep 0.4
    done
    echo "demo: placed $n orders"
    ;;

payment)
    psql -q -c "BEGIN;
                INSERT INTO payments (order_id, amount) VALUES (1, 19.98);
                SELECT kafgres_produce('payments.events', 'order-1',
                       '{\"order_id\":1,\"amount\":19.98}');
                COMMIT;" >/dev/null
    echo "demo: payment written and published in one transaction"

    psql -q -c "BEGIN;
                INSERT INTO payments (order_id, amount) VALUES (999, 1.00);
                SELECT kafgres_produce('payments.events', 'order-999', '{\"order_id\":999}');
                ROLLBACK;" >/dev/null
    echo "demo: rolled-back payment published nothing"
    ;;

show)
    echo "--- orders.events (from CDC) ---"
    docker run --rm --network host "$CLIENTS" kcat -b "$BROKER" -t orders.events \
        -C -o beginning -e -q -f '%k %s\n' 2>/dev/null | head -8
    echo "--- shipments ---"
    docker run --rm --network host "$CLIENTS" kcat -b "$BROKER" -t shipments \
        -C -o beginning -e -q -f '%k %s\n' 2>/dev/null | head -6
    echo "--- inventory.state (compacted) ---"
    docker run --rm --network host "$CLIENTS" kcat -b "$BROKER" -t inventory.state \
        -C -o beginning -e -q -f '%o %k %s\n' 2>/dev/null | tail -8
    echo "--- payments.events (transactional produce) ---"
    docker run --rm --network host "$CLIENTS" kcat -b "$BROKER" -t payments.events \
        -C -o beginning -e -q -f '%k %s\n' 2>/dev/null
    echo "--- consumer groups ---"
    docker run --rm --network host apache/kafka:4.1.0 /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server "$BROKER" --list 2>/dev/null
    ;;

stop)
    for s in fulfilment inventory notifier; do docker rm -f "demo-$s" >/dev/null 2>&1; done
    echo "demo: services stopped"
    ;;

*)
    echo "usage: demo/run.sh {setup|services|traffic [n]|payment|show|stop}" >&2
    exit 2
    ;;
esac
