# Demo: an order pipeline that would normally need three systems

A small event-driven architecture of the kind commonly built, running against one
kafgres. Every piece of it maps to something that would otherwise be a separate
deployment:

| Normally | Here |
|---|---|
| Postgres: the business database | the same Postgres |
| Debezium: change capture off the WAL | kafgres's CDC mappings (`kafgres_add_mapping`) |
| Kafka: the broker services talk over | the same Postgres |
| An outbox table plus a relay, to make a write and a publish atomic | `kafgres_produce()` inside the business transaction |

## The pipeline

```
  orders (SQL table)
        │  CDC mapping "orders-cdc"          ← replaces Debezium
        ▼
  orders.events ──────┬──────────────────────────────┐
        │             │                              │
        ▼             ▼                              ▼
   fulfilment    inventory                      (any consumer)
        │             │
        │             └─► inventory.state   compacted, keyed by SKU
        ▼                                   ← the current stock level per SKU,
   shipments                                  which is a table shaped like a topic
        │
        ▼
    notifier
```

Separately, `payments.py` inserts a payment row and produces the event in one Postgres
transaction, so there is no window where the row exists and the event does not.

## Running it

```sh
demo/run.sh setup     # tables, topics, CDC mapping, slot
demo/run.sh services  # fulfilment, inventory, notifier in the background
demo/run.sh traffic   # place some orders
demo/run.sh show      # what landed where
demo/run.sh stop
```