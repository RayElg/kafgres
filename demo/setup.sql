-- The business schema. Nothing about it knows there is a broker in the room, which is the
-- point: an order service writes rows, and change capture does the rest.
DROP TABLE IF EXISTS orders, payments, products CASCADE;

CREATE TABLE products (
    sku         text PRIMARY KEY,
    name        text NOT NULL,
    stock       int  NOT NULL
);

CREATE TABLE orders (
    id          bigserial PRIMARY KEY,
    customer    text   NOT NULL,
    sku         text   NOT NULL REFERENCES products(sku),
    qty         int    NOT NULL,
    total       numeric(10,2) NOT NULL,
    status      text   NOT NULL DEFAULT 'placed',
    placed_at   timestamptz NOT NULL DEFAULT now()
);

-- The transactional-produce demo writes here.
CREATE TABLE payments (
    id          bigserial PRIMARY KEY,
    order_id    bigint NOT NULL,
    amount      numeric(10,2) NOT NULL,
    taken_at    timestamptz NOT NULL DEFAULT now()
);

INSERT INTO products (sku, name, stock) VALUES
    ('SKU-1', 'widget',   100),
    ('SKU-2', 'sprocket', 100),
    ('SKU-3', 'gizmo',    100);

SELECT kafgres_drop_topic('orders.events');
SELECT kafgres_drop_topic('shipments');
SELECT kafgres_drop_topic('inventory.state');
SELECT kafgres_drop_topic('payments.events');
SELECT kafgres_create_topic('orders.events', 3);
SELECT kafgres_create_topic('shipments', 3);
SELECT kafgres_create_topic('inventory.state', 3);
SELECT kafgres_create_topic('payments.events', 1);

-- The Debezium replacement. One mapping, defined in SQL, enriched by a join at capture
-- time — the product name comes along without the consumer needing a second lookup.
DELETE FROM kafgres_cdc_mappings WHERE mapping_name = 'orders-cdc';
SELECT kafgres_add_mapping(
    'orders-cdc',
    'public.orders',
    'orders.events',
    $$jsonb_build_object(
        'order_id', new.id,
        'customer', new.customer,
        'sku',      new.sku,
        'qty',      new.qty,
        'total',    new.total,
        'status',   new.status,
        'product',  (SELECT p.name FROM products p WHERE p.sku = new.sku))$$,
    $$new.customer$$,   -- key by customer, so one customer's orders keep their order
    NULL
);
SELECT kafgres_cdc_create_slot();
