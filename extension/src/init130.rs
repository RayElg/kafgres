use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_130() {
    // entity_name NULL is Kafka's own encoding of the *default* quota of an entity type, not
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_client_quotas (
            entity_type text   NOT NULL CHECK (entity_type IN ('user', 'client-id')),
            entity_name text,
            quota_type  text   NOT NULL,
            quota_value double precision NOT NULL,
            updated_at  timestamptz NOT NULL DEFAULT now()
         )",
        "create client quotas table",
    );
    run_ddl(
        "CREATE UNIQUE INDEX IF NOT EXISTS kafgres_client_quotas_named
             ON kafgres_client_quotas (entity_type, entity_name, quota_type)
             WHERE entity_name IS NOT NULL",
        "index named client quotas",
    );
    run_ddl(
        "CREATE UNIQUE INDEX IF NOT EXISTS kafgres_client_quotas_default
             ON kafgres_client_quotas (entity_type, quota_type)
             WHERE entity_name IS NULL",
        "index default client quotas",
    );
}
