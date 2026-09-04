use pgrx::spi::Spi;

fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}

pub fn init_060() {
    // The CHECKs are the point of doing this in SQL: an operation typo that the authorizer
    run_ddl(
        "CREATE TABLE IF NOT EXISTS kafgres_acls (
            acl_id        bigserial PRIMARY KEY,
            /* Typed, as Kafka writes them: 'User:alice' for a SASL role,
               'User:CN=alice, O=kafgres' for a certificate subject, 'User:*' for
               anyone. The prefix is what keeps the two namespaces from colliding. */
            principal     text NOT NULL,
            host          text NOT NULL DEFAULT '*',
            operation     text NOT NULL CHECK (operation IN (
                              'READ','WRITE','CREATE','DELETE','ALTER','DESCRIBE',
                              'DESCRIBE_CONFIGS','ALTER_CONFIGS','IDEMPOTENT_WRITE','ALL')),
            permission    text NOT NULL CHECK (permission IN ('ALLOW','DENY')),
            resource_type text NOT NULL CHECK (resource_type IN (
                              'TOPIC','GROUP','CLUSTER','TRANSACTIONAL_ID')),
            resource_name text NOT NULL,
            pattern_type  text NOT NULL DEFAULT 'LITERAL'
                              CHECK (pattern_type IN ('LITERAL','PREFIXED')),
            created_at    timestamptz NOT NULL DEFAULT now(),
            UNIQUE (principal, host, operation, permission, resource_type,
                    resource_name, pattern_type)
        )",
        "create acl table",
    );

    run_ddl(
        "INSERT INTO kafgres_schema_version (version) VALUES ('0.6.0')
         ON CONFLICT (version) DO NOTHING",
        "record schema version",
    );

    // Without CLUSTER_ACTION here, WriteTxnMarkers answers CLUSTER_AUTHORIZATION_FAILED to every
    run_ddl(
        "ALTER TABLE kafgres_acls DROP CONSTRAINT IF EXISTS kafgres_acls_operation_check",
        "drop the acl operation constraint before widening it",
    );
    run_ddl(
        "ALTER TABLE kafgres_acls ADD CONSTRAINT kafgres_acls_operation_check
             CHECK (operation IN (
                 'READ','WRITE','CREATE','DELETE','ALTER','DESCRIBE',
                 'DESCRIBE_CONFIGS','ALTER_CONFIGS','IDEMPOTENT_WRITE','CLUSTER_ACTION','ALL'))",
        "widen the acl operation constraint to include CLUSTER_ACTION",
    );
}
