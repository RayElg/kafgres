//! Shared by the `init*` schema steps.

use pgrx::spi::Spi;

/// Errors out on failure, so init never leaves a half-created schema.
pub fn run_ddl(sql: &str, operation: &str) {
    Spi::run(sql).unwrap_or_else(|e| pgrx::error!("kafgres: failed to {}: {}", operation, e));
}
