use pgrx::prelude::*;

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::init_producer_id_request::InitProducerIdRequest;
use kafgres_codec::generated::init_producer_id_response::InitProducerIdResponse;

use super::HandlerError;
use crate::producer;

const MAX_TRANSACTION_TIMEOUT_MS: i32 = 900_000;

pub fn handle(
    req: &InitProducerIdRequest,
    authz: &crate::acl::Authz,
) -> Result<InitProducerIdResponse, HandlerError> {
    // IDEMPOTENT_WRITE on the cluster, as Kafka requires to take a producer id.
    if let Err(code) = authz.check(
        crate::acl::Operation::IdempotentWrite,
        crate::acl::ResourceType::Cluster,
        "kafka-cluster",
    ) {
        return Ok(InitProducerIdResponse {
            throttle_time_ms: 0,
            error_code: code.code(),
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        });
    }
    let txn_id = req.transactional_id.as_deref().filter(|s| !s.is_empty());
    let (producer_id, epoch) = producer::init_producer_id(txn_id)?;

    // Clamped as Kafka clamps: an unbounded timeout would let a client pin a partition's LSO forever.
    if let Some(id) = txn_id {
        let timeout = req
            .transaction_timeout_ms
            .clamp(1_000, MAX_TRANSACTION_TIMEOUT_MS);
        Spi::run_with_args(
            "INSERT INTO kafgres_txns
                    (producer_id, producer_epoch, transactional_id, state, started_at,
                     timeout_ms)
             VALUES ($1, $2, $3, 'empty', $4, $5)
             ON CONFLICT (producer_id) DO UPDATE
                SET producer_epoch = EXCLUDED.producer_epoch,
                    timeout_ms = EXCLUDED.timeout_ms",
            &[
                producer_id.into(),
                (epoch as i32).into(),
                id.to_string().into(),
                super::txn::now_millis().into(),
                timeout.into(),
            ],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
    }

    pgrx::log!(
        "kafgres: allocated producer id {} epoch {}{}",
        producer_id,
        epoch,
        txn_id.map(|t| format!(" for transactional id '{t}'")).unwrap_or_default()
    );

    Ok(InitProducerIdResponse {
        throttle_time_ms: 0,
        error_code: ErrorCode::None.code(),
        producer_id,
        producer_epoch: epoch,
        // KIP-939 v6 fields: unstable, never advertised or encoded; -1 is the no-transaction sentinel.
        ongoing_txn_producer_id: -1,
        ongoing_txn_producer_epoch: -1,
        unknown_tagged_fields: Vec::new(),
    })
}
