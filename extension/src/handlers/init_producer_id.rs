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
    // Before the epoch moves: the abort is performed at the epoch the transaction ran under.
    if let Some(id) = txn_id {
        // Serialised with the instance being fenced, or its transaction could begin unseen
        // and be adopted by the new epoch.
        super::txn::lock_transactional_id(id).map_err(|e| HandlerError::Internal(e.to_string()))?;
        Spi::run_with_args(
            "SELECT FROM kafgres_txns WHERE transactional_id = $1 ORDER BY producer_id FOR UPDATE",
            &[id.into()],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
        super::txn::abort_abandoned(id)?;
    }
    let (producer_id, epoch) = producer::init_producer_id(txn_id)?;

    // Clamped as Kafka clamps: an unbounded timeout would let a client pin a partition's LSO forever.
    if let Some(id) = txn_id {
        let timeout = req
            .transaction_timeout_ms
            .clamp(1_000, MAX_TRANSACTION_TIMEOUT_MS);
        // One row per transactional id: a finished row left by an evicted producer would be
        // listed beside the live one.
        for table in ["kafgres_txn_partitions", "kafgres_txn_offsets"] {
            Spi::run_with_args(
                &format!(
                    "DELETE FROM {table} WHERE producer_id IN
                        (SELECT producer_id FROM kafgres_txns
                          WHERE transactional_id = $1 AND producer_id <> $2
                            AND state <> 'ongoing')"
                ),
                &[id.to_string().into(), producer_id.into()],
            )
            .map_err(|e| HandlerError::Internal(e.to_string()))?;
        }
        Spi::run_with_args(
            "DELETE FROM kafgres_txns
              WHERE transactional_id = $1 AND producer_id <> $2 AND state <> 'ongoing'",
            &[id.to_string().into(), producer_id.into()],
        )
        .map_err(|e| HandlerError::Internal(e.to_string()))?;
        Spi::run_with_args(
            "INSERT INTO kafgres_txns
                    (producer_id, producer_epoch, transactional_id, state, started_at,
                     timeout_ms)
             VALUES ($1, $2, $3, 'empty', $4, $5)
             ON CONFLICT (producer_id) DO UPDATE
                SET producer_epoch = EXCLUDED.producer_epoch,
                    timeout_ms = EXCLUDED.timeout_ms,
                    forced_result = CASE WHEN kafgres_txns.state = 'ongoing'
                                         THEN kafgres_txns.forced_result END,
                    -- A completed transaction does not carry over to the new instance:
                    -- its state returns to 'empty', as Kafka's does, so a following
                    -- EndTxn is judged against nothing open.
                    state = CASE WHEN kafgres_txns.state = 'ongoing'
                                 THEN kafgres_txns.state ELSE 'empty' END",
            &[
                producer_id.into(),
                (epoch as i32).into(),
                id.to_string().into(),
                crate::clock::now_millis().into(),
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
