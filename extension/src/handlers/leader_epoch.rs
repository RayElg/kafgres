//! The epoch-end exchange that stops a consumer truncating against the wrong leader.

use kafgres_codec::errors::ErrorCode;
use kafgres_codec::generated::offset_for_leader_epoch_request::OffsetForLeaderEpochRequest;
use kafgres_codec::generated::offset_for_leader_epoch_response::{
    EpochEndOffset, OffsetForLeaderEpochResponse, OffsetForLeaderTopicResult,
};

use super::HandlerError;
use crate::meta;
use crate::storage::LogStore;

const NO_CURRENT_EPOCH: i32 = -1;

pub fn handle(
    req: &OffsetForLeaderEpochRequest,
    store: &dyn LogStore,
    authz: &crate::acl::Authz,
) -> Result<OffsetForLeaderEpochResponse, HandlerError> {
    // Cap the *product*, not each level — one frame carries vast numbers of cheap entries.
    let total: usize = req.topics.iter().map(|t| t.partitions.len()).sum();
    super::check_admin_len("leader epoch partition list", total)?;
    let mut topics = Vec::with_capacity(req.topics.len());

    for topic in &req.topics {
        let denied = authz
            .check(
                crate::acl::Operation::Describe,
                crate::acl::ResourceType::Topic,
                &topic.topic,
            )
            .err();

        let topic_id = match meta::topic_id_by_name(&topic.topic) {
            Ok(id) => id,
            Err(e) => {
                pgrx::log!("kafgres: leader epoch topic lookup failed: {e}");
                return Err(HandlerError::Internal(format!("topic lookup: {e}")));
            }
        };

        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for p in &topic.partitions {
            partitions.push(answer(store, topic_id, p, denied));
        }

        topics.push(OffsetForLeaderTopicResult {
            topic: topic.topic.clone(),
            partitions,
            ..Default::default()
        });
    }

    Ok(OffsetForLeaderEpochResponse {
        throttle_time_ms: 0,
        topics,
        ..Default::default()
    })
}

fn answer(
    store: &dyn LogStore,
    topic_id: Option<u32>,
    p: &kafgres_codec::generated::offset_for_leader_epoch_request::OffsetForLeaderPartition,
    denied: Option<ErrorCode>,
) -> EpochEndOffset {
    let fail = |code: ErrorCode| EpochEndOffset {
        error_code: code.code(),
        partition: p.partition,
        // -1/-1 is Kafka's "no answer"; a client must not truncate a plausible-looking 0.
        leader_epoch: -1,
        end_offset: -1,
        ..Default::default()
    };

    if let Some(code) = denied {
        return fail(code);
    }
    let tid = match topic_id {
        Some(t) => t,
        None => return fail(ErrorCode::UnknownTopicOrPartition),
    };

    // Fence first: behind means the leader moved on (FENCED, refresh metadata); ahead means
    let current = match store.leader_epoch(tid, p.partition) {
        Ok(e) => e,
        Err(e) => return fail(e.error_code()),
    };
    if p.current_leader_epoch != NO_CURRENT_EPOCH {
        if p.current_leader_epoch < current {
            return fail(ErrorCode::FencedLeaderEpoch);
        }
        if p.current_leader_epoch > current {
            return fail(ErrorCode::UnknownLeaderEpoch);
        }
    }

    match store.epoch_end_offset(tid, p.partition, p.leader_epoch) {
        Ok(end) => EpochEndOffset {
            error_code: ErrorCode::None.code(),
            partition: p.partition,
            leader_epoch: end.leader_epoch,
            end_offset: end.end_offset,
            ..Default::default()
        },
        Err(e) => fail(e.error_code()),
    }
}
