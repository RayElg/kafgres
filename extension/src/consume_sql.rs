//! `kafgres_read()`: a topic's records as rows, for SQL to filter, join and aggregate.

use std::collections::VecDeque;

use pgrx::datum::TimestampWithTimeZone;
use pgrx::prelude::*;

use kafgres_codec::consume::ConsumedRecord;

use crate::acl::{AsBootstrapSuperuser, Operation};
use crate::storage::cursor::{self, ReadBounds, RecordCursor};
use crate::storage::IsolationLevel;

/// Microseconds from the Unix epoch to the Postgres one, 2000-01-01.
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

/// One partition's records, or every partition's when `partition` is NULL, within the bounds
/// ([`ReadBounds`]). The range is fixed when the call starts.
///
/// Gated by `GRANT EXECUTE` and, with ACLs on, `READ` on the topic. The log is then read as
/// the bootstrap superuser, hence the pinned search path.
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
#[pg_extern]
#[search_path(pg_catalog, @extschema@, pg_temp)]
fn kafgres_read(
    topic: &str,
    partition: default!(Option<i32>, "NULL"),
    from_offset: default!(Option<i64>, "NULL"),
    to_offset: default!(Option<i64>, "NULL"),
    from_time: default!(Option<TimestampWithTimeZone>, "NULL"),
    to_time: default!(Option<TimestampWithTimeZone>, "NULL"),
    isolation: default!(&str, "'read_committed'"),
    max_records: default!(Option<i64>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(partition, Option<i32>),
        name!(record_offset, Option<i64>),
        name!(timestamp, Option<TimestampWithTimeZone>),
        name!(timestamp_type, Option<&'static str>),
        name!(key, Option<Vec<u8>>),
        name!(value, Option<Vec<u8>>),
        name!(header_keys, Option<Vec<String>>),
        name!(header_values, Option<Vec<Option<Vec<u8>>>>),
        name!(leader_epoch, Option<i32>),
        name!(producer_id, Option<i64>),
        name!(producer_epoch, Option<i16>),
        name!(sequence, Option<i32>),
        name!(is_transactional, Option<bool>),
    ),
> {
    let isolation = match isolation {
        "read_committed" => IsolationLevel::ReadCommitted,
        "read_uncommitted" => IsolationLevel::ReadUncommitted,
        other => error!(
            "kafgres: isolation must be 'read_committed' or 'read_uncommitted', not {other:?}"
        ),
    };
    let limit = match max_records {
        Some(n) if n < 0 => error!("kafgres: max_records must not be negative"),
        Some(n) => usize::try_from(n).unwrap_or(usize::MAX),
        None => usize::MAX,
    };
    let bounds = ReadBounds {
        from_offset,
        to_offset,
        from_time: from_time.map(epoch_ms_ceil),
        to_time: to_time.map(epoch_ms_ceil),
    };

    // Who is asking, before anything runs as someone else.
    let (role, host) = caller();
    crate::acl::sql_require(Operation::Read, &role, &host, topic);

    let (topic_id, partitions) = {
        let _owner = AsBootstrapSuperuser::enter();
        let topic_id = match crate::meta::topic_id_by_name(topic) {
            Ok(Some(id)) => id,
            Ok(None) => error!("kafgres: no such topic {topic:?}"),
            Err(e) => error!("kafgres: {e}"),
        };
        let count =
            crate::meta::partition_count(topic_id).unwrap_or_else(|e| error!("kafgres: {e}"));
        (topic_id, count)
    };
    let selected: Vec<i32> = match partition {
        Some(p) if !(0..partitions).contains(&p) => {
            error!("kafgres: topic {topic:?} has no partition {p}")
        }
        Some(p) => vec![p],
        None => (0..partitions).collect(),
    };

    // After a restart the broker may still be truncating a torn tail.
    crate::server::wait_for_epochs();

    let cursors = {
        let _owner = AsBootstrapSuperuser::enter();
        selected
            .into_iter()
            .map(|p| {
                let store = crate::storage::open();
                let range = cursor::resolve(&*store, topic_id, p, &bounds, isolation)
                    .unwrap_or_else(|e| error!("kafgres: partition {p}: {e}"));
                RecordCursor::new(store, topic_id, p, range, isolation)
            })
            .collect()
    };

    let rows = Rows {
        cursors,
        topic: topic.to_string(),
    };
    TableIterator::new(rows.take(limit).map(|(partition, r)| {
        let (header_keys, header_values) = r
            .headers
            .into_iter()
            .map(|(k, v)| {
                (
                    String::from_utf8_lossy(&k).into_owned(),
                    v.map(|v| v.to_vec()),
                )
            })
            .unzip();
        (
            Some(partition),
            Some(r.offset),
            timestamptz(r.timestamp),
            Some(if r.log_append_time {
                "LogAppendTime"
            } else {
                "CreateTime"
            }),
            r.key.map(|k| k.to_vec()),
            r.value.map(|v| v.to_vec()),
            Some(header_keys),
            Some(header_values),
            (r.leader_epoch >= 0).then_some(r.leader_epoch),
            (r.producer_id >= 0).then_some(r.producer_id),
            (r.producer_epoch >= 0).then_some(r.producer_epoch),
            (r.sequence >= 0).then_some(r.sequence),
            Some(r.is_transactional),
        )
    }))
}

/// Cursors still to read. Each step reads as the bootstrap superuser; rows reach the query
/// as the caller.
struct Rows {
    cursors: VecDeque<RecordCursor>,
    topic: String,
}

impl Iterator for Rows {
    type Item = (i32, ConsumedRecord);

    fn next(&mut self) -> Option<Self::Item> {
        let _owner = AsBootstrapSuperuser::enter();
        loop {
            let cursor = self.cursors.front_mut()?;
            match cursor.next() {
                Some(Ok(record)) => return Some((cursor.partition_index(), record)),
                Some(Err(e)) => error!(
                    "kafgres: reading {:?} partition {}: {e}",
                    self.topic,
                    cursor.partition_index()
                ),
                None => {
                    self.cursors.pop_front();
                }
            }
        }
    }
}

/// The calling role and its address, as `kafgres_produce()` names them to the ACLs.
fn caller() -> (String, String) {
    match Spi::get_two::<String, String>(
        "SELECT current_user::text, COALESCE(host(inet_client_addr()), 'localhost')",
    ) {
        Ok((role, host)) => (role.unwrap_or_default(), host.unwrap_or_default()),
        Err(e) => error!("kafgres: {e}"),
    }
}

/// Rounded up, so a bound between two milliseconds excludes the earlier one either way.
fn epoch_ms_ceil(ts: TimestampWithTimeZone) -> i64 {
    let pg_us: i64 = ts.into();
    match pg_us {
        i64::MIN => i64::MIN,
        i64::MAX => i64::MAX,
        us => {
            us.saturating_add(PG_EPOCH_OFFSET_US).div_euclid(1000)
                + i64::from(us.rem_euclid(1000) != 0)
        }
    }
}

/// NULL for a record with no timestamp, which Kafka writes as -1.
fn timestamptz(ms: i64) -> Option<TimestampWithTimeZone> {
    if ms < 0 {
        return None;
    }
    ms.checked_mul(1000)
        .and_then(|us| us.checked_sub(PG_EPOCH_OFFSET_US))
        .and_then(|us| TimestampWithTimeZone::try_from(us).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_bounds_round_up_to_the_millisecond() {
        let at = |us: i64| TimestampWithTimeZone::try_from(us - PG_EPOCH_OFFSET_US).unwrap();
        assert_eq!(epoch_ms_ceil(at(10_000_000)), 10_000);
        assert_eq!(epoch_ms_ceil(at(10_000_001)), 10_001);
        assert_eq!(epoch_ms_ceil(at(10_000_999)), 10_001);
        assert_eq!(epoch_ms_ceil(at(-1)), 0);
    }
}
