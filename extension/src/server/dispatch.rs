//! Routes one decoded request frame to its handler. `Ctx::tx` and `Ctx::direct` decode,
//! run and reply; APIs that park, charge quota or touch connection state have a `serve_*`.

use std::panic::{RefUnwindSafe, UnwindSafe};
use std::time::{Duration, Instant};

use pgrx::prelude::*;

use kafgres_codec::generated::alter_replica_log_dirs_request::AlterReplicaLogDirsRequest;
use kafgres_codec::generated::fetch_request::FetchRequest;
use kafgres_codec::generated::join_group_request::JoinGroupRequest;
use kafgres_codec::generated::produce_request::ProduceRequest;
use kafgres_codec::generated::sasl_authenticate_request::SaslAuthenticateRequest;
use kafgres_codec::generated::sasl_handshake_request::SaslHandshakeRequest;
use kafgres_codec::generated::sync_group_request::SyncGroupRequest;
use kafgres_codec::generated::*;
use kafgres_codec::prelude::*;

use super::{
    charge, conn_sasl, fetch_wait, principal_of, run_fetch, Disposition, Parked, Server, Waiting,
    MAX_PARKED,
};
use crate::acl::Authz;
use crate::dbtx::{contained_tx, guarded_tx};
use crate::handlers::{self, metadata::ClusterConfig, HandlerError};

/// Per-request state shared by every handler.
struct Ctx<'a> {
    out: &'a mut BytesMut,
    req: &'a handlers::Request,
    cfg: &'a ClusterConfig,
    srv: &'a mut Server,
    conn_id: i32,
    seq: u64,
}

/// What a handler closure sees: the caller's identity and the request's version and cluster.
struct Env<'a> {
    authz: Authz<'a>,
    version: i16,
    cfg: &'a ClusterConfig,
}

impl<'a> Ctx<'a> {
    fn decode<R: ApiMessage>(&self) -> Result<R, HandlerError> {
        let mut body = self.req.body.clone();
        Ok(R::decode(&mut body, self.req.api_version)?)
    }

    fn authz(&self) -> Authz<'_> {
        Authz {
            acls: &self.srv.acls,
            principal: principal_of(self.srv, self.conn_id),
        }
    }

    fn env(&self) -> Env<'_> {
        Env {
            authz: self.authz(),
            version: self.req.api_version,
            cfg: self.cfg,
        }
    }

    fn client_id(&self) -> &str {
        self.req.client_id.as_deref().unwrap_or("")
    }

    fn reply<T: Encodable>(&mut self, body: &T) -> Result<Disposition, HandlerError> {
        handlers::write_response(
            self.out,
            self.req.api_key,
            self.req.api_version,
            self.req.correlation_id,
            body,
        )?;
        Ok(Disposition::Reply)
    }

    /// Like `tx`, but returns the decoded request and body instead of replying.
    fn tx_with<R, T, F>(&self, handle: F) -> Result<(R, T), HandlerError>
    where
        R: ApiMessage + RefUnwindSafe,
        F: FnOnce(&R, &Env) -> Result<T, HandlerError> + UnwindSafe + RefUnwindSafe,
    {
        let request: R = self.decode()?;
        let env = self.env();
        let body = guarded_tx(|| handle(&request, &env))?;
        Ok((request, body))
    }

    /// Decode, run in a guarded transaction, reply.
    fn tx<R, T, F>(&mut self, handle: F) -> Result<Disposition, HandlerError>
    where
        R: ApiMessage + RefUnwindSafe,
        T: Encodable,
        F: FnOnce(&R, &Env) -> Result<T, HandlerError> + UnwindSafe + RefUnwindSafe,
    {
        let (_, body) = self.tx_with(handle)?;
        self.reply(&body)
    }

    /// Decode, run with no transaction, reply. Only for handlers that touch no table.
    fn direct<R, T>(
        &mut self,
        handle: impl FnOnce(&R, &Env) -> Result<T, HandlerError>,
    ) -> Result<Disposition, HandlerError>
    where
        R: ApiMessage,
        T: Encodable,
    {
        let request: R = self.decode()?;
        let body = handle(&request, &self.env())?;
        self.reply(&body)
    }

    /// Charge `bytes` against the client's quota; the delay to ask the client for, in ms.
    fn charge(&mut self, rate: crate::quota::Rate, bytes: i64) -> i32 {
        let client_id = self.req.client_id.as_deref().unwrap_or("");
        charge(self.srv, rate, self.conn_id, client_id, bytes)
    }

    fn parking_full(&self) -> bool {
        self.srv.parked.len() >= MAX_PARKED
    }

    fn park(&mut self, deadline: Instant, waiting: Waiting) -> Disposition {
        self.srv.parked.push(Parked {
            conn_id: self.conn_id,
            seq: self.seq,
            correlation_id: self.req.correlation_id,
            api_key: self.req.api_key,
            api_version: self.req.api_version,
            client_id: self.client_id().to_string(),
            deadline,
            waiting,
        });
        Disposition::Parked
    }
}

pub(super) fn dispatch(
    out: &mut BytesMut,
    req: &handlers::Request,
    cfg: &ClusterConfig,
    srv: &mut Server,
    conn_id: i32,
    seq: u64,
) -> Result<Disposition, HandlerError> {
    let mut c = Ctx {
        out,
        req,
        cfg,
        srv,
        conn_id,
        seq,
    };
    let c = &mut c;

    match req.api_key {
        kafgres_codec::header::API_VERSIONS_KEY => {
            c.reply(&handlers::api_versions::handle())
        }

        // APIs with their own flow: parking, quota, or connection state.
        ProduceRequest::API_KEY => serve_produce(c),
        FetchRequest::API_KEY => serve_fetch(c),
        JoinGroupRequest::API_KEY => serve_join_group(c),
        SyncGroupRequest::API_KEY => serve_sync_group(c),
        SaslHandshakeRequest::API_KEY => serve_sasl_handshake(c),
        SaslAuthenticateRequest::API_KEY => serve_sasl_authenticate(c),
        leave_group_request::LeaveGroupRequest::API_KEY => {
            let (request, body) = c.tx_with(|r: &leave_group_request::LeaveGroupRequest, e| {
                handlers::coordinator::leave_group(r, e.version, &e.authz)
            })?;
            c.srv.groups_changed.insert(request.group_id.clone());
            c.reply(&body)
        }

        // Data plane.
        list_offsets_request::ListOffsetsRequest::API_KEY => {
            c.tx(|r: &list_offsets_request::ListOffsetsRequest, e| {
                let store = crate::storage::open();
                handlers::list_offsets::handle(r, e.version, &*store, &e.authz)
            })
        }
        offset_for_leader_epoch_request::OffsetForLeaderEpochRequest::API_KEY => {
            c.tx(|r: &offset_for_leader_epoch_request::OffsetForLeaderEpochRequest, e| {
                let store = crate::storage::open();
                handlers::leader_epoch::handle(r, &*store, &e.authz)
            })
        }
        delete_records_request::DeleteRecordsRequest::API_KEY => {
            c.tx(|r: &delete_records_request::DeleteRecordsRequest, e| {
                let mut store = crate::storage::open();
                handlers::admin::delete_records(r, &mut *store, &e.authz)
            })
        }

        // Metadata and discovery.
        metadata_request::MetadataRequest::API_KEY => {
            c.tx(|r: &metadata_request::MetadataRequest, e| {
                handlers::metadata::handle(r, e.version, e.cfg, &e.authz)
            })
        }
        describe_topic_partitions_request::DescribeTopicPartitionsRequest::API_KEY => {
            c.tx(|r: &describe_topic_partitions_request::DescribeTopicPartitionsRequest, e| {
                handlers::metadata::describe_topic_partitions(r, e.cfg, &e.authz)
            })
        }
        find_coordinator_request::FindCoordinatorRequest::API_KEY => {
            c.direct(|r: &find_coordinator_request::FindCoordinatorRequest, e| {
                Ok(handlers::coordinator::find_coordinator(r, e.version, e.cfg, &e.authz))
            })
        }
        describe_cluster_request::DescribeClusterRequest::API_KEY => {
            c.direct(|r: &describe_cluster_request::DescribeClusterRequest, e| {
                handlers::admin::describe_cluster(r, e.cfg, &e.authz)
            })
        }

        // Classic consumer groups and offsets.
        offset_commit_request::OffsetCommitRequest::API_KEY => {
            c.tx(|r: &offset_commit_request::OffsetCommitRequest, e| {
                handlers::offsets::offset_commit(r, &e.authz)
            })
        }
        offset_fetch_request::OffsetFetchRequest::API_KEY => {
            c.tx(|r: &offset_fetch_request::OffsetFetchRequest, e| {
                handlers::offsets::offset_fetch(r, e.version, &e.authz)
            })
        }
        offset_delete_request::OffsetDeleteRequest::API_KEY => {
            c.tx(|r: &offset_delete_request::OffsetDeleteRequest, e| {
                handlers::offsets::offset_delete(r, &e.authz)
            })
        }
        heartbeat_request::HeartbeatRequest::API_KEY => {
            c.tx(|r: &heartbeat_request::HeartbeatRequest, e| {
                handlers::coordinator::heartbeat(r, &e.authz)
            })
        }
        describe_groups_request::DescribeGroupsRequest::API_KEY => {
            c.tx(|r: &describe_groups_request::DescribeGroupsRequest, e| {
                handlers::describe_groups::describe_groups(r, &e.authz)
            })
        }
        list_groups_request::ListGroupsRequest::API_KEY => {
            c.tx(|r: &list_groups_request::ListGroupsRequest, e| {
                handlers::describe_groups::list_groups(r, &e.authz)
            })
        }
        delete_groups_request::DeleteGroupsRequest::API_KEY => {
            c.tx(|r: &delete_groups_request::DeleteGroupsRequest, e| {
                handlers::admin::delete_groups(r, &e.authz)
            })
        }

        // KIP-848 consumer groups.
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest::API_KEY => {
            c.tx(|r: &consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest, e| {
                handlers::consumer_group::heartbeat(r, &e.authz)
            })
        }
        consumer_group_describe_request::ConsumerGroupDescribeRequest::API_KEY => {
            c.tx(|r: &consumer_group_describe_request::ConsumerGroupDescribeRequest, e| {
                handlers::consumer_group::describe(r, &e.authz)
            })
        }

        // Share groups.
        share_group_heartbeat_request::ShareGroupHeartbeatRequest::API_KEY => {
            c.tx(|r: &share_group_heartbeat_request::ShareGroupHeartbeatRequest, e| {
                handlers::share_group::heartbeat(r, &e.authz)
            })
        }
        share_group_describe_request::ShareGroupDescribeRequest::API_KEY => {
            c.tx(|r: &share_group_describe_request::ShareGroupDescribeRequest, e| {
                handlers::share_group::describe(r, &e.authz)
            })
        }
        share_fetch_request::ShareFetchRequest::API_KEY => {
            c.tx(|r: &share_fetch_request::ShareFetchRequest, e| {
                let store = crate::storage::open();
                handlers::share_group::share_fetch(r, e.version, &*store, &e.authz)
            })
        }
        share_acknowledge_request::ShareAcknowledgeRequest::API_KEY => {
            c.tx(|r: &share_acknowledge_request::ShareAcknowledgeRequest, e| {
                handlers::share_group::share_acknowledge(r, e.version, &e.authz)
            })
        }
        describe_share_group_offsets_request::DescribeShareGroupOffsetsRequest::API_KEY => {
            c.tx(|r: &describe_share_group_offsets_request::DescribeShareGroupOffsetsRequest, e| {
                let store = crate::storage::open();
                handlers::share_offsets::describe(r, &*store, &e.authz)
            })
        }
        alter_share_group_offsets_request::AlterShareGroupOffsetsRequest::API_KEY => {
            c.tx(|r: &alter_share_group_offsets_request::AlterShareGroupOffsetsRequest, e| {
                handlers::share_offsets::alter(r, &e.authz)
            })
        }
        delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest::API_KEY => {
            c.tx(|r: &delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest, e| {
                handlers::share_offsets::delete(r, &e.authz)
            })
        }

        // Transactions and producer ids.
        init_producer_id_request::InitProducerIdRequest::API_KEY => {
            c.tx(|r: &init_producer_id_request::InitProducerIdRequest, e| {
                handlers::init_producer_id::handle(r, &e.authz)
            })
        }
        add_partitions_to_txn_request::AddPartitionsToTxnRequest::API_KEY => {
            c.tx(|r: &add_partitions_to_txn_request::AddPartitionsToTxnRequest, e| {
                handlers::txn::handle_add_partitions(r, e.version)
            })
        }
        add_offsets_to_txn_request::AddOffsetsToTxnRequest::API_KEY => {
            c.tx(|r: &add_offsets_to_txn_request::AddOffsetsToTxnRequest, e| {
                handlers::txn::handle_add_offsets(r, e.version)
            })
        }
        txn_offset_commit_request::TxnOffsetCommitRequest::API_KEY => {
            c.tx(|r: &txn_offset_commit_request::TxnOffsetCommitRequest, e| {
                handlers::txn::handle_txn_offset_commit(r, e.version)
            })
        }
        end_txn_request::EndTxnRequest::API_KEY => {
            c.tx(|r: &end_txn_request::EndTxnRequest, e| handlers::txn::handle_end_txn(r, e.version))
        }
        write_txn_markers_request::WriteTxnMarkersRequest::API_KEY => {
            c.tx(|r: &write_txn_markers_request::WriteTxnMarkersRequest, e| {
                handlers::txn::write_txn_markers(r, &e.authz)
            })
        }
        describe_producers_request::DescribeProducersRequest::API_KEY => {
            c.tx(|r: &describe_producers_request::DescribeProducersRequest, e| {
                handlers::introspect::describe_producers(r, &*crate::storage::open(), &e.authz)
            })
        }
        describe_transactions_request::DescribeTransactionsRequest::API_KEY => {
            c.tx(|r: &describe_transactions_request::DescribeTransactionsRequest, e| {
                handlers::introspect::describe_transactions(r, &e.authz)
            })
        }
        list_transactions_request::ListTransactionsRequest::API_KEY => {
            c.tx(|r: &list_transactions_request::ListTransactionsRequest, e| {
                handlers::introspect::list_transactions(r, &e.authz)
            })
        }

        // Topics, configs and ACLs.
        create_topics_request::CreateTopicsRequest::API_KEY => {
            c.tx(|r: &create_topics_request::CreateTopicsRequest, e| {
                handlers::topics::create_topics(r, &e.authz)
            })
        }
        delete_topics_request::DeleteTopicsRequest::API_KEY => {
            c.tx(|r: &delete_topics_request::DeleteTopicsRequest, e| {
                handlers::topics::delete_topics(r, &e.authz)
            })
        }
        create_partitions_request::CreatePartitionsRequest::API_KEY => {
            c.tx(|r: &create_partitions_request::CreatePartitionsRequest, e| {
                handlers::topics::create_partitions(r, &e.authz)
            })
        }
        describe_configs_request::DescribeConfigsRequest::API_KEY => {
            c.tx(|r: &describe_configs_request::DescribeConfigsRequest, e| {
                handlers::configs::describe_configs(r, &e.authz)
            })
        }
        alter_configs_request::AlterConfigsRequest::API_KEY => {
            c.tx(|r: &alter_configs_request::AlterConfigsRequest, e| {
                handlers::configs::alter_configs(r, &e.authz)
            })
        }
        incremental_alter_configs_request::IncrementalAlterConfigsRequest::API_KEY => {
            c.tx(|r: &incremental_alter_configs_request::IncrementalAlterConfigsRequest, e| {
                handlers::configs::incremental_alter_configs(r, &e.authz)
            })
        }
        // Reads `kafgres_topics`, so it needs a transaction: SPI outside one takes the postmaster down.
        list_config_resources_request::ListConfigResourcesRequest::API_KEY => {
            c.tx(|r: &list_config_resources_request::ListConfigResourcesRequest, e| {
                handlers::singleton::list_config_resources(r, &e.authz)
            })
        }
        describe_acls_request::DescribeAclsRequest::API_KEY => {
            c.tx(|r: &describe_acls_request::DescribeAclsRequest, e| {
                handlers::acls::describe_acls(r, &e.authz)
            })
        }
        create_acls_request::CreateAclsRequest::API_KEY => {
            c.tx(|r: &create_acls_request::CreateAclsRequest, e| {
                handlers::acls::create_acls(r, &e.authz)
            })
        }
        delete_acls_request::DeleteAclsRequest::API_KEY => {
            c.tx(|r: &delete_acls_request::DeleteAclsRequest, e| {
                handlers::acls::delete_acls(r, &e.authz)
            })
        }

        // Cluster administration.
        describe_log_dirs_request::DescribeLogDirsRequest::API_KEY => {
            c.tx(|r: &describe_log_dirs_request::DescribeLogDirsRequest, e| {
                handlers::admin::describe_log_dirs(r, &*crate::storage::open(), &e.authz)
            })
        }
        elect_leaders_request::ElectLeadersRequest::API_KEY => {
            c.tx(|r: &elect_leaders_request::ElectLeadersRequest, e| {
                handlers::admin::elect_leaders(r, &e.authz)
            })
        }
        describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest::API_KEY => {
            c.tx(|r: &describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest, e| {
                handlers::admin::describe_user_scram_credentials(r, &e.authz)
            })
        }
        alter_user_scram_credentials_request::AlterUserScramCredentialsRequest::API_KEY => {
            c.tx(|r: &alter_user_scram_credentials_request::AlterUserScramCredentialsRequest, e| {
                handlers::admin::alter_user_scram_credentials(r, &e.authz)
            })
        }
        describe_client_quotas_request::DescribeClientQuotasRequest::API_KEY => {
            c.tx(|r: &describe_client_quotas_request::DescribeClientQuotasRequest, e| {
                handlers::admin::describe_client_quotas(r, &e.authz)
            })
        }
        alter_client_quotas_request::AlterClientQuotasRequest::API_KEY => {
            c.tx(|r: &alter_client_quotas_request::AlterClientQuotasRequest, e| {
                handlers::admin::alter_client_quotas(r, &e.authz)
            })
        }

        // Single-node answers: APIs a one-broker cluster can only refuse or echo.
        list_partition_reassignments_request::ListPartitionReassignmentsRequest::API_KEY => {
            c.direct(|r: &list_partition_reassignments_request::ListPartitionReassignmentsRequest, e| {
                handlers::admin::list_partition_reassignments(r, &e.authz)
            })
        }
        alter_partition_reassignments_request::AlterPartitionReassignmentsRequest::API_KEY => {
            c.direct(|r: &alter_partition_reassignments_request::AlterPartitionReassignmentsRequest, e| {
                handlers::singleton::alter_partition_reassignments(r, &e.authz)
            })
        }
        AlterReplicaLogDirsRequest::API_KEY => {
            c.direct(|r: &AlterReplicaLogDirsRequest, e| {
                let log_dir = crate::storage::open().log_dir();
                handlers::singleton::alter_replica_log_dirs(r, &log_dir, &e.authz)
            })
        }
        create_delegation_token_request::CreateDelegationTokenRequest::API_KEY => {
            c.direct(|r: &create_delegation_token_request::CreateDelegationTokenRequest, _| {
                handlers::singleton::create_delegation_token(r)
            })
        }
        renew_delegation_token_request::RenewDelegationTokenRequest::API_KEY => {
            c.direct(|r: &renew_delegation_token_request::RenewDelegationTokenRequest, _| {
                handlers::singleton::renew_delegation_token(r)
            })
        }
        expire_delegation_token_request::ExpireDelegationTokenRequest::API_KEY => {
            c.direct(|r: &expire_delegation_token_request::ExpireDelegationTokenRequest, _| {
                handlers::singleton::expire_delegation_token(r)
            })
        }
        describe_delegation_token_request::DescribeDelegationTokenRequest::API_KEY => {
            c.direct(|r: &describe_delegation_token_request::DescribeDelegationTokenRequest, _| {
                handlers::singleton::describe_delegation_token(r)
            })
        }
        update_features_request::UpdateFeaturesRequest::API_KEY => {
            c.direct(|r: &update_features_request::UpdateFeaturesRequest, e| {
                handlers::singleton::update_features(r, e.version, &e.authz)
            })
        }
        unregister_broker_request::UnregisterBrokerRequest::API_KEY => {
            c.direct(|r: &unregister_broker_request::UnregisterBrokerRequest, e| {
                handlers::singleton::unregister_broker(r, &e.authz)
            })
        }
        add_raft_voter_request::AddRaftVoterRequest::API_KEY => {
            c.direct(|r: &add_raft_voter_request::AddRaftVoterRequest, e| {
                handlers::singleton::add_raft_voter(r, &e.authz)
            })
        }
        remove_raft_voter_request::RemoveRaftVoterRequest::API_KEY => {
            c.direct(|r: &remove_raft_voter_request::RemoveRaftVoterRequest, e| {
                handlers::singleton::remove_raft_voter(r, &e.authz)
            })
        }

        // APIs for features kafgres does not implement: protocol-correct refusals.
        describe_quorum_request::DescribeQuorumRequest::API_KEY => {
            c.direct(|r: &describe_quorum_request::DescribeQuorumRequest, e| {
                handlers::absent_peers::describe_quorum(r, &e.authz)
            })
        }
        initialize_share_group_state_request::InitializeShareGroupStateRequest::API_KEY => {
            c.direct(|r: &initialize_share_group_state_request::InitializeShareGroupStateRequest, e| {
                handlers::absent_peers::initialize_share_group_state(r, &e.authz)
            })
        }
        read_share_group_state_request::ReadShareGroupStateRequest::API_KEY => {
            c.direct(|r: &read_share_group_state_request::ReadShareGroupStateRequest, e| {
                handlers::absent_peers::read_share_group_state(r, &e.authz)
            })
        }
        write_share_group_state_request::WriteShareGroupStateRequest::API_KEY => {
            c.direct(|r: &write_share_group_state_request::WriteShareGroupStateRequest, e| {
                handlers::absent_peers::write_share_group_state(r, &e.authz)
            })
        }
        delete_share_group_state_request::DeleteShareGroupStateRequest::API_KEY => {
            c.direct(|r: &delete_share_group_state_request::DeleteShareGroupStateRequest, e| {
                handlers::absent_peers::delete_share_group_state(r, &e.authz)
            })
        }
        read_share_group_state_summary_request::ReadShareGroupStateSummaryRequest::API_KEY => {
            c.direct(|r: &read_share_group_state_summary_request::ReadShareGroupStateSummaryRequest, e| {
                handlers::absent_peers::read_share_group_state_summary(r, &e.authz)
            })
        }
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest::API_KEY => {
            c.direct(|r: &streams_group_heartbeat_request::StreamsGroupHeartbeatRequest, _| {
                handlers::absent_peers::streams_group_heartbeat(r)
            })
        }
        streams_group_describe_request::StreamsGroupDescribeRequest::API_KEY => {
            c.direct(|r: &streams_group_describe_request::StreamsGroupDescribeRequest, _| {
                handlers::absent_peers::streams_group_describe(r)
            })
        }

        // Unreachable: negotiate() already rejected anything not in ADVERTISED, and
        other => Err(kafgres_codec::CodecError::UnknownApiKey(other).into()),
    }
}

fn serve_produce(c: &mut Ctx) -> Result<Disposition, HandlerError> {
    let request: ProduceRequest = c.decode()?;
    let authz = c.authz();
    let outcome = guarded_tx(|| {
        let mut store = crate::storage::open();
        handlers::produce::handle(&request, &mut *store, &authz)
    })?;
    // Ring the doorbell before answering: a consumer parked on this partition
    c.srv.appended.extend(outcome.appended.iter().copied());
    if crate::fsync_before_ack() && !outcome.appended.is_empty() {
        c.srv.unsynced.extend(outcome.appended.iter().copied());
        if let Some(conn) = c.srv.conns.get_mut(&c.conn_id) {
            conn.awaiting_sync = true;
        }
    }
    // Charged after the append, on the bytes actually written — not the request size, or rejected batches get billed.
    let throttle = c.charge(crate::quota::Rate::Producer, outcome.bytes as i64);
    match outcome.response {
        None => Ok(Disposition::NoReply),
        Some(mut body) => {
            body.throttle_time_ms = throttle;
            c.reply(&body)
        }
    }
}

fn serve_fetch(c: &mut Ctx) -> Result<Disposition, HandlerError> {
    let request: FetchRequest = c.decode()?;
    // One transaction: SPI outside an established transaction segfaults the worker.
    let (body, watching) = run_fetch(&request, &c.authz(), true)?;

    let min_bytes = request.min_bytes.max(1) as usize;
    let wait = fetch_wait(request.max_wait_ms);

    // Answer now if satisfied, if the client did not want to wait, on error, or at the parking cap.
    let satisfied = handlers::fetch::records_bytes(&body) >= min_bytes
        || handlers::fetch::has_error(&body)
        || wait.is_zero()
        || c.parking_full();

    if satisfied {
        // Charged on the record bytes actually returned; the parked path is charged in `complete_parked`.
        let mut body = body;
        body.throttle_time_ms = c.charge(
            crate::quota::Rate::Consumer,
            handlers::fetch::records_bytes(&body) as i64,
        );
        return c.reply(&body);
    }

    Ok(c.park(
        Instant::now() + wait,
        Waiting::Fetch {
            request,
            watching,
            min_bytes,
        },
    ))
}

fn serve_join_group(c: &mut Ctx) -> Result<Disposition, HandlerError> {
    let request: JoinGroupRequest = c.decode()?;
    let authz = c.authz();
    let version = c.req.api_version;
    let client_id = c.client_id().to_string();
    let peer = c
        .srv
        .conns
        .get(&c.conn_id)
        .map(|conn| conn.peer.clone())
        .unwrap_or_default();
    let outcome = guarded_tx(|| {
        handlers::join_sync::join_group(&request, version, &client_id, &peer, &authz)
    })?;
    c.srv.groups_changed.insert(request.group_id.clone());
    match outcome {
        handlers::join_sync::JoinOutcome::Reply(body) => c.reply(&*body),
        handlers::join_sync::JoinOutcome::Park { member_id } => {
            if c.parking_full() {
                // Refusing is better than parking past the ceiling: the client
                let body = handlers::join_sync::error_join(
                    kafgres_codec::ErrorCode::RebalanceInProgress,
                    member_id,
                );
                return c.reply(&body);
            }
            // The rebalance timeout is the client's own patience. Past it
            let deadline = Instant::now()
                + Duration::from_millis(crate::group::clamp_rebalance_timeout(
                    request.rebalance_timeout_ms,
                ) as u64);
            Ok(c.park(
                deadline,
                Waiting::Join {
                    group_id: request.group_id.clone(),
                    member_id,
                },
            ))
        }
    }
}

fn serve_sync_group(c: &mut Ctx) -> Result<Disposition, HandlerError> {
    let request: SyncGroupRequest = c.decode()?;
    let authz = c.authz();
    let outcome = guarded_tx(|| handlers::join_sync::sync_group(&request, &authz))?;
    c.srv.groups_changed.insert(request.group_id.clone());
    match outcome {
        handlers::join_sync::SyncOutcome::Reply(body) => c.reply(&*body),
        handlers::join_sync::SyncOutcome::Park => {
            if c.parking_full() {
                let body =
                    handlers::join_sync::error_sync(kafgres_codec::ErrorCode::RebalanceInProgress);
                return c.reply(&body);
            }
            Ok(c.park(
                Instant::now() + Duration::from_millis(60_000),
                Waiting::Sync {
                    group_id: request.group_id.clone(),
                    member_id: request.member_id.clone(),
                },
            ))
        }
    }
}

fn serve_sasl_handshake(c: &mut Ctx) -> Result<Disposition, HandlerError> {
    let request: SaslHandshakeRequest = c.decode()?;
    let state = conn_sasl(c.srv, c.conn_id);
    let (body, next) = handlers::auth::handshake(&request, &state);
    if let Some(conn) = c.srv.conns.get_mut(&c.conn_id) {
        match next {
            Some(next) => {
                // The accepted version decides the framing of everything after
                conn.sasl_raw = c.req.api_version == 0;
                conn.sasl = next;
            }
            None => conn.sasl_failures += 1,
        }
    }
    c.reply(&body)
}

fn serve_sasl_authenticate(c: &mut Ctx) -> Result<Disposition, HandlerError> {
    let request: SaslAuthenticateRequest = c.decode()?;
    // Credentials are checked against pg_authid, so this needs a transaction —
    let state = conn_sasl(c.srv, c.conn_id);
    // `contained`, not `guarded`: this reads pg_authid and touches none of our
    let (body, next) = contained_tx(|| Ok(handlers::auth::authenticate(&request, &state)))?;
    if let Some(conn) = c.srv.conns.get_mut(&c.conn_id) {
        match next {
            Some(next) => {
                if let crate::sasl::SaslState::Authenticated { principal } = &next {
                    log!("kafgres: {} authenticated as '{principal}'", conn.peer);
                }
                conn.sasl = next;
            }
            // A failed proof leaves the state at AwaitingFinal, so the same
            None => conn.sasl_failures += 1,
        }
    }
    c.reply(&body)
}
