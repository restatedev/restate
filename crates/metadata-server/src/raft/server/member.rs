// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arc_swap::ArcSwapOption;
use bytes::BytesMut;
use metrics::gauge;
use prost::Message as ProstMessage;
use protobuf::Message as ProtobufMessage;
use raft::{
    Config, Error as RaftError, INVALID_ID, RawNode, ReadOnlyOption, SnapshotStatus, Storage,
};
use raft_proto::ConfChangeI;
use raft_proto::eraftpb::{
    ConfChange, ConfChangeSingle, ConfChangeType, ConfChangeV2, Entry, EntryType, Message,
    Snapshot, SnapshotMetadata,
};
use raft_proto::prelude::MessageType;
use slog::o;
use std::collections::{HashMap, VecDeque};
use std::mem;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{debug, info, instrument, trace, warn};
use tracing_slog::TracingSlogDrain;
use ulid::Ulid;

use restate_core::{Metadata, MetadataWriter, TaskCenter};
use restate_metadata_providers::replicated::KnownLeader;
use restate_metadata_server_grpc::grpc::MetadataServerSnapshot;
use restate_metadata_server_grpc::{MetadataServerConfiguration, grpc};
use restate_metadata_store::serialize_value;
use restate_types::config::Configuration;
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{MetadataServerState, NodesConfiguration, Role};
use restate_types::{NodeId, PlainNodeId, Version};

use crate::metric_definitions::{
    METADATA_SERVER_REPLICATED_APPLIED_LSN, METADATA_SERVER_REPLICATED_COMMITTED_LSN,
    METADATA_SERVER_REPLICATED_FIRST_INDEX, METADATA_SERVER_REPLICATED_LAST_INDEX,
    METADATA_SERVER_REPLICATED_LEADER_ID, METADATA_SERVER_REPLICATED_SNAPSHOT_SIZE_BYTES,
    METADATA_SERVER_REPLICATED_TERM,
};
use crate::raft::kv_memory_storage::KvMemoryStorage;
use crate::raft::network::{ConnectionManager, Networking};
use crate::raft::server::standby::Standby;
use crate::raft::server::uninitialized::Uninitialized;
use crate::raft::server::{
    ConfChangeError, CreateSnapshotError, Error, RaftServerComponents, RestoreSnapshotError,
};
use crate::raft::storage::RocksDbStorage;
use crate::raft::{to_plain_node_id, to_raft_id};
use crate::{
    AddNodeError, CreatedAtMillis, JoinClusterError, JoinClusterReceiver, JoinClusterRequest,
    JoinClusterResponseSender, MemberId, MetadataCommand, MetadataCommandError,
    MetadataCommandReceiver, MetadataServerSummary, MetadataStoreRequest, PreconditionViolation,
    RaftSummary, RemoveNodeError, RemoveNodeResponseSender, Request, RequestError, RequestReceiver,
    SnapshotSummary, StatusSender, WriteRequest,
};

pub struct Member {
    _logger: slog::Logger,

    min_expected_nodes_config_version: Version,

    raw_node: RawNode<RocksDbStorage>,
    networking: Networking<Message>,
    raft_rx: mpsc::Receiver<Message>,

    tick_interval: Interval,
    status_update_interval: Interval,
    log_trim_threshold: u64,

    my_member_id: MemberId,
    configuration: MetadataServerConfiguration,
    kv_storage: KvMemoryStorage,
    is_leader: bool,
    pending_join_requests: HashMap<MemberId, JoinClusterResponseSender>,
    pending_remove_requests: HashMap<MemberId, RemoveNodeResponseSender>,
    read_index_to_request_id: VecDeque<(u64, Ulid)>,
    snapshot_summary: Option<SnapshotSummary>,
    is_leaving: bool,

    connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
    metadata_writer: Option<MetadataWriter>,

    request_rx: RequestReceiver,
    join_cluster_rx: JoinClusterReceiver,
    status_tx: StatusSender,
    command_rx: MetadataCommandReceiver,
}

impl Member {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        my_member_id: MemberId,
        min_expected_nodes_config_version: Version,
        connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
        storage: RocksDbStorage,
        request_rx: RequestReceiver,
        join_cluster_rx: JoinClusterReceiver,
        metadata_writer: Option<MetadataWriter>,
        status_tx: StatusSender,
        command_rx: MetadataCommandReceiver,
    ) -> Result<Self, Error> {
        let (raft_tx, raft_rx) = mpsc::channel(128);
        let new_connection_manager = ConnectionManager::new(my_member_id.node_id, raft_tx);
        let mut networking = Networking::new(new_connection_manager.clone());

        networking.register_address(
            my_member_id.node_id,
            TaskCenter::with_current(|tc| {
                Configuration::pinned()
                    .common
                    .advertised_address(tc.address_book())
            }),
        );

        // todo remove additional indirection from Arc
        connection_manager.store(Some(Arc::new(new_connection_manager)));

        let raft_options = &Configuration::pinned().metadata_server.raft_options;

        let mut config = Config {
            id: to_raft_id(my_member_id.node_id),
            election_tick: raft_options.raft_election_tick.get(),
            heartbeat_tick: raft_options.raft_heartbeat_tick.get(),
            read_only_option: ReadOnlyOption::Safe,
            check_quorum: true,
            pre_vote: true,
            // 64 KiB
            max_size_per_msg: 64 * 1024,
            max_inflight_msgs: 256,
            // 4 MiB
            max_uncommitted_size: 4 * 1024 * 1024,
            // 4 MiB
            max_committed_size_per_ready: 4 * 1024 * 1024,
            // only the leader should be allowed to accept proposals
            disable_proposal_forwarding: true,
            ..Config::default()
        };

        let drain = TracingSlogDrain;
        let logger = slog::Logger::root(drain, o!());

        let mut kv_storage = KvMemoryStorage::new(metadata_writer.clone());
        let mut snapshot_summary = None;
        let mut configuration = MetadataServerConfiguration::default();

        if let Ok(snapshot) = storage.snapshot(0, to_raft_id(my_member_id.node_id)) {
            config.applied = snapshot.get_metadata().get_index();
            Self::restore_fsm_snapshot(snapshot.get_data(), &mut configuration, &mut kv_storage)?;
            snapshot_summary = Some(SnapshotSummary::from_snapshot(&snapshot));
        }

        config.validate()?;
        let mut raw_node = RawNode::new(&config, storage, &logger)?;

        if raw_node
            .raft
            .prs()
            .conf()
            .voters()
            .contains(to_raft_id(my_member_id.node_id))
        {
            // Campaign if we are part of the voters to quickly become leader if there is none. This
            // won't cause the current leader to step down since pre-vote is enabled.
            raw_node.campaign()?;
        }

        let mut tick_interval = time::interval(raft_options.raft_tick_interval.into());
        tick_interval.set_missed_tick_behavior(MissedTickBehavior::Burst);
        let status_update_interval = time::interval(raft_options.status_update_interval.into());

        let member = Member {
            _logger: logger,
            is_leader: false,
            my_member_id,
            min_expected_nodes_config_version,
            configuration,
            raw_node,
            connection_manager,
            networking,
            raft_rx,
            kv_storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            tick_interval,
            status_update_interval,
            log_trim_threshold: raft_options.log_trim_threshold.unwrap_or(1000),
            status_tx,
            command_rx,
            pending_join_requests: HashMap::default(),
            pending_remove_requests: HashMap::default(),
            read_index_to_request_id: VecDeque::default(),
            snapshot_summary,
            is_leaving: false,
        };

        member.validate_metadata_server_configuration();

        Ok(member)
    }

    pub fn try_from_uninitialized(
        my_member_id: MemberId,
        min_expected_nodes_config_version: Version,
        unitialized: Uninitialized,
    ) -> Result<Self, Error> {
        let RaftServerComponents {
            storage,
            connection_manager,
            request_rx,
            status_tx,
            command_rx,
            join_cluster_rx,
            metadata_writer,
        } = unitialized.into_inner();
        Self::create(
            my_member_id,
            min_expected_nodes_config_version,
            connection_manager,
            storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
            command_rx,
        )
    }

    pub fn try_from_standby(
        my_member_id: MemberId,
        min_expected_nodes_config_version: Version,
        standby: Standby,
    ) -> Result<Self, Error> {
        let RaftServerComponents {
            storage,
            connection_manager,
            request_rx,
            status_tx,
            command_rx,
            join_cluster_rx,
            metadata_writer,
        } = standby.into_inner();
        Self::create(
            my_member_id,
            min_expected_nodes_config_version,
            connection_manager,
            storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
            command_rx,
        )
    }

    pub fn into_inner(self) -> RaftServerComponents {
        RaftServerComponents {
            storage: self.raw_node.raft.r.raft_log.store,
            connection_manager: self.connection_manager,
            request_rx: self.request_rx,
            status_tx: self.status_tx,
            command_rx: self.command_rx,
            join_cluster_rx: self.join_cluster_rx,
            metadata_writer: self.metadata_writer,
        }
    }

    #[instrument(level = "info", skip_all, fields(member_id = %self.my_member_id))]
    pub async fn run(&mut self) -> Result<(), Error> {
        info!(configuration = %self.configuration, "Run as member of the metadata cluster");
        self.update_status();

        let mut nodes_config_watch =
            Metadata::with_current(|m| m.watch(MetadataKind::NodesConfiguration));
        let mut nodes_config = Metadata::with_current(|m| m.updateable_nodes_config());

        loop {
            tokio::select! {
                biased;
                Some(raft) = self.raft_rx.recv() => {
                    if let Err(err) = self.raw_node.step(raft) {
                        match err {
                            RaftError::StepPeerNotFound => {
                                info!("Ignoring raft message from unknown node. This can happen if \
                                the node has been removed from the cluster. If not, then this \
                                indicates a misconfiguration of your cluster!");
                            }
                            // escalate, as we can't handle this error
                            err => Err(err)?
                        }
                    }
                },
                _ = self.tick_interval.tick() => {
                    self.raw_node.tick();
                },
                Ok(()) = nodes_config_watch.changed() => {
                    let nodes_config = nodes_config.live_load();
                    if self.should_leave(nodes_config) {
                        break;
                    }

                    self.update_node_addresses(nodes_config);
                },
                Some(request) = self.request_rx.recv() => {
                    self.handle_request(request, nodes_config.live_load());
                },
                Some(request) = self.join_cluster_rx.recv() => {
                    self.handle_join_request(request, nodes_config.live_load());
                }
                Some(command) = self.command_rx.recv() => {
                    self.handle_command(command, nodes_config.live_load());
                }
                _ = self.status_update_interval.tick() => {
                    self.update_status();
                },
            }

            let metadata_nodes_config = nodes_config.live_load();
            self.on_ready(metadata_nodes_config).await?;
            self.update_leadership(metadata_nodes_config);

            if self.is_leaving {
                break;
            }
        }

        self.shutdown().await?;

        let storage = self.raw_node.mut_store();

        let nodes_config =
            Self::latest_nodes_configuration(&self.kv_storage, nodes_config.live_load());

        // Set our raft server state to standby and store the latest nodes configuration so that we
        // don't start as a member and don't try to join again when restarting.
        let mut txn = storage.txn();
        txn.delete_raft_server_state()?;
        txn.store_nodes_configuration(nodes_config)?;
        txn.commit().await?;

        // Make sure that the latest nodes configuration has been updated by the metadata manager.
        // This is necessary if the asynchronous operation to update the NodesConfiguration,
        // issued by the KvMemoryStorage, has not been processed yet.
        Metadata::current()
            .wait_for_version(MetadataKind::NodesConfiguration, nodes_config.version())
            .await?;

        Ok(())
    }

    /// Shuts the member down by failing all pending requests and transferring leadership if it is the
    /// leader.
    pub async fn shutdown(&mut self) -> Result<(), Error> {
        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        self.fail_pending_requests(&nodes_config);

        if self.is_leader {
            debug!("Shutting down as leading member. Trying to transfer leadership.");

            let cluster_state = TaskCenter::with_current(|h| h.cluster_state().clone());

            if let Some(dedicated_leader) = self
                .configuration
                .members
                .keys()
                .filter(|&member| {
                    // only pick alive nodes that aren't me
                    member != &self.my_member_id.node_id
                        && cluster_state.is_alive(NodeId::from(*member))
                })
                .max_by_key(|&member| {
                    // pick the node with the most matched state
                    self.raw_node
                        .raft
                        .prs()
                        .get(to_raft_id(*member))
                        .map(|pr| pr.matched)
                })
            {
                info!(
                    "Transferring metadata cluster leadership to {dedicated_leader} because of shut down."
                );
                let dedicated_leader = to_raft_id(*dedicated_leader);

                // Prepare timeout now message for the dedicated leader. This will cause the
                // dedicated leader to start a leader election w/o pre-election.
                let mut timeout_now_message = Message::default();
                timeout_now_message.set_msg_type(MessageType::MsgTimeoutNow);
                timeout_now_message.from = self.raw_node.raft.id;
                timeout_now_message.to = dedicated_leader;
                timeout_now_message.term = self.raw_node.raft.term;
                self.send_messages(vec![timeout_now_message]);
            } else {
                debug!(
                    "Failed transferring metadata cluster leadership because there is no alive member."
                );
            }
        }

        Ok(())
    }

    fn should_leave(&self, nodes_config: &NodesConfiguration) -> bool {
        if self.min_expected_nodes_config_version > nodes_config.version() {
            // we haven't reached the min expected nodes_config version to act on yet
            return false;
        }

        let node_config = match nodes_config.find_node_by_id(self.my_member_id.node_id) {
            Ok(node_config) => node_config,
            Err(_err) => {
                warn!(
                    "Nodes configuration no longer contains node {}. This indicates that this node \
                    was removed from the cluster without removing it first from the metadata \
                    cluster. Leaving the metadata cluster now.",
                    self.my_member_id.node_id
                );
                return true;
            }
        };

        match node_config.metadata_server_config.metadata_server_state {
            MetadataServerState::Standby | MetadataServerState::Provisioning => {
                let is_member = self.is_member(self.my_member_id);

                if is_member {
                    info!(
                        "Asked to leave the metadata store cluster as of nodes configuration {} while \
                        still being a member of the configuration. This indicates that I missed the \
                        configuration change to remove me. Leaving metadata store cluster now.",
                        nodes_config.version()
                    );
                } else {
                    info!(
                        "Leaving metadata store cluster as of nodes configuration {}",
                        nodes_config.version()
                    );
                }

                true
            }
            MetadataServerState::Member => false,
        }
    }

    fn update_leadership(&mut self, metadata_nodes_config: &NodesConfiguration) {
        let previous_is_leader = self.is_leader;
        self.is_leader = self.raw_node.raft.leader_id == self.raw_node.raft.id;

        if previous_is_leader && !self.is_leader {
            let known_leader = self.fail_pending_requests(metadata_nodes_config);

            info!(
                possible_leader = ?known_leader,
                "Lost metadata cluster leadership"
            );
        } else if !previous_is_leader && self.is_leader {
            info!("Won metadata cluster leadership");
        }
    }

    fn fail_pending_requests(
        &mut self,
        metadata_nodes_config: &NodesConfiguration,
    ) -> Option<KnownLeader> {
        let known_leader = self.known_leader(metadata_nodes_config);
        // todo we might fail some of the request too eagerly here because the answer might be
        //  stored in the unapplied log entries. Better to fail the callbacks based on
        //  (term, index).
        // we lost leadership :-( notify callers that their requests might not get committed
        // because we don't know whether the leader will start with the same log as we have.
        self.kv_storage.fail_pending_requests(|| {
            RequestError::Unavailable("lost leadership".into(), known_leader.clone())
        });
        self.fail_join_callbacks(|| JoinClusterError::NotLeader(known_leader.clone()));
        self.fail_remove_callbacks(|| MetadataCommandError::NotLeader(known_leader.clone()));
        self.read_index_to_request_id.clear();
        known_leader
    }

    fn handle_request(
        &mut self,
        request: MetadataStoreRequest,
        metadata_nodes_config: &NodesConfiguration,
    ) {
        let (request, cluster_identity) = request.into_request();

        let nodes_config =
            Self::latest_nodes_configuration(&self.kv_storage, metadata_nodes_config);

        // Validate cluster identity: first fingerprint, then cluster_name. In some cases only the
        // cluster name is set (e.g. when a node is trying to join a cluster and asking for the
        // NodesConfiguration). Older versions of Restate < v1.6 won't set these fields.
        if let Some(incoming_fingerprint) = cluster_identity.fingerprint {
            let expected_fingerprint = nodes_config.cluster_fingerprint();

            if incoming_fingerprint != expected_fingerprint {
                request.fail(RequestError::ClusterIdentityMismatch(format!(
                    "cluster fingerprint mismatch: expected {}, got {}",
                    expected_fingerprint, incoming_fingerprint
                )));
                return;
            }
        }

        if let Some(incoming_cluster_name) = cluster_identity.cluster_name {
            let expected_cluster_name = nodes_config.cluster_name();

            if incoming_cluster_name != expected_cluster_name {
                request.fail(RequestError::ClusterIdentityMismatch(format!(
                    "cluster name mismatch: expected {}, got {}",
                    expected_cluster_name, incoming_cluster_name
                )));
                return;
            }
        }
        // If neither fingerprint nor cluster_name is provided, allow (backward compatibility)
        // todo in v1.7 no longer accept if no cluster identity was provided

        trace!("Handle metadata store request: {request:?}");

        if !self.is_leader {
            request.fail(RequestError::Unavailable(
                "not leader".into(),
                self.known_leader(nodes_config),
            ));
            return;
        }

        match request {
            Request::ReadOnly(read_only_request) => {
                let read_ctx = read_only_request.request_id.to_bytes().to_vec();

                let previous_ready_read_count = self.raw_node.raft.ready_read_count();
                let previous_pending_read_count = self.raw_node.raft.pending_read_count();

                self.raw_node.read_index(read_ctx);

                // check whether the read request was silently dropped
                if previous_ready_read_count == self.raw_node.raft.ready_read_count()
                    && previous_pending_read_count == self.raw_node.raft.pending_read_count()
                {
                    // fail the request if we cannot serve read-only requests yet
                    read_only_request.fail(RequestError::Unavailable("Cannot serve read-only queries yet because the latest commit index has not been retrieved. Try again in a bit".into(), self.known_leader(nodes_config)));
                } else {
                    self.kv_storage
                        .register_read_only_request(read_only_request);
                }
            }
            Request::Write { request, callback } => {
                match request.precondition() {
                    Precondition::MatchesVersion(expected_version) => {
                        // Important assumption: The version per key is monotonically increasing.
                        // It will never be deleted and reset to a lower value than before!
                        //
                        // The worst thing that can happen if this assumption does not hold is that
                        // we are wrongly rejecting a write which should be retried by the caller
                        // anyway.
                        if let Some(actual_version) = self.kv_storage.get_version(request.key())
                            && actual_version > expected_version
                        {
                            callback.fail(RequestError::FailedPrecondition(
                                PreconditionViolation::VersionMismatch {
                                    expected: expected_version,
                                    actual: Some(actual_version),
                                },
                            ));
                            return;
                        }
                    }
                    Precondition::DoesNotExist => {
                        // Important assumption: The version per key is monotonically increasing.
                        // It will never be deleted. Otherwise, we can't be sure that an existing key
                        // won't be deleted until this request is processed.
                        //
                        // The worst thing that can happen if this assumption does not hold is that
                        // we are wrongly rejecting a write which should be retried by the caller
                        // anyway.
                        if self.kv_storage.contains(request.key()) {
                            callback.fail(RequestError::FailedPrecondition(
                                PreconditionViolation::Exists,
                            ));
                            return;
                        }
                    }
                    Precondition::None => {
                        // nothing to do
                    }
                }

                if let Err(err) = request
                    .encode_to_vec()
                    .map_err(Into::into)
                    .and_then(|request| {
                        self.raw_node
                            .propose(vec![], request)
                            .map_err(RequestError::from)
                    })
                {
                    info!(%err, "Failed handling request");
                    callback.fail(err)
                } else {
                    self.kv_storage.register_callback(callback);
                }
            }
        }
    }

    fn handle_join_request(
        &mut self,
        join_cluster_request: JoinClusterRequest,
        metadata_nodes_config: &NodesConfiguration,
    ) {
        let (response_tx, joining_member_id, cluster_fingerprint) =
            join_cluster_request.into_inner();

        let nodes_config =
            Self::latest_nodes_configuration(&self.kv_storage, metadata_nodes_config);

        trace!("Handle join request from node '{}'", joining_member_id);

        if self.is_member(joining_member_id) {
            let _ = response_tx.send(Ok(self
                .kv_storage
                .last_seen_nodes_configuration()
                .version()));
            return;
        }

        // todo in v1.7 make this check mandatory and fail if cluster_fingerprint is None
        // Validate cluster fingerprint if provided (before other sanity checks)
        if let Some(incoming_fingerprint) = cluster_fingerprint {
            let expected_fingerprint = nodes_config.cluster_fingerprint();

            if incoming_fingerprint != expected_fingerprint {
                let _ = response_tx.send(Err(JoinClusterError::ClusterFingerprintMismatch));
                return;
            }
        }

        // sanity checks

        if !self.is_leader {
            let _ = response_tx.send(Err(JoinClusterError::NotLeader(
                self.known_leader(nodes_config),
            )));
            return;
        }

        if self.raw_node.raft.has_pending_conf() {
            let _ = response_tx.send(Err(JoinClusterError::PendingReconfiguration));
            return;
        }

        if self.is_member_plain_node_id(joining_member_id.node_id) {
            let warning = format!(
                "Node '{joining_member_id}' has registered before with a different storage id. This indicates that this node has lost its disk. Rejecting the join attempt."
            );
            warn!(warning);
            let _ = response_tx.send(Err(JoinClusterError::Internal(warning)));
            return;
        }

        let Ok(joining_node_config) = nodes_config.find_node_by_id(joining_member_id.node_id)
        else {
            let _ = response_tx.send(Err(JoinClusterError::UnknownNode(
                joining_member_id.node_id,
            )));
            return;
        };

        if !joining_node_config.has_role(Role::MetadataServer) {
            let _ = response_tx.send(Err(JoinClusterError::InvalidRole(
                joining_member_id.node_id,
            )));
            return;
        }

        // It's possible to batch multiple new joining nodes into a single conf change if we want.
        // This will, however, require joint consensus.
        let (conf_change, next_configuration) = self.add_member_conf_change(joining_member_id);

        let next_configuration_bytes =
            grpc::MetadataServerConfiguration::from(next_configuration).encode_to_vec();

        if let Err(err) = self
            .raw_node
            .propose_conf_change(next_configuration_bytes, conf_change)
        {
            let response = match err {
                RaftError::ProposalDropped => JoinClusterError::ProposalDropped,
                err => JoinClusterError::Internal(err.to_string()),
            };

            let _ = response_tx.send(Err(response));
        } else {
            info!(
                "Trying to add node '{}' to metadata cluster",
                joining_member_id.node_id
            );
            self.register_join_callback(joining_member_id, response_tx);
        }
    }

    fn add_member_conf_change(
        &self,
        joining_member_id: MemberId,
    ) -> (ConfChangeV2, MetadataServerConfiguration) {
        let mut conf_change_single = ConfChangeSingle::new();
        conf_change_single.change_type = ConfChangeType::AddNode;
        conf_change_single.node_id = to_raft_id(joining_member_id.node_id);

        let mut conf_change = ConfChangeV2::new();
        conf_change.set_changes(vec![conf_change_single].into());

        let mut next_configuration = self.configuration.clone();
        next_configuration.version = next_configuration.version.next();
        next_configuration.members.insert(
            joining_member_id.node_id,
            joining_member_id.created_at_millis,
        );
        (conf_change, next_configuration)
    }

    fn remove_member_conf_change(
        &self,
        leaving_member_id: MemberId,
    ) -> (ConfChangeV2, MetadataServerConfiguration) {
        let mut conf_change_single = ConfChangeSingle::new();
        conf_change_single.change_type = ConfChangeType::RemoveNode;
        conf_change_single.node_id = to_raft_id(leaving_member_id.node_id);

        let mut conf_change = ConfChangeV2::new();
        conf_change.set_changes(vec![conf_change_single].into());

        let mut next_configuration = self.configuration.clone();
        next_configuration.version = next_configuration.version.next();
        assert!(
            next_configuration
                .members
                .remove(&leaving_member_id.node_id,)
                .is_some(),
            "expect to remove member {leaving_member_id}"
        );

        (conf_change, next_configuration)
    }

    async fn on_ready(&mut self, metadata_nodes_config: &NodesConfiguration) -> Result<(), Error> {
        if !self.raw_node.has_ready() {
            return Ok(());
        }

        let mut ready = self.raw_node.ready();

        // first need to send outgoing messages
        if !ready.messages().is_empty() {
            self.send_messages(ready.take_messages());
        }

        // apply snapshot if one was sent
        if !ready.snapshot().is_empty() {
            self.apply_snapshot(ready.snapshot()).await?;
        }

        // handle read states
        self.handle_read_states(ready.take_read_states()).await?;

        // then handle committed entries
        self.handle_committed_entries(ready.take_committed_entries(), metadata_nodes_config)
            .await?;

        // append new Raft entries to storage
        self.raw_node.mut_store().append(ready.entries()).await?;

        // update the hard state if an update was produced (e.g. vote has happened)
        if let Some(hs) = ready.hs() {
            self.raw_node
                .mut_store()
                .store_hard_state(hs.clone())
                .await?;
        }

        // send persisted messages (after entries were appended and hard state was updated)
        if !ready.persisted_messages().is_empty() {
            self.send_messages(ready.take_persisted_messages());
        }

        // advance the raft node
        let mut light_ready = self.raw_node.advance(ready);

        // update the commit index if it changed
        if let Some(_commit) = light_ready.commit_index() {
            // todo update commit index in cached hard_state once we cache it; no need to persist it though
        }

        // send outgoing messages
        if !light_ready.messages().is_empty() {
            self.send_messages(light_ready.take_messages());
        }

        // handle committed entries
        if !light_ready.committed_entries().is_empty() {
            self.handle_committed_entries(
                light_ready.take_committed_entries(),
                metadata_nodes_config,
            )
            .await?;
        }

        self.raw_node.advance_apply();

        // after we have applied new entries, check whether we can fulfill some read-only requests
        self.handle_read_only_requests();

        self.try_trim_log().await?;
        self.check_requested_snapshot().await?;

        Ok(())
    }

    async fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Error> {
        Self::restore_fsm_snapshot(
            snapshot.get_data(),
            &mut self.configuration,
            &mut self.kv_storage,
        )?;
        info!(configuration = %self.configuration, "Restored configuration from snapshot");

        self.validate_metadata_server_configuration();

        self.raw_node.mut_store().apply_snapshot(snapshot).await?;

        self.snapshot_summary = Some(SnapshotSummary::from_snapshot(snapshot));

        Ok(())
    }

    fn restore_fsm_snapshot(
        mut data: &[u8],
        configuration: &mut MetadataServerConfiguration,
        kv_storage: &mut KvMemoryStorage,
    ) -> Result<(), RestoreSnapshotError> {
        if data.is_empty() {
            return Ok(());
        }

        let mut snapshot = MetadataServerSnapshot::decode(&mut data)?;
        *configuration = snapshot
            .configuration
            .take()
            .map(MetadataServerConfiguration::from)
            .expect("configuration metadata expected");

        kv_storage.restore(snapshot)?;
        Ok(())
    }

    fn send_messages(&mut self, messages: Vec<Message>) {
        for message in messages {
            let snapshot_target = if message.has_snapshot() {
                Some(message.to)
            } else {
                None
            };

            if let Err(err) = self.networking.try_send(message) {
                trace!("failed sending message: {err}");

                if let Some(message) = err.into_message() {
                    if message.has_snapshot() {
                        self.raw_node
                            .report_snapshot(message.to, SnapshotStatus::Failure);
                    } else {
                        self.raw_node.report_unreachable(message.to);
                    }
                }
            } else if let Some(snapshot_target) = snapshot_target {
                self.raw_node
                    .report_snapshot(snapshot_target, SnapshotStatus::Finish);
            }
        }
    }

    async fn handle_read_states(&mut self, read_states: Vec<raft::ReadState>) -> Result<(), Error> {
        for read_state in read_states {
            let request_id =
                Ulid::from_bytes(read_state.request_ctx.try_into().map_err(|_err| {
                    Error::DecodeRequest("could not deserialize Ulid from read request ctx".into())
                })?);

            if read_state.index <= self.raw_node.raft.raft_log.applied {
                self.kv_storage.handle_read_only_request(request_id);
            } else {
                self.read_index_to_request_id
                    .push_back((read_state.index, request_id));
            }
        }

        Ok(())
    }

    fn handle_read_only_requests(&mut self) {
        let applied_index = self.raw_node.raft.raft_log.applied;
        while self
            .read_index_to_request_id
            .front()
            .is_some_and(|(index, _)| *index <= applied_index)
        {
            let (_, request_id) = self
                .read_index_to_request_id
                .pop_front()
                .expect("to be present");
            self.kv_storage.handle_read_only_request(request_id);
        }
    }

    async fn handle_committed_entries(
        &mut self,
        committed_entries: Vec<Entry>,
        metadata_nodes_config: &NodesConfiguration,
    ) -> Result<(), Error> {
        for entry in committed_entries {
            if entry.data.is_empty() {
                // new leader was elected
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => self.handle_normal_entry(entry)?,
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    self.handle_conf_change(entry, metadata_nodes_config)
                        .await?
                }
            }
        }

        Ok(())
    }

    fn handle_normal_entry(&mut self, entry: Entry) -> Result<(), Error> {
        let request = WriteRequest::decode_from_bytes(entry.data)
            .map_err(|err| Error::DecodeRequest(err.into()))?;
        self.kv_storage.handle_request(request);

        Ok(())
    }

    async fn handle_conf_change(
        &mut self,
        entry: Entry,
        metadata_nodes_config: &NodesConfiguration,
    ) -> Result<(), ConfChangeError> {
        let cc_v2 = match entry.entry_type {
            EntryType::EntryNormal => {
                panic!("normal entries should be handled by handle_normal_entry")
            }
            EntryType::EntryConfChange => {
                let mut cc = ConfChange::default();
                cc.merge_from_bytes(&entry.data)?;
                cc.into_v2()
            }
            EntryType::EntryConfChangeV2 => {
                let mut cc = ConfChangeV2::default();
                cc.merge_from_bytes(&entry.data)?;
                cc
            }
        };

        let new_configuration = MetadataServerConfiguration::from(
            grpc::MetadataServerConfiguration::decode(entry.context)?,
        );

        // sanity checks
        let mut config_change_rejections = Vec::default();
        let nodes_config = self.kv_storage.last_seen_nodes_configuration();

        for conf_change in &cc_v2.changes {
            match conf_change.change_type {
                ConfChangeType::AddNode => {
                    let joining_node_id = to_plain_node_id(conf_change.node_id);

                    // check whether joining node still exists
                    let Ok(joining_node_config) = nodes_config.find_node_by_id(joining_node_id)
                    else {
                        config_change_rejections.push(format!("cannot add node '{joining_node_id}' because it is not part of the nodes configuration"));
                        continue;
                    };

                    // check whether the joining node actually runs the metadata server role
                    if !joining_node_config.has_role(Role::MetadataServer) {
                        config_change_rejections.push(format!(
                            "cannot add node '{joining_node_id}' because it does not run the metadata-server role"
                        ));
                    }
                }
                ConfChangeType::RemoveNode => {
                    // removing nodes should always be ok as long as the resulting configuration is
                    // non-empty which we checked before accepting the configuration change
                }
                ConfChangeType::AddLearnerNode => {
                    unimplemented!("Restate does not support learner nodes yet");
                }
            }
        }

        if config_change_rejections.is_empty() {
            self.raw_node.apply_conf_change(&cc_v2)?;

            // sanity checks
            assert_eq!(
                self.configuration.version.next(),
                new_configuration.version,
                "new configuration version must be '{}' but was '{}'",
                self.configuration.version.next(),
                new_configuration.version
            );

            info!(old_configuration = %self.configuration, %new_configuration, "Applied new configuration");

            self.update_configuration(new_configuration);

            self.create_snapshot(entry.index, entry.term).await?;

            self.update_leadership(metadata_nodes_config);
            self.update_node_addresses(metadata_nodes_config);
            self.update_status();

            // check whether we removed ourselves, and we should leave
            if self.should_leave(self.kv_storage.last_seen_nodes_configuration()) {
                self.is_leaving = true;
            }
        } else {
            info!(
                "Rejected configuration change because: {}",
                config_change_rejections.join(", ")
            );
        }

        self.answer_join_callbacks();
        self.answer_remove_callbacks();

        Ok(())
    }

    fn validate_metadata_server_configuration(&self) {
        assert_eq!(
            self.configuration.members.len(),
            self.raw_node.raft.prs().conf().voters().ids().len(),
            "number of members in configuration doesn't match number of voters in Raft"
        );
        for voter in self.raw_node.raft.prs().conf().voters().ids().iter() {
            assert!(
                self.configuration
                    .members
                    .contains_key(&to_plain_node_id(voter)),
                "voter '{voter}' in Raft configuration not found in MetadataServerConfiguration"
            );
        }
    }

    /// Checks whether it's time to snapshot the state machine and trim the Raft log.
    async fn try_trim_log(&mut self) -> Result<(), Error> {
        let applied_index = self.raw_node.raft.raft_log.applied();
        if applied_index.saturating_sub(self.raw_node.store().get_first_index())
            >= self.log_trim_threshold
        {
            debug!(
                "Trimming Raft log: [{}, {applied_index}]",
                self.raw_node.store().get_first_index()
            );
            self.create_snapshot(
                applied_index,
                self.raw_node.raft.raft_log.term(applied_index)?,
            )
            .await?;
        }

        Ok(())
    }

    /// Checks whether Raft requested a newer snapshot.
    async fn check_requested_snapshot(&mut self) -> Result<(), Error> {
        if let Some(index) = self.raw_node.mut_store().requested_snapshot() {
            let applied_index = self.raw_node.raft.raft_log.applied;
            if index <= applied_index {
                debug!("Creating requested snapshot for index '{index}'.");
                self.create_snapshot(
                    applied_index,
                    self.raw_node.raft.raft_log.term(applied_index)?,
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn create_snapshot(&mut self, index: u64, term: u64) -> Result<(), CreateSnapshotError> {
        let mut snapshot = MetadataServerSnapshot {
            configuration: Some(grpc::MetadataServerConfiguration::from(
                self.configuration.clone(),
            )),
            ..MetadataServerSnapshot::default()
        };
        self.kv_storage.snapshot(&mut snapshot);
        let mut data = BytesMut::new();
        snapshot.encode(&mut data)?;

        let mut snapshot = Snapshot::new();
        let mut metadata = SnapshotMetadata::new();
        metadata.set_index(index);
        metadata.set_term(term);
        metadata.set_conf_state(self.raw_node.raft.prs().conf().to_conf_state());
        snapshot.set_data(data.freeze());
        snapshot.set_metadata(metadata);

        debug!(%index, %term, "Created snapshot: '{}' bytes", snapshot.get_data().len());

        self.raw_node.mut_store().apply_snapshot(&snapshot).await?;
        self.snapshot_summary = Some(SnapshotSummary::from_snapshot(&snapshot));

        Ok(())
    }

    fn update_configuration(&mut self, new_configuration: MetadataServerConfiguration) {
        let previous_configuration = mem::replace(&mut self.configuration, new_configuration);
        self.validate_metadata_server_configuration();

        let mut new_nodes_configuration = self.kv_storage.last_seen_nodes_configuration().clone();
        let previous_version = new_nodes_configuration.version();

        for (node_id, node_config) in new_nodes_configuration.iter_mut() {
            if self.is_member_plain_node_id(node_id) {
                node_config.metadata_server_config.metadata_server_state =
                    MetadataServerState::Member;
            } else if previous_configuration.contains(node_id) {
                // node was part of the previous configuration, which means that it was removed from
                // the metadata cluster, and we should set its state to Standby
                node_config.metadata_server_config.metadata_server_state =
                    MetadataServerState::Standby;
            }
        }

        new_nodes_configuration.increment_version();

        debug!(
            "Update membership after reconfiguration in NodesConfiguration '{}'",
            new_nodes_configuration.version()
        );

        let versioned_value = serialize_value(&new_nodes_configuration)
            .expect("should be able to serialize NodesConfiguration");
        self.kv_storage
            .put(
                NODES_CONFIG_KEY.clone(),
                versioned_value,
                Precondition::MatchesVersion(previous_version),
            )
            .expect("should be able to update NodesConfiguration with new members");
    }

    fn register_join_callback(
        &mut self,
        member_id: MemberId,
        join_callback: JoinClusterResponseSender,
    ) {
        if let Some(previous_callback) = self.pending_join_requests.insert(member_id, join_callback)
        {
            let _ =
                previous_callback.send(Err(JoinClusterError::ConcurrentRequest(member_id.node_id)));
        }
    }

    fn fail_join_callbacks(&mut self, cause: impl Fn() -> JoinClusterError) {
        for (_, response_tx) in self.pending_join_requests.drain() {
            let _ = response_tx.send(Err(cause()));
        }
    }

    fn answer_join_callbacks(&mut self) {
        let pending_join_requests: Vec<_> = self.pending_join_requests.drain().collect();
        for (member_id, response_tx) in pending_join_requests {
            if self.is_member(member_id) {
                let _ = response_tx.send(Ok(self
                    .kv_storage
                    .last_seen_nodes_configuration()
                    .version()));
            } else {
                // latest reconfiguration didn't include this node, fail it so that caller can retry
                let _ = response_tx.send(Err(JoinClusterError::Internal(format!(
                    "failed to include node '{member_id}' in new configuration"
                ))));
            }
        }
    }

    fn register_remove_callback(
        &mut self,
        member_id: MemberId,
        remove_node_response_sender: RemoveNodeResponseSender,
    ) {
        if let Some(previous_sender) = self
            .pending_remove_requests
            .insert(member_id, remove_node_response_sender)
        {
            let _ = previous_sender.send(Err(MetadataCommandError::RemoveNode(
                RemoveNodeError::ConcurrentRequest(member_id.node_id),
            )));
        }
    }

    fn fail_remove_callbacks<T: Into<MetadataCommandError>>(&mut self, cause: impl Fn() -> T) {
        for (_, response_tx) in self.pending_remove_requests.drain() {
            let _ = response_tx.send(Err(cause().into()));
        }
    }

    fn answer_remove_callbacks(&mut self) {
        let pending_remove_requests: Vec<_> = self.pending_remove_requests.drain().collect();
        for (member_id, response_tx) in pending_remove_requests {
            if self.is_member(member_id) {
                let _ = response_tx.send(Err(MetadataCommandError::RemoveNode(
                    RemoveNodeError::Internal(format!(
                        "failed to remove node '{member_id}' from new configuration"
                    )),
                )));
            } else {
                let _ = response_tx.send(Ok(()));
            }
        }
    }

    fn update_node_addresses(&mut self, metadata_nodes_config: &NodesConfiguration) {
        let nodes_config =
            Self::latest_nodes_configuration(&self.kv_storage, metadata_nodes_config);

        trace!(
            "Update node addresses in networking based on NodesConfiguration '{}'",
            nodes_config.version()
        );

        for node_id in self.raw_node.raft.prs().conf().voters().ids().iter() {
            let plain_node_id = to_plain_node_id(node_id);
            if let Ok(node_config) = nodes_config.find_node_by_id(plain_node_id) {
                // todo remove addresses from nodes that are no longer needed
                self.networking
                    .register_address(plain_node_id, node_config.address.clone());
            }
        }
    }

    fn update_status(&self) {
        self.status_tx.send_modify(|current_status| {
            let current_leader = if self.raw_node.raft.leader_id == INVALID_ID {
                None
            } else {
                Some(to_plain_node_id(self.raw_node.raft.leader_id))
            };

            if let MetadataServerSummary::Member {
                leader,
                configuration,
                raft,
                snapshot,
            } = current_status
            {
                *leader = current_leader;
                if configuration.version != self.configuration.version {
                    *configuration = self.configuration.clone();
                }
                *raft = self.raft_summary();
                *snapshot = self.snapshot_summary.clone();
            } else {
                let raft = self.raft_summary();

                *current_status = MetadataServerSummary::Member {
                    leader: current_leader,
                    configuration: self.configuration.clone(),
                    raft,
                    snapshot: self.snapshot_summary.clone(),
                };
            }
        });

        self.record_summary_metrics(&self.status_tx.borrow());
    }

    fn handle_command(
        &mut self,
        command: MetadataCommand,
        metadata_nodes_config: &NodesConfiguration,
    ) {
        match command {
            MetadataCommand::AddNode(result_tx) => {
                let _ = result_tx.send(Err(MetadataCommandError::AddNode(
                    AddNodeError::StillMember,
                )));
            }
            MetadataCommand::RemoveNode {
                response_tx: result_tx,
                plain_node_id,
                created_at_millis,
            } => {
                self.remove_member(
                    result_tx,
                    plain_node_id,
                    created_at_millis,
                    metadata_nodes_config,
                );
            }
        }
    }

    fn remove_member(
        &mut self,
        response_tx: oneshot::Sender<Result<(), MetadataCommandError>>,
        plain_node_id: PlainNodeId,
        created_at_millis: Option<CreatedAtMillis>,
        metadata_nodes_config: &NodesConfiguration,
    ) {
        trace!("Handle removing node '{}'", plain_node_id);

        if !self.is_leader {
            let _ = response_tx.send(Err(MetadataCommandError::NotLeader(
                self.known_leader(metadata_nodes_config),
            )));
            return;
        }

        if self.raw_node.raft.has_pending_conf() {
            let _ = response_tx.send(Err(MetadataCommandError::RemoveNode(
                RemoveNodeError::PendingReconfiguration,
            )));
            return;
        }

        let leaving_member_id = if let Some(create_at_millis) = created_at_millis {
            let member_id = MemberId::new(plain_node_id, create_at_millis);

            if !self.is_member(member_id) {
                let _ = response_tx.send(Err(MetadataCommandError::RemoveNode(
                    RemoveNodeError::NotMember(member_id),
                )));
                return;
            }

            member_id
        } else {
            if !self.is_member_plain_node_id(plain_node_id) {
                let _ = response_tx.send(Err(MetadataCommandError::RemoveNode(
                    RemoveNodeError::NotMemberPlainNodeId(plain_node_id),
                )));
                return;
            }

            MemberId::new(
                plain_node_id,
                *self
                    .configuration
                    .members
                    .get(&plain_node_id)
                    .expect("to be present"),
            )
        };

        if self.configuration.members.len() == 1 {
            let _ = response_tx.send(Err(MetadataCommandError::RemoveNode(
                RemoveNodeError::OnlyMember(leaving_member_id),
            )));
            return;
        }

        let nodes_config =
            Self::latest_nodes_configuration(&self.kv_storage, metadata_nodes_config);

        if nodes_config
            .find_node_by_id(leaving_member_id.node_id)
            .is_err()
        {
            let _ = response_tx.send(Err(MetadataCommandError::RemoveNode(
                RemoveNodeError::UnknownNode(leaving_member_id.node_id),
            )));
            return;
        }

        let (conf_change, new_configuration) = self.remove_member_conf_change(leaving_member_id);

        let next_configuration_bytes =
            grpc::MetadataServerConfiguration::from(new_configuration).encode_to_vec();

        match self
            .raw_node
            .propose_conf_change(next_configuration_bytes, conf_change)
        {
            Ok(()) => {
                info!(
                    "Trying to remove node '{}' from metadata cluster",
                    leaving_member_id.node_id
                );
                self.register_remove_callback(leaving_member_id, response_tx);
            }
            Err(err) => {
                let _ = response_tx.send(Err(MetadataCommandError::Internal(format!(
                    "failed to propose conf change: {err}"
                ))));
            }
        }
    }

    fn record_summary_metrics(&self, summary: &MetadataServerSummary) {
        let MetadataServerSummary::Member {
            leader,
            raft,
            snapshot,
            ..
        } = summary
        else {
            return;
        };

        if let Some(id) = leader {
            gauge!(METADATA_SERVER_REPLICATED_LEADER_ID).set(u32::from(*id) as f64);
        } else {
            gauge!(METADATA_SERVER_REPLICATED_LEADER_ID).set(INVALID_ID as f64);
        }

        if let Some(snapshot) = snapshot {
            gauge!(METADATA_SERVER_REPLICATED_SNAPSHOT_SIZE_BYTES).set(snapshot.size as f64);
        } else {
            gauge!(METADATA_SERVER_REPLICATED_SNAPSHOT_SIZE_BYTES).set(0);
        }

        gauge!(METADATA_SERVER_REPLICATED_TERM).set(raft.term as f64);
        gauge!(METADATA_SERVER_REPLICATED_APPLIED_LSN).set(raft.applied as f64);
        gauge!(METADATA_SERVER_REPLICATED_COMMITTED_LSN).set(raft.committed as f64);
        gauge!(METADATA_SERVER_REPLICATED_FIRST_INDEX).set(raft.first_index as f64);
        gauge!(METADATA_SERVER_REPLICATED_LAST_INDEX).set(raft.last_index as f64);
    }

    fn raft_summary(&self) -> RaftSummary {
        RaftSummary {
            term: self.raw_node.raft.term,
            committed: self.raw_node.raft.raft_log.committed,
            applied: self.raw_node.raft.raft_log.applied,
            last_index: self.raw_node.store().get_last_index(),
            first_index: self.raw_node.store().get_first_index(),
        }
    }

    fn is_member(&self, member_id: MemberId) -> bool {
        self.configuration.members.get(&member_id.node_id) == Some(&member_id.created_at_millis)
    }

    fn is_member_plain_node_id(&self, node_id: PlainNodeId) -> bool {
        self.configuration.members.contains_key(&node_id)
    }

    fn latest_nodes_configuration<'a>(
        kv_storage: &'a KvMemoryStorage,
        metadata_nodes_config: &'a NodesConfiguration,
    ) -> &'a NodesConfiguration {
        let kv_storage_nodes_config = kv_storage.last_seen_nodes_configuration();

        if metadata_nodes_config.version() > kv_storage_nodes_config.version() {
            metadata_nodes_config
        } else {
            kv_storage_nodes_config
        }
    }

    /// Returns the known leader from the Raft instance or a random known leader from the
    /// current nodes configuration.
    fn known_leader(&self, metadata_nodes_config: &NodesConfiguration) -> Option<KnownLeader> {
        if self.raw_node.raft.leader_id == INVALID_ID {
            return None;
        }

        let leader = to_plain_node_id(self.raw_node.raft.leader_id);

        let nodes_config =
            Self::latest_nodes_configuration(&self.kv_storage, metadata_nodes_config);
        nodes_config
            .find_node_by_id(leader)
            .ok()
            .map(|node_config| KnownLeader {
                node_id: leader,
                address: node_config.address.clone(),
            })
    }
}
