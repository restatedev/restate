// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use futures::FutureExt;
use futures::future::{FusedFuture, OptionFuture};
use raft_proto::eraftpb::Message;
use rand::prelude::IteratorRandom;
use rand::rng;
use tracing::{Span, debug, instrument, trace};

use restate_core::network::net_util::{DNSResolution, create_tonic_channel};
use restate_core::{Metadata, MetadataWriter};
use restate_metadata_providers::replicated::KnownLeader;
use restate_time_util::DurationExt;
use restate_types::Version;
use restate_types::config::Configuration;
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::{MetadataServerState, NodesConfiguration, Role};
use restate_types::retries::RetryPolicy;

use crate::raft::network::ConnectionManager;
use crate::raft::network::grpc_svc::new_metadata_server_network_client;
use crate::raft::server::member::Member;
use crate::raft::server::uninitialized::Uninitialized;
use crate::raft::server::{Error, RaftServerComponents};
use crate::raft::storage::RocksDbStorage;
use crate::raft::{RaftServerState, network};
use crate::{
    AddNodeError, JoinClusterError, JoinClusterReceiver, JoinError, MemberId, MetadataCommand,
    MetadataCommandError, MetadataCommandReceiver, MetadataServerSummary, RequestError,
    RequestReceiver, StatusSender,
};

pub struct Standby {
    connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
    storage: RocksDbStorage,
    request_rx: RequestReceiver,
    join_cluster_rx: JoinClusterReceiver,
    metadata_writer: Option<MetadataWriter>,
    status_tx: StatusSender,
    command_rx: MetadataCommandReceiver,
}

impl Standby {
    pub fn new(
        storage: RocksDbStorage,
        connection_manager: Arc<ArcSwapOption<ConnectionManager<Message>>>,
        request_rx: RequestReceiver,
        join_cluster_rx: JoinClusterReceiver,
        metadata_writer: Option<MetadataWriter>,
        status_tx: StatusSender,
        command_rx: MetadataCommandReceiver,
    ) -> Self {
        connection_manager.store(None);

        Standby {
            connection_manager,
            storage,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
            command_rx,
        }
    }

    pub fn into_inner(self) -> RaftServerComponents {
        RaftServerComponents {
            storage: self.storage,
            connection_manager: self.connection_manager,
            request_rx: self.request_rx,
            status_tx: self.status_tx,
            command_rx: self.command_rx,
            join_cluster_rx: self.join_cluster_rx,
            metadata_writer: self.metadata_writer,
        }
    }

    #[instrument(level = "info", skip_all, fields(member_id = tracing::field::Empty))]
    pub async fn run(&mut self) -> Result<(MemberId, Version), Error> {
        debug!("Run as standby metadata server.");

        let _ = self.status_tx.send(MetadataServerSummary::Standby);

        let created_at_millis = self
            .storage
            .get_marker()?
            .expect("StorageMarker must be present")
            .created_at()
            .timestamp_millis();

        let mut join_cluster: std::pin::Pin<&mut OptionFuture<_>> = std::pin::pin!(None.into());
        let mut pending_response_txs = Vec::default();

        let mut nodes_config_watcher =
            Metadata::with_current(|m| m.watch(MetadataKind::NodesConfiguration));
        let mut nodes_config = Metadata::with_current(|m| m.updateable_nodes_config());
        let my_node_name = Configuration::pinned().common.node_name().to_owned();
        let mut my_member_id = None;

        loop {
            tokio::select! {
                Some(request) = self.request_rx.recv() => {
                    let (request, cluster_identity) = request.into_request();

                    let nodes_config = nodes_config.live_load();

                    // Validate cluster identity: fingerprint first, then cluster_name
                    if let Some(incoming_fingerprint) = cluster_identity.fingerprint {
                        // nodes_config might still be uninitialized if we haven't joined a cluster yet.
                        // Once nodes store the last seen nodes configuration, we can assume that the
                        // nodes configuration is valid when starting the metadata server in standby.
                        if let Some(my_cluster_fingerprint) = nodes_config.try_cluster_fingerprint() && my_cluster_fingerprint != incoming_fingerprint {
                            request.fail(RequestError::ClusterIdentityMismatch(format!("cluster fingerprint mismatch: expected {}, got {}", my_cluster_fingerprint, incoming_fingerprint)));
                            continue;
                        }
                    }

                    if let Some(incoming_cluster_name) = cluster_identity.cluster_name {
                        let my_cluster_name = nodes_config.cluster_name();

                        if my_cluster_name != incoming_cluster_name {
                            request.fail(RequestError::ClusterIdentityMismatch(format!("cluster name mismatch: expected {}, got {}", my_cluster_name, incoming_cluster_name)));
                            continue;
                        }
                    }
                    // If neither fingerprint nor cluster_name is provided, allow (backward compatibility)
                    // todo in v1.7 no longer accept if no cluster identity was provided

                    request.fail(RequestError::Unavailable(
                        "Not being part of the metadata store cluster.".into(),
                        Standby::random_member(),
                    ))
                },
                Some(request) = self.join_cluster_rx.recv() => {
                    let _ = request.response_tx.send(Err(JoinClusterError::NotMember(Standby::random_member())));
                },
                Some(request) = self.command_rx.recv() => {
                    match request {
                        MetadataCommand::AddNode(result_tx) => {
                            if let Some(my_member_id) = my_member_id {
                                pending_response_txs.push(result_tx);

                                if join_cluster.is_terminated() {
                                    debug!("Node is asked to join the metadata cluster. Trying to join.");
                                    join_cluster.set(Some(Self::join_cluster(my_member_id).fuse()).into());
                                }
                            } else {
                                let _ = result_tx.send(Err(MetadataCommandError::AddNode(AddNodeError::NotReadyToJoin)));
                            }
                        }
                        MetadataCommand::RemoveNode{ .. } => {
                            request.fail(MetadataCommandError::NotLeader(Standby::random_member()))
                        }
                    }
                },
                Some((my_member_id, min_expected_nodes_config_version)) = &mut join_cluster => {
                    let mut txn = self.storage.txn();
                    txn.store_raft_server_state(&RaftServerState::Member{ my_member_id, min_expected_nodes_config_version: Some(min_expected_nodes_config_version) })?;
                    // Persist the latest NodesConfiguration so that we know about the peers as of now.
                    txn.store_nodes_configuration(nodes_config.live_load())?;
                    txn.commit().await?;

                    for response_tx in pending_response_txs {
                        let _ = response_tx.send(Ok(()));
                    }

                    return Ok((my_member_id, min_expected_nodes_config_version));
                }
                _ = nodes_config_watcher.changed() => {
                    let nodes_config = nodes_config.live_load();

                    if let Some(node_config) = nodes_config.find_node_by_name(&my_node_name) {
                        // we first need to wait until we have joined the Restate cluster to obtain our node id and thereby our member id
                        if my_member_id.is_none() {
                            let member_id = MemberId::new(node_config.current_generation.as_plain(), created_at_millis);
                            Span::current().record("member_id", member_id.to_string());
                            my_member_id = Some(member_id);
                        }

                        if join_cluster.is_terminated() && matches!(node_config.metadata_server_config.metadata_server_state, MetadataServerState::Member | MetadataServerState::Provisioning) {
                            debug!("Node's metadata server state as of nodes configuration {}: {}. Trying to join the raft cluster.", nodes_config.version(), node_config.metadata_server_config.metadata_server_state);

                            // Persist the latest NodesConfiguration so that we know about the MetadataServerState at least
                            // as of now when restarting.
                            self.storage
                                .store_nodes_configuration(nodes_config)
                                .await?;
                            join_cluster.set(Some(Self::join_cluster(my_member_id.expect("MemberId to be known")).fuse()).into());
                        }
                    } else {
                        trace!("Node '{}' has not joined the cluster yet as of NodesConfiguration {}", my_node_name, nodes_config.version());
                    }
                }
            }
        }
    }

    async fn join_cluster(member_id: MemberId) -> (MemberId, Version) {
        // todo make configurable
        let mut join_retry_policy = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            None,
            Some(Duration::from_secs(1)),
        )
        .into_iter();

        let mut known_leader = None;

        let mut nodes_config = Metadata::with_current(|m| m.updateable_nodes_config());

        loop {
            let err = match Self::attempt_to_join(
                known_leader.clone(),
                member_id,
                nodes_config.live_load(),
            )
            .await
            {
                Ok(version) => return (member_id, version),
                Err(err) => err,
            };

            match err {
                JoinError::Rpc(err, Some(new_known_leader)) => {
                    trace!(%err, "Failed joining metadata cluster. Retrying at known leader");
                    // try immediately again if there is a known leader
                    known_leader = Some(new_known_leader);
                }
                err => {
                    let delay = join_retry_policy.next().expect("infinite retry policy");
                    trace!(%err, "Failed joining metadata cluster. Retrying in {}",
                        delay.friendly());
                    tokio::time::sleep(delay).await
                }
            }
        }
    }

    async fn attempt_to_join(
        known_leader: Option<KnownLeader>,
        member_id: MemberId,
        nodes_config: &NodesConfiguration,
    ) -> Result<Version, JoinError> {
        let address = if let Some(known_leader) = known_leader {
            debug!(
                "Trying to join metadata store at node '{}'",
                known_leader.node_id
            );
            known_leader.address
        } else {
            // pick random metadata store member node
            let member_node = nodes_config.iter().filter_map(|(node, config)| {
                if config.has_role(Role::MetadataServer) && node != member_id.node_id && matches!(config.metadata_server_config.metadata_server_state, MetadataServerState::Member) {
                    Some(node)
                } else {
                    None
                }
            }).choose(&mut rng()).ok_or(JoinError::Other("No other metadata store member present in the cluster. This indicates a misconfiguration.".into()))?;

            debug!(
                "Trying to join metadata store cluster at randomly chosen node '{}'",
                member_node
            );

            nodes_config
                .find_node_by_id(member_node)
                .expect("must be present")
                .address
                .clone()
        };

        let channel = create_tonic_channel(
            address,
            &Configuration::pinned().networking,
            DNSResolution::Gai,
        );

        match new_metadata_server_network_client(channel)
            .join_cluster(network::grpc_svc::JoinClusterRequest {
                node_id: u32::from(member_id.node_id),
                created_at_millis: member_id.created_at_millis,
                cluster_fingerprint: nodes_config.cluster_fingerprint().to_u64(),
                cluster_name: Some(nodes_config.cluster_name().to_owned()),
            })
            .await
        {
            Ok(response) => Ok(response
                .into_inner()
                .nodes_config_version
                .map(Version::from)
                .unwrap_or(Metadata::with_current(|m| m.nodes_config_version()))),
            Err(status) => {
                let known_leader = KnownLeader::from_status(&status);
                Err(JoinError::Rpc(Box::new(status), known_leader))
            }
        }
    }

    /// Returns a random metadata store member from the current nodes configuration.
    fn random_member() -> Option<KnownLeader> {
        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());

        nodes_config
            .iter_role(Role::MetadataServer)
            .filter_map(|(node_id, node_config)| {
                if node_config.metadata_server_config.metadata_server_state
                    == MetadataServerState::Member
                {
                    Some((node_id, node_config))
                } else {
                    None
                }
            })
            .choose(&mut rng())
            .map(|(node_id, node_config)| KnownLeader {
                node_id,
                address: node_config.address.clone(),
            })
    }
}

impl From<Uninitialized> for Standby {
    fn from(value: Uninitialized) -> Self {
        let RaftServerComponents {
            storage,
            connection_manager,
            request_rx,
            status_tx,
            command_rx,
            join_cluster_rx,
            metadata_writer,
        } = value.into_inner();
        Standby::new(
            storage,
            connection_manager,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
            command_rx,
        )
    }
}

impl From<Member> for Standby {
    fn from(value: Member) -> Self {
        let RaftServerComponents {
            storage,
            connection_manager,
            request_rx,
            status_tx,
            command_rx,
            join_cluster_rx,
            metadata_writer,
        } = value.into_inner();
        Standby::new(
            storage,
            connection_manager,
            request_rx,
            join_cluster_rx,
            metadata_writer,
            status_tx,
            command_rx,
        )
    }
}
