// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod cluster_controller_state;
mod scheduler;
mod scheduler_task;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use codederror::CodedError;
use futures::never::Never;
use rand::rng;
use rand::seq::IteratorRandom;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio::time::{Instant, Interval, MissedTickBehavior};
use tracing::{debug, info, warn};

use restate_bifrost::{Bifrost, MaybeSealedSegment};
use restate_core::network::tonic_service_filter::{TonicServiceFilter, WaitForReady};
use restate_core::network::{
    NetworkSender, NetworkServerBuilder, Networking, Swimlane, TransportConnect,
};
use restate_core::{Metadata, MetadataWriter, ShutdownError, TaskCenter, TaskKind};
use restate_core::{cancellation_token, my_node_id};
use restate_metadata_store::ReadModifyWriteError;
use restate_storage_query_datafusion::BuildError;
use restate_storage_query_datafusion::context::{ClusterTables, QueryContext};
use restate_types::cluster::cluster_state::LegacyClusterState;
use restate_types::config::{AdminOptions, Configuration};
use restate_types::health::HealthStatus;
use restate_types::identifiers::PartitionId;
use restate_types::live::Live;
use restate_types::logs::metadata::{
    LogletParams, Logs, LogsConfiguration, ProviderConfiguration, ProviderKind,
    ReplicatedLogletConfig, SealMetadata, SegmentIndex,
};
use restate_types::logs::{LogId, LogletId, Lsn};
use restate_types::net::node::NodeState;
use restate_types::net::partition_processor_manager::{CreateSnapshotRequest, Snapshot};
use restate_types::nodes_config::{NodesConfiguration, StorageState};
use restate_types::partition_table::{
    self, PartitionReplication, PartitionTable, PartitionTableBuilder,
};
use restate_types::partitions::Partition;
use restate_types::partitions::state::{MembershipState, PartitionReplicaSetStates};
use restate_types::protobuf::common::AdminStatus;
use restate_types::replicated_loglet::ReplicatedLogletParams;
use restate_types::replication::{NodeSet, NodeSetChecker, ReplicationProperty};
use restate_types::{GenerationalNodeId, NodeId, Version};

use crate::cluster_controller::cluster_state_refresher::ClusterStateRefresher;
use crate::cluster_controller::grpc_svc_handler::ClusterCtrlSvcHandler;
use crate::cluster_controller::service::cluster_controller_state::ClusterControllerState;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("error")]
    #[code(unknown)]
    Error,
}

pub struct Service<T> {
    networking: Networking<T>,
    bifrost: Bifrost,
    cluster_state_refresher: ClusterStateRefresher<T>,
    replica_set_states: PartitionReplicaSetStates,
    configuration: Live<Configuration>,
    metadata_writer: MetadataWriter,

    processor_manager_client: PartitionProcessorManagerClient<Networking<T>>,
    command_tx: mpsc::Sender<ClusterControllerCommand>,
    command_rx: mpsc::Receiver<ClusterControllerCommand>,
    health_status: HealthStatus<AdminStatus>,
    heartbeat_interval: Interval,
}

impl<T> Service<T>
where
    T: TransportConnect,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        mut configuration: Live<Configuration>,
        health_status: HealthStatus<AdminStatus>,
        replica_set_states: PartitionReplicaSetStates,
        bifrost: Bifrost,
        networking: Networking<T>,
        server_builder: &mut NetworkServerBuilder,
        metadata_writer: MetadataWriter,
    ) -> Result<Self, BuildError> {
        let (command_tx, command_rx) = mpsc::channel(2);

        let cluster_state_refresher = ClusterStateRefresher::new(networking.clone());

        let processor_manager_client = PartitionProcessorManagerClient::new(networking.clone());

        let options = configuration.live_load();
        let heartbeat_interval = Self::create_heartbeat_interval(&options.admin);

        let cluster_query_context = QueryContext::create(
            &options.admin.query_engine,
            ClusterTables::new(
                replica_set_states.clone(),
                cluster_state_refresher.cluster_state_watcher().watch(),
            ),
        )
        .await?;

        // Registering ClusterCtrlSvc grpc service to network server
        server_builder.register_grpc_service(
            TonicServiceFilter::new(
                ClusterCtrlSvcHandler::new(
                    ClusterControllerHandle {
                        tx: command_tx.clone(),
                    },
                    bifrost.clone(),
                    metadata_writer.clone(),
                    cluster_query_context,
                    replica_set_states.clone(),
                )
                .into_server(&configuration.live_load().networking),
                WaitForReady::new(health_status.clone(), AdminStatus::Ready),
            ),
            restate_core::protobuf::cluster_ctrl_svc::FILE_DESCRIPTOR_SET,
        );

        Ok(Service {
            configuration,
            health_status,
            networking,
            bifrost,
            cluster_state_refresher,
            replica_set_states,
            metadata_writer,
            processor_manager_client,
            command_tx,
            command_rx,
            heartbeat_interval,
        })
    }

    fn create_heartbeat_interval(options: &AdminOptions) -> Interval {
        let mut heartbeat_interval = time::interval_at(
            Instant::now() + options.heartbeat_interval.into(),
            options.heartbeat_interval.into(),
        );
        heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        heartbeat_interval
    }
}

#[derive(Debug)]
pub struct ChainExtension {
    /// Segment index to seal. Last if None
    pub segment_index_to_seal: Option<SegmentIndex>,
    pub provider_kind: ProviderKind,
    pub nodeset: Option<NodeSet>,
    pub sequencer: Option<NodeId>,
    pub replication: Option<ReplicationProperty>,
}

#[derive(Debug)]
enum ClusterControllerCommand {
    GetClusterState(oneshot::Sender<Arc<LegacyClusterState>>),
    TrimLog {
        log_id: LogId,
        trim_point: Lsn,
        response_tx: oneshot::Sender<anyhow::Result<()>>,
    },
    CreateSnapshot {
        partition_id: PartitionId,
        min_target_lsn: Option<Lsn>,
        response_tx: oneshot::Sender<anyhow::Result<Snapshot>>,
    },
    UpdateClusterConfiguration {
        partition_replication: Option<ReplicationProperty>,
        default_provider: ProviderConfiguration,
        num_partitions: u16,
        response_tx: oneshot::Sender<anyhow::Result<()>>,
    },
    SealAndExtendChain {
        log_id: LogId,
        min_version: Version,
        extension: Option<ChainExtension>,
        response_tx: oneshot::Sender<anyhow::Result<MaybeSealedSegment>>,
    },
    SealChain {
        log_id: LogId,
        segment_index: Option<SegmentIndex>,
        permanent_seal: bool,
        context: std::collections::HashMap<String, String>,
        response_tx: oneshot::Sender<anyhow::Result<Lsn>>,
    },
}

pub struct ClusterControllerHandle {
    tx: mpsc::Sender<ClusterControllerCommand>,
}

impl ClusterControllerHandle {
    pub async fn get_cluster_state(&self) -> Result<Arc<LegacyClusterState>, ShutdownError> {
        let (response_tx, response_rx) = oneshot::channel();
        // ignore the error, we own both tx and rx at this point.
        let _ = self
            .tx
            .send(ClusterControllerCommand::GetClusterState(response_tx))
            .await;
        response_rx.await.map_err(|_| ShutdownError)
    }

    pub async fn trim_log(
        &self,
        log_id: LogId,
        trim_point: Lsn,
    ) -> Result<Result<(), anyhow::Error>, ShutdownError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.tx
            .send(ClusterControllerCommand::TrimLog {
                log_id,
                trim_point,
                response_tx,
            })
            .await
            .map_err(|_| ShutdownError)?;

        response_rx.await.map_err(|_| ShutdownError)
    }

    pub async fn create_partition_snapshot(
        &self,
        partition_id: PartitionId,
        min_target_lsn: Option<Lsn>,
        trim_log: bool,
    ) -> Result<anyhow::Result<Snapshot>, ShutdownError> {
        let (response_tx, response_rx) = oneshot::channel();

        let log_id = Metadata::with_current(|m| {
            m.partition_table_ref()
                .get(&partition_id)
                .map(Partition::log_id)
        })
        .expect("partition is in partition table");

        let _ = self
            .tx
            .send(ClusterControllerCommand::CreateSnapshot {
                partition_id,
                min_target_lsn,
                response_tx,
            })
            .await;

        let create_snapshot_response = response_rx.await.map_err(|_| ShutdownError)?;

        if let (Ok(snapshot), true) = (&create_snapshot_response, trim_log) {
            // We have successfully archived the target LSN to the snapshot repository. For added
            // safety, we could optionally download and test the snapshot in the future.
            if let Err(trim_error) = self.trim_log(log_id, snapshot.min_applied_lsn).await? {
                return Ok(Err(trim_error));
            }
        }

        Ok(create_snapshot_response)
    }

    pub async fn update_cluster_configuration(
        &self,
        partition_replication: Option<ReplicationProperty>,
        default_provider: ProviderConfiguration,
        num_partitions: u16,
    ) -> Result<anyhow::Result<()>, ShutdownError> {
        let (response_tx, response_rx) = oneshot::channel();

        let _ = self
            .tx
            .send(ClusterControllerCommand::UpdateClusterConfiguration {
                partition_replication,
                default_provider,
                num_partitions,
                response_tx,
            })
            .await;

        response_rx.await.map_err(|_| ShutdownError)
    }

    pub async fn seal_chain(
        &self,
        log_id: LogId,
        segment_index: Option<SegmentIndex>,
        permanent_seal: bool,
        context: std::collections::HashMap<String, String>,
    ) -> Result<anyhow::Result<Lsn>, ShutdownError> {
        let (response_tx, response_rx) = oneshot::channel();

        let _ = self
            .tx
            .send(ClusterControllerCommand::SealChain {
                log_id,
                segment_index,
                permanent_seal,
                context,
                response_tx,
            })
            .await;

        response_rx.await.map_err(|_| ShutdownError)
    }

    pub async fn seal_and_extend_chain(
        &self,
        log_id: LogId,
        min_version: Version,
        extension: Option<ChainExtension>,
    ) -> Result<anyhow::Result<MaybeSealedSegment>, ShutdownError> {
        let (response_tx, response_rx) = oneshot::channel();

        let _ = self
            .tx
            .send(ClusterControllerCommand::SealAndExtendChain {
                log_id,
                min_version,
                extension,
                response_tx,
            })
            .await;

        response_rx.await.map_err(|_| ShutdownError)
    }
}

impl<T: TransportConnect> Service<T> {
    pub fn handle(&self) -> ClusterControllerHandle {
        ClusterControllerHandle {
            tx: self.command_tx.clone(),
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let health_status = self.health_status.clone();
        health_status.update(AdminStatus::Ready);

        let _ = cancellation_token()
            .run_until_cancelled(self.run_inner())
            .await;

        health_status.update(AdminStatus::Unknown);
        Ok(())
    }

    async fn run_inner(mut self) -> Never {
        let mut config_watcher = Configuration::watcher();

        let mut state = ClusterControllerState::Follower;

        let cs = TaskCenter::with_current(|tc| tc.cluster_state().clone());
        let mut cs_changed = std::pin::pin!(cs.changed());
        let mut nodes_config = Metadata::with_current(|m| m.updateable_nodes_config());

        // initialize the state based on the initial cluster state
        state.update(&self, nodes_config.live_load(), &cs).await;

        loop {
            tokio::select! {
                _ = self.heartbeat_interval.tick() => {
                    // Ignore error if system is shutting down
                    let _ = self.cluster_state_refresher.schedule_refresh();
                },
                () = &mut cs_changed => {
                    // register waiting for the next update
                    cs_changed.set(cs.changed());

                    let nodes_config = nodes_config.live_load();
                    state.update(&self, nodes_config, &cs).await;
                },
                Some(cmd) = self.command_rx.recv() => {
                    // it is still safe to handle cluster commands as a follower
                    self.on_cluster_cmd(cmd).await;
                }
                _ = config_watcher.changed() => {
                    debug!("Updating the cluster controller settings.");
                    let configuration = self.configuration.live_load();
                    self.heartbeat_interval = Self::create_heartbeat_interval(&configuration.admin);
                }
            }
        }
    }

    /// Triggers a snapshot of the given partition by sending an RPC to an appropriate node.
    fn spawn_create_partition_snapshot_task(
        &self,
        partition_id: PartitionId,
        min_target_lsn: Option<Lsn>,
        response_tx: oneshot::Sender<anyhow::Result<Snapshot>>,
    ) {
        let cluster_state = self.cluster_state_refresher.get_cluster_state();

        // For now, we just pick the leader node since we know that every partition is likely to
        // have one. We'll want to update the algorithm to be smart about scheduling snapshot tasks
        // in the future to avoid disrupting the leader when there are up-to-date followers.
        let leader_node = cluster_state
            .alive_nodes()
            .filter_map(|node| {
                node.partitions
                    .get(&partition_id)
                    .filter(|status| status.is_effective_leader())
                    .map(|_| node)
                    .cloned()
            })
            .next();

        match leader_node {
            Some(node) => {
                debug!(
                    node_id = %node.generational_node_id,
                    ?partition_id,
                    "Asking node to snapshot partition"
                );

                let node_rpc_client = self.processor_manager_client.clone();
                let _ = TaskCenter::spawn_child(
                    TaskKind::Disposable,
                    "create-snapshot-response",
                    async move {
                        let _ = response_tx.send(
                            node_rpc_client
                                .create_snapshot(
                                    node.generational_node_id,
                                    partition_id,
                                    min_target_lsn,
                                )
                                .await,
                        );
                        Ok(())
                    },
                );
            }

            None => {
                let _ = response_tx.send(Err(anyhow::anyhow!(
                    "Can not find a suitable node to take snapshot of partition {partition_id}"
                )));
            }
        };
    }

    async fn on_cluster_cmd(&self, command: ClusterControllerCommand) {
        match command {
            ClusterControllerCommand::GetClusterState(tx) => {
                let _ = tx.send(self.cluster_state_refresher.get_cluster_state());
            }
            ClusterControllerCommand::TrimLog {
                log_id,
                trim_point,
                response_tx,
            } => {
                info!(
                    ?log_id,
                    trim_point_inclusive = ?trim_point,
                    "Trim log command received");
                {
                    let bifrost = self.bifrost.clone();

                    // receiver will get error if response_tx is dropped
                    let _ = TaskCenter::spawn(TaskKind::Disposable, "trim-log", async move {
                        let result = bifrost.admin().trim(log_id, trim_point).await;
                        let _ = response_tx.send(result.map_err(Into::into));
                        Ok(())
                    });
                };
            }
            ClusterControllerCommand::CreateSnapshot {
                partition_id,
                min_target_lsn,
                response_tx,
            } => {
                info!(?partition_id, "Create snapshot command received");
                self.spawn_create_partition_snapshot_task(
                    partition_id,
                    min_target_lsn,
                    response_tx,
                );
            }
            ClusterControllerCommand::UpdateClusterConfiguration {
                partition_replication,
                default_provider,
                num_partitions,
                response_tx,
            } => {
                let metadata_writer = self.metadata_writer.clone();

                // receiver will get error if response_tx is dropped
                let _ =
                    TaskCenter::spawn(TaskKind::Disposable, "update-cluster-config", async move {
                        match tokio::time::timeout(
                            Duration::from_secs(2),
                            update_cluster_configuration(
                                metadata_writer,
                                partition_replication,
                                default_provider,
                                num_partitions,
                            ),
                        )
                        .await
                        {
                            Ok(result) => {
                                let _ = response_tx.send(result);
                            }
                            Err(_timeout) => {
                                let _ = response_tx.send(Err(anyhow!(
                                    "Timeout writing updated configuration to metadata store"
                                )));
                            }
                        }

                        Ok(())
                    });
            }
            ClusterControllerCommand::SealChain {
                log_id,
                segment_index,
                permanent_seal,
                mut context,
                response_tx,
            } => {
                let bifrost = self.bifrost.clone();

                // receiver will get error if response_tx is dropped
                _ = TaskCenter::spawn(TaskKind::Disposable, "seal-chain", async move {
                    context.insert("node".to_owned(), my_node_id().to_string());

                    let result = SealChainTask {
                        log_id,
                        segment_index,
                        permanent_seal,
                        context,
                        bifrost,
                    }
                    .run()
                    .await;

                    _ = response_tx.send(result);
                    Ok(())
                });
            }
            ClusterControllerCommand::SealAndExtendChain {
                log_id,
                min_version,
                extension,
                response_tx,
            } => {
                let membership_state = Metadata::with_current(|metadata| {
                    metadata
                        .partition_table_ref()
                        .iter()
                        .find(|(_, partition)| partition.log_id() == log_id)
                        .map(|(partition_id, _)| *partition_id)
                })
                .map(|partition_id| self.replica_set_states.membership_state(partition_id))
                .unwrap_or_default();

                let bifrost = self.bifrost.clone();

                // receiver will get error if response_tx is dropped
                _ = TaskCenter::spawn(TaskKind::Disposable, "seal-and-extend", async move {
                    let result = SealAndExtendTask {
                        log_id,
                        extension,
                        min_version,
                        bifrost,
                        membership_state,
                    }
                    .run()
                    .await;

                    _ = response_tx.send(result);
                    Ok(())
                });
            }
        }
    }
}

async fn update_cluster_configuration(
    metadata_writer: MetadataWriter,
    partition_replication: Option<ReplicationProperty>,
    default_provider: ProviderConfiguration,
    num_partitions: u16,
) -> anyhow::Result<()> {
    let logs = metadata_writer
        .global_metadata()
        .read_modify_write(|current: Option<Arc<Logs>>| {
            let logs = current.expect("logs should be initialized by BifrostService");

            // allow to switch the default provider from a non-replicated loglet to the
            // replicated loglet
            if logs.configuration().default_provider.kind() != default_provider.kind()
                && default_provider.kind() != ProviderKind::Replicated
            {
                return Err(ClusterConfigurationUpdateError::ChooseReplicatedLoglet(
                    default_provider.kind(),
                ));
            }

            let mut builder = logs.as_ref().clone().into_builder();

            builder.set_configuration(LogsConfiguration {
                default_provider: default_provider.clone(),
            });

            let Some(logs) = builder.build_if_modified() else {
                return Err(ClusterConfigurationUpdateError::Unchanged);
            };

            Ok(logs)
        })
        .await;

    match logs {
        Ok(_) => {}
        Err(ReadModifyWriteError::FailedOperation(ClusterConfigurationUpdateError::Unchanged)) => {
            // nothing to do
        }
        Err(err) => return Err(err.into()),
    };

    let partition_table = metadata_writer
                .global_metadata()
                .read_modify_write(
                    |current: Option<Arc<PartitionTable>>| {
                        let partition_table =
                            current.ok_or(ClusterConfigurationUpdateError::MissingPartitionTable)?;

                        let mut builder = PartitionTableBuilder::from(partition_table.as_ref().clone());

                        if let Some(partition_replication) = &partition_replication
                            && !matches!(builder.partition_replication(), PartitionReplication::Limit(current) if current == partition_replication) {
                                builder.set_partition_replication(partition_replication.clone().into());

                        }


                        if builder.num_partitions() != num_partitions {
                            if builder.num_partitions() != 0 {
                                return Err(ClusterConfigurationUpdateError::Repartitioning);
                            }

                            builder.with_equally_sized_partitions(num_partitions)?;
                        }

                        builder
                            .build_if_modified()
                            .ok_or(ClusterConfigurationUpdateError::Unchanged)
                    },
                )
                .await;

    match partition_table {
        Ok(_) => {}
        Err(ReadModifyWriteError::FailedOperation(ClusterConfigurationUpdateError::Unchanged)) => {
            // nothing to do
        }
        Err(err) => return Err(err.into()),
    };

    Ok(())
}

#[derive(thiserror::Error, Debug)]
enum ClusterConfigurationUpdateError {
    #[error("unchanged")]
    Unchanged,
    #[error("changing default provider kind to {0} is not supported. Choose 'replicated' instead")]
    ChooseReplicatedLoglet(ProviderKind),
    #[error(transparent)]
    BuildError(#[from] partition_table::BuilderError),
    #[error("missing partition table; cluster seems to be not provisioned")]
    MissingPartitionTable,
    #[error("changing the number of partitions is not yet supported by Restate")]
    Repartitioning,
}

#[derive(Clone)]
struct PartitionProcessorManagerClient<N> {
    network_sender: N,
}

impl<N> PartitionProcessorManagerClient<N>
where
    N: NetworkSender + 'static,
{
    pub fn new(network_sender: N) -> Self {
        PartitionProcessorManagerClient { network_sender }
    }

    pub async fn create_snapshot(
        &self,
        node_id: GenerationalNodeId,
        partition_id: PartitionId,
        min_target_lsn: Option<Lsn>,
    ) -> anyhow::Result<Snapshot> {
        self.network_sender
            .call_rpc(
                node_id,
                Swimlane::default(),
                CreateSnapshotRequest {
                    partition_id,
                    min_target_lsn,
                },
                Some(partition_id.into()),
                None,
            )
            .await?
            .result
            .map_err(|e| anyhow!("Failed to create snapshot: {:?}", e))
    }
}

struct SealChainTask {
    log_id: LogId,
    segment_index: Option<SegmentIndex>,
    permanent_seal: bool,
    context: std::collections::HashMap<String, String>,
    bifrost: Bifrost,
}

impl SealChainTask {
    async fn run(self) -> anyhow::Result<Lsn> {
        let logs = Metadata::with_current(|m| m.logs_ref());
        let actual_tail_segment = logs
            .chain(&self.log_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown log id"))?
            .tail()
            .index();

        let segment_index = self.segment_index.unwrap_or(actual_tail_segment);
        let seal_metadata = SealMetadata::with_context(self.permanent_seal, self.context);

        let tail_lsn = self
            .bifrost
            .admin()
            .seal(self.log_id, segment_index, seal_metadata)
            .await?;

        Ok(tail_lsn)
    }
}

struct SealAndExtendTask {
    log_id: LogId,
    min_version: Version,
    extension: Option<ChainExtension>,
    bifrost: Bifrost,
    membership_state: MembershipState,
}

impl SealAndExtendTask {
    async fn run(self) -> anyhow::Result<MaybeSealedSegment> {
        let last_segment_index = self
            .extension
            .as_ref()
            .and_then(|ext| ext.segment_index_to_seal);

        let (provider, params) = self.next_segment()?;

        let sealed_segment = self
            .bifrost
            .admin()
            .seal_and_extend_chain(
                self.log_id,
                last_segment_index,
                self.min_version,
                provider,
                params,
            )
            .await?;

        Ok(sealed_segment)
    }

    fn next_segment(&self) -> anyhow::Result<(ProviderKind, LogletParams)> {
        let logs = Metadata::with_current(|m| m.logs_ref());

        let segment = logs
            .chain(&self.log_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown log id"))?
            .tail();

        let next_loglet_id = LogletId::new(self.log_id, segment.index().next());
        let previous_params = if segment.config.kind == ProviderKind::Replicated {
            let replicated_loglet_params =
                ReplicatedLogletParams::deserialize_from(segment.config.params.as_bytes())
                    .context("Invalid replicated loglet params")?;

            Some(replicated_loglet_params)
        } else {
            None
        };

        // override the provider configuration, if extension is set.
        let provider_config = match &self.extension {
            None => logs.configuration().default_provider.clone(),
            Some(ext) => match ext.provider_kind {
                ProviderKind::InMemory => ProviderConfiguration::InMemory,
                ProviderKind::Local => ProviderConfiguration::Local,
                ProviderKind::Replicated => {
                    ProviderConfiguration::Replicated(ReplicatedLogletConfig {
                        replication_property: ext
                            .replication
                            .clone()
                            .ok_or_else(|| anyhow::anyhow!("replication property is required"))?,
                        // use the provided nodeset size or 0
                        target_nodeset_size: ext
                            .nodeset
                            .as_ref()
                            .map(|n| n.len() as u16)
                            .unwrap_or_default()
                            .try_into()?,
                    })
                }
            },
        };

        let preferred_nodes = self
            .extension
            .as_ref()
            .and_then(|ext| ext.nodeset.as_ref())
            .or_else(|| previous_params.as_ref().map(|params| &params.nodeset));

        let preferred_sequencer = self
            .extension
            .as_ref()
            .and_then(|ext| ext.sequencer)
            .or_else(|| {
                if self.membership_state.current_leader().current_leader
                    != GenerationalNodeId::INVALID
                {
                    Some(NodeId::from(
                        self.membership_state.current_leader().current_leader,
                    ))
                } else {
                    TaskCenter::with_current(|handle| {
                        self.membership_state
                            .first_alive_node(handle.cluster_state())
                            .map(NodeId::from)
                    })
                }
            });

        let (provider, params) = match &provider_config {
            ProviderConfiguration::InMemory => (
                ProviderKind::InMemory,
                u64::from(next_loglet_id).to_string().into(),
            ),
            ProviderConfiguration::Local => (
                ProviderKind::Local,
                u64::from(next_loglet_id).to_string().into(),
            ),
            ProviderConfiguration::Replicated(config) => {
                let loglet_params = build_new_replicated_loglet_configuration(
                    self.log_id,
                    config,
                    next_loglet_id,
                    &Metadata::with_current(|m| m.nodes_config_ref()),
                    preferred_nodes,
                    preferred_sequencer,
                )
                .ok_or_else(|| anyhow::anyhow!("Insufficient writeable nodes in the nodeset"))?;

                (
                    ProviderKind::Replicated,
                    LogletParams::from(loglet_params.serialize()?),
                )
            }
        };

        Ok((provider, params))
    }
}

/// Build a new segment configuration for a replicated loglet based on the observed cluster state
/// and the previous configuration.
pub fn build_new_replicated_loglet_configuration(
    log_id: LogId,
    replicated_loglet_config: &ReplicatedLogletConfig,
    loglet_id: LogletId,
    nodes_config: &NodesConfiguration,
    preferred_nodes: Option<&NodeSet>,
    preferred_sequencer: Option<NodeId>,
) -> Option<ReplicatedLogletParams> {
    use restate_types::replication::{NodeSetSelector, NodeSetSelectorOptions};
    use tracing::warn;

    let mut rng = rng();

    let replication = replicated_loglet_config.replication_property.clone();

    let sequencer = preferred_sequencer
        .and_then(|node_id| {
            TaskCenter::with_current(|h| {
                h.cluster_state()
                    .get_node_state_and_generation(node_id.id())
                    .and_then(|(node_id, node_state)| {
                        (node_state == NodeState::Alive).then_some(node_id)
                    })
            })
        })
        .or_else(||
        // choose any alive node if no preferred sequencer was specified
        TaskCenter::with_current(|h| {
            h.cluster_state()
                .all()
                .into_iter()
                .filter(|(_, node_state)| *node_state == NodeState::Alive)
                .map(|(node_id, _)| node_id)
                .choose(&mut rng)
        }))?;

    let opts = NodeSetSelectorOptions::new(u32::from(log_id) as u64)
        .with_target_size(replicated_loglet_config.target_nodeset_size)
        .with_preferred_nodes_opt(preferred_nodes)
        .with_top_priority_node(sequencer.id());

    let selection = NodeSetSelector::select(
        nodes_config,
        &replication,
        restate_types::replicated_loglet::logserver_candidate_filter,
        |_, config| {
            matches!(
                config.log_server_config.storage_state,
                StorageState::ReadWrite
            )
        },
        opts,
    );

    match selection {
        Ok(nodeset) => {
            // todo(asoli): here is the right place to do additional validation and reject the nodeset if it
            //  fails to meet some safety margin. For now, we'll accept the nodeset if it fulfills the replication
            //  property.
            let mut node_set_checker = NodeSetChecker::new(&nodeset, nodes_config, &replication);
            node_set_checker.fill_with(true);

            // check that the new node set fulfills the replication property
            if !node_set_checker.check_write_quorum(|attr| *attr) {
                // we couldn't find a nodeset that fulfills the desired replication property
                return None;
            }

            if replication.num_copies() > 1 && nodeset.len() == replication.num_copies() as usize {
                warn!(
                    ?log_id,
                    %replication,
                    generated_nodeset_size = nodeset.len(),
                    "The number of writeable log-servers is too small for the configured \
                    replication, there will be no fault-tolerance until you add more nodes."
                );
            }
            Some(ReplicatedLogletParams {
                loglet_id,
                sequencer,
                replication,
                nodeset,
            })
        }
        Err(err) => {
            warn!(?log_id, "Cannot select node-set for log: {err}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Service;

    use googletest::assert_that;
    use googletest::matchers::eq;
    use test_log::test;

    use restate_bifrost::providers::memory_loglet;
    use restate_bifrost::{BifrostService, ErrorRecoveryStrategy};
    use restate_core::network::NetworkServerBuilder;
    use restate_core::{TaskCenter, TaskKind, TestCoreEnvBuilder};
    use restate_types::config::Configuration;
    use restate_types::health::HealthStatus;
    use restate_types::live::Live;
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::partitions::state::PartitionReplicaSetStates;

    #[test(restate_core::test)]
    async fn manual_log_trim() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);
        let builder = TestCoreEnvBuilder::with_incoming_only_connector();
        let bifrost_svc = BifrostService::new(builder.metadata_writer.clone())
            .with_factory(memory_loglet::Factory::default());
        let bifrost = bifrost_svc.handle();

        let replica_set_states = PartitionReplicaSetStates::default();

        let svc = Service::create(
            Live::from_value(Configuration::default()),
            HealthStatus::default(),
            replica_set_states,
            bifrost.clone(),
            builder.networking.clone(),
            &mut NetworkServerBuilder::default(),
            builder.metadata_writer.clone(),
        )
        .await?;

        let svc_handle = svc.handle();

        let _ = builder.build().await;
        bifrost_svc.start().await?;

        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::default())?;

        TaskCenter::spawn(TaskKind::SystemService, "cluster-controller", svc.run())?;

        for _ in 1..=5 {
            appender.append("").await?;
        }

        svc_handle.trim_log(LOG_ID, Lsn::from(3)).await??;

        let record = bifrost.read(LOG_ID, Lsn::OLDEST).await?.unwrap();
        assert_that!(record.sequence_number(), eq(Lsn::OLDEST));
        assert_that!(record.trim_gap_to_sequence_number(), eq(Some(Lsn::new(3))));

        Ok(())
    }
}
