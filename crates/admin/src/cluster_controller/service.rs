// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod state;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use codederror::CodedError;
use futures::never::Never;
use restate_storage_query_datafusion::BuildError;
use restate_storage_query_datafusion::context::{ClusterTables, QueryContext};
use restate_types::replication::{NodeSet, ReplicationProperty};
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio::time::{Instant, Interval, MissedTickBehavior};
use tonic::codec::CompressionEncoding;
use tracing::{debug, info, trace, warn};

use restate_metadata_server::ReadModifyWriteError;
use restate_types::logs::metadata::{
    LogletParams, Logs, LogsConfiguration, ProviderConfiguration, ProviderKind,
    ReplicatedLogletConfig, SegmentIndex,
};
use restate_types::metadata_store::keys::{BIFROST_CONFIG_KEY, PARTITION_TABLE_KEY};
use restate_types::partition_table::{
    self, PartitionReplication, PartitionTable, PartitionTableBuilder,
};
use restate_types::replicated_loglet::ReplicatedLogletParams;

use restate_bifrost::{Bifrost, SealedSegment};
use restate_core::network::rpc_router::RpcRouter;
use restate_core::network::tonic_service_filter::{TonicServiceFilter, WaitForReady};
use restate_core::network::{
    MessageRouterBuilder, NetworkSender, NetworkServerBuilder, Networking, TransportConnect,
};
use restate_core::{
    Metadata, MetadataWriter, ShutdownError, TargetVersion, TaskCenter, TaskKind,
    cancellation_watcher,
};
use restate_types::cluster::cluster_state::ClusterState;
use restate_types::config::{AdminOptions, Configuration};
use restate_types::health::HealthStatus;
use restate_types::identifiers::PartitionId;
use restate_types::live::Live;
use restate_types::logs::{LogId, LogletId, Lsn};
use restate_types::net::metadata::MetadataKind;
use restate_types::net::partition_processor_manager::{CreateSnapshotRequest, Snapshot};
use restate_types::protobuf::common::AdminStatus;
use restate_types::{GenerationalNodeId, NodeId, Version};

use self::state::ClusterControllerState;
use super::cluster_state_refresher::ClusterStateRefresher;
use super::grpc_svc_handler::ClusterCtrlSvcHandler;
use super::protobuf::cluster_ctrl_svc_server::ClusterCtrlSvcServer;
use crate::cluster_controller::logs_controller::{self, NodeSetSelectorHints};
use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
use crate::cluster_controller::scheduler::PartitionTableNodeSetSelectorHints;

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
    configuration: Live<Configuration>,
    metadata_writer: MetadataWriter,

    processor_manager_client: PartitionProcessorManagerClient<Networking<T>>,
    command_tx: mpsc::Sender<ClusterControllerCommand>,
    command_rx: mpsc::Receiver<ClusterControllerCommand>,
    health_status: HealthStatus<AdminStatus>,
    heartbeat_interval: Interval,
    observed_cluster_state: ObservedClusterState,
}

impl<T> Service<T>
where
    T: TransportConnect,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        mut configuration: Live<Configuration>,
        health_status: HealthStatus<AdminStatus>,
        bifrost: Bifrost,
        networking: Networking<T>,
        router_builder: &mut MessageRouterBuilder,
        server_builder: &mut NetworkServerBuilder,
        metadata_writer: MetadataWriter,
    ) -> Result<Self, BuildError> {
        let (command_tx, command_rx) = mpsc::channel(2);

        let cluster_state_refresher =
            ClusterStateRefresher::new(networking.clone(), router_builder);

        let processor_manager_client =
            PartitionProcessorManagerClient::new(networking.clone(), router_builder);

        let options = configuration.live_load();
        let heartbeat_interval = Self::create_heartbeat_interval(&options.admin);

        let cluster_query_context = QueryContext::create(
            &options.admin.query_engine,
            ClusterTables::new(cluster_state_refresher.cluster_state_watcher().watch()),
        )
        .await?;

        // Registering ClusterCtrlSvc grpc service to network server
        server_builder.register_grpc_service(
            TonicServiceFilter::new(
                ClusterCtrlSvcServer::new(ClusterCtrlSvcHandler::new(
                    ClusterControllerHandle {
                        tx: command_tx.clone(),
                    },
                    bifrost.clone(),
                    metadata_writer.clone(),
                    cluster_query_context,
                ))
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
                WaitForReady::new(health_status.clone(), AdminStatus::Ready),
            ),
            crate::cluster_controller::protobuf::FILE_DESCRIPTOR_SET,
        );

        Ok(Service {
            configuration,
            health_status,
            networking,
            bifrost,
            cluster_state_refresher,
            metadata_writer,
            processor_manager_client,
            command_tx,
            command_rx,
            heartbeat_interval,
            observed_cluster_state: ObservedClusterState::default(),
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
    GetClusterState(oneshot::Sender<Arc<ClusterState>>),
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
        partition_replication: PartitionReplication,
        default_provider: ProviderConfiguration,
        response_tx: oneshot::Sender<anyhow::Result<()>>,
    },
    SealAndExtendChain {
        log_id: LogId,
        min_version: Version,
        extension: Option<ChainExtension>,
        response_tx: oneshot::Sender<anyhow::Result<SealedSegment>>,
    },
}

pub struct ClusterControllerHandle {
    tx: mpsc::Sender<ClusterControllerCommand>,
}

impl ClusterControllerHandle {
    pub async fn get_cluster_state(&self) -> Result<Arc<ClusterState>, ShutdownError> {
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

        let _ = self
            .tx
            .send(ClusterControllerCommand::TrimLog {
                log_id,
                trim_point,
                response_tx,
            })
            .await;

        response_rx.await.map_err(|_| ShutdownError)
    }

    pub async fn create_partition_snapshot(
        &self,
        partition_id: PartitionId,
        min_target_lsn: Option<Lsn>,
        trim_log: bool,
    ) -> Result<anyhow::Result<Snapshot>, ShutdownError> {
        let (response_tx, response_rx) = oneshot::channel();

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
            // todo(pavel): this is currently as safe as the cluster auto-trim safety check
            // at this point, we know that we have successfully archived the to-be-trimmed LSN
            // to the snapshot repository; what we are missing here and in cluster auto-trim
            // is closing the loop on the new snapshot being visible to other cluster members.
            if let Err(trim_error) = self
                .trim_log(LogId::from(partition_id), snapshot.min_applied_lsn)
                .await?
            {
                return Ok(Err(trim_error));
            }
        }

        Ok(create_snapshot_response)
    }

    pub async fn update_cluster_configuration(
        &self,
        partition_replication: PartitionReplication,
        default_provider: ProviderConfiguration,
    ) -> Result<anyhow::Result<()>, ShutdownError> {
        let (response_tx, response_rx) = oneshot::channel();

        let _ = self
            .tx
            .send(ClusterControllerCommand::UpdateClusterConfiguration {
                partition_replication,
                default_provider,
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
    ) -> Result<anyhow::Result<SealedSegment>, ShutdownError> {
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

        TaskCenter::spawn_child(
            TaskKind::SystemService,
            "cluster-controller-metadata-sync",
            sync_cluster_controller_metadata(),
        )?;

        tokio::select! {
            biased;
            _ = cancellation_watcher() => {
                health_status.update(AdminStatus::Unknown);
                Ok(())
            }
            _ = self.run_inner() => {
                unreachable!("Cluster controller service has terminated unexpectedly.");
            }
        }
    }

    async fn run_inner(mut self) -> Never {
        let mut config_watcher = Configuration::watcher();
        let mut cluster_state_watcher = self.cluster_state_refresher.cluster_state_watcher();

        let mut state: ClusterControllerState<T> = ClusterControllerState::Follower;

        loop {
            tokio::select! {
                _ = self.heartbeat_interval.tick() => {
                    // Ignore error if system is shutting down
                    let _ = self.cluster_state_refresher.schedule_refresh();
                },
                Ok(cluster_state) = cluster_state_watcher.next_cluster_state() => {
                    self.observed_cluster_state.update(&cluster_state);
                    trace!(observed_cluster_state = ?self.observed_cluster_state, "Observed cluster state updated");
                    // todo quarantine this cluster controller if errors re-occur too often so that
                    //  another cluster controller can take over
                    if let Err(err) = state.update(&self) {
                        warn!(%err, "Failed to update cluster state. This can impair the overall cluster operations");
                        continue;
                    }

                    if let Err(err) = state.on_observed_cluster_state(&self.observed_cluster_state).await {
                        warn!(%err, "Failed to handle observed cluster state. This can impair the overall cluster operations");
                    }
                }
                Some(cmd) = self.command_rx.recv() => {
                    // it is still safe to handle cluster commands as a follower
                    self.on_cluster_cmd(cmd).await;
                }
                _ = config_watcher.changed() => {
                    debug!("Updating the cluster controller settings.");
                    let configuration = self.configuration.live_load();
                    self.heartbeat_interval = Self::create_heartbeat_interval(&configuration.admin);
                    state.reconfigure(configuration);
                }
                result = state.run() => {
                    let leader_event = match result {
                        Ok(leader_event) => leader_event,
                        Err(err) => {
                            warn!(
                                %err,
                                "Failed to run cluster controller operations. This can impair the overall cluster operations"
                            );
                            continue;
                        }
                    };

                    if let Err(err) = state.on_leader_event(&self.observed_cluster_state, leader_event).await {
                        warn!(
                            %err,
                            "Failed to handle leader event. This can impair the overall cluster operations"
                        );
                    }
                }
            }
        }
    }

    /// Triggers a snapshot creation for the given partition by issuing an RPC
    /// to the node hosting the active leader.
    async fn create_partition_snapshot(
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

                let mut node_rpc_client = self.processor_manager_client.clone();
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

    async fn update_cluster_configuration(
        &self,
        partition_replication: PartitionReplication,
        default_provider: ProviderConfiguration,
    ) -> anyhow::Result<()> {
        let logs = self
            .metadata_writer
            .metadata_store_client()
            .read_modify_write(BIFROST_CONFIG_KEY.clone(), |current: Option<Logs>| {
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

                let mut builder = logs.into_builder();

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
            Ok(logs) => {
                self.metadata_writer.update(Arc::new(logs)).await?;
            }
            Err(ReadModifyWriteError::FailedOperation(
                ClusterConfigurationUpdateError::Unchanged,
            )) => {
                // nothing to do
            }
            Err(err) => return Err(err.into()),
        };

        let partition_table = self
            .metadata_writer
            .metadata_store_client()
            .read_modify_write(
                PARTITION_TABLE_KEY.clone(),
                |current: Option<PartitionTable>| {
                    let partition_table =
                        current.ok_or(ClusterConfigurationUpdateError::MissingPartitionTable)?;

                    let mut builder: PartitionTableBuilder = partition_table.into();

                    if builder.partition_replication() != &partition_replication {
                        builder.set_partition_replication(partition_replication.clone());
                    }

                    builder
                        .build_if_modified()
                        .ok_or(ClusterConfigurationUpdateError::Unchanged)
                },
            )
            .await;

        match partition_table {
            Ok(partition_table) => {
                self.metadata_writer
                    .update(Arc::new(partition_table))
                    .await?;
            }
            Err(ReadModifyWriteError::FailedOperation(
                ClusterConfigurationUpdateError::Unchanged,
            )) => {
                // nothing to do
            }
            Err(err) => return Err(err.into()),
        };

        Ok(())
    }

    fn seal_and_extend_chain(
        &self,
        log_id: LogId,
        min_version: Version,
        extension: Option<ChainExtension>,
        response_tx: oneshot::Sender<anyhow::Result<SealedSegment>>,
    ) {
        let task = SealAndExtendTask {
            log_id,
            extension,
            min_version,
            bifrost: self.bifrost.clone(),
            observed_cluster_state: self.observed_cluster_state.clone(),
        };

        _ = TaskCenter::spawn(TaskKind::Disposable, "seal-and-extend", async move {
            let result = task.run().await;
            _ = response_tx.send(result);
            Ok(())
        });
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
                let result = self.bifrost.admin().trim(log_id, trim_point).await;
                let _ = response_tx.send(result.map_err(Into::into));
            }
            ClusterControllerCommand::CreateSnapshot {
                partition_id,
                min_target_lsn,
                response_tx,
            } => {
                info!(?partition_id, "Create snapshot command received");
                self.create_partition_snapshot(partition_id, min_target_lsn, response_tx)
                    .await;
            }
            ClusterControllerCommand::UpdateClusterConfiguration {
                partition_replication: replication_strategy,
                default_provider,
                response_tx,
            } => {
                match tokio::time::timeout(
                    Duration::from_secs(2),
                    self.update_cluster_configuration(replication_strategy, default_provider),
                )
                .await
                {
                    Ok(result) => {
                        let _ = response_tx.send(result);
                    }
                    Err(_timeout) => {
                        let _ =
                            response_tx.send(Err(anyhow!("Timeout on writing to metadata store")));
                    }
                }
            }
            ClusterControllerCommand::SealAndExtendChain {
                log_id,
                min_version,
                extension,
                response_tx,
            } => self.seal_and_extend_chain(log_id, min_version, extension, response_tx),
        }
    }
}

async fn sync_cluster_controller_metadata() -> anyhow::Result<()> {
    // todo make this configurable
    let mut interval = time::interval(Duration::from_secs(10));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut cancel = std::pin::pin!(cancellation_watcher());
    let metadata = Metadata::current();

    loop {
        tokio::select! {
            _ = &mut cancel => {
                break;
            },
            _ = interval.tick() => {
                tokio::select! {
                    _ = &mut cancel => {
                        break;
                    },
                    _ = futures::future::join3(
                        metadata.sync(MetadataKind::NodesConfiguration, TargetVersion::Latest),
                        metadata.sync(MetadataKind::PartitionTable, TargetVersion::Latest),
                        metadata.sync(MetadataKind::Logs, TargetVersion::Latest)) => {}
                }
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
enum ClusterConfigurationUpdateError {
    #[error("Unchanged")]
    Unchanged,
    #[error("Changing default provider kind to {0} is not supported. Choose 'replicated' instead")]
    ChooseReplicatedLoglet(ProviderKind),
    #[error(transparent)]
    BuildError(#[from] partition_table::BuilderError),
    #[error("missing partition table; cluster seems to be not provisioned")]
    MissingPartitionTable,
}

#[derive(Clone)]
struct PartitionProcessorManagerClient<N> {
    network_sender: N,
    create_snapshot_router: RpcRouter<CreateSnapshotRequest>,
}

impl<N> PartitionProcessorManagerClient<N>
where
    N: NetworkSender + 'static,
{
    pub fn new(network_sender: N, router_builder: &mut MessageRouterBuilder) -> Self {
        let create_snapshot_router = RpcRouter::new(router_builder);

        PartitionProcessorManagerClient {
            network_sender,
            create_snapshot_router,
        }
    }

    pub async fn create_snapshot(
        &mut self,
        node_id: GenerationalNodeId,
        partition_id: PartitionId,
        min_target_lsn: Option<Lsn>,
    ) -> anyhow::Result<Snapshot> {
        self.create_snapshot_router
            .call(
                &self.network_sender,
                node_id,
                CreateSnapshotRequest {
                    partition_id,
                    min_target_lsn,
                },
            )
            .await?
            .into_body()
            .result
            .map_err(|e| anyhow!("Failed to create snapshot: {:?}", e))
    }
}

struct SealAndExtendTask {
    log_id: LogId,
    min_version: Version,
    extension: Option<ChainExtension>,
    bifrost: Bifrost,
    observed_cluster_state: ObservedClusterState,
}

impl SealAndExtendTask {
    async fn run(self) -> anyhow::Result<SealedSegment> {
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
        let previous_params = if matches!(segment.config.kind, ProviderKind::Replicated) {
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
                #[cfg(any(test, feature = "memory-loglet"))]
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
                Metadata::with_current(|m| {
                    PartitionTableNodeSetSelectorHints::from(m.partition_table_snapshot())
                })
                .preferred_sequencer(&self.log_id)
            });

        let (provider, params) = match &provider_config {
            #[cfg(any(test, feature = "memory-loglet"))]
            ProviderConfiguration::InMemory => (
                ProviderKind::InMemory,
                u64::from(next_loglet_id).to_string().into(),
            ),
            ProviderConfiguration::Local => (
                ProviderKind::Local,
                u64::from(next_loglet_id).to_string().into(),
            ),
            #[cfg(feature = "replicated-loglet")]
            ProviderConfiguration::Replicated(config) => {
                let loglet_params = logs_controller::build_new_replicated_loglet_configuration(
                    self.log_id,
                    config,
                    next_loglet_id,
                    &Metadata::with_current(|m| m.nodes_config_ref()),
                    &self.observed_cluster_state,
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
#[cfg(test)]
mod tests {
    use super::Service;

    use std::collections::BTreeSet;
    use std::num::NonZero;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use googletest::assert_that;
    use googletest::matchers::eq;
    use test_log::test;
    use tracing::{debug, info, warn};

    use crate::cluster_controller::cluster_state_refresher::ClusterStateWatcher;
    use restate_bifrost::loglet::FindTailOptions;
    use restate_bifrost::providers::memory_loglet;
    use restate_bifrost::{Bifrost, BifrostService, ErrorRecoveryStrategy};
    use restate_core::network::{
        FailingConnector, Incoming, MessageHandler, MockPeerConnection, NetworkServerBuilder,
    };
    use restate_core::test_env::NoOpMessageHandler;
    use restate_core::{TaskCenter, TaskKind, TestCoreEnv, TestCoreEnvBuilder};
    use restate_types::cluster::cluster_state::{NodeState, PartitionProcessorStatus};
    use restate_types::config::{
        AdminOptionsBuilder, BifrostOptions, Configuration, NetworkingOptions,
    };
    use restate_types::health::HealthStatus;
    use restate_types::identifiers::PartitionId;
    use restate_types::live::Live;
    use restate_types::locality::NodeLocation;
    use restate_types::logs::metadata::ProviderKind;
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::net::AdvertisedAddress;
    use restate_types::net::node::{GetNodeState, NodeStateResponse};
    use restate_types::net::partition_processor_manager::ControlProcessors;
    use restate_types::nodes_config::{
        LogServerConfig, MetadataServerConfig, NodeConfig, NodesConfiguration, Role,
    };
    use restate_types::{GenerationalNodeId, PlainNodeId, Version};

    #[test(restate_core::test)]
    async fn manual_log_trim() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);
        let mut builder = TestCoreEnvBuilder::with_incoming_only_connector();
        let bifrost_svc = BifrostService::new(builder.metadata_writer.clone())
            .with_factory(memory_loglet::Factory::default());
        let bifrost = bifrost_svc.handle();

        let svc = Service::create(
            Live::from_value(Configuration::default()),
            HealthStatus::default(),
            bifrost.clone(),
            builder.networking.clone(),
            &mut builder.router_builder,
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

    #[derive(Default)]
    struct MockNodeStateHandler {
        applied_lsn: Arc<AtomicU64>,
        persisted_lsn: Arc<AtomicU64>,
        archived_lsn: Arc<AtomicU64>,
        // node ids for which the handler won't send a response to the caller, simulating dead nodes
        block_list: BTreeSet<GenerationalNodeId>,
    }

    impl MessageHandler for MockNodeStateHandler {
        type MessageType = GetNodeState;

        async fn on_message(&self, msg: Incoming<Self::MessageType>) {
            let peer_id = msg.peer();
            if self.block_list.contains(&peer_id) {
                warn!("Ignoring blocked destination: {}", peer_id);
                return;
            }

            let partition_processor_status = PartitionProcessorStatus {
                last_applied_log_lsn: Some(Lsn::from(self.applied_lsn.load(Ordering::Relaxed))),
                last_persisted_log_lsn: Some(Lsn::from(self.persisted_lsn.load(Ordering::Relaxed))),
                last_archived_log_lsn: match self.archived_lsn.load(Ordering::Relaxed) {
                    0 => None,
                    n => Some(Lsn::from(n)),
                },
                ..PartitionProcessorStatus::new()
            };

            let state = [(PartitionId::MIN, partition_processor_status)].into();
            let response = msg.to_rpc_response(NodeStateResponse {
                partition_processor_state: Some(state),
                uptime: Duration::from_secs(100),
            });

            // We are not really sending something back to target, we just need to provide a known
            // node_id. The response will be sent to a handler running on the very same node.
            debug!("{} sending response: {:?}", peer_id, &response);
            response.send().await.expect("send should succeed");
        }
    }

    #[test(restate_core::test(start_paused = true))]
    async fn auto_log_trim() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let interval_duration = Duration::from_secs(10);
        let admin_options = AdminOptionsBuilder::default()
            .log_trim_check_interval(interval_duration.into())
            .build()
            .unwrap();
        let networking = NetworkingOptions {
            // we are using failing transport so we only want to use the mock connection we created.
            num_concurrent_connections: NonZero::new(1).unwrap(),
            ..Default::default()
        };
        let config = Configuration {
            networking,
            admin: admin_options,
            ..Default::default()
        };

        let applied_lsn = Arc::new(AtomicU64::new(0));
        let persisted_lsn = Arc::new(AtomicU64::new(0));
        let archived_lsn = Arc::new(AtomicU64::new(0));

        let get_node_state_handler = Arc::new(MockNodeStateHandler {
            applied_lsn: applied_lsn.clone(),
            persisted_lsn: persisted_lsn.clone(),
            archived_lsn: archived_lsn.clone(),
            block_list: BTreeSet::new(),
        });

        let (node_env, bifrost, _) = create_test_env(config, |builder| {
            builder
                .add_message_handler(get_node_state_handler.clone())
                .add_message_handler(NoOpMessageHandler::<ControlProcessors>::default())
        })
        .await?;

        // simulate a connection from node 2 so we can have a connection between the two nodes
        let node_2 = MockPeerConnection::connect(
            GenerationalNodeId::new(2, 2),
            node_env.metadata.nodes_config_version(),
            node_env
                .metadata
                .nodes_config_ref()
                .cluster_name()
                .to_owned(),
            node_env.networking.connection_manager(),
            10,
        )
        .await?;
        // let node2 receive messages and use the same message handler as node1
        let (_node_2, _node2_reactor) =
            node_2.process_with_message_handler(get_node_state_handler)?;

        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::default())?;
        for i in 1..=20 {
            let lsn = appender.append("").await?;
            assert_eq!(Lsn::from(i), lsn);
        }
        applied_lsn.store(
            bifrost
                .find_tail(LOG_ID, FindTailOptions::default())
                .await?
                .offset()
                .prev()
                .as_u64(),
            Ordering::Relaxed,
        );

        tokio::time::sleep(interval_duration * 2).await;
        assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

        archived_lsn.store(6, Ordering::Relaxed);
        tokio::time::sleep(interval_duration * 2).await;
        assert_eq!(Lsn::from(6), bifrost.get_trim_point(LOG_ID).await?);

        archived_lsn.store(11, Ordering::Relaxed);
        tokio::time::sleep(interval_duration * 2).await;
        assert_eq!(Lsn::from(11), bifrost.get_trim_point(LOG_ID).await?);

        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn auto_trim_log() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let networking = NetworkingOptions {
            // we are using failing transport so we only want to use the mock connection we created.
            num_concurrent_connections: NonZero::new(1).unwrap(),
            ..Default::default()
        };

        let interval_duration = Duration::from_secs(10);
        let admin_options = AdminOptionsBuilder::default()
            .log_trim_check_interval(interval_duration.into())
            .build()
            .unwrap();
        let interval_duration = Duration::from_secs(10);
        let config = Configuration {
            networking,
            admin: admin_options,
            ..Default::default()
        };

        let applied_lsn = Arc::new(AtomicU64::new(0));
        let persisted_lsn = Arc::new(AtomicU64::new(0));
        let get_node_state_handler = Arc::new(MockNodeStateHandler {
            applied_lsn: applied_lsn.clone(),
            persisted_lsn: Arc::clone(&persisted_lsn),
            archived_lsn: Arc::new(AtomicU64::new(0)), // not used in this test
            block_list: BTreeSet::new(),
        });
        let (node_env, bifrost, _) = create_test_env(config, |builder| {
            builder
                .add_message_handler(get_node_state_handler.clone())
                .add_message_handler(NoOpMessageHandler::<ControlProcessors>::default())
        })
        .await?;

        let node_2 = MockPeerConnection::connect(
            GenerationalNodeId::new(2, 2),
            node_env.metadata.nodes_config_version(),
            node_env
                .metadata
                .nodes_config_ref()
                .cluster_name()
                .to_owned(),
            node_env.networking.connection_manager(),
            10,
        )
        .await?;
        // let node2 receive messages and use the same message handler as node1
        let (_node_2, _node2_reactor) =
            node_2.process_with_message_handler(get_node_state_handler)?;

        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::default())?;
        for i in 1..=20 {
            let lsn = appender.append(format!("record{i}")).await?;
            assert_eq!(Lsn::from(i), lsn);
        }
        applied_lsn.store(
            bifrost
                .find_tail(LOG_ID, FindTailOptions::default())
                .await?
                .offset()
                .prev()
                .as_u64(),
            Ordering::Relaxed,
        );
        tokio::time::sleep(interval_duration * 2).await;
        assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

        persisted_lsn.store(3, Ordering::Relaxed);
        tokio::time::sleep(interval_duration * 2).await;
        assert_eq!(bifrost.get_trim_point(LOG_ID).await?, Lsn::from(3));

        let v = bifrost.read(LOG_ID, Lsn::from(4)).await?.unwrap();
        assert_that!(v.sequence_number(), eq(Lsn::new(4)));
        assert!(v.is_data_record());
        assert_that!(v.decode_unchecked::<String>(), eq("record4".to_owned()));

        persisted_lsn.store(20, Ordering::Relaxed);
        tokio::time::sleep(interval_duration * 2).await;
        assert_eq!(Lsn::from(20), bifrost.get_trim_point(LOG_ID).await?);

        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn do_not_trim_unless_all_nodes_report_persisted_lsn() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let interval_duration = Duration::from_secs(10);
        let admin_options = AdminOptionsBuilder::default()
            .log_trim_check_interval(interval_duration.into())
            .build()
            .unwrap();
        let mut bifrost_options = BifrostOptions::default();
        bifrost_options.default_provider = ProviderKind::InMemory;

        let networking = NetworkingOptions {
            // we are using failing transport so we only want to use the mock connection we created.
            num_concurrent_connections: NonZero::new(1).unwrap(),
            ..Default::default()
        };
        let config = Configuration {
            networking,
            admin: admin_options,
            bifrost: bifrost_options,
            ..Default::default()
        };

        let applied_lsn = Arc::new(AtomicU64::new(0));
        let persisted_lsn = Arc::new(AtomicU64::new(0));

        let (_node_env, bifrost, _) = create_test_env(config, |builder| {
            let block_list = builder
                .nodes_config
                .iter()
                .next()
                .map(|(_, node_config)| node_config.current_generation)
                .into_iter()
                .collect();

            let get_node_state_handler = MockNodeStateHandler {
                applied_lsn: applied_lsn.clone(),
                persisted_lsn: persisted_lsn.clone(),
                archived_lsn: Arc::new(AtomicU64::new(0)), // unused in this test
                block_list,
            };

            builder
                .add_message_handler(get_node_state_handler)
                .add_message_handler(NoOpMessageHandler::<ControlProcessors>::default())
        })
        .await?;

        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::default())?;
        for i in 1..=5 {
            let lsn = appender.append(format!("record{i}")).await?;
            assert_eq!(Lsn::from(i), lsn);
        }
        applied_lsn.store(
            bifrost
                .find_tail(LOG_ID, FindTailOptions::default())
                .await?
                .offset()
                .prev()
                .as_u64(),
            Ordering::Relaxed,
        );
        tokio::time::sleep(interval_duration * 10).await;

        // no trimming should have happened because no nodes report archived_lsn
        assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn do_not_trim_by_persisted_lsn_if_snapshot_repository_configured() -> anyhow::Result<()>
    {
        const LOG_ID: LogId = LogId::new(0);

        let networking = NetworkingOptions {
            // we are using failing transport so we only want to use the mock connection we created.
            num_concurrent_connections: NonZero::new(1).unwrap(),
            ..Default::default()
        };

        let interval_duration = Duration::from_secs(10);
        let admin_options = AdminOptionsBuilder::default()
            .log_trim_check_interval(interval_duration.into())
            .build()
            .unwrap();
        let mut config: Configuration = Configuration {
            networking,
            admin: admin_options,
            ..Default::default()
        };
        config.bifrost.default_provider = ProviderKind::InMemory;
        config.worker.snapshots.destination = Some("a-repository-somewhere".to_string());

        let lsn = AtomicU64::new(0);
        let applied_persisted_lsn = Arc::new(lsn);

        let (node_env, bifrost, cluster_state) = create_test_env(config, |builder| {
            let get_node_state_handler = MockNodeStateHandler {
                applied_lsn: applied_persisted_lsn.clone(),
                persisted_lsn: applied_persisted_lsn.clone(),
                archived_lsn: Arc::new(AtomicU64::new(0)), // unused in this test
                block_list: Default::default(),
            };

            builder
                .add_message_handler(get_node_state_handler)
                .add_message_handler(NoOpMessageHandler::<ControlProcessors>::default())
        })
        .await?;

        let get_node_2_state_handler = MockNodeStateHandler {
            applied_lsn: applied_persisted_lsn.clone(),
            persisted_lsn: applied_persisted_lsn.clone(),
            archived_lsn: Arc::new(AtomicU64::new(0)), // unused in this test
            block_list: Default::default(),
        };
        let node_2 = MockPeerConnection::connect(
            GenerationalNodeId::new(2, 2),
            node_env.metadata.nodes_config_version(),
            node_env
                .metadata
                .nodes_config_ref()
                .cluster_name()
                .to_owned(),
            node_env.networking.connection_manager(),
            10,
        )
        .await?;
        // let node2 receive messages and use the same message handler as node1
        let (_node_2, _node2_reactor) =
            node_2.process_with_message_handler(get_node_2_state_handler)?;

        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::default())?;
        for i in 1..=20 {
            let lsn = appender.append(format!("record{i}")).await?;
            assert_eq!(Lsn::from(i), lsn);
        }
        applied_persisted_lsn.store(
            bifrost
                .find_tail(LOG_ID, FindTailOptions::default())
                .await?
                .offset()
                .prev()
                .as_u64(),
            Ordering::Relaxed,
        );

        tokio::time::sleep(interval_duration * 2).await;
        assert!(
            cluster_state
                .current()
                .nodes
                .get(&PlainNodeId::new(1))
                .is_some_and(|n| matches!(n, NodeState::Alive(_)))
        );
        assert!(
            cluster_state
                .current()
                .nodes
                .get(&PlainNodeId::new(2))
                .is_some_and(|n| matches!(n, NodeState::Alive(_)))
        );
        // no trimming should have happened because no nodes report archived_lsn
        assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn auto_trim_by_archived_lsn_when_dead_nodes_present() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let interval_duration = Duration::from_secs(10);
        let admin_options = AdminOptionsBuilder::default()
            .log_trim_check_interval(interval_duration.into())
            .build()
            .unwrap();

        let mut bifrost_options = BifrostOptions::default();
        bifrost_options.default_provider = ProviderKind::InMemory;
        let config = Configuration {
            admin: admin_options,
            bifrost: bifrost_options,
            ..Default::default()
        };

        let applied_lsn = Arc::new(AtomicU64::new(0));
        let persisted_lsn = Arc::new(AtomicU64::new(0));
        let archived_lsn = Arc::new(AtomicU64::new(0));

        let (_node_env, bifrost, cluster_state) = create_test_env(config, |builder| {
            assert_eq!(
                builder.nodes_config.iter().count(),
                2,
                "expect two nodes in config"
            );

            let blocked_node = builder
                .nodes_config
                .iter()
                .nth(1)
                .map(|(_, node_config)| node_config.current_generation)
                .expect("a second node in cluster");

            info!("Block list: {:?}", blocked_node);

            let get_node_state_handler = MockNodeStateHandler {
                applied_lsn: applied_lsn.clone(),
                persisted_lsn: persisted_lsn.clone(),
                archived_lsn: archived_lsn.clone(),
                block_list: [GenerationalNodeId::new(2, 2)].into(),
            };

            builder
                .add_message_handler(get_node_state_handler)
                .add_message_handler(NoOpMessageHandler::<ControlProcessors>::default())
        })
        .await?;

        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::default())?;
        for i in 1..=20 {
            let lsn = appender.append(format!("record{i}")).await?;
            assert_eq!(Lsn::from(i), lsn);
        }
        applied_lsn.store(
            bifrost
                .find_tail(LOG_ID, FindTailOptions::default())
                .await?
                .offset()
                .prev()
                .as_u64(),
            Ordering::Relaxed,
        );
        tokio::time::sleep(interval_duration * 2).await;
        assert!(
            cluster_state
                .current()
                .nodes
                .get(&PlainNodeId::new(1))
                .is_some_and(|n| matches!(n, NodeState::Alive(_)))
        );
        assert!(
            cluster_state
                .current()
                .nodes
                .get(&PlainNodeId::new(2))
                .is_some_and(|n| matches!(n, NodeState::Dead(_)))
        );
        assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

        archived_lsn.store(10, Ordering::Relaxed);
        tokio::time::sleep(interval_duration * 2).await;
        assert_eq!(Lsn::from(10), bifrost.get_trim_point(LOG_ID).await?);

        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn do_not_trim_by_archived_lsn_if_slow_nodes_present() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let interval_duration = Duration::from_secs(10);
        let admin_options = AdminOptionsBuilder::default()
            .log_trim_check_interval(interval_duration.into())
            .build()
            .unwrap();

        let mut bifrost_options = BifrostOptions::default();
        bifrost_options.default_provider = ProviderKind::InMemory;
        let networking = NetworkingOptions {
            // we are using failing transport so we only want to use the mock connection we created.
            num_concurrent_connections: NonZero::new(1).unwrap(),
            ..Default::default()
        };

        let config = Configuration {
            networking,
            admin: admin_options,
            bifrost: bifrost_options,
            ..Default::default()
        };

        let n2_applied_lsn = Arc::new(AtomicU64::new(10)); // initially less than N1's archived LSN
        let n2_msg_handler = MockNodeStateHandler {
            applied_lsn: n2_applied_lsn.clone(),
            ..Default::default()
        };

        let (node_env, bifrost, cluster_state) = create_test_env(config, |builder| {
            assert_eq!(
                builder.nodes_config.iter().count(),
                2,
                "expect two nodes in config"
            );
            let n1_msg_handler = MockNodeStateHandler {
                applied_lsn: Arc::new(AtomicU64::new(20)),
                archived_lsn: Arc::new(AtomicU64::new(15)),
                ..Default::default()
            };
            builder
                .add_message_handler(n1_msg_handler)
                .add_message_handler(NoOpMessageHandler::<ControlProcessors>::default())
        })
        .await?;

        let node_2_conn = MockPeerConnection::connect(
            GenerationalNodeId::new(2, 2),
            node_env.metadata.nodes_config_version(),
            node_env
                .metadata
                .nodes_config_ref()
                .cluster_name()
                .to_owned(),
            node_env.networking.connection_manager(),
            10,
        )
        .await?;
        node_2_conn.process_with_message_handler(n2_msg_handler)?;

        let mut appender = bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::default())?;
        for i in 1..=20 {
            let lsn = appender.append(format!("record{i}")).await?;
            assert_eq!(Lsn::from(i), lsn);
        }

        tokio::time::sleep(interval_duration * 2).await;
        assert!(matches!(
            cluster_state.current().nodes.get(&PlainNodeId::new(1)),
            Some(NodeState::Alive(_))
        ));
        assert!(matches!(
            cluster_state.current().nodes.get(&PlainNodeId::new(2)),
            Some(NodeState::Alive(_))
        ));
        assert_eq!(Lsn::from(15), bifrost.get_trim_point(LOG_ID).await?);

        n2_applied_lsn.store(20, Ordering::Relaxed);
        tokio::time::sleep(interval_duration * 2).await;
        assert_eq!(Lsn::from(15), bifrost.get_trim_point(LOG_ID).await?);

        Ok(())
    }

    async fn create_test_env<F>(
        config: Configuration,
        mut modify_builder: F,
    ) -> anyhow::Result<(TestCoreEnv<FailingConnector>, Bifrost, ClusterStateWatcher)>
    where
        F: FnMut(TestCoreEnvBuilder<FailingConnector>) -> TestCoreEnvBuilder<FailingConnector>,
    {
        restate_types::config::set_current_config(config);
        let mut builder = TestCoreEnvBuilder::with_incoming_only_connector();
        let bifrost_svc = BifrostService::new(builder.metadata_writer.clone())
            .with_factory(memory_loglet::Factory::default());
        let bifrost = bifrost_svc.handle();

        let mut server_builder = NetworkServerBuilder::default();

        let svc = Service::create(
            Configuration::updateable(),
            HealthStatus::default(),
            bifrost.clone(),
            builder.networking.clone(),
            &mut builder.router_builder,
            &mut server_builder,
            builder.metadata_writer.clone(),
        )
        .await?;

        let cluster_state_watcher = svc.cluster_state_refresher.cluster_state_watcher();

        let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());
        nodes_config.upsert_node(NodeConfig::new(
            "node-1".to_owned(),
            GenerationalNodeId::new(1, 1),
            NodeLocation::default(),
            AdvertisedAddress::Uds("foobar".into()),
            Role::Admin | Role::Worker,
            LogServerConfig::default(),
            MetadataServerConfig::default(),
        ));
        nodes_config.upsert_node(NodeConfig::new(
            "node-2".to_owned(),
            GenerationalNodeId::new(2, 2),
            NodeLocation::default(),
            AdvertisedAddress::Uds("bar".into()),
            Role::Worker.into(),
            LogServerConfig::default(),
            MetadataServerConfig::default(),
        ));
        let builder = modify_builder(builder.set_nodes_config(nodes_config));

        let node_env = builder.build().await;
        bifrost_svc.start().await?;

        TaskCenter::spawn(TaskKind::SystemService, "cluster-controller", svc.run())?;
        Ok((node_env, bifrost, cluster_state_watcher))
    }
}
