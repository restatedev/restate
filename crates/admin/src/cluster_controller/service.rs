// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use codederror::CodedError;
use futures::future::OptionFuture;
use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio::time::{Instant, Interval, MissedTickBehavior};
use tracing::{debug, info, warn};

use restate_bifrost::{Bifrost, BifrostAdmin};
use restate_core::metadata_store::{retry_on_network_error, MetadataStoreClient};
use restate_core::network::rpc_router::RpcRouter;
use restate_core::network::{
    Incoming, MessageRouterBuilder, NetworkSender, Networking, TransportConnect,
};
use restate_core::{
    cancellation_watcher, Metadata, MetadataWriter, ShutdownError, TargetVersion, TaskCenter,
    TaskKind,
};
use restate_types::cluster::cluster_state::{AliveNode, ClusterState, NodeState};
use restate_types::config::{AdminOptions, Configuration};
use restate_types::health::HealthStatus;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::live::Live;
use restate_types::logs::{LogId, Lsn, SequenceNumber};
use restate_types::metadata_store::keys::PARTITION_TABLE_KEY;
use restate_types::net::cluster_controller::{AttachRequest, AttachResponse};
use restate_types::net::metadata::MetadataKind;
use restate_types::net::partition_processor_manager::CreateSnapshotRequest;
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::common::AdminStatus;
use restate_types::{GenerationalNodeId, Version};

use super::cluster_state_refresher::{ClusterStateRefresher, ClusterStateWatcher};
use crate::cluster_controller::logs_controller::{
    LogsBasedPartitionProcessorPlacementHints, LogsController,
};
use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
use crate::cluster_controller::scheduler::{Scheduler, SchedulingPlanNodeSetSelectorHints};

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("error")]
    #[code(unknown)]
    Error,
}

pub struct Service<T> {
    task_center: TaskCenter,
    health_status: HealthStatus<AdminStatus>,
    metadata: Metadata,
    networking: Networking<T>,
    incoming_messages: Pin<Box<dyn Stream<Item = Incoming<AttachRequest>> + Send + Sync + 'static>>,
    cluster_state_refresher: ClusterStateRefresher<T>,
    processor_manager_client: PartitionProcessorManagerClient<Networking<T>>,
    command_tx: mpsc::Sender<ClusterControllerCommand>,
    command_rx: mpsc::Receiver<ClusterControllerCommand>,

    configuration: Live<Configuration>,
    metadata_writer: MetadataWriter,
    metadata_store_client: MetadataStoreClient,
    heartbeat_interval: Interval,
    log_trim_interval: Option<Interval>,
    log_trim_threshold: Lsn,
}

impl<T> Service<T>
where
    T: TransportConnect,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mut configuration: Live<Configuration>,
        health_status: HealthStatus<AdminStatus>,
        task_center: TaskCenter,
        metadata: Metadata,
        networking: Networking<T>,
        router_builder: &mut MessageRouterBuilder,
        metadata_writer: MetadataWriter,
        metadata_store_client: MetadataStoreClient,
    ) -> Self {
        let incoming_messages = router_builder.subscribe_to_stream(10);
        let (command_tx, command_rx) = mpsc::channel(2);

        let cluster_state_refresher = ClusterStateRefresher::new(
            task_center.clone(),
            metadata.clone(),
            networking.clone(),
            router_builder,
        );

        let processor_manager_client =
            PartitionProcessorManagerClient::new(networking.clone(), router_builder);

        let options = configuration.live_load();
        let heartbeat_interval = Self::create_heartbeat_interval(&options.admin);
        let (log_trim_interval, log_trim_threshold) =
            Self::create_log_trim_interval(&options.admin);

        Service {
            configuration,
            health_status,
            task_center,
            metadata,
            networking,
            incoming_messages,
            cluster_state_refresher,
            metadata_writer,
            metadata_store_client,
            processor_manager_client,
            command_tx,
            command_rx,
            heartbeat_interval,
            log_trim_interval,
            log_trim_threshold,
        }
    }

    fn create_heartbeat_interval(options: &AdminOptions) -> Interval {
        let mut heartbeat_interval = time::interval_at(
            Instant::now() + options.heartbeat_interval.into(),
            options.heartbeat_interval.into(),
        );
        heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        heartbeat_interval
    }

    fn create_log_trim_interval(options: &AdminOptions) -> (Option<Interval>, Lsn) {
        let log_trim_interval = options.log_trim_interval.map(|interval| {
            let mut interval = tokio::time::interval(interval.into());
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            interval
        });

        let log_trim_threshold = Lsn::new(options.log_trim_threshold);

        (log_trim_interval, log_trim_threshold)
    }
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
        response_tx: oneshot::Sender<anyhow::Result<SnapshotId>>,
    },
}

pub struct ClusterControllerHandle {
    tx: mpsc::Sender<ClusterControllerCommand>,
}

impl ClusterControllerHandle {
    pub async fn get_cluster_state(&self) -> Result<Arc<ClusterState>, ShutdownError> {
        let (tx, rx) = oneshot::channel();
        // ignore the error, we own both tx and rx at this point.
        let _ = self
            .tx
            .send(ClusterControllerCommand::GetClusterState(tx))
            .await;
        rx.await.map_err(|_| ShutdownError)
    }

    pub async fn trim_log(
        &self,
        log_id: LogId,
        trim_point: Lsn,
    ) -> Result<Result<(), anyhow::Error>, ShutdownError> {
        let (tx, rx) = oneshot::channel();

        let _ = self
            .tx
            .send(ClusterControllerCommand::TrimLog {
                log_id,
                trim_point,
                response_tx: tx,
            })
            .await;

        rx.await.map_err(|_| ShutdownError)
    }

    pub async fn create_partition_snapshot(
        &self,
        partition_id: PartitionId,
    ) -> Result<Result<SnapshotId, anyhow::Error>, ShutdownError> {
        let (tx, rx) = oneshot::channel();

        let _ = self
            .tx
            .send(ClusterControllerCommand::CreateSnapshot {
                partition_id,
                response_tx: tx,
            })
            .await;

        rx.await.map_err(|_| ShutdownError)
    }
}

impl<T: TransportConnect> Service<T> {
    pub fn handle(&self) -> ClusterControllerHandle {
        ClusterControllerHandle {
            tx: self.command_tx.clone(),
        }
    }

    pub async fn run(
        mut self,
        bifrost: Bifrost,
        all_partitions_started_tx: Option<oneshot::Sender<()>>,
    ) -> anyhow::Result<()> {
        self.init_partition_table().await?;

        let bifrost_admin =
            BifrostAdmin::new(&bifrost, &self.metadata_writer, &self.metadata_store_client);

        let mut shutdown = std::pin::pin!(cancellation_watcher());
        let mut config_watcher = Configuration::watcher();
        let mut cluster_state_watcher = self.cluster_state_refresher.cluster_state_watcher();

        // todo: This is a temporary band-aid for https://github.com/restatedev/restate/issues/1651
        //  Remove once it is properly fixed.
        if let Some(all_partition_started_tx) = all_partitions_started_tx {
            self.task_center.spawn_child(
                TaskKind::SystemBoot,
                "signal-all-partitions-started",
                None,
                signal_all_partitions_started(
                    cluster_state_watcher.clone(),
                    self.metadata.clone(),
                    all_partition_started_tx,
                ),
            )?;
        }

        self.task_center.spawn_child(
            TaskKind::SystemService,
            "cluster-controller-metadata-sync",
            None,
            sync_cluster_controller_metadata(self.metadata.clone()),
        )?;

        let configuration = self.configuration.live_load();

        let mut scheduler = Scheduler::init(
            configuration,
            self.task_center.clone(),
            self.metadata_store_client.clone(),
            self.networking.clone(),
        )
        .await?;

        let mut logs_controller = LogsController::init(
            configuration,
            self.metadata.clone(),
            bifrost.clone(),
            self.metadata_store_client.clone(),
            self.metadata_writer.clone(),
        )
        .await?;

        let mut observed_cluster_state = ObservedClusterState::default();

        let mut logs_watcher = self.metadata.watch(MetadataKind::Logs);
        let mut partition_table_watcher = self.metadata.watch(MetadataKind::PartitionTable);
        let mut logs = self.metadata.updateable_logs_metadata();
        let mut partition_table = self.metadata.updateable_partition_table();
        let mut nodes_config = self.metadata.updateable_nodes_config();
        let mut tail_udpates_watcher = logs_controller.watch_logs_tail_updates();

        self.health_status.update(AdminStatus::Ready);

        loop {
            tokio::select! {
                _ = self.heartbeat_interval.tick() => {
                    // Ignore error if system is shutting down
                    let _ = self.cluster_state_refresher.schedule_refresh();
                    let _ = logs_controller.schedule_refresh_tails();
                },
                _ = OptionFuture::from(self.log_trim_interval.as_mut().map(|interval| interval.tick())) => {
                    let result = self.trim_logs(bifrost_admin).await;

                    if let Err(err) = result {
                        warn!("Could not trim the logs. This can lead to increased disk usage: {err}");
                    }
                }
                Ok(_) = tail_udpates_watcher.changed() => {
                    // we clone here to avoid holding the read lock in the watcher
                    // more than necessary.
                    // todo(azmy): do we really need to keep the updates inside an Arc?
                    // the on_logs_tail_updates should return quick enough.
                    let tail = tail_udpates_watcher.borrow_and_update().clone();
                    logs_controller.on_logs_tail_updates(&tail);
                }
                Ok(cluster_state) = cluster_state_watcher.next_cluster_state() => {
                    observed_cluster_state.update(&cluster_state);
                    logs_controller.on_observed_cluster_state_update(&observed_cluster_state, SchedulingPlanNodeSetSelectorHints::from(&scheduler))?;
                    scheduler.on_observed_cluster_state(&observed_cluster_state, nodes_config.live_load(), LogsBasedPartitionProcessorPlacementHints::from(&logs_controller)).await?;
                }
                result = logs_controller.run_async_operations() => {
                    result?;
                }
                Ok(_) = logs_watcher.changed() => {
                    logs_controller.on_logs_update(self.metadata.logs_ref())?;
                    // tell the scheduler about potentially newly provisioned logs
                    scheduler.on_logs_update(logs.live_load(), partition_table.live_load()).await?
                }
                Ok(_) = partition_table_watcher.changed() => {
                    let partition_table = partition_table.live_load();
                    let logs = logs.live_load();

                    logs_controller.on_partition_table_update(partition_table);
                    scheduler.on_logs_update(logs, partition_table).await?;
                }
                Some(cmd) = self.command_rx.recv() => {
                    self.on_cluster_cmd(cmd, bifrost_admin).await;
                }
                Some(message) = self.incoming_messages.next() => {
                    self.on_attach_request(&mut scheduler, message).await?;
                }
                _ = config_watcher.changed() => {
                    debug!("Updating the cluster controller settings.");
                    let options = &self.configuration.live_load().admin;

                    self.heartbeat_interval = Self::create_heartbeat_interval(options);
                    (self.log_trim_interval, self.log_trim_threshold) = Self::create_log_trim_interval(options);
                }
                _ = &mut shutdown => {
                    self.health_status.update(AdminStatus::Unknown);
                    return Ok(());
                }
            }
        }
    }

    async fn init_partition_table(&mut self) -> anyhow::Result<()> {
        let configuration = self.configuration.live_load();

        let partition_table = retry_on_network_error(
            configuration.common.network_error_retry_policy.clone(),
            || {
                self.metadata_store_client
                    .get_or_insert(PARTITION_TABLE_KEY.clone(), || {
                        let partition_table = if configuration.common.auto_provision_partitions {
                            PartitionTable::with_equally_sized_partitions(
                                Version::MIN,
                                configuration.common.bootstrap_num_partitions(),
                            )
                        } else {
                            PartitionTable::with_equally_sized_partitions(Version::MIN, 0)
                        };

                        debug!("Initializing the partition table with '{partition_table:?}'");

                        partition_table
                    })
            },
        )
        .await?;

        self.metadata_writer.update(partition_table).await?;

        Ok(())
    }

    async fn trim_logs(
        &self,
        bifrost_admin: BifrostAdmin<'_>,
    ) -> Result<(), restate_bifrost::Error> {
        let cluster_state = self.cluster_state_refresher.get_cluster_state();

        let mut persisted_lsns_per_partition: BTreeMap<
            PartitionId,
            BTreeMap<GenerationalNodeId, Lsn>,
        > = BTreeMap::default();

        for node_state in cluster_state.nodes.values() {
            match node_state {
                NodeState::Alive(AliveNode {
                    generational_node_id,
                    partitions,
                    ..
                }) => {
                    for (partition_id, partition_processor_status) in partitions.iter() {
                        let lsn = partition_processor_status
                            .last_persisted_log_lsn
                            .unwrap_or(Lsn::INVALID);
                        persisted_lsns_per_partition
                            .entry(*partition_id)
                            .or_default()
                            .insert(*generational_node_id, lsn);
                    }
                }
                NodeState::Dead(_) => {
                    // nothing to do
                }
            }
        }

        for (partition_id, persisted_lsns) in persisted_lsns_per_partition.into_iter() {
            let log_id = LogId::from(partition_id);

            // todo: Remove once Restate nodes can share partition processor snapshots
            // only try to trim if we know about the persisted lsns of all known nodes; otherwise we
            // risk that a node cannot fully replay the log; this assumes that no new nodes join the
            // cluster after the first trimming has happened
            if persisted_lsns.len() >= cluster_state.nodes.len() {
                let min_persisted_lsn = persisted_lsns.into_values().min().unwrap_or(Lsn::INVALID);
                // trim point is before the oldest record
                let current_trim_point = bifrost_admin.get_trim_point(log_id).await?;

                if min_persisted_lsn >= current_trim_point + self.log_trim_threshold {
                    debug!(
                    "Automatic trim log '{log_id}' for all records before='{min_persisted_lsn}'"
                );
                    bifrost_admin.trim(log_id, min_persisted_lsn).await?
                }
            } else {
                warn!("Stop automatically trimming log '{log_id}' because not all nodes are running a partition processor applying this log.");
            }
        }

        Ok(())
    }

    /// Triggers a snapshot creation for the given partition by issuing an RPC
    /// to the node hosting the active leader.
    async fn create_partition_snapshot(
        &self,
        partition_id: PartitionId,
        response_tx: oneshot::Sender<anyhow::Result<SnapshotId>>,
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
                let _ = self.task_center.spawn_child(
                    TaskKind::Disposable,
                    "create-snapshot-response",
                    Some(partition_id),
                    async move {
                        let _ = response_tx.send(
                            node_rpc_client
                                .create_snapshot(node.generational_node_id, partition_id)
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

    async fn on_cluster_cmd(
        &self,
        command: ClusterControllerCommand,
        bifrost_admin: BifrostAdmin<'_>,
    ) {
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
                    "Manual trim log command received");
                let result = bifrost_admin.trim(log_id, trim_point).await;
                let _ = response_tx.send(result.map_err(Into::into));
            }
            ClusterControllerCommand::CreateSnapshot {
                partition_id,
                response_tx,
            } => {
                info!(?partition_id, "Create snapshot command received");
                self.create_partition_snapshot(partition_id, response_tx)
                    .await;
            }
        }
    }

    async fn on_attach_request(
        &self,
        scheduler: &mut Scheduler<T>,
        request: Incoming<AttachRequest>,
    ) -> Result<(), ShutdownError> {
        let actions = scheduler.on_attach_node(request.peer()).await?;
        self.task_center.spawn(
            TaskKind::Disposable,
            "attachment-response",
            None,
            async move {
                Ok(request
                    .to_rpc_response(AttachResponse { actions })
                    .send()
                    .await?)
            },
        )?;
        Ok(())
    }
}

async fn sync_cluster_controller_metadata(metadata: Metadata) -> anyhow::Result<()> {
    // todo make this configurable
    let mut interval = time::interval(Duration::from_secs(10));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut cancel = std::pin::pin!(cancellation_watcher());

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

async fn signal_all_partitions_started(
    mut cluster_state_watcher: ClusterStateWatcher,
    metadata: Metadata,
    all_partitions_started_tx: oneshot::Sender<()>,
) -> anyhow::Result<()> {
    loop {
        let cluster_state = cluster_state_watcher.next_cluster_state().await?;

        if cluster_state.partition_table_version != metadata.partition_table_version()
            || metadata.partition_table_version() == Version::INVALID
        {
            // syncing of PartitionTable since we obviously don't have up-to-date information
            metadata
                .sync(
                    MetadataKind::PartitionTable,
                    TargetVersion::Version(cluster_state.partition_table_version.max(Version::MIN)),
                )
                .await?;
        } else {
            let partition_table = metadata.partition_table_ref();

            let mut pending_partitions_wo_leader = partition_table.num_partitions();

            for alive_node in cluster_state.alive_nodes() {
                alive_node
                    .partitions
                    .iter()
                    .for_each(|(_, partition_state)| {
                        // currently, there can only be a single leader per partition
                        if partition_state.is_effective_leader() {
                            pending_partitions_wo_leader =
                                pending_partitions_wo_leader.saturating_sub(1);
                        }
                    })
            }

            if pending_partitions_wo_leader == 0 {
                // send result can be ignored because rx should only go away in case of a shutdown
                let _ = all_partitions_started_tx.send(());
                return Ok(());
            }
        }
    }
}

#[derive(Clone)]
struct PartitionProcessorManagerClient<N>
where
    N: Clone,
{
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
    ) -> anyhow::Result<SnapshotId> {
        // todo(pavel): make snapshot RPC timeout configurable, especially if this includes remote upload in the future
        let response = tokio::time::timeout(
            Duration::from_secs(30),
            self.create_snapshot_router.call(
                &self.network_sender,
                node_id,
                CreateSnapshotRequest { partition_id },
            ),
        )
        .await?;
        let create_snapshot_response = response?.into_body();
        create_snapshot_response
            .result
            .map_err(|e| anyhow!("Failed to create snapshot: {:?}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::Service;

    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use googletest::assert_that;
    use googletest::matchers::eq;
    use test_log::test;

    use restate_bifrost::Bifrost;
    use restate_core::network::{FailingConnector, Incoming, MessageHandler, MockPeerConnection};
    use restate_core::{NoOpMessageHandler, TaskKind, TestCoreEnv, TestCoreEnvBuilder};
    use restate_types::cluster::cluster_state::PartitionProcessorStatus;
    use restate_types::config::{AdminOptions, Configuration};
    use restate_types::health::HealthStatus;
    use restate_types::identifiers::PartitionId;
    use restate_types::live::Live;
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::net::partition_processor_manager::{
        ControlProcessors, GetProcessorsState, ProcessorsStateResponse,
    };
    use restate_types::net::AdvertisedAddress;
    use restate_types::nodes_config::{LogServerConfig, NodeConfig, NodesConfiguration, Role};
    use restate_types::{GenerationalNodeId, Version};

    #[test(tokio::test)]
    async fn manual_log_trim() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);
        let mut builder = TestCoreEnvBuilder::with_incoming_only_connector();

        let svc = Service::new(
            Live::from_value(Configuration::default()),
            HealthStatus::default(),
            builder.tc.clone(),
            builder.metadata.clone(),
            builder.networking.clone(),
            &mut builder.router_builder,
            builder.metadata_writer.clone(),
            builder.metadata_store_client.clone(),
        );
        let metadata = builder.metadata.clone();
        let svc_handle = svc.handle();

        let node_env = builder.build().await;

        let bifrost = node_env
            .tc
            .run_in_scope("init", None, Bifrost::init_in_memory(metadata))
            .await;
        let mut appender = bifrost.create_appender(LOG_ID)?;

        node_env.tc.spawn(
            TaskKind::SystemService,
            "cluster-controller",
            None,
            svc.run(bifrost.clone(), None),
        )?;

        node_env
            .tc
            .run_in_scope("test", None, async move {
                for _ in 1..=5 {
                    appender.append("").await?;
                }

                svc_handle.trim_log(LOG_ID, Lsn::from(3)).await??;

                let record = bifrost.read(LOG_ID, Lsn::OLDEST).await?.unwrap();
                assert_that!(record.sequence_number(), eq(Lsn::OLDEST));
                assert_that!(record.trim_gap_to_sequence_number(), eq(Some(Lsn::new(3))));
                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    struct PartitionProcessorStatusHandler {
        persisted_lsn: Arc<AtomicU64>,
        // set of node ids for which the handler won't send a response to the caller, this allows to simulate
        // dead nodes
        block_list: BTreeSet<GenerationalNodeId>,
    }

    impl MessageHandler for PartitionProcessorStatusHandler {
        type MessageType = GetProcessorsState;

        async fn on_message(&self, msg: Incoming<Self::MessageType>) {
            if self.block_list.contains(msg.peer()) {
                return;
            }

            let partition_processor_status = PartitionProcessorStatus {
                last_persisted_log_lsn: Some(Lsn::from(self.persisted_lsn.load(Ordering::Relaxed))),
                ..PartitionProcessorStatus::new()
            };

            let state = [(PartitionId::MIN, partition_processor_status)].into();
            let response = msg.to_rpc_response(ProcessorsStateResponse { state });

            // We are not really sending something back to target, we just need to provide a known
            // node_id. The response will be sent to a handler running on the very same node.
            response.send().await.expect("send should succeed");
        }
    }

    #[test(tokio::test(start_paused = true))]
    async fn auto_log_trim() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let mut admin_options = AdminOptions::default();
        admin_options.log_trim_threshold = 5;
        let interval_duration = Duration::from_secs(10);
        admin_options.log_trim_interval = Some(interval_duration.into());
        let config = Configuration {
            admin: admin_options,
            ..Default::default()
        };

        let persisted_lsn = Arc::new(AtomicU64::new(0));
        let get_processor_state_handler = Arc::new(PartitionProcessorStatusHandler {
            persisted_lsn: Arc::clone(&persisted_lsn),
            block_list: BTreeSet::new(),
        });

        let (node_env, bifrost) = create_test_env(config, |builder| {
            builder
                .add_message_handler(get_processor_state_handler.clone())
                .add_message_handler(NoOpMessageHandler::<ControlProcessors>::default())
        })
        .await?;

        node_env
            .tc
            .clone()
            .run_in_scope("test", None, async move {
                // simulate a connection from node 2 so we can have a connection between the two
                // nodes
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
                let (_node_2, _node2_reactor) = node_2
                    .process_with_message_handler(&node_env.tc, get_processor_state_handler)?;

                let mut appender = bifrost.create_appender(LOG_ID)?;
                for i in 1..=20 {
                    let lsn = appender.append("").await?;
                    assert_eq!(Lsn::from(i), lsn);
                }

                tokio::time::sleep(interval_duration * 10).await;

                assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

                // report persisted lsn back to cluster controller
                persisted_lsn.store(6, Ordering::Relaxed);

                tokio::time::sleep(interval_duration * 10).await;
                // we delete 1-6.
                assert_eq!(Lsn::from(6), bifrost.get_trim_point(LOG_ID).await?);

                // increase by 4 more, this should not overcome the threshold
                persisted_lsn.store(10, Ordering::Relaxed);

                tokio::time::sleep(interval_duration * 10).await;
                assert_eq!(Lsn::from(6), bifrost.get_trim_point(LOG_ID).await?);

                // now we have reached the min threshold wrt to the last trim point
                persisted_lsn.store(11, Ordering::Relaxed);

                tokio::time::sleep(interval_duration * 10).await;
                assert_eq!(Lsn::from(11), bifrost.get_trim_point(LOG_ID).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[test(tokio::test(start_paused = true))]
    async fn auto_log_trim_zero_threshold() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);
        let mut admin_options = AdminOptions::default();
        admin_options.log_trim_threshold = 0;
        let interval_duration = Duration::from_secs(10);
        admin_options.log_trim_interval = Some(interval_duration.into());
        let config = Configuration {
            admin: admin_options,
            ..Default::default()
        };

        let persisted_lsn = Arc::new(AtomicU64::new(0));
        let get_processor_state_handler = Arc::new(PartitionProcessorStatusHandler {
            persisted_lsn: Arc::clone(&persisted_lsn),
            block_list: BTreeSet::new(),
        });
        let (node_env, bifrost) = create_test_env(config, |builder| {
            builder
                .add_message_handler(get_processor_state_handler.clone())
                .add_message_handler(NoOpMessageHandler::<ControlProcessors>::default())
        })
        .await?;

        node_env
            .tc
            .clone()
            .run_in_scope("test", None, async move {
                // simulate a connection from node 2 so we can have a connection between the two
                // nodes
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
                let (_node_2, _node2_reactor) = node_2
                    .process_with_message_handler(&node_env.tc, get_processor_state_handler)?;

                let mut appender = bifrost.create_appender(LOG_ID)?;
                for i in 1..=20 {
                    let lsn = appender.append(format!("record{}", i)).await?;
                    assert_eq!(Lsn::from(i), lsn);
                }
                tokio::time::sleep(interval_duration * 10).await;
                assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

                // report persisted lsn back to cluster controller
                persisted_lsn.store(3, Ordering::Relaxed);

                tokio::time::sleep(interval_duration * 10).await;
                // everything before the persisted_lsn.
                assert_eq!(bifrost.get_trim_point(LOG_ID).await?, Lsn::from(3));
                // we should be able to after the last persisted lsn
                let v = bifrost.read(LOG_ID, Lsn::from(4)).await?.unwrap();
                assert_that!(v.sequence_number(), eq(Lsn::new(4)));
                assert!(v.is_data_record());
                assert_that!(v.decode_unchecked::<String>(), eq("record4".to_owned()));

                persisted_lsn.store(20, Ordering::Relaxed);

                tokio::time::sleep(interval_duration * 10).await;
                assert_eq!(Lsn::from(20), bifrost.get_trim_point(LOG_ID).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[test(tokio::test(start_paused = true))]
    async fn do_not_trim_if_not_all_nodes_report_persisted_lsn() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let mut admin_options = AdminOptions::default();
        admin_options.log_trim_threshold = 0;
        let interval_duration = Duration::from_secs(10);
        admin_options.log_trim_interval = Some(interval_duration.into());
        let config = Configuration {
            admin: admin_options,
            ..Default::default()
        };

        let persisted_lsn = Arc::new(AtomicU64::new(0));

        let (node_env, bifrost) = create_test_env(config, |builder| {
            let black_list = builder
                .nodes_config
                .iter()
                .next()
                .map(|(_, node_config)| node_config.current_generation)
                .into_iter()
                .collect();

            let get_processor_state_handler = PartitionProcessorStatusHandler {
                persisted_lsn: Arc::clone(&persisted_lsn),
                block_list: black_list,
            };

            builder.add_message_handler(get_processor_state_handler)
        })
        .await?;

        node_env
            .tc
            .run_in_scope("test", None, async move {
                let mut appender = bifrost.create_appender(LOG_ID)?;
                for i in 1..=5 {
                    let lsn = appender.append(format!("record{}", i)).await?;
                    assert_eq!(Lsn::from(i), lsn);
                }

                // report persisted lsn back to cluster controller for a subset of the nodes
                persisted_lsn.store(5, Ordering::Relaxed);

                tokio::time::sleep(interval_duration * 10).await;
                // no trimming should have happened because one node did not report the persisted lsn
                assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    async fn create_test_env<F>(
        config: Configuration,
        mut modify_builder: F,
    ) -> anyhow::Result<(TestCoreEnv<FailingConnector>, Bifrost)>
    where
        F: FnMut(TestCoreEnvBuilder<FailingConnector>) -> TestCoreEnvBuilder<FailingConnector>,
    {
        let mut builder = TestCoreEnvBuilder::with_incoming_only_connector();
        let metadata = builder.metadata.clone();

        let svc = Service::new(
            Live::from_value(config),
            HealthStatus::default(),
            builder.tc.clone(),
            builder.metadata.clone(),
            builder.networking.clone(),
            &mut builder.router_builder,
            builder.metadata_writer.clone(),
            builder.metadata_store_client.clone(),
        );

        let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());
        nodes_config.upsert_node(NodeConfig::new(
            "node-1".to_owned(),
            GenerationalNodeId::new(1, 1),
            AdvertisedAddress::Uds("foobar".into()),
            Role::Worker.into(),
            LogServerConfig::default(),
        ));
        nodes_config.upsert_node(NodeConfig::new(
            "node-2".to_owned(),
            GenerationalNodeId::new(2, 2),
            AdvertisedAddress::Uds("bar".into()),
            Role::Worker.into(),
            LogServerConfig::default(),
        ));
        let builder = modify_builder(builder.set_nodes_config(nodes_config));

        let node_env = builder.build().await;

        let bifrost = node_env
            .tc
            .run_in_scope("init", None, Bifrost::init_in_memory(metadata))
            .await;

        node_env.tc.spawn(
            TaskKind::SystemService,
            "cluster-controller",
            None,
            svc.run(bifrost.clone(), None),
        )?;
        Ok((node_env, bifrost))
    }
}
