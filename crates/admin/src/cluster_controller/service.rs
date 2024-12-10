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

use anyhow::anyhow;
use codederror::CodedError;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio::time::{Instant, Interval, MissedTickBehavior};
use tonic::codec::CompressionEncoding;
use tracing::{debug, info};

use restate_bifrost::{Bifrost, BifrostAdmin};
use restate_core::metadata_store::{retry_on_network_error, MetadataStoreClient};
use restate_core::network::rpc_router::RpcRouter;
use restate_core::network::{
    MessageRouterBuilder, NetworkSender, NetworkServerBuilder, Networking, TransportConnect,
};
use restate_core::{
    cancellation_watcher, Metadata, MetadataWriter, ShutdownError, TargetVersion, TaskCenter,
    TaskKind,
};
use restate_types::cluster::cluster_state::ClusterState;
use restate_types::config::{AdminOptions, Configuration};
use restate_types::health::HealthStatus;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::live::Live;
use restate_types::logs::{LogId, Lsn};
use restate_types::metadata_store::keys::PARTITION_TABLE_KEY;
use restate_types::net::metadata::MetadataKind;
use restate_types::net::partition_processor_manager::CreateSnapshotRequest;
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::common::AdminStatus;
use restate_types::{GenerationalNodeId, Version};

use super::cluster_state_refresher::ClusterStateRefresher;
use super::grpc_svc_handler::ClusterCtrlSvcHandler;
use super::protobuf::cluster_ctrl_svc_server::ClusterCtrlSvcServer;
use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
use state::ClusterControllerState;

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
    metadata_store_client: MetadataStoreClient,

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
    pub fn new(
        mut configuration: Live<Configuration>,
        health_status: HealthStatus<AdminStatus>,
        bifrost: Bifrost,
        networking: Networking<T>,
        router_builder: &mut MessageRouterBuilder,
        server_builder: &mut NetworkServerBuilder,
        metadata_writer: MetadataWriter,
        metadata_store_client: MetadataStoreClient,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(2);

        let cluster_state_refresher =
            ClusterStateRefresher::new(networking.clone(), router_builder);

        let processor_manager_client =
            PartitionProcessorManagerClient::new(networking.clone(), router_builder);

        let options = configuration.live_load();
        let heartbeat_interval = Self::create_heartbeat_interval(&options.admin);

        // Registering ClusterCtrlSvc grpc service to network server
        server_builder.register_grpc_service(
            ClusterCtrlSvcServer::new(ClusterCtrlSvcHandler::new(
                ClusterControllerHandle {
                    tx: command_tx.clone(),
                },
                metadata_store_client.clone(),
                bifrost.clone(),
                metadata_writer.clone(),
            ))
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip),
            crate::cluster_controller::protobuf::FILE_DESCRIPTOR_SET,
        );

        Service {
            configuration,
            health_status,
            networking,
            bifrost,
            cluster_state_refresher,
            metadata_writer,
            metadata_store_client,
            processor_manager_client,
            command_tx,
            command_rx,
            heartbeat_interval,
            observed_cluster_state: ObservedClusterState::default(),
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

    pub async fn run(mut self) -> anyhow::Result<()> {
        self.init_partition_table().await?;

        let mut config_watcher = Configuration::watcher();
        let mut cluster_state_watcher = self.cluster_state_refresher.cluster_state_watcher();

        TaskCenter::spawn_child(
            TaskKind::SystemService,
            "cluster-controller-metadata-sync",
            sync_cluster_controller_metadata(),
        )?;

        let mut shutdown = std::pin::pin!(cancellation_watcher());

        let bifrost_admin = BifrostAdmin::new(
            &self.bifrost,
            &self.metadata_writer,
            &self.metadata_store_client,
        );

        let mut state: ClusterControllerState<T> = ClusterControllerState::Follower;

        self.health_status.update(AdminStatus::Ready);

        loop {
            tokio::select! {
                _ = self.heartbeat_interval.tick() => {
                    // Ignore error if system is shutting down
                    let _ = self.cluster_state_refresher.schedule_refresh();
                },
                Ok(cluster_state) = cluster_state_watcher.next_cluster_state() => {
                    self.observed_cluster_state.update(&cluster_state);
                    state.update(&self).await?;

                    state.on_observed_cluster_state(&self.observed_cluster_state).await?;
                }
                Some(cmd) = self.command_rx.recv() => {
                    // it is still safe to handle cluster commands as a follower
                    self.on_cluster_cmd(cmd, bifrost_admin).await;
                }
                _ = config_watcher.changed() => {
                    debug!("Updating the cluster controller settings.");
                    let configuration = self.configuration.live_load();
                    self.heartbeat_interval = Self::create_heartbeat_interval(&configuration.admin);
                    state.reconfigure(configuration);
                }
                result = state.run() => {
                    let leader_event = result?;
                    state.on_leader_event(leader_event).await?;
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

        self.metadata_writer
            .update(Arc::new(partition_table))
            .await?;

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
                let _ = TaskCenter::spawn_child(
                    TaskKind::Disposable,
                    "create-snapshot-response",
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
            Duration::from_secs(90),
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

    use restate_bifrost::providers::memory_loglet;
    use restate_bifrost::{Bifrost, BifrostService};
    use restate_core::network::{
        FailingConnector, Incoming, MessageHandler, MockPeerConnection, NetworkServerBuilder,
    };
    use restate_core::test_env::NoOpMessageHandler;
    use restate_core::{TaskCenter, TaskKind, TestCoreEnv, TestCoreEnvBuilder};
    use restate_types::cluster::cluster_state::PartitionProcessorStatus;
    use restate_types::config::{AdminOptions, Configuration};
    use restate_types::health::HealthStatus;
    use restate_types::identifiers::PartitionId;
    use restate_types::live::Live;
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::net::node::{GetNodeState, NodeStateResponse};
    use restate_types::net::partition_processor_manager::ControlProcessors;
    use restate_types::net::AdvertisedAddress;
    use restate_types::nodes_config::{LogServerConfig, NodeConfig, NodesConfiguration, Role};
    use restate_types::{GenerationalNodeId, Version};

    #[test(restate_core::test)]
    async fn manual_log_trim() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);
        let mut builder = TestCoreEnvBuilder::with_incoming_only_connector();
        let bifrost_svc = BifrostService::new().with_factory(memory_loglet::Factory::default());
        let bifrost = bifrost_svc.handle();

        let svc = Service::new(
            Live::from_value(Configuration::default()),
            HealthStatus::default(),
            bifrost.clone(),
            builder.networking.clone(),
            &mut builder.router_builder,
            &mut NetworkServerBuilder::default(),
            builder.metadata_writer.clone(),
            builder.metadata_store_client.clone(),
        );
        let svc_handle = svc.handle();

        let _ = builder.build().await;
        bifrost_svc.start().await?;

        let mut appender = bifrost.create_appender(LOG_ID)?;

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

    struct NodeStateHandler {
        // persisted_lsn: Arc<AtomicU64>,
        archived_lsn: Arc<AtomicU64>,
        // set of node ids for which the handler won't send a response to the caller, this allows to simulate
        // dead nodes
        block_list: BTreeSet<GenerationalNodeId>,
    }

    impl MessageHandler for NodeStateHandler {
        type MessageType = GetNodeState;

        async fn on_message(&self, msg: Incoming<Self::MessageType>) {
            if self.block_list.contains(&msg.peer()) {
                return;
            }

            let partition_processor_status = PartitionProcessorStatus {
                last_persisted_log_lsn: None, // deprecated
                last_archived_log_lsn: Some(Lsn::from(self.archived_lsn.load(Ordering::Relaxed))),
                ..PartitionProcessorStatus::new()
            };

            let state = [(PartitionId::MIN, partition_processor_status)].into();
            let response = msg.to_rpc_response(NodeStateResponse {
                partition_processor_state: Some(state),
            });

            // We are not really sending something back to target, we just need to provide a known
            // node_id. The response will be sent to a handler running on the very same node.
            response.send().await.expect("send should succeed");
        }
    }

    #[test(restate_core::test(start_paused = true))]
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

        let _persisted_lsn = Arc::new(AtomicU64::new(0));
        let archived_lsn = Arc::new(AtomicU64::new(0));

        let get_node_state_handler = Arc::new(NodeStateHandler {
            // persisted_lsn: Arc::clone(&persisted_lsn),
            archived_lsn: Arc::clone(&archived_lsn),
            block_list: BTreeSet::new(),
        });

        let (node_env, bifrost) = create_test_env(config, |builder| {
            builder
                .add_message_handler(get_node_state_handler.clone())
                .add_message_handler(NoOpMessageHandler::<ControlProcessors>::default())
        })
        .await?;

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
        let (_node_2, _node2_reactor) =
            node_2.process_with_message_handler(get_node_state_handler)?;

        let mut appender = bifrost.create_appender(LOG_ID)?;
        for i in 1..=20 {
            let lsn = appender.append("").await?;
            assert_eq!(Lsn::from(i), lsn);
        }

        tokio::time::sleep(interval_duration * 10).await;
        assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

        archived_lsn.store(6, Ordering::Relaxed);
        tokio::time::sleep(interval_duration * 10).await;
        assert_eq!(Lsn::from(6), bifrost.get_trim_point(LOG_ID).await?);

        archived_lsn.store(11, Ordering::Relaxed);
        tokio::time::sleep(interval_duration * 10).await;
        assert_eq!(Lsn::from(11), bifrost.get_trim_point(LOG_ID).await?);

        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn do_not_trim_if_no_nodes_report_archived_lsn() -> anyhow::Result<()> {
        const LOG_ID: LogId = LogId::new(0);

        let mut admin_options = AdminOptions::default();
        // admin_options.log_trim_threshold = 0;
        let interval_duration = Duration::from_secs(10);
        admin_options.log_trim_interval = Some(interval_duration.into());
        let config = Configuration {
            admin: admin_options,
            ..Default::default()
        };

        let _persisted_lsn = Arc::new(AtomicU64::new(0));
        let archived_lsn = Arc::new(AtomicU64::new(0));

        let (_node_env, bifrost) = create_test_env(config, |builder| {
            let block_list = builder
                .nodes_config
                .iter()
                .next()
                .map(|(_, node_config)| node_config.current_generation)
                .into_iter()
                .collect();

            let get_node_state_handler = NodeStateHandler {
                archived_lsn: Arc::clone(&archived_lsn),
                block_list,
            };

            builder.add_message_handler(get_node_state_handler)
        })
        .await?;

        let mut appender = bifrost.create_appender(LOG_ID)?;
        for i in 1..=5 {
            let lsn = appender.append(format!("record{i}")).await?;
            assert_eq!(Lsn::from(i), lsn);
        }
        // archived_lsn.store(5, Ordering::Relaxed);
        tokio::time::sleep(interval_duration * 10).await;

        // no trimming should have happened because no nodes report archived_lsn
        assert_eq!(Lsn::INVALID, bifrost.get_trim_point(LOG_ID).await?);

        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn do_not_trim_if_dead_nodes_present() -> anyhow::Result<()> {
        todo!()
    }

    #[test(restate_core::test(start_paused = true))]
    async fn do_not_trim_if_slow_nodes_present() -> anyhow::Result<()> {
        todo!()
    }

    async fn create_test_env<F>(
        config: Configuration,
        mut modify_builder: F,
    ) -> anyhow::Result<(TestCoreEnv<FailingConnector>, Bifrost)>
    where
        F: FnMut(TestCoreEnvBuilder<FailingConnector>) -> TestCoreEnvBuilder<FailingConnector>,
    {
        let mut builder = TestCoreEnvBuilder::with_incoming_only_connector();
        let bifrost_svc = BifrostService::new().with_factory(memory_loglet::Factory::default());
        let bifrost = bifrost_svc.handle();

        let mut server_builder = NetworkServerBuilder::default();

        let svc = Service::new(
            Live::from_value(config),
            HealthStatus::default(),
            bifrost.clone(),
            builder.networking.clone(),
            &mut builder.router_builder,
            &mut server_builder,
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
        bifrost_svc.start().await?;

        TaskCenter::spawn(TaskKind::SystemService, "cluster-controller", svc.run())?;
        Ok((node_env, bifrost))
    }
}
