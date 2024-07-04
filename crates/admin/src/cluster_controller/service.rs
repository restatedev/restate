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

use codederror::CodedError;
use futures::future::OptionFuture;
use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio::time::{Instant, Interval, MissedTickBehavior};
use tracing::{debug, warn};

use restate_types::config::{AdminOptions, Configuration};
use restate_types::live::LiveLoad;
use restate_types::net::cluster_controller::{Action, AttachRequest, AttachResponse, RunPartition};
use restate_types::net::RequestId;
use restate_types::partition_table::{FixedPartitionTable, KeyRange};

use restate_bifrost::Bifrost;
use restate_core::network::{MessageRouterBuilder, NetworkSender};
use restate_core::{cancellation_watcher, Metadata, ShutdownError, TaskCenter, TaskKind};
use restate_types::cluster::cluster_state::RunMode;
use restate_types::cluster::cluster_state::{AliveNode, ClusterState, NodeState};
use restate_types::identifiers::PartitionId;
use restate_types::logs::{LogId, Lsn, SequenceNumber};
use restate_types::net::metadata::MetadataKind;
use restate_types::net::MessageEnvelope;
use restate_types::{GenerationalNodeId, Version};

use super::cluster_state::{ClusterStateRefresher, ClusterStateWatcher};

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("error")]
    #[code(unknown)]
    Error,
}

pub struct Service<N> {
    task_center: TaskCenter,
    metadata: Metadata,
    networking: N,
    incoming_messages:
        Pin<Box<dyn Stream<Item = MessageEnvelope<AttachRequest>> + Send + Sync + 'static>>,
    cluster_state_refresher: ClusterStateRefresher<N>,
    command_tx: mpsc::Sender<ClusterControllerCommand>,
    command_rx: mpsc::Receiver<ClusterControllerCommand>,

    configuration: Box<dyn LiveLoad<AdminOptions> + Send + Sync>,
    heartbeat_interval: time::Interval,
    log_trim_interval: Option<time::Interval>,
    log_trim_threshold: Lsn,
}

impl<N> Service<N>
where
    N: NetworkSender + 'static,
{
    pub fn new(
        mut configuration: impl LiveLoad<AdminOptions> + Send + Sync + 'static,
        task_center: TaskCenter,
        metadata: Metadata,
        networking: N,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let incoming_messages = router_builder.subscribe_to_stream(10);
        let (command_tx, command_rx) = mpsc::channel(2);

        let cluster_state_refresher = ClusterStateRefresher::new(
            task_center.clone(),
            metadata.clone(),
            networking.clone(),
            router_builder,
        );

        let options = configuration.live_load();

        let heartbeat_interval = Self::create_heartbeat_interval(options);
        let (log_trim_interval, log_trim_threshold) = Self::create_log_trim_interval(options);

        Service {
            configuration: Box::new(configuration),
            task_center,
            metadata,
            networking,
            incoming_messages,
            cluster_state_refresher,
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

enum ClusterControllerCommand {
    GetClusterState(oneshot::Sender<Arc<ClusterState>>),
    TrimLog {
        log_id: LogId,
        before_lsn: Lsn,
        response_tx: oneshot::Sender<anyhow::Result<()>>,
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
        before_lsn: Lsn,
    ) -> Result<Result<(), anyhow::Error>, ShutdownError> {
        let (tx, rx) = oneshot::channel();

        let _ = self
            .tx
            .send(ClusterControllerCommand::TrimLog {
                log_id,
                before_lsn,
                response_tx: tx,
            })
            .await;

        rx.await.map_err(|_| ShutdownError)
    }
}

impl<N> Service<N>
where
    N: NetworkSender + 'static,
{
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
        // Make sure we have partition table before starting
        let _ = self.metadata.wait_for_partition_table(Version::MIN).await?;

        let mut shutdown = std::pin::pin!(cancellation_watcher());
        let mut config_watcher = Configuration::watcher();
        let cluster_state_watcher = self.cluster_state_refresher.cluster_state_watcher();

        // todo: This is a temporary band-aid for https://github.com/restatedev/restate/issues/1651
        //  Remove once it is properly fixed.
        if let Some(all_partition_started_tx) = all_partitions_started_tx {
            self.task_center.spawn_child(
                TaskKind::SystemBoot,
                "signal-all-partitions-started",
                None,
                signal_all_partitions_started(
                    cluster_state_watcher,
                    self.metadata.clone(),
                    all_partition_started_tx,
                ),
            )?;
        }

        loop {
            tokio::select! {
                _ = self.heartbeat_interval.tick() => {
                    // Ignore error if system is shutting down
                    let _ = self.cluster_state_refresher.schedule_refresh();
                },
                _ = OptionFuture::from(self.log_trim_interval.as_mut().map(|interval| interval.tick())) => {
                    let result = self.trim_logs(&bifrost).await;

                    if let Err(err) = result {
                        warn!("Could not trim the logs. This can lead to increased disk usage: {err}");
                    }
                }
                Some(cmd) = self.command_rx.recv() => {
                    self.on_cluster_cmd(cmd, &bifrost).await;
                }
                Some(message) = self.incoming_messages.next() => {
                    let (from, message) = message.split();
                    self.on_attach_request(from, message)?;
                }
                _ = config_watcher.changed() => {
                    self.on_config_update();
                }
                _ = &mut shutdown => {
                    return Ok(());
                }
            }
        }
    }

    fn on_config_update(&mut self) {
        debug!("Updating the cluster controller settings.");
        let options = self.configuration.live_load();

        self.heartbeat_interval = Self::create_heartbeat_interval(options);
        (self.log_trim_interval, self.log_trim_threshold) = Self::create_log_trim_interval(options);
    }

    async fn trim_logs(&self, bifrost: &Bifrost) -> Result<(), restate_bifrost::Error> {
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
            // todo: Check that we haven't forgotten a replica which is not part of the cluster state, yet
            let min_persisted_lsn = persisted_lsns.into_values().min().unwrap_or(Lsn::INVALID);
            let log_id = LogId::from(partition_id);
            // trim point is before the oldest record
            let current_trim_point = bifrost.get_trim_point(log_id).await?;

            if min_persisted_lsn >= current_trim_point.next() + self.log_trim_threshold {
                debug!(
                    "Automatic trim log '{log_id}' for all records before='{min_persisted_lsn}'"
                );
                bifrost.trim_prefix(log_id, min_persisted_lsn).await?
            }
        }

        Ok(())
    }

    async fn on_cluster_cmd(&self, command: ClusterControllerCommand, bifrost: &Bifrost) {
        match command {
            ClusterControllerCommand::GetClusterState(tx) => {
                let _ = tx.send(self.cluster_state_refresher.get_cluster_state());
            }
            ClusterControllerCommand::TrimLog {
                log_id,
                before_lsn,
                response_tx,
            } => {
                debug!("Manual trim log '{log_id}' until (exclusive) lsn='{before_lsn}'");
                let result = bifrost.trim_prefix(log_id, before_lsn).await;
                let _ = response_tx.send(result.map_err(Into::into));
            }
        }
    }

    fn on_attach_request(
        &mut self,
        from: GenerationalNodeId,
        request: AttachRequest,
    ) -> Result<(), ShutdownError> {
        let partition_table = self
            .metadata
            .partition_table()
            .expect("partition table is loaded before run");
        let networking = self.networking.clone();
        let response = self.create_attachment_response(&partition_table, from, request.request_id);
        self.task_center.spawn(
            restate_core::TaskKind::Disposable,
            "attachment-response",
            None,
            async move { Ok(networking.send(from.into(), &response).await?) },
        )?;
        Ok(())
    }

    fn create_attachment_response(
        &self,
        partition_table: &FixedPartitionTable,
        _node: GenerationalNodeId,
        request_id: RequestId,
    ) -> AttachResponse {
        // simulating a plan after initial attachement
        let actions = partition_table
            .partitioner()
            .map(|(partition_id, key_range)| {
                Action::RunPartition(RunPartition {
                    partition_id,
                    key_range_inclusive: KeyRange {
                        from: *key_range.start(),
                        to: *key_range.end(),
                    },
                    mode: RunMode::Leader,
                })
            })
            .collect();
        AttachResponse {
            request_id,
            actions,
        }
    }
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
            metadata.sync(MetadataKind::PartitionTable).await?;
        } else {
            let partition_table = metadata
                .partition_table()
                .expect("valid partition table must be present");

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

#[cfg(test)]
mod tests {
    use super::Service;
    use bytes::Bytes;
    use googletest::matchers::eq;
    use googletest::{assert_that, pat};
    use restate_bifrost::{Bifrost, Record, TrimGap};
    use restate_core::network::{MessageHandler, NetworkSender};
    use restate_core::{MockNetworkSender, TaskKind, TestCoreEnvBuilder};
    use restate_types::cluster::cluster_state::{PartitionProcessorStatus, RunMode};
    use restate_types::config::AdminOptions;
    use restate_types::identifiers::PartitionId;
    use restate_types::live::Constant;
    use restate_types::logs::{LogId, Lsn, Payload, SequenceNumber};
    use restate_types::net::partition_processor_manager::{
        GetProcessorsState, ProcessorsStateResponse,
    };
    use restate_types::net::{AdvertisedAddress, MessageEnvelope};
    use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
    use restate_types::{GenerationalNodeId, Version};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use test_log::test;

    #[test(tokio::test)]
    async fn manual_log_trim() -> anyhow::Result<()> {
        let mut builder = TestCoreEnvBuilder::new_with_mock_network();

        let svc = Service::new(
            Constant::new(AdminOptions::default()),
            builder.tc.clone(),
            builder.metadata.clone(),
            builder.network_sender.clone(),
            &mut builder.router_builder,
        );
        let metadata = builder.metadata.clone();
        let svc_handle = svc.handle();

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

        let log_id = LogId::from(0);

        node_env
            .tc
            .run_in_scope("test", None, async move {
                for _ in 1..=5 {
                    bifrost.append(log_id, Payload::default()).await?;
                }

                svc_handle.trim_log(log_id, Lsn::from(4)).await??;

                let record = bifrost.read_next_single(log_id, Lsn::OLDEST).await?;
                assert_that!(
                    record.record,
                    pat!(Record::TrimGap(pat!(TrimGap {
                        to: eq(Lsn::from(3)),
                    })))
                );
                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    struct PartitionProcessorStatusHandler {
        network_sender: MockNetworkSender,
        persisted_lsn: Arc<AtomicU64>,
    }

    impl MessageHandler for PartitionProcessorStatusHandler {
        type MessageType = GetProcessorsState;

        async fn on_message(&self, msg: MessageEnvelope<Self::MessageType>) {
            let (sender, msg) = msg.split();

            let partition_processor_status = PartitionProcessorStatus {
                last_persisted_log_lsn: Some(Lsn::from(self.persisted_lsn.load(Ordering::Relaxed))),
                ..PartitionProcessorStatus::new(RunMode::Leader)
            };

            let state = [(PartitionId::MIN, partition_processor_status)].into();
            let response = ProcessorsStateResponse {
                request_id: msg.request_id,
                state,
            };

            self.network_sender
                .send(sender.into(), &response)
                .await
                .expect("send should succeed");
        }
    }

    #[test(tokio::test(start_paused = true))]
    async fn auto_log_trim() -> anyhow::Result<()> {
        let mut builder = TestCoreEnvBuilder::new_with_mock_network();

        let metadata = builder.metadata.clone();
        let mut admin_options = AdminOptions::default();
        admin_options.log_trim_threshold = 5;
        let interval_duration = Duration::from_secs(10);
        admin_options.log_trim_interval = Some(interval_duration.into());

        let svc = Service::new(
            Constant::new(admin_options),
            builder.tc.clone(),
            builder.metadata.clone(),
            builder.network_sender.clone(),
            &mut builder.router_builder,
        );

        let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());
        nodes_config.upsert_node(NodeConfig::new(
            "node".to_owned(),
            GenerationalNodeId::new(1, 1),
            AdvertisedAddress::Uds("foobar".into()),
            Role::Worker.into(),
        ));
        let persisted_lsn = Arc::new(AtomicU64::new(0));

        let get_processor_state_handler = PartitionProcessorStatusHandler {
            network_sender: builder.network_sender.clone(),
            persisted_lsn: Arc::clone(&persisted_lsn),
        };

        let node_env = builder
            .add_message_handler(get_processor_state_handler)
            .with_nodes_config(nodes_config)
            .build()
            .await;

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

        let log_id = LogId::from(0);

        node_env
            .tc
            .run_in_scope("test", None, async move {
                for i in 1..=20 {
                    let lsn = bifrost.append(log_id, Payload::default()).await?;
                    assert_eq!(Lsn::from(i), lsn);
                }

                tokio::time::sleep(interval_duration * 10).await;

                assert_eq!(Lsn::INVALID, bifrost.get_trim_point(log_id).await?);

                // report persisted lsn back to cluster controller
                persisted_lsn.store(6, Ordering::Relaxed);

                tokio::time::sleep(interval_duration * 10).await;
                // we delete 1-5, 6 is left.
                assert_eq!(bifrost.get_trim_point(log_id).await?, Lsn::from(5));

                // increase by 4 more, this should not overcome the threshold
                persisted_lsn.store(10, Ordering::Relaxed);

                tokio::time::sleep(interval_duration * 10).await;
                assert_eq!(Lsn::from(5), bifrost.get_trim_point(log_id).await?);

                // now we have reached the min threshold wrt to the last trim point
                persisted_lsn.store(11, Ordering::Relaxed);

                tokio::time::sleep(interval_duration * 10).await;
                assert_eq!(Lsn::from(10), bifrost.get_trim_point(log_id).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[test(tokio::test(start_paused = true))]
    async fn auto_log_trim_zero_threshold() -> anyhow::Result<()> {
        let mut builder = TestCoreEnvBuilder::new_with_mock_network();

        let metadata = builder.metadata.clone();
        let mut admin_options = AdminOptions::default();
        admin_options.log_trim_threshold = 0;
        let interval_duration = Duration::from_secs(10);
        admin_options.log_trim_interval = Some(interval_duration.into());

        let svc = Service::new(
            Constant::new(admin_options),
            builder.tc.clone(),
            builder.metadata.clone(),
            builder.network_sender.clone(),
            &mut builder.router_builder,
        );

        let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());
        nodes_config.upsert_node(NodeConfig::new(
            "node".to_owned(),
            GenerationalNodeId::new(1, 1),
            AdvertisedAddress::Uds("foobar".into()),
            Role::Worker.into(),
        ));
        let persisted_lsn = Arc::new(AtomicU64::new(0));

        let get_processor_state_handler = PartitionProcessorStatusHandler {
            network_sender: builder.network_sender.clone(),
            persisted_lsn: Arc::clone(&persisted_lsn),
        };

        let node_env = builder
            .add_message_handler(get_processor_state_handler)
            .with_nodes_config(nodes_config)
            .build()
            .await;

        let bifrost = node_env
            .tc
            .run_in_scope("init", None, Bifrost::init_in_memory(metadata))
            .await;

        node_env.tc.spawn(
            TaskKind::SystemService,
            "cluster-controller",
            None,
            svc.run(bifrost.clone()),
        )?;

        let log_id = LogId::from(0);

        node_env
            .tc
            .run_in_scope("test", None, async move {
                for i in 1..=20 {
                    let lsn = bifrost
                        .append(log_id, Payload::new(format!("record{}", i)))
                        .await?;
                    assert_eq!(Lsn::from(i), lsn);
                }
                tokio::time::sleep(interval_duration * 10).await;
                assert_eq!(Lsn::INVALID, bifrost.get_trim_point(log_id).await?);

                // report persisted lsn back to cluster controller
                persisted_lsn.store(3, Ordering::Relaxed);

                tokio::time::sleep(interval_duration * 10).await;
                // everything before the persisted_lsn, 3 is left.
                assert_eq!(bifrost.get_trim_point(log_id).await?, Lsn::from(2));
                // we should be able to read the last persisted lsn
                let v = bifrost.read_next_single(log_id, Lsn::from(3)).await?;
                assert_eq!(Lsn::from(3), v.offset);
                assert!(v.record.is_data());
                assert_eq!(
                    &Bytes::from_static(b"record3"),
                    v.record.try_as_data().unwrap().body()
                );

                // increase by 4 more, this should not overcome the threshold
                persisted_lsn.store(20, Ordering::Relaxed);

                tokio::time::sleep(interval_duration * 10).await;
                assert_eq!(Lsn::from(19), bifrost.get_trim_point(log_id).await?);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }
}
