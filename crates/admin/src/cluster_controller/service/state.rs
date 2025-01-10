// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use futures::future::OptionFuture;
use itertools::Itertools;
use tokio::sync::watch;
use tokio::time;
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{debug, info, warn};

use restate_bifrost::Bifrost;
use restate_core::network::TransportConnect;
use restate_core::{my_node_id, Metadata};
use restate_types::cluster::cluster_state::{AliveNode, NodeState};
use restate_types::config::{AdminOptions, Configuration};
use restate_types::identifiers::PartitionId;
use restate_types::logs::{LogId, Lsn, SequenceNumber};
use restate_types::net::metadata::MetadataKind;
use restate_types::{GenerationalNodeId, Version};

use crate::cluster_controller::cluster_state_refresher::ClusterStateWatcher;
use crate::cluster_controller::logs_controller::{
    LogsBasedPartitionProcessorPlacementHints, LogsController,
};
use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
use crate::cluster_controller::scheduler::{PartitionTableNodeSetSelectorHints, Scheduler};
use crate::cluster_controller::service::Service;

pub enum ClusterControllerState<T> {
    Follower,
    Leader(Leader<T>),
}

impl<T> ClusterControllerState<T>
where
    T: TransportConnect,
{
    pub async fn update(&mut self, service: &Service<T>) -> anyhow::Result<()> {
        let maybe_leader = {
            let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
            nodes_config
                .get_admin_nodes()
                .filter(|node| {
                    service
                        .observed_cluster_state
                        .is_node_alive(node.current_generation)
                })
                .map(|node| node.current_generation)
                .sorted()
                .next()
        };

        // A Cluster Controller is a leader if the node holds the smallest PlainNodeID
        // If no other node was found to take leadership, we assume leadership

        let is_leader = match maybe_leader {
            None => true,
            Some(leader) => leader == my_node_id(),
        };

        match (is_leader, &self) {
            (true, ClusterControllerState::Leader(_))
            | (false, ClusterControllerState::Follower) => {
                // nothing to do
            }
            (true, ClusterControllerState::Follower) => {
                info!("Cluster controller switching to leader mode");
                *self = ClusterControllerState::Leader(Leader::from_service(service).await?);
            }
            (false, ClusterControllerState::Leader(_)) => {
                info!("Cluster controller switching to follower mode");
                *self = ClusterControllerState::Follower;
            }
        };

        Ok(())
    }

    pub async fn on_leader_event(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        leader_event: LeaderEvent,
    ) -> anyhow::Result<()> {
        match self {
            ClusterControllerState::Follower => Ok(()),
            ClusterControllerState::Leader(leader) => {
                leader
                    .on_leader_event(observed_cluster_state, leader_event)
                    .await
            }
        }
    }

    /// Runs the cluster controller state related tasks. It returns [`LeaderEvent`] which need to
    /// be processed by calling [`Self::on_leader_event`].
    pub async fn run(&mut self) -> anyhow::Result<LeaderEvent> {
        match self {
            Self::Follower => futures::future::pending::<anyhow::Result<_>>().await,
            Self::Leader(leader) => leader.run().await,
        }
    }

    pub async fn on_observed_cluster_state(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
    ) -> anyhow::Result<()> {
        match self {
            Self::Follower => Ok(()),
            Self::Leader(leader) => {
                leader
                    .on_observed_cluster_state(observed_cluster_state)
                    .await
            }
        }
    }

    pub fn reconfigure(&mut self, configuration: &Configuration) {
        match self {
            Self::Follower => {}
            Self::Leader(leader) => leader.reconfigure(configuration),
        }
    }
}

/// Events that are emitted by a leading cluster controller that need to be processed explicitly
/// because their operations are not cancellation safe.
#[derive(Debug)]
pub enum LeaderEvent {
    TrimLogs,
    LogsUpdate,
    PartitionTableUpdate,
}

pub struct Leader<T> {
    bifrost: Bifrost,
    logs_watcher: watch::Receiver<Version>,
    partition_table_watcher: watch::Receiver<Version>,
    find_logs_tail_interval: Interval,
    log_trim_interval: Option<Interval>,
    logs_controller: LogsController,
    scheduler: Scheduler<T>,
    cluster_state_watcher: ClusterStateWatcher,
    log_trim_threshold: Lsn,
}

impl<T> Leader<T>
where
    T: TransportConnect,
{
    async fn from_service(service: &Service<T>) -> anyhow::Result<Leader<T>> {
        let configuration = service.configuration.pinned();

        let scheduler = Scheduler::new(service.metadata_writer.clone(), service.networking.clone());

        let logs_controller =
            LogsController::new(service.bifrost.clone(), service.metadata_writer.clone())?;

        let (log_trim_interval, log_trim_threshold) =
            create_log_trim_interval(&configuration.admin);

        let mut find_logs_tail_interval =
            time::interval(configuration.admin.log_tail_update_interval.into());
        find_logs_tail_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let metadata = Metadata::current();
        let mut leader = Self {
            bifrost: service.bifrost.clone(),
            logs_watcher: metadata.watch(MetadataKind::Logs),
            partition_table_watcher: metadata.watch(MetadataKind::PartitionTable),
            cluster_state_watcher: service.cluster_state_refresher.cluster_state_watcher(),
            find_logs_tail_interval,
            log_trim_interval,
            log_trim_threshold,
            logs_controller,
            scheduler,
        };

        leader.logs_watcher.mark_changed();
        leader.partition_table_watcher.mark_changed();

        Ok(leader)
    }

    async fn on_observed_cluster_state(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
    ) -> anyhow::Result<()> {
        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        self.logs_controller.on_observed_cluster_state_update(
            &nodes_config,
            observed_cluster_state,
            Metadata::with_current(|m| {
                PartitionTableNodeSetSelectorHints::from(m.partition_table_ref())
            }),
        )?;

        self.scheduler
            .on_observed_cluster_state(
                observed_cluster_state,
                &nodes_config,
                LogsBasedPartitionProcessorPlacementHints::from(&self.logs_controller),
            )
            .await?;

        Ok(())
    }

    fn reconfigure(&mut self, configuration: &Configuration) {
        (self.log_trim_interval, self.log_trim_threshold) =
            create_log_trim_interval(&configuration.admin);
    }

    async fn run(&mut self) -> anyhow::Result<LeaderEvent> {
        loop {
            tokio::select! {
                _ = self.find_logs_tail_interval.tick() => {
                    self.logs_controller.find_logs_tail();
                }
                _ = OptionFuture::from(self.log_trim_interval.as_mut().map(|interval| interval.tick())) => {
                    return Ok(LeaderEvent::TrimLogs);
                }
                result = self.logs_controller.run_async_operations() => {
                    result?;
                }
                Ok(_) = self.logs_watcher.changed() => {
                    return Ok(LeaderEvent::LogsUpdate);
                }
                Ok(_) = self.partition_table_watcher.changed() => {
                    return Ok(LeaderEvent::PartitionTableUpdate);
                }
            }
        }
    }

    pub async fn on_leader_event(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        leader_event: LeaderEvent,
    ) -> anyhow::Result<()> {
        match leader_event {
            LeaderEvent::TrimLogs => {
                self.trim_logs().await;
            }
            LeaderEvent::LogsUpdate => {
                self.on_logs_update(observed_cluster_state).await?;
            }
            LeaderEvent::PartitionTableUpdate => {
                self.on_partition_table_update(observed_cluster_state)
                    .await?;
            }
        }

        Ok(())
    }

    async fn on_logs_update(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
    ) -> anyhow::Result<()> {
        self.logs_controller
            .on_logs_update(Metadata::with_current(|m| m.logs_ref()))?;

        self.scheduler
            .on_observed_cluster_state(
                observed_cluster_state,
                &Metadata::with_current(|m| m.nodes_config_ref()),
                LogsBasedPartitionProcessorPlacementHints::from(&self.logs_controller),
            )
            .await?;
        Ok(())
    }

    async fn on_partition_table_update(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
    ) -> anyhow::Result<()> {
        let partition_table = Metadata::with_current(|m| m.partition_table_ref());

        self.logs_controller
            .on_partition_table_update(&partition_table);

        self.scheduler
            .on_observed_cluster_state(
                observed_cluster_state,
                &Metadata::with_current(|m| m.nodes_config_ref()),
                LogsBasedPartitionProcessorPlacementHints::from(&self.logs_controller),
            )
            .await?;

        Ok(())
    }

    async fn trim_logs(&self) {
        let result = self.trim_logs_inner().await;

        if let Err(err) = result {
            warn!("Could not trim the logs. This can lead to increased disk usage: {err}");
        }
    }

    async fn trim_logs_inner(&self) -> Result<(), restate_bifrost::Error> {
        let cluster_state = self.cluster_state_watcher.current();

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
                NodeState::Dead(_) | NodeState::Suspect(_) => {
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
                let current_trim_point = self.bifrost.get_trim_point(log_id).await?;

                if min_persisted_lsn >= current_trim_point + self.log_trim_threshold {
                    debug!(
                    "Automatic trim log '{log_id}' for all records before='{min_persisted_lsn}'"
                );
                    self.bifrost.admin().trim(log_id, min_persisted_lsn).await?
                }
            } else {
                warn!("Stop automatically trimming log '{log_id}' because not all nodes are running a partition processor applying this log.");
            }
        }

        Ok(())
    }
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
