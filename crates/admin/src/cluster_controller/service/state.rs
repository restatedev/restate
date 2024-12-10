// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use futures::future::OptionFuture;
use itertools::Itertools;
use tokio::sync::watch;
use tokio::time;
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{info, warn};

use restate_bifrost::{Bifrost, BifrostAdmin};
use restate_core::metadata_store::MetadataStoreClient;
use restate_core::network::TransportConnect;
use restate_core::{my_node_id, Metadata, MetadataWriter};
use restate_types::cluster::cluster_state::{
    AliveNode, ClusterState, NodeState, PartitionProcessorStatus,
};
use restate_types::config::{AdminOptions, Configuration};
use restate_types::identifiers::PartitionId;
use restate_types::live::Live;
use restate_types::logs::metadata::Logs;
use restate_types::logs::{LogId, Lsn, SequenceNumber};
use restate_types::net::metadata::MetadataKind;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::{GenerationalNodeId, Version};

use crate::cluster_controller::cluster_state_refresher::ClusterStateWatcher;
use crate::cluster_controller::logs_controller::{
    LogsBasedPartitionProcessorPlacementHints, LogsController,
};
use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
use crate::cluster_controller::scheduler::{Scheduler, SchedulingPlanNodeSetSelectorHints};
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

    pub async fn on_leader_event(&mut self, leader_event: LeaderEvent) -> anyhow::Result<()> {
        match self {
            ClusterControllerState::Follower => Ok(()),
            ClusterControllerState::Leader(leader) => leader.on_leader_event(leader_event).await,
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
    metadata: Metadata,
    bifrost: Bifrost,
    metadata_store_client: MetadataStoreClient,
    metadata_writer: MetadataWriter,
    logs_watcher: watch::Receiver<Version>,
    partition_table_watcher: watch::Receiver<Version>,
    partition_table: Live<PartitionTable>,
    nodes_config: Live<NodesConfiguration>,
    find_logs_tail_interval: Interval,
    log_trim_check_interval: Option<Interval>,
    logs_controller: LogsController,
    scheduler: Scheduler<T>,
    cluster_state_watcher: ClusterStateWatcher,
    logs: Live<Logs>,
    log_trim_threshold: Lsn,
}

impl<T> Leader<T>
where
    T: TransportConnect,
{
    async fn from_service(service: &Service<T>) -> anyhow::Result<Leader<T>> {
        let configuration = service.configuration.pinned();

        let metadata = Metadata::current();

        let scheduler = Scheduler::init(
            &configuration,
            service.metadata_store_client.clone(),
            service.networking.clone(),
        )
        .await?;

        let logs_controller = LogsController::init(
            &configuration,
            metadata.clone(),
            service.bifrost.clone(),
            service.metadata_store_client.clone(),
            service.metadata_writer.clone(),
        )
        .await?;

        let (log_trim_interval, log_trim_threshold) =
            create_log_trim_interval(&configuration.admin);

        let mut find_logs_tail_interval =
            time::interval(configuration.admin.log_tail_update_interval.into());
        find_logs_tail_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut leader = Self {
            metadata: metadata.clone(),
            bifrost: service.bifrost.clone(),
            metadata_store_client: service.metadata_store_client.clone(),
            metadata_writer: service.metadata_writer.clone(),
            logs_watcher: metadata.watch(MetadataKind::Logs),
            nodes_config: metadata.updateable_nodes_config(),
            partition_table_watcher: metadata.watch(MetadataKind::PartitionTable),
            cluster_state_watcher: service.cluster_state_refresher.cluster_state_watcher(),
            partition_table: metadata.updateable_partition_table(),
            logs: metadata.updateable_logs_metadata(),
            find_logs_tail_interval,
            log_trim_check_interval: log_trim_interval,
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
        let nodes_config = &self.nodes_config.live_load();
        self.logs_controller.on_observed_cluster_state_update(
            nodes_config,
            observed_cluster_state,
            SchedulingPlanNodeSetSelectorHints::from(&self.scheduler),
        )?;
        self.scheduler
            .on_observed_cluster_state(
                observed_cluster_state,
                nodes_config,
                LogsBasedPartitionProcessorPlacementHints::from(&self.logs_controller),
            )
            .await?;

        Ok(())
    }

    fn reconfigure(&mut self, configuration: &Configuration) {
        (self.log_trim_check_interval, self.log_trim_threshold) =
            create_log_trim_interval(&configuration.admin);
    }

    async fn run(&mut self) -> anyhow::Result<LeaderEvent> {
        loop {
            tokio::select! {
                _ = self.find_logs_tail_interval.tick() => {
                    self.logs_controller.find_logs_tail();
                }
                _ = OptionFuture::from(self.log_trim_check_interval.as_mut().map(|interval| interval.tick())) => {
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

    pub async fn on_leader_event(&mut self, leader_event: LeaderEvent) -> anyhow::Result<()> {
        match leader_event {
            LeaderEvent::TrimLogs => {
                self.trim_logs().await;
            }
            LeaderEvent::LogsUpdate => {
                self.on_logs_update().await?;
            }
            LeaderEvent::PartitionTableUpdate => {
                self.on_partition_table_update().await?;
            }
        }

        Ok(())
    }

    async fn on_logs_update(&mut self) -> anyhow::Result<()> {
        self.logs_controller
            .on_logs_update(self.metadata.logs_ref())?;
        // tell the scheduler about potentially newly provisioned logs
        self.scheduler
            .on_logs_update(self.logs.live_load(), self.partition_table.live_load())
            .await?;

        Ok(())
    }

    async fn on_partition_table_update(&mut self) -> anyhow::Result<()> {
        let partition_table = self.partition_table.live_load();
        let logs = self.logs.live_load();

        self.logs_controller
            .on_partition_table_update(partition_table);
        self.scheduler.on_logs_update(logs, partition_table).await?;

        Ok(())
    }

    async fn trim_logs(&self) {
        let result = self.trim_logs_inner().await;

        if let Err(err) = result {
            warn!("Could not trim the logs. This can lead to increased disk usage on log servers: {err}");
        }
    }

    async fn trim_logs_inner(&self) -> Result<(), restate_bifrost::Error> {
        let bifrost_admin = BifrostAdmin::new(
            &self.bifrost,
            &self.metadata_writer,
            &self.metadata_store_client,
        );

        let cluster_state = self.cluster_state_watcher.current();

        let current_trim_points = get_trim_points(&cluster_state, &bifrost_admin).await?;
        let new_trim_points = compute_new_safe_trim_points(cluster_state, &current_trim_points);

        for (log_id, (trim_point, partition_id)) in new_trim_points {
            info!(
                %partition_id,
                "Automatic trim log '{log_id}' for all records before='{trim_point}'"
            );
            bifrost_admin.trim(log_id, trim_point).await?
        }

        Ok(())
    }
}

async fn get_trim_points(
    cluster_state: &Arc<ClusterState>,
    bifrost_admin: &BifrostAdmin<'_>,
) -> Result<HashMap<LogId, Lsn>, restate_bifrost::Error> {
    let partition_ids: Vec<PartitionId> = cluster_state
        .nodes
        .values()
        .filter_map(|node_state| match node_state {
            NodeState::Alive(alive_node) => Some(alive_node),
            _ => None,
        })
        .flat_map(|node| node.partitions.keys())
        .cloned()
        .collect();

    let mut trim_points = HashMap::new();
    for partition in partition_ids {
        let log_id = LogId::from(partition);
        let current_trim_point = bifrost_admin.get_trim_point(log_id).await?;
        trim_points.insert(log_id, current_trim_point);
    }

    Ok(trim_points)
}

fn compute_new_safe_trim_points(
    cluster_state: Arc<ClusterState>,
    trim_points: &HashMap<LogId, Lsn>,
) -> BTreeMap<LogId, (Lsn, PartitionId)> {
    let mut partition_statuses: BTreeMap<
        PartitionId,
        BTreeMap<GenerationalNodeId, &PartitionProcessorStatus>,
    > = BTreeMap::new();
    let mut safe_trim_points = BTreeMap::new();

    for node_state in cluster_state.nodes.values() {
        match node_state {
            NodeState::Alive(AliveNode {
                generational_node_id,
                partitions,
                ..
            }) => {
                for (partition_id, partition_processor_status) in partitions.iter() {
                    partition_statuses
                        .entry(*partition_id)
                        .or_default()
                        .insert(*generational_node_id, partition_processor_status);
                }
            }
            NodeState::Dead(_) | NodeState::Suspect(_) => {
                // todo(pavel): until we implement trim-gap support (https://github.com/restatedev/restate/issues/2247),
                //  pause trimming unless we know the status of all nodes dead nodes might come
                //  back and will be stuck if their applied lsn < trim point.
                warn!("Automatic log trimming paused until all nodes are reporting applied LSN status. This can lead to increased disk usage on log servers.");
                return safe_trim_points;
            }
        }
    }

    for (partition_id, processor_status) in partition_statuses.into_iter() {
        let log_id = LogId::from(partition_id);

        // We trust that if a single node from the cluster reports a partition's archived LSN,
        // that this is accessible to all other nodes that may need it.
        // todo(pavel): read snapshot archived LSN from repository, so we can switch to min(archived_lsn)
        let archived_lsn = processor_status
            .values()
            .map(|s| s.last_archived_log_lsn.unwrap_or(Lsn::INVALID))
            .max()
            .unwrap_or(Lsn::INVALID);

        let min_applied_lsn = processor_status
            .values()
            .map(|s| s.last_applied_log_lsn.unwrap_or(Lsn::INVALID))
            .min()
            .unwrap_or(Lsn::INVALID);

        let current_trim_point = trim_points.get(&log_id).unwrap_or(&Lsn::INVALID).clone();

        if archived_lsn == Lsn::INVALID {
            warn!("Not trimming log '{log_id}' because no node is reporting a valid archived LSN");
        } else if min_applied_lsn < current_trim_point {
            // todo(pavel): remove this check once we implement trim-gap support (https://github.com/restatedev/restate/issues/2247)
            warn!(
                %partition_id,
                "Not trimming log '{log_id}' because some nodes have not applied the log up to the archived LSN"
            );
        } else if archived_lsn <= min_applied_lsn && archived_lsn > current_trim_point {
            safe_trim_points.insert(log_id, (archived_lsn, partition_id));
        }
    }

    safe_trim_points
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use crate::cluster_controller::service::state::compute_new_safe_trim_points;
    use restate_types::cluster::cluster_state::{
        AliveNode, ClusterState, DeadNode, NodeState, PartitionProcessorStatus, RunMode,
    };
    use restate_types::identifiers::PartitionId;
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{GenerationalNodeId, PlainNodeId, Version};
    use RunMode::{Follower, Leader};

    #[test]
    fn test_compute_new_safe_trim_points() {
        let p1 = PartitionId::from(0);
        let p2 = PartitionId::from(1);
        let p3 = PartitionId::from(2);
        let p4 = PartitionId::from(3);

        let n1 = GenerationalNodeId::new(1, 0);
        let n1_partitions = [
            (p1, processor_status(Leader, applied(10), None)),
            (p2, processor_status(Follower, applied(20), None)),
            (p3, processor_status(Leader, applied(30), archived(30))),
            (p4, processor_status(Follower, applied(40), archived(40))),
        ]
        .into_iter()
        .collect();

        let n2 = GenerationalNodeId::new(2, 0);
        let n2_partitions = [
            (p1, processor_status(Follower, applied(10), None)),
            (p2, processor_status(Leader, applied(20), archived(10))),
            (p3, processor_status(Follower, applied(10), None)),
            (p4, processor_status(Leader, applied(45), None)),
        ]
        .into_iter()
        .collect();

        let cluster_state = Arc::new(ClusterState {
            last_refreshed: None,
            nodes_config_version: Version::MIN,
            partition_table_version: Version::MIN,
            logs_metadata_version: Version::MIN,
            nodes: [
                (n1.as_plain(), alive_node(n1, n1_partitions)),
                (n2.as_plain(), alive_node(n2, n2_partitions)),
            ]
            .into(),
        });

        let mut current_trim_points = HashMap::new();
        current_trim_points.insert(LogId::from(p1), Lsn::INVALID);
        current_trim_points.insert(LogId::from(p2), Lsn::INVALID);

        let trim_points = compute_new_safe_trim_points(cluster_state.clone(), &current_trim_points);

        assert_eq!(
            trim_points,
            BTreeMap::from([
                // p1 does not report archived LSN - no trim
                (LogId::from(p2), (Lsn::new(10), p2)),
                // p3 has applied LSN = archived LSN - no trim necessary
                (LogId::from(p4), (Lsn::new(40), p4)),
            ])
        );

        let mut nodes = cluster_state.nodes.clone();
        nodes.insert(PlainNodeId::new(3), dead_node());
        let cluster_state = Arc::new(ClusterState {
            nodes,
            ..*cluster_state
        });

        let trim_points = compute_new_safe_trim_points(cluster_state.clone(), &current_trim_points);

        assert!(trim_points.is_empty());
    }

    fn applied(sn: u64) -> Option<Lsn> {
        Some(Lsn::new(sn))
    }

    fn archived(sn: u64) -> Option<Lsn> {
        Some(Lsn::new(sn))
    }

    fn processor_status(
        mode: RunMode,
        applied_lsn: Option<Lsn>,
        archived_lsn: Option<Lsn>,
    ) -> PartitionProcessorStatus {
        PartitionProcessorStatus {
            planned_mode: mode,
            effective_mode: mode,
            last_applied_log_lsn: applied_lsn,
            last_archived_log_lsn: archived_lsn,
            ..PartitionProcessorStatus::default()
        }
    }

    fn alive_node(
        generational_node_id: GenerationalNodeId,
        partitions: BTreeMap<PartitionId, PartitionProcessorStatus>,
    ) -> NodeState {
        NodeState::Alive(AliveNode {
            generational_node_id,
            last_heartbeat_at: MillisSinceEpoch::now(),
            partitions,
        })
    }

    fn dead_node() -> NodeState {
        NodeState::Dead(DeadNode {
            last_seen_alive: None,
        })
    }
}
