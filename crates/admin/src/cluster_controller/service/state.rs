// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Add, Deref};
use std::sync::Arc;

use futures::future::OptionFuture;
use itertools::Itertools;
use tokio::sync::watch;
use tokio::time;
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{debug, info, instrument, trace, warn};

use restate_bifrost::Bifrost;
use restate_core::network::TransportConnect;
use restate_core::{my_node_id, Metadata};
use restate_types::cluster::cluster_state::{
    AliveNode, ClusterState, NodeState, PartitionProcessorStatus,
};
use restate_types::config::{AdminOptions, Configuration};
use restate_types::identifiers::PartitionId;
use restate_types::logs::{LogId, Lsn, SequenceNumber};
use restate_types::net::metadata::MetadataKind;
use restate_types::retries::with_jitter;
use restate_types::{GenerationalNodeId, PlainNodeId, Version};

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
    pub fn update(&mut self, service: &Service<T>) -> anyhow::Result<()> {
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
                *self = ClusterControllerState::Leader(Leader::from_service(service)?);
            }
            (false, ClusterControllerState::Leader(_)) => {
                info!(
                    "Cluster controller switching to follower mode, I think the leader is {}",
                    maybe_leader.expect("a leader must be identified"),
                );
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
    logs_controller: LogsController,
    scheduler: Scheduler<T>,
    cluster_state_watcher: ClusterStateWatcher,
    log_trim_check_interval: Option<Interval>,
    snapshots_repository_configured: bool,
}

impl<T> Leader<T>
where
    T: TransportConnect,
{
    fn from_service(service: &Service<T>) -> anyhow::Result<Leader<T>> {
        let configuration = service.configuration.pinned();

        let scheduler = Scheduler::new(service.metadata_writer.clone(), service.networking.clone());

        let logs_controller =
            LogsController::new(service.bifrost.clone(), service.metadata_writer.clone())?;

        let log_trim_check_interval = create_log_trim_check_interval(&configuration.admin);

        let mut find_logs_tail_interval =
            time::interval(configuration.admin.log_tail_update_interval.into());
        find_logs_tail_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let metadata = Metadata::current();
        let mut leader = Self {
            bifrost: service.bifrost.clone(),
            logs_watcher: metadata.watch(MetadataKind::Logs),
            partition_table_watcher: metadata.watch(MetadataKind::PartitionTable),
            find_logs_tail_interval,
            logs_controller,
            scheduler,
            cluster_state_watcher: service.cluster_state_refresher.cluster_state_watcher(),
            log_trim_check_interval,
            snapshots_repository_configured: configuration.worker.snapshots.destination.is_some(),
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
                PartitionTableNodeSetSelectorHints::from(m.partition_table_snapshot())
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
        self.log_trim_check_interval = create_log_trim_check_interval(&configuration.admin);
    }

    async fn run(&mut self) -> anyhow::Result<LeaderEvent> {
        loop {
            tokio::select! {
                _ = self.find_logs_tail_interval.tick() => {
                    self.logs_controller.find_logs_tail();
                }
                Some(_) = OptionFuture::from(self.log_trim_check_interval.as_mut().map(|interval| interval.tick())) => {
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

    #[instrument(
        level = "debug",
        skip(self),
        fields(
            partition_table_version = %self.partition_table_watcher.borrow().deref(),
            logs_metadata_version = tracing::field::Empty,
        ),
    )]
    async fn trim_logs(&mut self) {
        let cluster_state = self.cluster_state_watcher.current();
        if tracing::level_enabled!(tracing::Level::DEBUG) {
            tracing::Span::current().record(
                "logs_metadata_version",
                tracing::field::display(cluster_state.logs_metadata_version),
            );
        }

        match TrimMode::from(self.snapshots_repository_configured, &cluster_state)
            .calculate_safe_trim_points()
        {
            Ok(trim_points) => {
                trace!(?trim_points, "Calculated safe log trim points");

                for (log_id, (trim_point, partition_id)) in trim_points {
                    let result = self.bifrost.admin().trim(log_id, trim_point).await;
                    if let Err(err) = result {
                        warn!(
                            %partition_id,
                            "Failed to trim log {log_id}. This can lead to increased disk usage: {err}"
                        );
                    }
                }
            }

            Err(TrimPointsUnavailable::UnknownPartitionStatus(blocking_nodes)) => {
                warn!(
                    ?blocking_nodes,
                    "Log trimming is suspended until we can determine the processor state on all known cluster nodes. \
                    Remove decommissioned nodes from the cluster and/or enable partition snapshotting to unblock log trimming"
                );
            }
        }
    }
}

fn create_log_trim_check_interval(options: &AdminOptions) -> Option<Interval> {
    options
        .log_trim_threshold
        .inspect(|_| info!("The log trim threshold setting is deprecated and will be ignored"));

    options.log_trim_check_interval().map(|interval| {
        // delay the initial trim check, and introduces small amount of jitter (+/-10%) to avoid synchronization
        // among partition leaders in case of coordinated cluster restarts
        let effective_interval = with_jitter(interval, 0.1);
        let start_at = time::Instant::now().add(effective_interval);

        let mut interval = time::interval_at(start_at, effective_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval
    })
}

enum TrimMode {
    PersistedLsn {
        partition_status:
            BTreeMap<PartitionId, BTreeMap<GenerationalNodeId, PartitionProcessorStatus>>,
        blocking_nodes: BTreeSet<PlainNodeId>,
    },
    ArchivedLsn {
        partition_status:
            BTreeMap<PartitionId, BTreeMap<GenerationalNodeId, PartitionProcessorStatus>>,
    },
}

#[derive(Debug, PartialEq)]
enum TrimPointsUnavailable {
    /// Trim points can not be determined because some (dead or otherwise
    /// unreachable) nodes have not reported the state of partitions.
    UnknownPartitionStatus(BTreeSet<PlainNodeId>),
}

impl TrimMode {
    fn from(snapshots_repository_configured: bool, cluster_state: &Arc<ClusterState>) -> TrimMode {
        let mut partition_status: BTreeMap<
            PartitionId,
            BTreeMap<GenerationalNodeId, PartitionProcessorStatus>,
        > = BTreeMap::new();

        for node in cluster_state.alive_nodes() {
            let AliveNode {
                generational_node_id,
                partitions,
                ..
            } = node;
            for (partition_id, partition_processor_status) in partitions.iter() {
                partition_status
                    .entry(*partition_id)
                    .or_default()
                    .insert(*generational_node_id, partition_processor_status.clone());
            }
        }

        if snapshots_repository_configured
            || Self::any_processors_report_archived_lsn(cluster_state)
        {
            TrimMode::ArchivedLsn { partition_status }
        } else {
            TrimMode::PersistedLsn {
                partition_status,
                blocking_nodes: cluster_state.dead_or_suspect_nodes().copied().collect(),
            }
        }
    }

    /// Compute the safe trim points for each log, assuming that partitions are mapped to logs 1:1.
    /// For a given cluster state, determines the set of trim points for all partitions' logs.
    ///
    /// The presence of any dead or suspect nodes in the cluster state struct will prevent logs from
    /// being trimmed if archived LSN is *not* reported for all known partitions. Conversely, as long
    /// as archived LSNs are reported for all partitions, trimming can continue even in the presence of
    /// some dead nodes. This is because we assume that if those nodes are only temporarily down, they
    /// can fast-forward state from the snapshot repository when they return into service.
    fn calculate_safe_trim_points(
        &self,
    ) -> Result<BTreeMap<LogId, (Lsn, PartitionId)>, TrimPointsUnavailable> {
        if let Some(blocking_nodes) = self.get_trim_blockers() {
            return Err(TrimPointsUnavailable::UnknownPartitionStatus(
                blocking_nodes.clone(),
            ));
        }

        let mut safe_trim_points = BTreeMap::new();
        match self {
            TrimMode::ArchivedLsn { partition_status } => {
                for (partition_id, processor_status) in partition_status.iter() {
                    let log_id = LogId::from(*partition_id);

                    // We allow trimming of archived partitions even in the presence of dead/suspect nodes; such
                    // nodes will be forced to fast-forward over any potential trim gaps when they return.
                    // However, if we have alive nodes that report applied LSNs smaller than the highest
                    // archived LSN, we allow them to catch up from the log before trimming. There is a risk
                    // that a slow applier may hold off trimming indefinitely.
                    let min_applied_lsn = processor_status
                        .values()
                        .filter_map(|s| s.last_applied_log_lsn)
                        .min()
                        .unwrap_or(Lsn::INVALID);

                    // We trust that if a single node from the cluster reports a partition's archived LSN,
                    // that this snapshot will be available to all other nodes that may need it. Thus, it is
                    // safe to take the max reported archived LSN across the board as the safe trim level.
                    let archived_lsn = processor_status
                        .values()
                        .filter_map(|s| s.last_archived_log_lsn)
                        .max()
                        .unwrap_or(Lsn::INVALID);

                    if archived_lsn > min_applied_lsn {
                        debug!(
                            ?partition_id,
                            ?min_applied_lsn,
                            "Some alive nodes have not applied the log up to the archived LSN"
                        );
                    }
                    safe_trim_points.insert(log_id, (archived_lsn, *partition_id));
                }
            }
            TrimMode::PersistedLsn {
                partition_status, ..
            } => {
                // If no partitions are reporting archived LSN, we fall back to using the
                // min(persisted LSN) across the board as the safe trim point. Note that at this
                // point we know that there are no known dead nodes, so it's safe to take the min of
                // persisted LSNs reported by all the partition processors as the safe trim point.
                for (partition_id, processor_status) in partition_status.iter() {
                    let log_id = LogId::from(*partition_id);
                    let min_persisted_lsn = processor_status
                        .values()
                        .map(|s| s.last_persisted_log_lsn.unwrap_or(Lsn::INVALID))
                        .min()
                        .unwrap_or(Lsn::INVALID);

                    trace!(
                        ?partition_id,
                        "Safe trim point for log {}: {:?}",
                        log_id,
                        min_persisted_lsn
                    );
                    safe_trim_points.insert(log_id, (min_persisted_lsn, *partition_id));
                }
            }
        }

        Ok(safe_trim_points)
    }

    // If any partitions are reporting an archived LSN, they must have snapshotting enabled.
    // We use this signal as a heuristic when selecting archived LSN-based trimming in case of
    // heterogeneous cluster node configuration.
    fn any_processors_report_archived_lsn(cluster_state: &ClusterState) -> bool {
        cluster_state
            .nodes
            .values()
            .any(|node_state| match node_state {
                NodeState::Alive(AliveNode { partitions, .. }) => {
                    partitions.values().any(|partition| {
                        // doesn't matter if it's INVALID!
                        partition.last_archived_log_lsn.is_some()
                    })
                }
                NodeState::Dead(_) | NodeState::Suspect(_) => false,
            })
    }

    fn get_trim_blockers(&self) -> Option<&BTreeSet<PlainNodeId>> {
        match self {
            TrimMode::PersistedLsn {
                blocking_nodes: dead_nodes,
                ..
            } => {
                if dead_nodes.is_empty() {
                    None
                } else {
                    Some(dead_nodes)
                }
            }
            TrimMode::ArchivedLsn { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::cluster_controller::service::state::{TrimMode, TrimPointsUnavailable};
    use restate_types::cluster::cluster_state::{
        AliveNode, ClusterState, DeadNode, NodeState, PartitionProcessorStatus, RunMode,
        SuspectNode,
    };
    use restate_types::identifiers::PartitionId;
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{GenerationalNodeId, PlainNodeId, Version};
    use RunMode::{Follower, Leader};

    #[test]
    fn safe_trim_points_no_snapshots() {
        let p1 = PartitionId::from(0);
        let p2 = PartitionId::from(1);
        let p3 = PartitionId::from(2);

        let n1 = GenerationalNodeId::new(1, 0);
        let n1_partitions: BTreeMap<PartitionId, PartitionProcessorStatus> = [
            (
                p1,
                ProcessorStatus {
                    mode: Leader,
                    applied: Some(Lsn::new(10)),
                    persisted: None,
                    archived: None,
                }
                .into(),
            ),
            (
                p2,
                ProcessorStatus {
                    mode: Follower,
                    applied: Some(Lsn::new(10)),
                    persisted: Some(Lsn::new(5)),
                    archived: None,
                }
                .into(),
            ),
            (
                p3,
                ProcessorStatus {
                    mode: Leader,
                    applied: Some(Lsn::new(10)),
                    persisted: Some(Lsn::new(5)),
                    archived: None,
                }
                .into(),
            ),
        ]
        .into_iter()
        .collect();

        let n2 = GenerationalNodeId::new(2, 0);
        let n2_partitions: BTreeMap<PartitionId, PartitionProcessorStatus> = [
            (
                p1,
                ProcessorStatus {
                    mode: Follower,
                    applied: Some(Lsn::new(10)),
                    persisted: None,
                    archived: None,
                }
                .into(),
            ),
            (
                p2,
                ProcessorStatus {
                    mode: Follower,
                    applied: Some(Lsn::new(10)),
                    persisted: None,
                    archived: None,
                }
                .into(),
            ),
            (
                p3,
                ProcessorStatus {
                    mode: Follower,
                    applied: Some(Lsn::new(10)),
                    persisted: Some(Lsn::new(5)),
                    archived: None,
                }
                .into(),
            ),
        ]
        .into_iter()
        .collect();

        let cluster_state = Arc::new(ClusterState {
            last_refreshed: None,
            nodes_config_version: Version::MIN,
            partition_table_version: Version::MIN,
            logs_metadata_version: Version::MIN,
            nodes: [
                (n1.as_plain(), alive_node(n1, n1_partitions.clone())),
                (n2.as_plain(), alive_node(n2, n2_partitions.clone())),
            ]
            .into(),
        });

        let trim_mode = TrimMode::from(false, &cluster_state);
        let trim_points = trim_mode.calculate_safe_trim_points();

        assert!(matches!(trim_mode, TrimMode::PersistedLsn { .. }));
        assert_eq!(
            trim_points,
            Ok(BTreeMap::from([
                (LogId::from(p1), (Lsn::INVALID, p1)),
                (LogId::from(p2), (Lsn::INVALID, p2)),
                (LogId::from(p3), (Lsn::new(5), p3)),
            ])),
            "Use min persisted LSN across the cluster as the safe point when not archiving"
        );

        let cluster_state = Arc::new(ClusterState {
            last_refreshed: None,
            nodes_config_version: Version::MIN,
            partition_table_version: Version::MIN,
            logs_metadata_version: Version::MIN,
            nodes: [
                (n1.as_plain(), alive_node(n1, n1_partitions)),
                (n2.as_plain(), alive_node(n2, n2_partitions)),
                (PlainNodeId::new(3), dead_node()),
            ]
            .into(),
        });

        let trim_mode = TrimMode::from(false, &cluster_state);
        let trim_points = trim_mode.calculate_safe_trim_points();

        assert!(matches!(trim_mode, TrimMode::PersistedLsn { .. }));
        assert_eq!(
            trim_points,
            Err(TrimPointsUnavailable::UnknownPartitionStatus(
                [PlainNodeId::new(3)].iter().copied().collect()
            )),
            "Any dead nodes in cluster block trimming when using persisted LSN mode"
        );

        let mut nodes = cluster_state.nodes.clone();
        nodes.insert(PlainNodeId::new(3), dead_node());
        let cluster_state = Arc::new(ClusterState {
            nodes,
            ..*cluster_state
        });

        let trim_mode = TrimMode::from(false, &cluster_state);
        let trim_points = trim_mode.calculate_safe_trim_points();

        assert!(matches!(trim_mode, TrimMode::PersistedLsn { .. }));
        assert_eq!(
            trim_points,
            Err(TrimPointsUnavailable::UnknownPartitionStatus(
                [PlainNodeId::new(3)].iter().copied().collect()
            )),
            "Any dead nodes in cluster block trimming when using persisted LSN mode"
        );

        let mut nodes = cluster_state.nodes.clone();
        nodes.insert(
            PlainNodeId::new(3),
            suspect_node(GenerationalNodeId::new(3, 0)),
        );
        let cluster_state = Arc::new(ClusterState {
            nodes,
            ..*cluster_state
        });

        let trim_mode = TrimMode::from(false, &cluster_state);
        let trim_points = trim_mode.calculate_safe_trim_points();

        assert!(matches!(trim_mode, TrimMode::PersistedLsn { .. }));
        assert_eq!(
            trim_points,
            Err(TrimPointsUnavailable::UnknownPartitionStatus(
                [PlainNodeId::new(3)].iter().copied().collect()
            )),
            "Any dead nodes in cluster block trimming when using persisted LSN mode"
        );
    }

    #[test]
    fn safe_trim_points_with_snapshots() {
        let p1 = PartitionId::from(0);
        let p2 = PartitionId::from(1);
        let p3 = PartitionId::from(2);
        let p4 = PartitionId::from(3);

        let n1 = GenerationalNodeId::new(1, 0);
        let n1_partitions = [
            (
                p1,
                ProcessorStatus {
                    mode: Leader,
                    applied: Some(Lsn::new(10)),
                    persisted: None,
                    archived: None,
                }
                .into(),
            ),
            (
                p2,
                ProcessorStatus {
                    mode: Follower,
                    applied: Some(Lsn::new(18)),
                    persisted: None,
                    archived: None,
                }
                .into(),
            ),
            (
                p3,
                ProcessorStatus {
                    mode: Leader,
                    applied: Some(Lsn::new(30)),
                    persisted: None,
                    archived: Some(Lsn::new(30)),
                }
                .into(),
            ),
            (
                p4,
                ProcessorStatus {
                    mode: Leader,
                    applied: Some(Lsn::new(40)),
                    persisted: None,
                    archived: Some(Lsn::new(40)),
                }
                .into(),
            ),
        ]
        .into_iter()
        .collect();

        let n2 = GenerationalNodeId::new(2, 0);
        let n2_partitions = [
            (
                p1,
                ProcessorStatus {
                    mode: Follower,
                    applied: Some(Lsn::new(10)),
                    persisted: None,
                    archived: None,
                }
                .into(),
            ),
            (
                p2,
                ProcessorStatus {
                    mode: Leader,
                    applied: Some(Lsn::new(20)),
                    persisted: None,
                    archived: Some(Lsn::new(10)),
                }
                .into(),
            ),
            (
                p3,
                ProcessorStatus {
                    mode: Follower,
                    applied: Some(Lsn::new(10)),
                    persisted: None,
                    archived: None,
                }
                .into(),
            ),
            (
                p4,
                ProcessorStatus {
                    mode: Follower,
                    applied: Some(Lsn::new(35)), // behind the archived lsn reported by n1
                    persisted: None,
                    archived: None,
                }
                .into(),
            ),
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

        let trim_mode = TrimMode::from(false, &cluster_state);
        let trim_points = trim_mode.calculate_safe_trim_points();

        assert_eq!(
            trim_points,
            Ok(BTreeMap::from([
                (LogId::from(p1), (Lsn::INVALID, p1)),
                (LogId::from(p2), (Lsn::new(10), p2)),
                (LogId::from(p3), (Lsn::new(30), p3)),
                (LogId::from(p4), (Lsn::new(40), p4)),
            ]))
        );

        let mut nodes = cluster_state.nodes.clone();
        nodes.insert(
            PlainNodeId::new(3),
            suspect_node(GenerationalNodeId::new(3, 0)),
        );
        nodes.insert(PlainNodeId::new(4), dead_node());

        let cluster_state = Arc::new(ClusterState {
            nodes,
            ..*cluster_state
        });

        let trim_mode = TrimMode::from(false, &cluster_state);
        let trim_points = trim_mode.calculate_safe_trim_points();

        assert_eq!(
            trim_points,
            Ok(BTreeMap::from([
                (LogId::from(p1), (Lsn::INVALID, p1)),
                (LogId::from(p2), (Lsn::new(10), p2)),
                (LogId::from(p3), (Lsn::new(30), p3)),
                (LogId::from(p4), (Lsn::new(40), p4)),
            ])),
            "presence of dead or suspect nodes does not block trimming when archived LSN is reported"
        );
    }

    struct ProcessorStatus {
        mode: RunMode,
        applied: Option<Lsn>,
        persisted: Option<Lsn>,
        archived: Option<Lsn>,
    }

    impl From<ProcessorStatus> for PartitionProcessorStatus {
        fn from(val: ProcessorStatus) -> Self {
            PartitionProcessorStatus {
                planned_mode: val.mode,
                effective_mode: val.mode,
                last_applied_log_lsn: val.applied,
                last_persisted_log_lsn: val.persisted,
                last_archived_log_lsn: val.archived,
                ..PartitionProcessorStatus::default()
            }
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
            uptime: Duration::default(),
        })
    }

    fn suspect_node(generational_node_id: GenerationalNodeId) -> NodeState {
        NodeState::Suspect(SuspectNode {
            generational_node_id,
            last_attempt: MillisSinceEpoch::now(),
        })
    }

    fn dead_node() -> NodeState {
        NodeState::Dead(DeadNode {
            last_seen_alive: None,
        })
    }
}
