// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use futures::future::OptionFuture;
use itertools::Itertools;
use tokio::sync::watch;
use tokio::time;
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{debug, info, instrument, warn};

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
    logs_controller: LogsController,
    scheduler: Scheduler<T>,
    cluster_state_watcher: ClusterStateWatcher,
    log_trim_check_interval: Option<Interval>,
    log_trim_threshold: Lsn,
    snapshots_repository_configured: bool,
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

        let (log_trim_check_interval, log_trim_threshold) = log_trim_options(&configuration.admin);

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
            log_trim_threshold,
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
        (self.log_trim_check_interval, self.log_trim_threshold) =
            log_trim_options(&configuration.admin);
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
            partition_table_version = ?self.partition_table_watcher,
            logs_metadata_version = ?self.cluster_state_watcher.current().logs_metadata_version
        ),
    )]
    async fn trim_logs(&self) {
        let cluster_state = self.cluster_state_watcher.current();
        let current_trim_points = match get_current_trim_points(&cluster_state, &self.bifrost).await
        {
            Ok(trim_points) => trim_points,
            Err(err) => {
                warn!("Failed to get current logs trim points. This can lead to increased log server disk usage: {err}");
                return;
            }
        };

        let new_trim_points = TrimMode::from(self.snapshots_repository_configured, cluster_state)
            .safe_trim_points(&current_trim_points, Some(self.log_trim_threshold));
        debug!(?new_trim_points, "Safe trim points");

        for (log_id, (trim_point, partition_id)) in new_trim_points {
            info!(
                %partition_id,
                "Trimming log {log_id} records ..='{trim_point}'"
            );
            let result = self.bifrost.admin().trim(log_id, trim_point).await;

            if let Err(err) = result {
                warn!(
                    %partition_id,
                    "Failed to trim log {log_id}. This can lead to increased log server disk usage: {err}"
                );
            }
        }
    }
}

async fn get_current_trim_points(
    cluster_state: &ClusterState,
    bifrost: &Bifrost,
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
        let current_trim_point = bifrost.get_trim_point(log_id).await?;
        trim_points.insert(log_id, current_trim_point);
    }

    Ok(trim_points)
}

fn log_trim_options(options: &AdminOptions) -> (Option<Interval>, Lsn) {
    let log_trim_interval = options.log_trim_interval.map(|interval| {
        let mut interval = tokio::time::interval(interval.into());
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval
    });

    let log_trim_threshold = Lsn::new(options.log_trim_threshold);

    (log_trim_interval, log_trim_threshold)
}

enum TrimMode {
    PersistedLsn {
        cluster_state: Arc<ClusterState>,
        partition_statuses:
            BTreeMap<PartitionId, BTreeMap<GenerationalNodeId, PartitionProcessorStatus>>,
    },
    ArchivedLsn {
        // cluster_state: Arc<ClusterState>,
        // archived_lsns: BTreeMap<PartitionId, Lsn>,
        partition_statuses:
            BTreeMap<PartitionId, BTreeMap<GenerationalNodeId, PartitionProcessorStatus>>,
    },
}

impl TrimMode {
    fn from(snapshots_repository_configured: bool, cluster_state: Arc<ClusterState>) -> TrimMode {
        let mut archived_lsns: BTreeMap<PartitionId, Lsn> = BTreeMap::new();
        let mut suspect_or_dead_nodes: BTreeSet<PlainNodeId> = BTreeSet::new();

        let mut partition_statuses: BTreeMap<
            PartitionId,
            BTreeMap<GenerationalNodeId, PartitionProcessorStatus>,
        > = BTreeMap::new();

        for (node_id, node_state) in cluster_state.nodes.iter() {
            match node_state {
                NodeState::Alive(AliveNode {
                    generational_node_id,
                    partitions,
                    ..
                }) => {
                    for (partition_id, partition_processor_status) in partitions.iter() {
                        let reported_archived_lsn = &partition_processor_status
                            .last_archived_log_lsn
                            .unwrap_or(Lsn::INVALID);

                        archived_lsns
                            .entry(*partition_id)
                            .and_modify(|stored| {
                                *stored = *(stored as &Lsn).max(reported_archived_lsn)
                            })
                            .or_insert(*reported_archived_lsn);

                        partition_statuses
                            .entry(*partition_id)
                            .or_default()
                            .insert(*generational_node_id, partition_processor_status.clone());
                    }
                }
                NodeState::Dead(_) | NodeState::Suspect(_) => {
                    suspect_or_dead_nodes.insert(*node_id);
                }
            }
        }

        match snapshots_repository_configured
            || Self::any_processors_report_archived_lsn(&cluster_state)
        {
            true => TrimMode::ArchivedLsn {
                // archived_lsns,
                // cluster_state,
                partition_statuses,
            },
            false => TrimMode::PersistedLsn {
                cluster_state,
                partition_statuses,
            },
        }
    }

    /// Compute the safe trim points for each log, assuming that partitions are mapped to logs 1:1.
    /// For a given cluster state and known log trim points, determines a new set of trim points for
    /// those logs which are deemed safe to trim. Only logs that need action will contain an entry in
    /// the result.
    ///
    /// The presence of any dead or suspect nodes in the cluster state struct will prevent logs from
    /// being trimmed if archived LSN is *not* reported for all known partitions. Conversely, as long
    /// as archived LSNs are reported for all partitions, trimming can continue even in the presence of
    /// some dead nodes. This is because we assume that if those nodes are only temporarily down, they
    /// can fast-forward state from the snapshot repository when they return into service.
    fn safe_trim_points(
        &self,
        current_trim_points: &HashMap<LogId, Lsn>,
        threshold: Option<Lsn>,
    ) -> BTreeMap<LogId, (Lsn, PartitionId)> {
        let mut safe_trim_points = BTreeMap::new();

        if self.is_trimming_suspended() {
            warn!(
                "Log trimming is suspended until we can determine the processor state on all known cluster nodes. \
                This may result in increased log usage. Prune permanently decommissioned nodes and/or enable partition \
                snapshotting to unblock trimming."
            );
            return safe_trim_points;
        }

        match self {
            TrimMode::ArchivedLsn { partition_statuses } => {
                for (partition_id, processor_status) in partition_statuses.iter() {
                    let log_id = LogId::from(*partition_id);

                    // We allow trimming of archived partitions even in the presence of dead/suspect nodes; such
                    // nodes will be forced to fast-forward over any potential trim gaps when they return.
                    // However, if we have alive nodes that report applied LSNs smaller than the highest
                    // archived LSN, we allow them to catch up from the log before trimming. There is a risk
                    // that a slow applier may hold off trimming indefinitely.
                    let min_applied_lsn = processor_status
                        .values()
                        .map(|s| s.last_applied_log_lsn.unwrap_or(Lsn::INVALID))
                        .min()
                        .unwrap_or(Lsn::INVALID);

                    // We trust that if a single node from the cluster reports a partition's archived LSN,
                    // that this snapshot will be available to all other nodes that may need it. Thus, it is
                    // safe to take the max reported archived LSN across the board as the safe trim level.
                    let archived_lsn = processor_status
                        .values()
                        .map(|s| s.last_archived_log_lsn.unwrap_or(Lsn::INVALID))
                        .max()
                        .unwrap_or(Lsn::INVALID);

                    let current_trim_point =
                        *current_trim_points.get(&log_id).unwrap_or(&Lsn::INVALID);

                    if archived_lsn >= current_trim_point + threshold.unwrap_or(Lsn::from(1)) {
                        if archived_lsn <= min_applied_lsn {
                            debug!(
                                ?partition_id,
                                "Safe trim point for log {}: {:?}", log_id, archived_lsn
                            );
                            safe_trim_points.insert(log_id, (archived_lsn, *partition_id));
                        } else {
                            warn!(?partition_id, "Some alive nodes have not applied the log up to the archived LSN; not trimming")
                        }
                    }
                }
            }
            TrimMode::PersistedLsn {
                partition_statuses, ..
            } => {
                // If no partitions are reporting archived LSN, we fall back to using the
                // min(persisted LSN) across the board as the safe trim point. Note that at this
                // point we know that there are no known dead nodes, so it's safe to take the min of
                // persisted LSNs reported by all the partition processors as the safe trim point.
                for (partition_id, processor_status) in partition_statuses.iter() {
                    let log_id = LogId::from(*partition_id);

                    let min_persisted_lsn = processor_status
                        .values()
                        .map(|s| s.last_persisted_log_lsn.unwrap_or(Lsn::INVALID))
                        .min()
                        .unwrap_or(Lsn::INVALID);

                    let current_trim_point =
                        *current_trim_points.get(&log_id).unwrap_or(&Lsn::INVALID);

                    if min_persisted_lsn >= current_trim_point + threshold.unwrap_or(Lsn::from(1)) {
                        debug!(
                            ?partition_id,
                            "Safe trim point for log {}: {:?}", log_id, min_persisted_lsn
                        );
                        safe_trim_points.insert(log_id, (min_persisted_lsn, *partition_id));
                    }
                }
            }
        }

        safe_trim_points
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

    fn is_trimming_suspended(&self) -> bool {
        match self {
            TrimMode::PersistedLsn { cluster_state, .. } => cluster_state
                .nodes
                .values()
                .any(|node_state| matches!(node_state, NodeState::Suspect(_) | NodeState::Dead(_))),
            TrimMode::ArchivedLsn { .. } => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use crate::cluster_controller::service::state::TrimMode;
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
    fn no_safe_trim_points() {
        let p1 = PartitionId::from(0);

        let n1 = GenerationalNodeId::new(1, 0);
        let n1_partitions = [(
            p1,
            ProcessorStatus {
                mode: Leader,
                applied: Some(Lsn::new(10)),
                persisted: None,
                archived: None,
            }
            .into(),
        )]
        .into_iter()
        .collect();

        let cluster_state = Arc::new(ClusterState {
            last_refreshed: None,
            nodes_config_version: Version::MIN,
            partition_table_version: Version::MIN,
            logs_metadata_version: Version::MIN,
            nodes: [(n1.as_plain(), alive_node(n1, n1_partitions))].into(),
        });

        let mut current_trim_points = HashMap::new();
        current_trim_points.insert(LogId::from(p1), Lsn::INVALID);

        let trim_mode = TrimMode::from(false, cluster_state);
        let trim_points = trim_mode.safe_trim_points(&current_trim_points, None);

        assert!(matches!(trim_mode, TrimMode::PersistedLsn { .. }));
        assert_eq!(
            trim_points,
            BTreeMap::new(),
            "No safe trim points when neither persisted or archived LSNs reported"
        );
    }

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

        let mut current_trim_points = HashMap::new();
        current_trim_points.insert(LogId::from(p1), Lsn::INVALID);
        current_trim_points.insert(LogId::from(p2), Lsn::INVALID);
        current_trim_points.insert(LogId::from(p3), Lsn::INVALID);

        let trim_mode = TrimMode::from(false, cluster_state);
        let trim_points = trim_mode.safe_trim_points(&current_trim_points, None);

        assert!(matches!(trim_mode, TrimMode::PersistedLsn { .. }));
        assert_eq!(
            trim_points,
            BTreeMap::from([
                // neither node reports persisted LSN for p1 - no trim
                // only one node reports persisted LSN for p2 - no trim
                (LogId::from(p3), (Lsn::new(5), p3)),
            ]),
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

        let trim_mode = TrimMode::from(false, cluster_state.clone());
        let trim_points = trim_mode.safe_trim_points(&current_trim_points, None);

        assert!(matches!(trim_mode, TrimMode::PersistedLsn { .. }));
        assert_eq!(
            trim_points,
            BTreeMap::new(),
            "Any dead nodes in cluster block trimming when using persisted LSN mode"
        );

        let mut nodes = cluster_state.nodes.clone();
        nodes.insert(PlainNodeId::new(3), dead_node());
        let cluster_state = Arc::new(ClusterState {
            nodes,
            ..*cluster_state
        });

        let trim_mode = TrimMode::from(false, cluster_state.clone());
        let trim_points = trim_mode.safe_trim_points(&current_trim_points, None);

        assert!(matches!(trim_mode, TrimMode::PersistedLsn { .. }));
        assert_eq!(
            trim_points,
            BTreeMap::new(),
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

        let trim_mode = TrimMode::from(false, cluster_state);
        let trim_points = trim_mode.safe_trim_points(&current_trim_points, None);

        assert!(matches!(trim_mode, TrimMode::PersistedLsn { .. }));
        assert_eq!(
            trim_points,
            BTreeMap::new(),
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

        let current_trim_points = [
            (LogId::from(p1), Lsn::INVALID),
            (LogId::from(p2), Lsn::INVALID),
            (LogId::from(p3), Lsn::INVALID),
            (LogId::from(p4), 10.into()),
        ]
        .into();

        let trim_mode = TrimMode::from(false, cluster_state.clone());
        let trim_points = trim_mode.safe_trim_points(&current_trim_points, None);

        assert_eq!(
            trim_points,
            BTreeMap::from([
                // p1 does not report archived LSN - no trim
                (LogId::from(p2), (Lsn::new(10), p2)),
                // p3 has applied LSN = archived LSN - no trim necessary
                // p4 has a node whose applied LSN is behind the latest archived LSN - no trim yet
            ])
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

        let trim_mode = TrimMode::from(false, cluster_state);
        let trim_points = trim_mode.safe_trim_points(&current_trim_points, None);

        assert_eq!(
            trim_points,
            BTreeMap::from([
                // p1 does not report archived LSN - no trim
                (LogId::from(p2), (Lsn::new(10), p2)),
                // p3 has applied LSN = archived LSN - no trim necessary
                // p4 applied LSN is behind the latest archived LSN - no trim yet
            ]),
            "presence of dead or suspect nodes does not block trimming"
        );
    }

    #[test]
    fn trim_points_respect_min_trim_threshold() {
        let p1 = PartitionId::from(0);
        let n1 = GenerationalNodeId::new(1, 0);
        let n1_partitions = [(
            p1,
            ProcessorStatus {
                mode: Leader,
                applied: Some(Lsn::new(101)),
                persisted: Some(Lsn::new(100)),
                archived: None,
            }
            .into(),
        )]
        .into_iter()
        .collect();

        let cluster_state = Arc::new(ClusterState {
            last_refreshed: None,
            nodes_config_version: Version::MIN,
            partition_table_version: Version::MIN,
            logs_metadata_version: Version::MIN,
            nodes: [(n1.as_plain(), alive_node(n1, n1_partitions))].into(),
        });

        let current_trim_points = [(LogId::from(p1), Lsn::from(81))].into();
        let trim_mode = TrimMode::from(false, cluster_state.clone());

        assert!(matches!(trim_mode, TrimMode::PersistedLsn { .. }));
        assert_eq!(
            trim_mode.safe_trim_points(&current_trim_points, Some(Lsn::from(20))),
            BTreeMap::new(),
            "No trim when the threshold since the existing trim point is not cleared"
        );

        let current_trim_points = [(LogId::from(p1), Lsn::from(80))].into();
        assert_eq!(
            trim_mode.safe_trim_points(&current_trim_points, Some(Lsn::from(20))),
            [(LogId::from(p1), (Lsn::new(100), p1))].into(),
            "Trim to the persisted LSN only when the threshold since the existing trim point is cleared"
        );

        let n1_partitions = [(
            p1,
            ProcessorStatus {
                mode: Leader,
                applied: Some(Lsn::new(101)),
                persisted: Some(Lsn::new(100)),
                archived: Some(Lsn::new(90)),
            }
            .into(),
        )]
        .into_iter()
        .collect();

        let cluster_state = Arc::new(ClusterState {
            nodes: [(n1.as_plain(), alive_node(n1, n1_partitions))].into(),
            ..*cluster_state
        });
        let trim_mode = TrimMode::from(true, cluster_state);
        let current_trim_points = [(LogId::from(p1), Lsn::from(71))].into();

        assert!(matches!(trim_mode, TrimMode::ArchivedLsn { .. }));
        assert_eq!(
            trim_mode.safe_trim_points(&current_trim_points, Some(Lsn::from(20))),
            BTreeMap::new(),
            "No trim when the threshold since the existing trim point is not cleared"
        );

        let current_trim_points = [(LogId::from(p1), Lsn::from(70))].into();
        assert_eq!(
            trim_mode.safe_trim_points(&current_trim_points, Some(Lsn::from(20))),
            [(LogId::from(p1), (Lsn::new(90), p1))].into(),
            "Trim to the archived LSN only when the threshold since the existing trim point is cleared"
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
