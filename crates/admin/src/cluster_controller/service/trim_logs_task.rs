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
use std::ops::Add;
use std::sync::Arc;

use futures::future::OptionFuture;
use futures::never::Never;
use tokio::time;
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{debug, instrument, trace, warn};

use restate_bifrost::Bifrost;
use restate_core::{Metadata, cancellation_token};
use restate_types::GenerationalNodeId;
use restate_types::cluster::cluster_state::{
    AliveNode, LegacyClusterState, PartitionProcessorStatus,
};
use restate_types::config::{AdminOptions, Configuration};
use restate_types::identifiers::PartitionId;
use restate_types::live::LiveLoad;
use restate_types::logs::{LogId, Lsn, SequenceNumber};
use restate_types::partitions::Partition;
use restate_types::retries::with_jitter;

use crate::cluster_controller::cluster_state_refresher::ClusterStateWatcher;

pub struct TrimLogsTask {
    cluster_state_watcher: ClusterStateWatcher,
    snapshots_repository_configured: bool,
    bifrost: Bifrost,
    log_trim_check_interval: Option<Interval>,
}

impl TrimLogsTask {
    pub fn new(bifrost: Bifrost, cluster_state_watcher: ClusterStateWatcher) -> Self {
        let configuration = Configuration::pinned();

        Self {
            cluster_state_watcher,
            bifrost,
            snapshots_repository_configured: configuration.worker.snapshots.destination.is_some(),
            log_trim_check_interval: Self::create_log_trim_check_interval(&configuration.admin),
        }
    }

    pub async fn run(mut self) {
        trace!("Running TrimLogsTask");

        cancellation_token()
            .run_until_cancelled(self.run_inner())
            .await;
    }

    async fn run_inner(&mut self) -> Never {
        let mut config_watcher = Configuration::watcher();
        let mut live_admin_config = Configuration::map_live(|config| &config.admin);

        loop {
            tokio::select! {
                Some(_) = OptionFuture::from(self.log_trim_check_interval.as_mut().map(|interval| interval.tick())) => {
                    self.trim_logs().await;
                }
                () = config_watcher.changed() => {
                    self.log_trim_check_interval = Self::create_log_trim_check_interval(live_admin_config.live_load())
                }
            }
        }
    }

    #[instrument(
        level = "debug",
        skip(self),
        fields(
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

        let trim_points = TrimMode::from(self.snapshots_repository_configured, &cluster_state)
            .calculate_safe_trim_points();
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

    fn create_log_trim_check_interval(options: &AdminOptions) -> Option<Interval> {
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
}

enum TrimMode {
    /// Trim logs based on partition processors' reported Durable LSN. This strategy is only
    /// appropriate for single-node deployments.
    DurableLsn {
        partition_status:
            BTreeMap<PartitionId, BTreeMap<GenerationalNodeId, PartitionProcessorStatus>>,
    },
    /// Trim logs based on the highest reported archived LSN per partition.
    ArchivedLsn {
        partition_status:
            BTreeMap<PartitionId, BTreeMap<GenerationalNodeId, PartitionProcessorStatus>>,
    },
}

impl TrimMode {
    /// In clusters with more than one node, or on single nodes with a snapshot repository
    /// configured, trimming is driven by archived LSN. The durable LSN method is only used on
    /// single-nodes with no snapshots configured.
    fn from(
        snapshots_repository_configured: bool,
        cluster_state: &Arc<LegacyClusterState>,
    ) -> TrimMode {
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

        if snapshots_repository_configured || cluster_state.nodes.len() > 1 {
            if !snapshots_repository_configured {
                warn!(
                    "Detected cluster environment with no snapshot repository configured. \
                    Automatic log trimming is disabled, please refer to \
                    https://docs.restate.dev/operate/snapshots/ for more."
                );
            }
            TrimMode::ArchivedLsn { partition_status }
        } else {
            TrimMode::DurableLsn { partition_status }
        }
    }

    /// Compute the safe trim points for each log, assuming that partitions are mapped to logs 1:1.
    /// For a given cluster state, determines the set of trim points for all partitions' logs.
    fn calculate_safe_trim_points(&self) -> BTreeMap<LogId, (Lsn, PartitionId)> {
        let mut safe_trim_points = BTreeMap::new();
        match self {
            // todo(pavel): revisit this logic with a stronger signal from the rest of the cluster
            // we are currently relying on a single node reporting the archived LSN, which does
            // not guarantee that the new snapshot is visible to other cluster members.
            TrimMode::ArchivedLsn { partition_status } => {
                for (partition_id, processor_status) in partition_status.iter() {
                    let log_id = Metadata::with_current(|m| {
                        m.partition_table_ref()
                            .get(partition_id)
                            .map(Partition::log_id)
                    })
                    .expect("partition is in partition table");

                    // We allow trimming of archived partitions even in the presence of dead nodes; such
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
                            "Some alive nodes have not applied the log up to the archived LSN; trimming will be delayed until caught up"
                        );
                        safe_trim_points.insert(log_id, (Lsn::INVALID, *partition_id));
                    } else {
                        safe_trim_points.insert(log_id, (archived_lsn, *partition_id));
                    }
                }
            }
            TrimMode::DurableLsn {
                partition_status, ..
            } => {
                // FREEZE BLOCK:
                // - full review of trim and snapshot logic
                // - see if we can pull snapshots from peers using a new grpc end-point or through
                // message fabric
                // - how can we trim only after N minutes of the last snapshot or durable LSN. we
                // need to know the LSN -> timestamp mapping. Should we add a new API to Bifrost to
                // estimate the timestamp?

                // If no partitions are reporting archived LSN, we fall back to using the
                // min(durable_lsn) across the board as the safe trim point. Note that at this point
                // we know that there are no known dead nodes, so it's safe to take the min of all
                // durable LSNs reported by all the partition processors as the safe trim point.
                for (partition_id, processor_status) in partition_status.iter() {
                    let log_id = Metadata::with_current(|m| {
                        m.partition_table_ref()
                            .get(partition_id)
                            .map(Partition::log_id)
                    })
                    .expect("partition is in partition table");

                    let min_durable_lsn = processor_status
                        .values()
                        .map(|s| s.last_persisted_log_lsn.unwrap_or(Lsn::INVALID))
                        .min()
                        .unwrap_or(Lsn::INVALID);

                    trace!(
                        ?partition_id,
                        "Safe trim point for log {}: {:?}", log_id, min_durable_lsn
                    );
                    safe_trim_points.insert(log_id, (min_durable_lsn, *partition_id));
                }
            }
        }

        safe_trim_points
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::cluster_controller::service::trim_logs_task::TrimMode;
    use RunMode::{Follower, Leader};
    use restate_core::TestCoreEnvBuilder;
    use restate_types::cluster::cluster_state::{
        AliveNode, DeadNode, LegacyClusterState, NodeState, PartitionProcessorStatus, RunMode,
    };
    use restate_types::identifiers::PartitionId;
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{GenerationalNodeId, PlainNodeId, Version};

    #[restate_core::test(start_paused = true)]
    async fn cluster_without_snapshots_does_not_trim() {
        TestCoreEnvBuilder::with_incoming_only_connector()
            .build()
            .await;

        let p1 = PartitionId::from(0);
        let p2 = PartitionId::from(1);

        let n1 = GenerationalNodeId::new(1, 0);
        let n1_partitions: BTreeMap<PartitionId, PartitionProcessorStatus> = [(
            p1,
            ProcessorStatus {
                mode: Leader,
                applied_lsn: Some(Lsn::new(10)),
                durable_lsn: Some(Lsn::new(10)),
                archived_lsn: None,
            }
            .into(),
        )]
        .into_iter()
        .collect();

        let n2 = GenerationalNodeId::new(2, 0);
        let n2_partitions: BTreeMap<PartitionId, PartitionProcessorStatus> = [(
            p2,
            ProcessorStatus {
                mode: Follower,
                applied_lsn: Some(Lsn::new(10)),
                durable_lsn: Some(Lsn::new(10)),
                archived_lsn: None,
            }
            .into(),
        )]
        .into_iter()
        .collect();

        let cluster_state = Arc::new(LegacyClusterState {
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
        assert!(matches!(trim_mode, TrimMode::ArchivedLsn { .. }));

        let trim_points = trim_mode.calculate_safe_trim_points();
        assert_eq!(
            trim_points,
            BTreeMap::from([
                (LogId::from(p1), (Lsn::INVALID, p1)),
                (LogId::from(p2), (Lsn::INVALID, p2)),
            ]),
            "If no archived LSN reported, we will not trim in a cluster"
        );
    }

    #[restate_core::test(start_paused = true)]
    async fn cluster_with_dead_node_no_snapshots_does_not_trim() {
        TestCoreEnvBuilder::with_incoming_only_connector()
            .build()
            .await;

        let p1 = PartitionId::from(0);
        let p2 = PartitionId::from(1);

        let n1 = GenerationalNodeId::new(1, 0);
        let n1_partitions: BTreeMap<PartitionId, PartitionProcessorStatus> = [
            (
                p1,
                ProcessorStatus {
                    mode: Leader,
                    applied_lsn: Some(Lsn::new(10)),
                    durable_lsn: Some(Lsn::new(10)),
                    archived_lsn: None,
                }
                .into(),
            ),
            (
                p2,
                ProcessorStatus {
                    mode: Follower,
                    applied_lsn: Some(Lsn::new(10)),
                    durable_lsn: Some(Lsn::new(5)),
                    archived_lsn: None,
                }
                .into(),
            ),
        ]
        .into_iter()
        .collect();

        let n2 = GenerationalNodeId::new(2, 0);
        let cluster_state = Arc::new(LegacyClusterState {
            last_refreshed: None,
            nodes_config_version: Version::MIN,
            partition_table_version: Version::MIN,
            logs_metadata_version: Version::MIN,
            nodes: [
                (n1.as_plain(), alive_node(n1, n1_partitions.clone())),
                (n2.as_plain(), dead_node()),
            ]
            .into(),
        });

        let trim_mode = TrimMode::from(false, &cluster_state);
        assert!(matches!(trim_mode, TrimMode::ArchivedLsn { .. }));

        let trim_points = trim_mode.calculate_safe_trim_points();
        assert_eq!(
            trim_points,
            BTreeMap::from([
                (LogId::from(p1), (Lsn::INVALID, p1)),
                (LogId::from(p2), (Lsn::INVALID, p2)),
            ]),
            "Even with a single alive node, we will not trim in a cluster not using snapshots"
        );
    }

    #[restate_core::test(start_paused = true)]
    async fn single_node_may_trim_by_durable_lsn() {
        TestCoreEnvBuilder::with_incoming_only_connector()
            .build()
            .await;

        let p1 = PartitionId::from(0);
        let p2 = PartitionId::from(1);
        let p3 = PartitionId::from(2);

        let n1 = GenerationalNodeId::new(1, 0);
        let n1_partitions: BTreeMap<PartitionId, PartitionProcessorStatus> = [
            (
                p1,
                ProcessorStatus {
                    mode: Leader,
                    applied_lsn: Some(Lsn::new(10)),
                    durable_lsn: None,
                    archived_lsn: None,
                }
                .into(),
            ),
            (
                p2,
                ProcessorStatus {
                    mode: Follower,
                    applied_lsn: Some(Lsn::new(10)),
                    durable_lsn: Some(Lsn::new(5)),
                    archived_lsn: None,
                }
                .into(),
            ),
            (
                p3,
                ProcessorStatus {
                    mode: Leader,
                    applied_lsn: Some(Lsn::new(10)),
                    durable_lsn: Some(Lsn::new(10)),
                    archived_lsn: None,
                }
                .into(),
            ),
        ]
        .into_iter()
        .collect();

        let cluster_state = Arc::new(LegacyClusterState {
            last_refreshed: None,
            nodes_config_version: Version::MIN,
            partition_table_version: Version::MIN,
            logs_metadata_version: Version::MIN,
            nodes: [(n1.as_plain(), alive_node(n1, n1_partitions.clone()))].into(),
        });

        let trim_mode = TrimMode::from(false, &cluster_state);
        let trim_points = trim_mode.calculate_safe_trim_points();

        assert!(matches!(trim_mode, TrimMode::DurableLsn { .. }));
        assert_eq!(
            trim_points,
            BTreeMap::from([
                (LogId::from(p1), (Lsn::new(0), p1)),
                (LogId::from(p2), (Lsn::new(5), p2)),
                (LogId::from(p3), (Lsn::new(10), p3)),
            ]),
            "Use min durable LSN per partition as the safe point in single-node mode when not archiving"
        );
    }

    #[restate_core::test(start_paused = true)]
    async fn cluster_with_snapshots_trims_by_archived_lsn() {
        TestCoreEnvBuilder::with_incoming_only_connector()
            .build()
            .await;

        let p1 = PartitionId::from(0);
        let p2 = PartitionId::from(1);
        let p3 = PartitionId::from(2);

        let n1 = GenerationalNodeId::new(1, 0);
        let n1_partitions = [
            (
                p1,
                ProcessorStatus {
                    mode: Leader,
                    applied_lsn: Some(Lsn::new(10)),
                    durable_lsn: Some(Lsn::new(10)), // should not make any difference
                    archived_lsn: None,
                }
                .into(),
            ),
            (
                p2,
                ProcessorStatus {
                    mode: Follower,
                    applied_lsn: Some(Lsn::new(18)),
                    durable_lsn: Some(Lsn::new(15)), // should not make any difference
                    archived_lsn: None,
                }
                .into(),
            ),
        ]
        .into_iter()
        .collect();

        let n2 = GenerationalNodeId::new(2, 0);
        let n2_partitions = [
            (
                p2,
                ProcessorStatus {
                    mode: Leader,
                    applied_lsn: Some(Lsn::new(20)),
                    durable_lsn: None,
                    archived_lsn: Some(Lsn::new(10)),
                }
                .into(),
            ),
            (
                p3,
                ProcessorStatus {
                    mode: Follower,
                    applied_lsn: Some(Lsn::new(10)),
                    durable_lsn: None,
                    archived_lsn: Some(Lsn::new(5)),
                }
                .into(),
            ),
        ]
        .into_iter()
        .collect();

        let cluster_state = Arc::new(LegacyClusterState {
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
        assert!(matches!(trim_mode, TrimMode::ArchivedLsn { .. }));

        let trim_points = trim_mode.calculate_safe_trim_points();

        assert_eq!(
            trim_points,
            BTreeMap::from([
                (LogId::from(p1), (Lsn::INVALID, p1)),
                (LogId::from(p2), (Lsn::new(10), p2)),
                (LogId::from(p3), (Lsn::new(5), p3)),
            ])
        );
    }

    #[restate_core::test(start_paused = true)]
    async fn cluster_with_snapshots_trims_by_min_of_applied_or_archived_lsn() {
        TestCoreEnvBuilder::with_incoming_only_connector()
            .build()
            .await;

        let p1 = PartitionId::from(0);

        let n1 = GenerationalNodeId::new(1, 0);
        let n1_partitions = [(
            p1,
            ProcessorStatus {
                mode: Leader,
                applied_lsn: Some(Lsn::new(15)),
                durable_lsn: None,
                archived_lsn: Some(Lsn::new(10)),
            }
            .into(),
        )]
        .into_iter()
        .collect();

        let n2 = GenerationalNodeId::new(2, 0);
        let n2_partitions = [(
            p1,
            ProcessorStatus {
                mode: Leader,
                applied_lsn: Some(Lsn::new(5)), // behind the archived LSN reported by n1
                durable_lsn: None,
                archived_lsn: None,
            }
            .into(),
        )]
        .into_iter()
        .collect();

        let cluster_state = Arc::new(LegacyClusterState {
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
        assert!(matches!(trim_mode, TrimMode::ArchivedLsn { .. }));

        let trim_points = trim_mode.calculate_safe_trim_points();

        assert_eq!(
            trim_points,
            BTreeMap::from([(LogId::from(p1), (Lsn::INVALID, p1))]),
            "Wait for slow appliers to catch up when behind the archived LSN"
        );
    }

    #[restate_core::test(start_paused = true)]
    async fn cluster_with_dead_node_and_snapshots_trims_by_archived_lsn() {
        TestCoreEnvBuilder::with_incoming_only_connector()
            .build()
            .await;

        let p1 = PartitionId::from(0);
        let p2 = PartitionId::from(1);
        let p3 = PartitionId::from(2);

        let n1 = GenerationalNodeId::new(1, 0);
        let n1_partitions = [
            (
                p1,
                ProcessorStatus {
                    mode: Leader,
                    applied_lsn: Some(Lsn::new(10)),
                    durable_lsn: Some(Lsn::new(10)), // should not make any difference
                    archived_lsn: None,
                }
                .into(),
            ),
            (
                p2,
                ProcessorStatus {
                    mode: Follower,
                    applied_lsn: Some(Lsn::new(18)),
                    durable_lsn: Some(Lsn::new(15)), // should not make any difference
                    archived_lsn: None,
                }
                .into(),
            ),
        ]
        .into_iter()
        .collect();

        let n2 = GenerationalNodeId::new(2, 0);
        let n2_partitions = [
            (
                p2,
                ProcessorStatus {
                    mode: Leader,
                    applied_lsn: Some(Lsn::new(20)),
                    durable_lsn: None,
                    archived_lsn: Some(Lsn::new(10)),
                }
                .into(),
            ),
            (
                p3,
                ProcessorStatus {
                    mode: Follower,
                    applied_lsn: Some(Lsn::new(10)),
                    durable_lsn: None,
                    archived_lsn: Some(Lsn::new(5)),
                }
                .into(),
            ),
        ]
        .into_iter()
        .collect();

        let cluster_state = Arc::new(LegacyClusterState {
            last_refreshed: None,
            nodes_config_version: Version::MIN,
            partition_table_version: Version::MIN,
            logs_metadata_version: Version::MIN,
            nodes: [
                (n1.as_plain(), alive_node(n1, n1_partitions)),
                (n2.as_plain(), alive_node(n2, n2_partitions)),
                (PlainNodeId::new(4), dead_node()),
            ]
            .into(),
        });

        let trim_mode = TrimMode::from(false, &cluster_state);
        assert!(matches!(trim_mode, TrimMode::ArchivedLsn { .. }));
        let trim_points = trim_mode.calculate_safe_trim_points();

        assert_eq!(
            trim_points,
            BTreeMap::from([
                (LogId::from(p1), (Lsn::INVALID, p1)),
                (LogId::from(p2), (Lsn::new(10), p2)),
                (LogId::from(p3), (Lsn::new(5), p3)),
            ]),
            "Dead nodes do not block trimming when archived LSN is used"
        );
    }

    struct ProcessorStatus {
        mode: RunMode,
        applied_lsn: Option<Lsn>,
        durable_lsn: Option<Lsn>,
        archived_lsn: Option<Lsn>,
    }

    impl From<ProcessorStatus> for PartitionProcessorStatus {
        fn from(status: ProcessorStatus) -> Self {
            PartitionProcessorStatus {
                planned_mode: status.mode,
                effective_mode: status.mode,
                last_applied_log_lsn: status.applied_lsn,
                last_persisted_log_lsn: status.durable_lsn,
                last_archived_log_lsn: status.archived_lsn,
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

    fn dead_node() -> NodeState {
        NodeState::Dead(DeadNode {
            last_seen_alive: None,
        })
    }
}
