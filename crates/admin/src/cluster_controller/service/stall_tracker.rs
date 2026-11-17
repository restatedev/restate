// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, Instant};

use ahash::{HashMap, HashSet};
use metrics::counter;
use tracing::info;

use restate_types::PlainNodeId;
use restate_types::cluster::cluster_state::{LegacyClusterState, NodeState, ReplayStatus};
use restate_types::identifiers::PartitionId;
use restate_types::logs::Lsn;
use restate_types::time::MillisSinceEpoch;

use crate::metric_definitions::STALLED_NODES;

/// A node is treated as recently stalled for this many stall thresholds after its last stall.
const RECOVERY_FACTOR: u32 = 10;

/// Detects nodes whose partition processors can no longer write to their storage, and decides
/// which node should give up its partition leaderships because of it.
///
/// A running partition processor refreshes the `updated_at` timestamp of its status
/// periodically, even when it is idle. If the timestamp stays the same across cluster state
/// refreshes, then the processor's event loop is blocked. Partition processors of a node share
/// its storage. A node is therefore considered stalled if at least one of its processors is
/// blocked for the stall threshold, while none of its processors applied a record in that time.
///
/// A stalled node is not necessarily a bad node, since the workload itself can stall storage. In
/// that case, moving leadership only moves the problem. To tell the two apart, nodes are
/// compared with each other:
///
/// * A stalled node is only suspended from leadership if no other node stalled recently.
/// * A partition only moves away from the suspended node to a replica that applied at least as
///   much of the partition's log without stalling, see [`Self::can_take_over`].
///
/// The suspension ends once the node has not been stalled for the recovery period.
///
/// Timestamps are only compared for equality and the elapsed time is measured between cluster
/// state refreshes on the cluster controller. The detection is therefore unaffected by clock
/// differences between nodes and by slow cluster state refreshes.
#[derive(Debug, Default)]
pub(super) struct StallTracker {
    processors: HashMap<(PartitionId, PlainNodeId), Processor>,
    /// Nodes that are stalled as of the last cluster state refresh.
    stalled_nodes: HashSet<PlainNodeId>,
    /// Refresh time of the last cluster state in which a node was stalled.
    last_stalled_at: HashMap<PlainNodeId, Instant>,
    suspended_node: Option<PlainNodeId>,
    last_refreshed: Option<Instant>,
}

#[derive(Debug)]
struct Processor {
    updated_at: MillisSinceEpoch,
    /// Refresh time of the cluster state in which `updated_at` was first observed.
    reported_at: Instant,
    applied_lsn: Option<Lsn>,
    /// Refresh time of the cluster state in which `applied_lsn` was first observed.
    applied_at: Instant,
    /// Processors that are still starting, or that failed to start, don't refresh their status.
    started: bool,
    stalled: bool,
}

impl StallTracker {
    /// Processors that did not report progress for at least `threshold` are considered stalled.
    /// `None` disables stall detection.
    pub fn observe(&mut self, cluster_state: &LegacyClusterState, threshold: Option<Duration>) {
        let Some(threshold) = threshold else {
            *self = Self::default();
            return;
        };
        let Some(refreshed_at) = cluster_state.last_refreshed else {
            return;
        };
        if self.last_refreshed == Some(refreshed_at) {
            // we have seen this cluster state already
            return;
        }
        self.last_refreshed = Some(refreshed_at);

        // Forget processors that are no longer running. Processors of nodes that didn't respond
        // are kept since we don't know whether they made progress.
        self.processors.retain(|(partition_id, node_id), _| {
            match cluster_state.nodes.get(node_id) {
                Some(NodeState::Alive(node)) => node.partitions.contains_key(partition_id),
                Some(NodeState::Dead(_)) => true,
                None => false,
            }
        });
        self.last_stalled_at
            .retain(|node_id, _| cluster_state.nodes.contains_key(node_id));

        let mut stalled_nodes = HashSet::default();
        for (node_id, node) in &cluster_state.nodes {
            // we can only tell whether a node is stalled if it responded
            let NodeState::Alive(node) = node else {
                continue;
            };
            let mut stalled_processors = 0;
            let mut is_writing = false;
            for (partition_id, status) in &node.partitions {
                let processor =
                    self.processors
                        .entry((*partition_id, *node_id))
                        .or_insert(Processor {
                            updated_at: status.updated_at,
                            reported_at: refreshed_at,
                            applied_lsn: status.last_applied_log_lsn,
                            applied_at: refreshed_at,
                            started: false,
                            stalled: false,
                        });
                if processor.updated_at != status.updated_at {
                    processor.updated_at = status.updated_at;
                    processor.reported_at = refreshed_at;
                }
                if processor.applied_lsn != status.last_applied_log_lsn {
                    processor.applied_lsn = status.last_applied_log_lsn;
                    processor.applied_at = refreshed_at;
                }
                processor.started = status.replay_status != ReplayStatus::Starting;
                processor.stalled =
                    refreshed_at.saturating_duration_since(processor.reported_at) >= threshold;

                stalled_processors += usize::from(processor.started && processor.stalled);
                is_writing |=
                    refreshed_at.saturating_duration_since(processor.applied_at) < threshold;
            }

            if stalled_processors > 0 && !is_writing {
                stalled_nodes.insert(*node_id);
                self.last_stalled_at.insert(*node_id, refreshed_at);
                if !self.stalled_nodes.contains(node_id) {
                    info!(
                        "Node {node_id} is stalled: {stalled_processors} of its partition processors \
                        did not report progress for {threshold:?} and none applied a record"
                    );
                }
            }
        }

        let recovery = threshold * RECOVERY_FACTOR;
        let stalled_recently = |node_id: &PlainNodeId| {
            self.last_stalled_at
                .get(node_id)
                .is_some_and(|at| refreshed_at.saturating_duration_since(*at) < recovery)
        };

        if let Some(suspended) = self.suspended_node
            && !stalled_recently(&suspended)
        {
            info!("Node {suspended} recovered from its stall and can lead partitions again");
            self.suspended_node = None;
        }

        for node_id in &stalled_nodes {
            if self.suspended_node.is_none()
                && !self
                    .last_stalled_at
                    .keys()
                    .any(|other| other != node_id && stalled_recently(other))
            {
                info!(
                    "Node {node_id} is the only node that stalled recently. Its partition leaders \
                    fail over to caught up replicas if stall failover is enabled"
                );
                counter!(STALLED_NODES, "outcome" => "suspended").increment(1);
                self.suspended_node = Some(*node_id);
            } else if self.suspended_node != Some(*node_id) && !self.stalled_nodes.contains(node_id)
            {
                info!(
                    "Ignoring the stall of node {node_id} because another node stalled recently, \
                    which hints at the workload and not at the node"
                );
                counter!(STALLED_NODES, "outcome" => "ignored").increment(1);
            }
        }
        self.stalled_nodes = stalled_nodes;
    }

    /// The node that should give up its partition leaderships, if any.
    pub fn suspended_node(&self) -> Option<PlainNodeId> {
        self.suspended_node
    }

    /// Returns true if `candidate` showed that it copes with the workload of the given partition
    /// that `stalled` is stuck on: it applied at least as much of the partition's log, and
    /// neither its partition processor nor its node are stalled.
    pub fn can_take_over(
        &self,
        partition_id: PartitionId,
        candidate: PlainNodeId,
        stalled: PlainNodeId,
    ) -> bool {
        let Some(processor) = self.processors.get(&(partition_id, candidate)) else {
            return false;
        };
        let stalled_lsn = self
            .processors
            .get(&(partition_id, stalled))
            .and_then(|stalled| stalled.applied_lsn);

        !processor.stalled
            && !self.stalled_nodes.contains(&candidate)
            && processor.applied_lsn >= stalled_lsn
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::BTreeMap;

    use restate_types::GenerationalNodeId;
    use restate_types::cluster::cluster_state::{AliveNode, DeadNode, PartitionProcessorStatus};

    use super::*;

    pub const THRESHOLD: Duration = Duration::from_secs(60);

    /// Creates a node from the `(partition, updated_at, applied_lsn)` of its processors.
    pub fn alive(node_id: u32, processors: &[(u16, u64, u64)]) -> (u32, NodeState) {
        alive_with(node_id, processors, ReplayStatus::Active)
    }

    fn alive_with(
        node_id: u32,
        processors: &[(u16, u64, u64)],
        replay_status: ReplayStatus,
    ) -> (u32, NodeState) {
        let node = AliveNode {
            last_heartbeat_at: MillisSinceEpoch::now(),
            generational_node_id: GenerationalNodeId::new(node_id, 1),
            partitions: processors
                .iter()
                .map(|(partition_id, updated_at, applied_lsn)| {
                    (
                        PartitionId::new_unchecked(*partition_id),
                        PartitionProcessorStatus {
                            updated_at: MillisSinceEpoch::new(*updated_at),
                            last_applied_log_lsn: Some(Lsn::new(*applied_lsn)),
                            replay_status,
                            ..PartitionProcessorStatus::new()
                        },
                    )
                })
                .collect(),
            uptime: Duration::ZERO,
        };
        (node_id, NodeState::Alive(node))
    }

    pub fn cluster_state(
        refreshed_at: Instant,
        nodes: Vec<(u32, NodeState)>,
    ) -> LegacyClusterState {
        LegacyClusterState {
            last_refreshed: Some(refreshed_at),
            nodes: nodes
                .into_iter()
                .map(|(node_id, state)| (PlainNodeId::from(node_id), state))
                .collect::<BTreeMap<_, _>>(),
            ..LegacyClusterState::empty()
        }
    }

    #[test]
    fn suspends_the_only_stalled_node() {
        let p0 = PartitionId::new_unchecked(0);
        let [n1, n2, n3] = [1, 2, 3].map(PlainNodeId::from);
        let start = Instant::now();
        let at = |secs| start + Duration::from_secs(secs);
        let observe = |tracker: &mut StallTracker, secs, nodes| {
            tracker.observe(&cluster_state(at(secs), nodes), Some(THRESHOLD));
        };

        // n1 is stuck on p0 while its idle processor of p1 keeps reporting. n2 and n3 keep up.
        let mut tracker = StallTracker::default();
        for secs in [0, 59, 60] {
            if secs == 60 {
                // not yet stalled for the threshold
                assert_eq!(tracker.suspended_node(), None);
                assert!(tracker.can_take_over(p0, n2, n1));
            }
            let stalled = cluster_state(
                at(secs),
                vec![
                    alive(1, &[(0, 100, 10), (1, secs, 5)]),
                    alive(2, &[(0, secs, 10 + secs)]),
                    alive(3, &[(0, secs, 5)]),
                ],
            );
            tracker.observe(&stalled, Some(THRESHOLD));
            // observing the same cluster state again must not change anything
            tracker.observe(&stalled, Some(THRESHOLD));
        }
        assert_eq!(tracker.suspended_node(), Some(n1));
        // only a replica that applied as much of the log as the stalled one can take over
        assert!(tracker.can_take_over(p0, n2, n1));
        assert!(!tracker.can_take_over(p0, n3, n1));

        // the suspension outlasts the stall by the recovery period, also if n1 doesn't respond
        observe(
            &mut tracker,
            61,
            vec![
                (
                    1,
                    NodeState::Dead(DeadNode {
                        last_seen_alive: None,
                    }),
                ),
                alive(2, &[(0, 61, 80)]),
            ],
        );
        let recovered = |secs| vec![alive(1, &[(0, secs, 10)]), alive(2, &[(0, secs, 80)])];
        observe(&mut tracker, 659, recovered(659));
        assert_eq!(tracker.suspended_node(), Some(n1));
        observe(&mut tracker, 660, recovered(660));
        assert_eq!(tracker.suspended_node(), None);

        // a node whose other processors apply records can still write and is not stalled
        let mut tracker = StallTracker::default();
        for secs in [0, 30, 60] {
            observe(
                &mut tracker,
                secs,
                vec![alive(1, &[(0, 100, 10), (1, secs, secs)])],
            );
        }
        assert_eq!(tracker.suspended_node(), None);

        // processors that don't finish starting say nothing about their node
        let mut tracker = StallTracker::default();
        for secs in [0, 60] {
            observe(
                &mut tracker,
                secs,
                vec![alive_with(1, &[(0, 100, 10)], ReplayStatus::Starting)],
            );
        }
        assert_eq!(tracker.suspended_node(), None);

        // if several nodes stall, it's likely the workload: nobody is suspended, also not the
        // node that stays stalled for longer than the others
        let mut tracker = StallTracker::default();
        for secs in [0, 60] {
            observe(
                &mut tracker,
                secs,
                vec![alive(1, &[(0, 100, 10)]), alive(2, &[(0, 100, 10)])],
            );
        }
        observe(
            &mut tracker,
            120,
            vec![alive(1, &[(0, 100, 10)]), alive(2, &[(0, 200, 20)])],
        );
        assert_eq!(tracker.suspended_node(), None);
        assert!(!tracker.can_take_over(p0, n1, n2));
        // until the other node has been free of stalls for the recovery period
        observe(
            &mut tracker,
            660,
            vec![alive(1, &[(0, 100, 10)]), alive(2, &[(0, 300, 30)])],
        );
        assert_eq!(tracker.suspended_node(), Some(n1));

        // nothing is suspended if stall detection is disabled
        tracker.observe(&cluster_state(at(661), vec![]), None);
        assert_eq!(tracker.suspended_node(), None);
    }
}
