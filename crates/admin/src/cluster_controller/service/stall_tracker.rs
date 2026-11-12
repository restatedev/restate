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

use ahash::HashMap;

use restate_types::PlainNodeId;
use restate_types::cluster::cluster_state::{LegacyClusterState, NodeState};
use restate_types::identifiers::PartitionId;
use restate_types::time::MillisSinceEpoch;

/// Detects partition processors that stopped reporting progress.
///
/// A running partition processor refreshes the `updated_at` timestamp of its status
/// periodically, even when it is idle. If the timestamp stays the same across cluster state
/// refreshes, then the processor's event loop is blocked.
///
/// Timestamps are only compared for equality and the elapsed time is measured between cluster
/// state refreshes on the cluster controller. The detection is therefore unaffected by clock
/// differences between nodes and by slow cluster state refreshes.
#[derive(Debug, Default)]
pub(super) struct StallTracker {
    progress: HashMap<(PartitionId, PlainNodeId), Progress>,
    last_refreshed: Option<Instant>,
}

#[derive(Debug)]
struct Progress {
    updated_at: MillisSinceEpoch,
    /// Refresh time of the cluster state in which `updated_at` was first observed.
    changed_at: Instant,
}

impl StallTracker {
    pub fn observe(&mut self, cluster_state: &LegacyClusterState) {
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
        self.progress.retain(
            |(partition_id, node_id), _| match cluster_state.nodes.get(node_id) {
                Some(NodeState::Alive(node)) => node.partitions.contains_key(partition_id),
                Some(NodeState::Dead(_)) => true,
                None => false,
            },
        );

        for (node_id, node) in &cluster_state.nodes {
            let NodeState::Alive(node) = node else {
                continue;
            };
            for (partition_id, status) in &node.partitions {
                let progress = self
                    .progress
                    .entry((*partition_id, *node_id))
                    .or_insert(Progress {
                        updated_at: status.updated_at,
                        changed_at: refreshed_at,
                    });
                if progress.updated_at != status.updated_at {
                    progress.updated_at = status.updated_at;
                    progress.changed_at = refreshed_at;
                }
            }
        }
    }

    /// Returns true if the partition processor did not report progress for at least `threshold`.
    pub fn is_stalled(
        &self,
        partition_id: PartitionId,
        node_id: PlainNodeId,
        threshold: Duration,
    ) -> bool {
        let Some(last_refreshed) = self.last_refreshed else {
            return false;
        };
        self.progress
            .get(&(partition_id, node_id))
            .is_some_and(|progress| {
                last_refreshed.saturating_duration_since(progress.changed_at) >= threshold
            })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use restate_types::GenerationalNodeId;
    use restate_types::cluster::cluster_state::{AliveNode, DeadNode, PartitionProcessorStatus};

    use super::*;

    const THRESHOLD: Duration = Duration::from_secs(10);

    fn alive(updated_at: &[(u16, u64)]) -> NodeState {
        NodeState::Alive(AliveNode {
            last_heartbeat_at: MillisSinceEpoch::now(),
            generational_node_id: GenerationalNodeId::new(1, 1),
            partitions: updated_at
                .iter()
                .map(|(partition_id, updated_at)| {
                    (
                        PartitionId::new_unchecked(*partition_id),
                        PartitionProcessorStatus {
                            updated_at: MillisSinceEpoch::new(*updated_at),
                            ..PartitionProcessorStatus::new()
                        },
                    )
                })
                .collect(),
            uptime: Duration::ZERO,
        })
    }

    fn cluster_state(refreshed_at: Instant, nodes: Vec<(u32, NodeState)>) -> LegacyClusterState {
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
    fn detects_processors_that_stop_reporting_progress() {
        let p0 = PartitionId::new_unchecked(0);
        let p1 = PartitionId::new_unchecked(1);
        let (n1, n2) = (PlainNodeId::from(1), PlainNodeId::from(2));
        let start = Instant::now();
        let at = |secs| start + Duration::from_secs(secs);

        let mut tracker = StallTracker::default();
        assert!(!tracker.is_stalled(p0, n1, THRESHOLD));

        // n1 never advances p0, but keeps advancing p1; n2 advances p0
        tracker.observe(&cluster_state(
            at(0),
            vec![(1, alive(&[(0, 100), (1, 100)])), (2, alive(&[(0, 100)]))],
        ));
        tracker.observe(&cluster_state(
            at(9),
            vec![(1, alive(&[(0, 100), (1, 200)])), (2, alive(&[(0, 200)]))],
        ));
        assert!(!tracker.is_stalled(p0, n1, THRESHOLD));

        let stalled = cluster_state(
            at(10),
            vec![(1, alive(&[(0, 100), (1, 300)])), (2, alive(&[(0, 300)]))],
        );
        tracker.observe(&stalled);
        // observing the same cluster state again must not change anything
        tracker.observe(&stalled);
        assert!(tracker.is_stalled(p0, n1, THRESHOLD));
        assert!(!tracker.is_stalled(p1, n1, THRESHOLD));
        assert!(!tracker.is_stalled(p0, n2, THRESHOLD));

        // a node that doesn't respond keeps its stalled processors
        tracker.observe(&cluster_state(
            at(11),
            vec![
                (
                    1,
                    NodeState::Dead(DeadNode {
                        last_seen_alive: None,
                    }),
                ),
                (2, alive(&[(0, 400)])),
            ],
        ));
        assert!(tracker.is_stalled(p0, n1, THRESHOLD));

        // the processor recovers as soon as it reports progress again
        tracker.observe(&cluster_state(
            at(12),
            vec![(1, alive(&[(0, 500), (1, 500)])), (2, alive(&[(0, 500)]))],
        ));
        assert!(!tracker.is_stalled(p0, n1, THRESHOLD));

        // a processor that is stopped and started again starts with a clean slate
        tracker.observe(&cluster_state(at(30), vec![(1, alive(&[(1, 600)]))]));
        tracker.observe(&cluster_state(
            at(31),
            vec![(1, alive(&[(0, 500), (1, 700)]))],
        ));
        assert!(!tracker.is_stalled(p0, n1, THRESHOLD));
    }
}
