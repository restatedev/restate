// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt;

use restate_types::identifiers::PartitionId;
use restate_types::partitions::state::{MembershipState, PartitionReplicaSetStates};
use restate_types::replication::NodeSet;
use restate_types::{PlainNodeId, Version};

use super::processor_state::ProcessorState;

/// Processor starts and stops needed to match observed replica-set membership.
#[derive(Default)]
pub(super) struct ReconciliationPlan {
    pub(super) starts: BTreeMap<PartitionId, MembershipSnapshot>,
    pub(super) stops: BTreeMap<PartitionId, PlannedStop>,
}

impl fmt::Display for ReconciliationPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("starts=[")?;
        let mut separator = "";
        for (partition_id, start) in &self.starts {
            write!(f, "{separator}P{partition_id}({start})")?;
            separator = ", ";
        }

        f.write_str("] stops=[")?;
        separator = "";
        for (partition_id, stop) in &self.stops {
            write!(f, "{separator}P{partition_id}({stop})")?;
            separator = ", ";
        }
        f.write_str("]")
    }
}

impl ReconciliationPlan {
    pub(super) fn build(
        processor_states: &BTreeMap<PartitionId, ProcessorState>,
        replica_set_states: &PartitionReplicaSetStates,
        my_node_id: PlainNodeId,
    ) -> Self {
        let mut plan = Self {
            starts: BTreeMap::new(),
            stops: processor_states
                .iter()
                .map(|(&partition_id, processor_state)| {
                    let disposition = if processor_state.is_broken() {
                        StopDisposition::ForgetBroken
                    } else {
                        StopDisposition::RequestStop
                    };
                    (
                        partition_id,
                        PlannedStop {
                            reason: StopReason::NoObservedReplicaSet,
                            disposition,
                        },
                    )
                })
                .collect(),
        };

        for (partition_id, membership) in replica_set_states.iter() {
            if membership.contains(my_node_id) {
                plan.stops.remove(&partition_id);
                if !processor_states.contains_key(&partition_id) {
                    plan.starts.insert(partition_id, (&membership).into());
                }
            } else if let Some(planned_stop) = plan.stops.get_mut(&partition_id) {
                planned_stop.reason = StopReason::NotInObservedReplicaSet((&membership).into());
            }
        }

        plan
    }

    pub(super) fn is_empty(&self) -> bool {
        self.starts.is_empty() && self.stops.is_empty()
    }
}

pub(super) struct MembershipSnapshot {
    current_version: Version,
    next_version: Option<Version>,
    replica_set: NodeSet,
}

impl fmt::Display for MembershipSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.current_version)?;
        if let Some(next_version) = self.next_version {
            write!(f, "->{next_version}")?;
        }
        write!(f, " {:#}", self.replica_set)
    }
}

impl From<&MembershipState> for MembershipSnapshot {
    fn from(membership: &MembershipState) -> Self {
        Self {
            current_version: membership.observed_current_membership.version,
            next_version: membership
                .observed_next_membership
                .as_ref()
                .map(|membership| membership.version),
            replica_set: membership.replica_set_union().collect(),
        }
    }
}

enum StopReason {
    NotInObservedReplicaSet(MembershipSnapshot),
    NoObservedReplicaSet,
}

impl fmt::Display for StopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInObservedReplicaSet(membership) => membership.fmt(f),
            Self::NoObservedReplicaSet => f.write_str("unobserved"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StopDisposition {
    RequestStop,
    ForgetBroken,
}

pub(super) struct PlannedStop {
    reason: StopReason,
    disposition: StopDisposition,
}

impl fmt::Display for PlannedStop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(f)?;
        if self.disposition == StopDisposition::ForgetBroken {
            f.write_str("; forget-broken")?;
        }
        Ok(())
    }
}

impl PlannedStop {
    pub(super) fn disposition(&self) -> StopDisposition {
        self.disposition
    }
}

#[cfg(test)]
mod tests {
    use restate_types::cluster::cluster_state::{BrokenReason, RunMode};
    use restate_types::logs::{Lsn, SequenceNumber};
    use restate_types::partitions::state::{LeadershipState, MemberState, ReplicaSetState};

    use super::*;

    fn replica_set(version: Version, members: &[PlainNodeId]) -> ReplicaSetState {
        ReplicaSetState {
            version,
            members: members
                .iter()
                .map(|&node_id| MemberState {
                    node_id,
                    durable_lsn: Lsn::INVALID,
                })
                .collect(),
        }
    }

    #[test]
    fn plans_membership_reconciliation_with_reasons() {
        let my_node_id = PlainNodeId::from(1);
        let other_node_id = PlainNodeId::from(2);
        let retained = PartitionId::from(0);
        let stopped = PartitionId::from(1);
        let forgotten = PartitionId::from(2);
        let not_observed = PartitionId::from(3);
        let started = PartitionId::from(4);
        let irrelevant = PartitionId::from(5);

        let processor_states = BTreeMap::from([
            (retained, ProcessorState::starting(RunMode::Follower, None)),
            (stopped, ProcessorState::starting(RunMode::Follower, None)),
            (forgotten, ProcessorState::broken(BrokenReason::AheadOfLog)),
            (
                not_observed,
                ProcessorState::starting(RunMode::Follower, None),
            ),
        ]);
        let replica_set_states = PartitionReplicaSetStates::default();

        for (partition_id, members) in [
            (retained, &[my_node_id][..]),
            (stopped, &[other_node_id][..]),
            (forgotten, &[other_node_id][..]),
            (started, &[other_node_id][..]),
            (irrelevant, &[other_node_id][..]),
        ] {
            let current = replica_set(Version::MIN, members);
            let next =
                (partition_id == started).then(|| replica_set(Version::MIN.next(), &[my_node_id]));
            replica_set_states.note_observed_membership(
                partition_id,
                LeadershipState::default(),
                &current,
                &next,
            );
        }

        let plan = ReconciliationPlan::build(&processor_states, &replica_set_states, my_node_id);

        assert_eq!(
            plan.to_string(),
            "starts=[P4(v1->v2 [N1, N2])] stops=[P1(v1 [N2]), P2(v1 [N2]; forget-broken), P3(unobserved)]"
        );
    }
}
