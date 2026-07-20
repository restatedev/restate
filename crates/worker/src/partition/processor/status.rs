// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::time::Instant;
use tracing::info;

use restate_clock::time::MillisSinceEpoch;
use restate_types::GenerationalNodeId;
use restate_types::cluster::cluster_state::{ReplayStatus, RunMode};
use restate_types::identifiers::LeaderEpoch;
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::sharding::PartitionId;

/// Read access to the processor's live [`Status`] — replay progress, planned run mode, and the
/// last observed leader. This is the in-memory data surfaced through the partition-processor
/// status API; it is not persisted.
pub trait HasStatus {
    /// Returns the current status.
    fn status(&self) -> &Status;
}

/// Mutating access to the processor's live [`Status`].
pub trait HasStatusMut: HasStatus {
    /// Returns the status for in-place mutation.
    fn status_mut(&mut self) -> &mut Status;
}

pub struct Status {
    /// The time point when the processor is considered to have started
    started_at: Instant,
    last_lsn_applied_at: Option<MillisSinceEpoch>,
    planned_run_mode: RunMode,
    replay_status: ReplayStatus,
    target_tail_lsn: Option<Lsn>,
    last_observed_leader_epoch: LeaderEpoch,
    last_observed_leader_node: Option<GenerationalNodeId>,
}

impl Status {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            last_lsn_applied_at: None,
            planned_run_mode: RunMode::Follower,
            replay_status: ReplayStatus::Starting,
            target_tail_lsn: None,
            last_observed_leader_node: None,
            last_observed_leader_epoch: LeaderEpoch::INVALID,
        }
    }

    pub fn set_last_observed_leader_epoch(&mut self, leader_epoch: LeaderEpoch) {
        self.last_observed_leader_epoch = leader_epoch;
    }

    pub fn last_observed_leader_epoch(&self) -> LeaderEpoch {
        self.last_observed_leader_epoch
    }

    pub fn set_last_observed_leader_node(&mut self, node_id: GenerationalNodeId) {
        self.last_observed_leader_node = Some(node_id);
    }

    pub fn last_observed_leader_node(&self) -> Option<GenerationalNodeId> {
        self.last_observed_leader_node
    }

    pub fn last_lsn_applied_at(&self) -> Option<MillisSinceEpoch> {
        self.last_lsn_applied_at
    }

    pub fn set_planned_run_mode(&mut self, planned_run_mode: RunMode) {
        self.planned_run_mode = planned_run_mode;
    }

    pub fn planned_mode(&self) -> RunMode {
        self.planned_run_mode
    }

    pub fn replay_status(&self) -> ReplayStatus {
        self.replay_status
    }

    pub(super) fn set_catchup_lsn(
        &mut self,
        partition_id: PartitionId,
        catch_up_tail: Lsn,
        last_applied_lsn: Lsn,
    ) {
        if catch_up_tail == last_applied_lsn.next() {
            if self.replay_status != ReplayStatus::Active {
                self.target_tail_lsn = None;
                self.replay_status = ReplayStatus::Active;
                info!("Partition {partition_id} started");
            }
        } else {
            // catching up.
            self.target_tail_lsn = Some(catch_up_tail);
            self.replay_status = ReplayStatus::CatchingUp;
            let catchup_len = catch_up_tail.as_u64() - last_applied_lsn.next().as_u64();
            info!(
                "Partition {partition_id} started. Replaying {catchup_len} record(s) in range: [{}..{}]",
                last_applied_lsn.next(),
                catch_up_tail.prev()
            );
        }
    }

    /// Returns true if we transitioned from CatchingUp to Active
    pub(super) fn update_last_applied_lsn(&mut self, lsn: Lsn) -> bool {
        self.last_lsn_applied_at = Some(MillisSinceEpoch::now());
        // Update replay status
        match self.replay_status {
            ReplayStatus::CatchingUp
                if self
                    .target_tail_lsn()
                    .is_some_and(|tail| lsn.next() >= tail) =>
            {
                // finished catching up
                self.replay_status = ReplayStatus::Active;
                self.target_tail_lsn = None;
                true
            }
            _ => false,
        }
    }

    pub fn target_tail_lsn(&self) -> Option<Lsn> {
        self.target_tail_lsn
    }

    pub fn set_started_at(&mut self, started_at: Instant) {
        self.started_at = started_at;
    }

    pub fn started_at(&self) -> Instant {
        self.started_at
    }
}

// -- Boilerplate --
impl<P: HasStatus> HasStatus for &P {
    #[inline]
    fn status(&self) -> &Status {
        (**self).status()
    }
}

impl<P: HasStatus> HasStatus for &mut P {
    #[inline]
    fn status(&self) -> &Status {
        (**self).status()
    }
}

impl<P: HasStatusMut> HasStatusMut for &mut P {
    #[inline]
    fn status_mut(&mut self) -> &mut Status {
        (**self).status_mut()
    }
}
