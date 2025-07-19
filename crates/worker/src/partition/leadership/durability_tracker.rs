// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_stream::wrappers::{IntervalStream, WatchStream};
use tracing::{debug, warn};

use restate_core::Metadata;
use restate_types::config::{Configuration, DurabilityMode};
use restate_types::identifiers::PartitionId;
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::nodes_config::Role;
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_types::time::MillisSinceEpoch;
use restate_wal_protocol::control::PartitionDurability;

const WARN_PERIOD: Duration = Duration::from_secs(60);

/// A stream that tracks the last reported durable Lsn, replica-set durable points, and
/// last archived lsn and emits a [`PartitionDurability`] when the durable Lsn changes.
///
/// The stream has an internal timer that is used to check regularly if the durable Lsn has
/// changed in the replica-set, but it'll react immediately to changed in the archived Lsn
/// watch.
pub struct DurabilityTracker {
    partition_id: PartitionId,
    last_reported_durable_lsn: Lsn,
    replica_set_states: PartitionReplicaSetStates,
    archived_lsn_watch: WatchStream<Option<Lsn>>,
    check_timer: IntervalStream,
    last_warning_at: Instant,
    /// cache of the last archived_lsn
    last_archived: Lsn,
    terminated: bool,
}

impl DurabilityTracker {
    pub fn new(
        partition_id: PartitionId,
        last_reported_durable_lsn: Option<Lsn>,
        replica_set_states: PartitionReplicaSetStates,
        archived_lsn_watch: watch::Receiver<Option<Lsn>>,
        check_interval: Duration,
    ) -> Self {
        let mut check_timer =
            tokio::time::interval_at(Instant::now() + check_interval, check_interval);
        check_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let check_timer = IntervalStream::new(check_timer);

        Self {
            partition_id,
            last_reported_durable_lsn: last_reported_durable_lsn.unwrap_or(Lsn::INVALID),
            replica_set_states,
            archived_lsn_watch: WatchStream::new(archived_lsn_watch),
            check_timer,
            last_warning_at: Instant::now() - WARN_PERIOD,
            last_archived: Lsn::INVALID,
            terminated: false,
        }
    }

    fn sanitize_durability_mode(
        &mut self,
        input_durability_mode: Option<DurabilityMode>,
    ) -> DurabilityMode {
        let configuration = Configuration::pinned();
        let has_snapshot_repository = configuration.worker.snapshots.destination.is_some();
        let is_cluster =
            Metadata::with_current(|m| m.nodes_config_ref().iter_role(Role::Worker).count() > 1);

        let require_snapshots_notice = |durability_mode| {
            if !has_snapshot_repository && self.last_warning_at.elapsed() > WARN_PERIOD {
                warn!(
                    %durability_mode,
                    "Detected cluster environment with no snapshot repository configured. \
                    Automatic log trimming is disabled, please refer to \
                    https://docs.restate.dev/operate/snapshots/ for more."
                );
            }
        };

        let risk_notice = |durability_mode| {
            if !has_snapshot_repository && self.last_warning_at.elapsed() > WARN_PERIOD {
                warn!(
                    "No snapshot repository is configured. \
                    Due to the configured durability mode '{durability_mode}', the cluster might \
                    not be able to move partitions to other nodes for fail-over or rebalancing. \
                    Please configure the cluster to use a shared snapshot repository. Please refer \
                    to https://docs.restate.dev/operate/snapshots/ for more."
                );
            }
        };

        // ## Special cases:
        // - A standalone node:
        //   - no snapshot repository configured. Trim after local durability -> `ReplicaSetOnly`
        //   - snapshot repository configured. Trim after a snapshot is taken + local durability -> `Balanced` || `SnapshotAndReplicaSet`
        //
        //   Simply, if snapshot is not configured, our options are:
        //   - None
        //   - ReplicaSetOnly
        //
        //   If snapshot is configured:
        //   - None
        //   - ReplicaSetOnly (not recommended, can't recover if cluster)
        //   - SnapshotAndReplicaSet (not supported / disabled)
        //   - SnapshotOnly
        let durability_mode = input_durability_mode.unwrap_or({
            // default depends on whether we have snapshot repository configured or not
            if has_snapshot_repository {
                DurabilityMode::Balanced
            } else {
                DurabilityMode::ReplicaSetOnly
            }
        });

        match durability_mode {
            DurabilityMode::None => {}
            DurabilityMode::ReplicaSetOnly if !is_cluster => {}
            DurabilityMode::ReplicaSetOnly => {
                // todo: remove when we support ad-hoc snapshot sharing
                risk_notice(durability_mode);
                self.last_warning_at = Instant::now();
            }
            DurabilityMode::SnapshotAndReplicaSet
            | DurabilityMode::SnapshotOnly
            | DurabilityMode::Balanced => {
                require_snapshots_notice(durability_mode);
                self.last_warning_at = Instant::now();
            }
        }
        durability_mode
    }
}

impl Stream for DurabilityTracker {
    type Item = PartitionDurability;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        // Making sure we always poll both streams to register the waker.
        let watch_tick = self.archived_lsn_watch.poll_next_unpin(cx);
        let timer_tick = self.check_timer.poll_next_unpin(cx);

        self.last_archived = match (watch_tick, timer_tick) {
            (Poll::Ready(None), _) | (_, Poll::Ready(None)) => {
                self.terminated = true;
                return Poll::Ready(None);
            }
            (Poll::Ready(Some(archived)), _) => archived.unwrap_or(Lsn::INVALID),
            (_, Poll::Ready(_)) => self.last_archived,
            (Poll::Pending, Poll::Pending) => return Poll::Pending,
        };

        let durability_mode =
            self.sanitize_durability_mode(Configuration::pinned().worker.durability_mode);
        let suggested = match durability_mode {
            DurabilityMode::None => {
                // Skip, maybe by next tick the durability mode changes
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            DurabilityMode::ReplicaSetOnly => self
                .replica_set_states
                .get_min_durable_lsn(self.partition_id),
            DurabilityMode::SnapshotAndReplicaSet => {
                let min_durable_lsn = self
                    .replica_set_states
                    .get_min_durable_lsn(self.partition_id);
                self.last_archived.min(min_durable_lsn)
            }
            // disabled until ad-hoc snapshot sharing is supported
            // DurabilityMode::SnapshotOrReplicaSet => {
            //     let min_durable_lsn = self
            //         .replica_set_states
            //         .get_min_durable_lsn(self.partition_id);
            //     self.last_archived.max(min_durable_lsn)
            // }
            DurabilityMode::SnapshotOnly => self.last_archived,
            DurabilityMode::Balanced => {
                let max_durable_lsn = self
                    .replica_set_states
                    .get_max_durable_lsn(self.partition_id);
                self.last_archived.min(max_durable_lsn)
            }
        };

        if suggested <= Lsn::INVALID || suggested <= self.last_reported_durable_lsn {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let partition_durability = PartitionDurability {
            partition_id: self.partition_id,
            durable_point: suggested,
            modification_time: MillisSinceEpoch::now(),
        };
        // We don't want to keep reporting the same durable Lsn over and over.
        self.last_reported_durable_lsn = suggested;

        if Configuration::pinned()
            .worker
            .experimental_partition_driven_log_trimming
        {
            debug!(
                partition_id = %self.partition_id,
                durability_mode = %durability_mode,
                "Reporting {suggested:?} as a durable point for partition"
            );

            Poll::Ready(Some(partition_durability))
        } else {
            debug!(
                partition_id = %self.partition_id,
                durability_mode = %durability_mode,
                "Would have reported {suggested:?} as a durable point for partition \
                    but 'experimental-partition-driven-log-trimming' feature is set to 'false'"
            );
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
