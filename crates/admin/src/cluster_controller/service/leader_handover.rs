// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Decides when the scheduler hands partition leadership to another replica.
//!
//! Leadership moves to a replica only once it is close to the log tail, because the partition has
//! no leader until the new leader has applied the log up to its own announcement. This module
//! holds the measurements (lag and replay rate of each replica) and the deferral state machine
//! that is shared by leader switches and by reconfigurations that remove the current leader. It
//! is pure logic over the polled partition processor statuses: all time comes from the statuses'
//! `updated_at`, so passes that reuse the same statuses do not advance anything.

use std::fmt;
use std::num::NonZeroU32;
use std::time::Duration;

use ahash::HashMap;
use tracing::{info, warn};

use restate_types::cluster::cluster_state::{
    LegacyClusterState, NodeState, PartitionProcessorStatus,
};
use restate_types::config::{AdminOptions, LeaderHandoverPolicy};
use restate_types::identifiers::PartitionId;
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::replication::NodeSet;
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, PlainNodeId, Version};

/// Weight of the newest sample in the smoothed replay rate.
const RATE_SMOOTHING: f64 = 0.5;

/// The leader handover options of one scheduling pass.
#[derive(Debug, Clone, Copy)]
pub struct HandoverOptions {
    pub policy: LeaderHandoverPolicy,
    pub max_catch_up_time: Duration,
    pub probe_interval: Duration,
    pub max_attempts: NonZeroU32,
}

impl From<&AdminOptions> for HandoverOptions {
    fn from(options: &AdminOptions) -> Self {
        Self {
            policy: options.leader_handover_policy,
            max_catch_up_time: options.leader_handover_max_catch_up_time.to_std(),
            probe_interval: options.leader_handover_probe_interval.to_std(),
            max_attempts: options.leader_handover_max_attempts,
        }
    }
}

/// The status of the partition's processor on `node`, together with the generation of the node
/// that reported it. `None` if the node is not alive in the polled state or doesn't run the
/// processor.
pub fn replica_status(
    legacy_cluster_state: &LegacyClusterState,
    node: PlainNodeId,
    partition_id: PartitionId,
) -> Option<(GenerationalNodeId, &PartitionProcessorStatus)> {
    match legacy_cluster_state.nodes.get(&node)? {
        NodeState::Alive(alive) => Some((
            alive.generational_node_id,
            alive.partitions.get(&partition_id)?,
        )),
        NodeState::Dead(_) => None,
    }
}

/// Replay rate of a replica in records per second.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReplayRate {
    /// Rate between the last two samples.
    pub sample: f64,
    /// Exponential moving average of the samples.
    pub smoothed: f64,
}

#[derive(Debug, Clone)]
struct RateTracker {
    generation: GenerationalNodeId,
    applied: Lsn,
    updated_at: MillisSinceEpoch,
    rate: Option<ReplayRate>,
}

impl RateTracker {
    fn new(generation: GenerationalNodeId, applied: Lsn, updated_at: MillisSinceEpoch) -> Self {
        Self {
            generation,
            applied,
            updated_at,
            rate: None,
        }
    }

    fn observe(
        &mut self,
        generation: GenerationalNodeId,
        applied: Lsn,
        updated_at: MillisSinceEpoch,
        probe_interval: Duration,
    ) {
        if generation != self.generation || applied < self.applied || updated_at < self.updated_at {
            *self = Self::new(generation, applied, updated_at);
            return;
        }
        let elapsed = updated_at.duration_since(self.updated_at);
        if elapsed < probe_interval || elapsed.is_zero() {
            return;
        }
        let sample = (applied.as_u64() - self.applied.as_u64()) as f64 / elapsed.as_secs_f64();
        let smoothed = self.rate.map_or(sample, |rate| {
            RATE_SMOOTHING * sample + (1.0 - RATE_SMOOTHING) * rate.smoothed
        });
        self.rate = Some(ReplayRate { sample, smoothed });
        self.applied = applied;
        self.updated_at = updated_at;
    }
}

/// Replay rates of the members of one partition.
#[derive(Debug, Clone, Default)]
pub struct ReplayRates(HashMap<PlainNodeId, RateTracker>);

impl ReplayRates {
    /// Takes a new sample of every member that reports an applied LSN, and forgets replicas that
    /// left the partition's membership.
    pub fn sample(
        &mut self,
        partition_id: PartitionId,
        current: &NodeSet,
        next: Option<&NodeSet>,
        legacy_cluster_state: &LegacyClusterState,
        probe_interval: Duration,
    ) {
        let is_member =
            |node: &PlainNodeId| current.contains(*node) || next.is_some_and(|n| n.contains(*node));
        self.0.retain(|node, _| is_member(node));

        for node in current
            .iter()
            .chain(next.into_iter().flat_map(NodeSet::iter))
        {
            let Some((generation, status)) =
                replica_status(legacy_cluster_state, *node, partition_id)
            else {
                continue;
            };
            let Some(applied) = status.last_applied_log_lsn else {
                continue;
            };
            self.0
                .entry(*node)
                .and_modify(|tracker| {
                    tracker.observe(generation, applied, status.updated_at, probe_interval)
                })
                .or_insert_with(|| RateTracker::new(generation, applied, status.updated_at));
        }
    }

    fn get(&self, node: PlainNodeId, generation: GenerationalNodeId) -> Option<ReplayRate> {
        self.0
            .get(&node)
            .filter(|tracker| tracker.generation == generation)
            .and_then(|tracker| tracker.rate)
    }
}

/// What the scheduler knows about a replica's progress in one pass.
#[derive(Debug, Clone, Copy)]
pub struct ReplicaProgress {
    pub generation: GenerationalNodeId,
    pub updated_at: MillisSinceEpoch,
    pub applied: Option<Lsn>,
    /// Records between the replica's applied LSN and the log tail.
    pub lag: Option<u64>,
    pub rate: Option<ReplayRate>,
}

impl ReplicaProgress {
    /// Lag divided by the smoothed replay rate. Zero if there is no lag, `None` if the lag is
    /// unknown or no positive rate is known.
    pub fn catch_up_time(&self) -> Option<Duration> {
        match (self.lag?, self.rate) {
            (0, _) => Some(Duration::ZERO),
            (lag, Some(rate)) if rate.smoothed > 0.0 => {
                Duration::try_from_secs_f64(lag as f64 / rate.smoothed).ok()
            }
            _ => None,
        }
    }

    /// The gap check: the replica is close enough to the log tail to take over leadership.
    pub fn is_close_enough(&self, max_catch_up_time: Duration) -> bool {
        self.lag == Some(0)
            || self
                .catch_up_time()
                .is_some_and(|catch_up_time| catch_up_time < max_catch_up_time)
    }
}

/// Progress of the replicas of one partition in one pass.
pub struct Progress<'a> {
    partition_id: PartitionId,
    legacy_cluster_state: &'a LegacyClusterState,
    rates: &'a ReplayRates,
    tail: Option<Lsn>,
}

impl<'a> Progress<'a> {
    /// The log tail is the highest tail any alive member reports.
    pub fn new(
        partition_id: PartitionId,
        legacy_cluster_state: &'a LegacyClusterState,
        rates: &'a ReplayRates,
        alive_members: impl Iterator<Item = PlainNodeId>,
    ) -> Self {
        let tail = alive_members
            .filter_map(|node| {
                replica_status(legacy_cluster_state, node, partition_id)?
                    .1
                    .target_tail_lsn
            })
            .max();
        Self {
            partition_id,
            legacy_cluster_state,
            rates,
            tail,
        }
    }

    pub fn tail(&self) -> Option<Lsn> {
        self.tail
    }

    pub fn of(&self, node: PlainNodeId) -> Option<ReplicaProgress> {
        let (generation, status) =
            replica_status(self.legacy_cluster_state, node, self.partition_id)?;
        let applied = status.last_applied_log_lsn;
        Some(ReplicaProgress {
            generation,
            updated_at: status.updated_at,
            applied,
            lag: lag(self.tail, applied),
            rate: self.rates.get(node, generation),
        })
    }

    pub fn lag(&self, node: PlainNodeId) -> Option<u64> {
        self.of(node)?.lag
    }

    /// The node with the smallest lag. Nodes with unknown lag rank last; ties go to the first
    /// node.
    pub fn least_lagging(&self, nodes: impl Iterator<Item = PlainNodeId>) -> Option<PlainNodeId> {
        nodes.min_by_key(|node| {
            let lag = self.lag(*node);
            (lag.is_none(), lag)
        })
    }

    /// The node that reports leader mode, the one with the highest observed leader epoch if
    /// several do.
    pub fn leading(&self, nodes: impl Iterator<Item = PlainNodeId>) -> Option<PlainNodeId> {
        nodes
            .filter(|node| {
                self.legacy_cluster_state
                    .runs_partition_processor_leader(node, &self.partition_id)
            })
            .max_by_key(|node| {
                replica_status(self.legacy_cluster_state, *node, self.partition_id)
                    .and_then(|(_, status)| status.last_observed_leader_epoch)
            })
    }

    /// Displays the lags of the given nodes, e.g. `[N1: 500, N2: unknown]`.
    pub fn display_lags(&self, nodes: impl Iterator<Item = PlainNodeId>) -> impl fmt::Display {
        let lags: Vec<_> = nodes.map(|node| (node, self.lag(node))).collect();
        fmt::from_fn(move |f| {
            f.write_str("[")?;
            for (i, (node, lag)) in lags.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{node}: {}", OrUnknown(*lag))?;
            }
            f.write_str("]")
        })
    }
}

/// Records between `applied` and the log tail, the formula of the applied LSN lag gauge.
fn lag(tail: Option<Lsn>, applied: Option<Lsn>) -> Option<u64> {
    Some(tail?.prev().as_u64().saturating_sub(applied?.as_u64()))
}

/// Handover state of one partition. All of it is in memory and starts empty whenever this node
/// becomes the cluster controller leader.
#[derive(Debug, Clone, Default)]
pub struct PartitionHandover {
    pub rates: ReplayRates,
    /// Deferred switch of the target leader to the preferred replica.
    pub leader_switch: Option<Deferral>,
    /// Deferred completion of a reconfiguration that removes the current leader.
    pub reconfiguration: Option<Deferral>,
}

/// What is deferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandoverKind {
    LeaderSwitch,
    Reconfiguration,
}

impl fmt::Display for HandoverKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            HandoverKind::LeaderSwitch => "leader switch",
            HandoverKind::Reconfiguration => "reconfiguration completion",
        })
    }
}

/// Why a deferral proceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProceedReason {
    CloseEnoughAtFirstCheck,
    CloseEnough,
    AttemptsUsedUp,
    LagUnknown,
}

impl fmt::Display for ProceedReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ProceedReason::CloseEnoughAtFirstCheck => "close enough at the first check",
            ProceedReason::CloseEnough => "close enough",
            ProceedReason::AttemptsUsedUp => "attempts used up",
            ProceedReason::LagUnknown => "lag unknown",
        })
    }
}

/// Why a deferral was dropped without proceeding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    LeadershipFrozen,
    PolicyImmediate,
    NoLeader,
    LeaderDied,
    LeaderNotLeading,
    LeaderIsPreferred,
    LeaderInNewReplicaSet,
    CandidateNotPreferred,
    CandidateRestarted,
    ConfigurationChanged,
    LagUnknown,
    ReconfigurationNotReady,
    ReconfigurationUngated,
}

impl fmt::Display for DropReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DropReason::LeadershipFrozen => "leadership frozen",
            DropReason::PolicyImmediate => "leader handover policy is immediate",
            DropReason::NoLeader => "no current leader",
            DropReason::LeaderDied => "current leader died",
            DropReason::LeaderNotLeading => "current leader stopped leading",
            DropReason::LeaderIsPreferred => "current leader is the preferred replica again",
            DropReason::LeaderInNewReplicaSet => {
                "current leader is a member of the new replica set"
            }
            DropReason::CandidateNotPreferred => "candidate is no longer the preferred replica",
            DropReason::CandidateRestarted => "candidate restarted",
            DropReason::ConfigurationChanged => "replica set or pending reconfiguration changed",
            DropReason::LagUnknown => "candidate lag unknown",
            DropReason::ReconfigurationNotReady => "reconfiguration no longer ready to complete",
            DropReason::ReconfigurationUngated => {
                "new replica set empty or all current workers disabled"
            }
        })
    }
}

/// Outcome of one pass of the deferral state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Switch the target leader or complete the reconfiguration now.
    Proceed(ProceedReason),
    /// Keep the current leader and keep waiting for the candidate.
    Defer,
    /// The candidate applied nothing since the last probe; keep the current leader.
    Hold,
}

impl Decision {
    pub fn is_proceed(self) -> bool {
        matches!(self, Decision::Proceed(_))
    }
}

impl fmt::Display for Decision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Decision::Proceed(reason) => write!(f, "proceed ({reason})"),
            Decision::Defer => f.write_str("keep deferring"),
            Decision::Hold => f.write_str("hold"),
        }
    }
}

/// Versions of the current and the pending partition configuration.
pub type ConfigurationVersions = (Version, Option<Version>);

/// The inputs of one pass of the deferral state machine.
pub struct Context {
    pub partition_id: PartitionId,
    pub kind: HandoverKind,
    pub leader: PlainNodeId,
    pub candidate: PlainNodeId,
    pub versions: ConfigurationVersions,
    pub tail: Option<Lsn>,
}

/// A deferred handover: the candidate catches up in rounds, each ending once it applied the log
/// up to the tail at the round's start.
#[derive(Debug, Clone)]
pub struct Deferral {
    candidate: PlainNodeId,
    candidate_generation: GenerationalNodeId,
    versions: ConfigurationVersions,
    round: u32,
    round_target: Lsn,
    round_start_lag: u64,
    last_probe_at: MillisSinceEpoch,
    last_probe_applied: Lsn,
    last_advanced_at: MillisSinceEpoch,
}

impl Deferral {
    pub fn candidate(&self) -> PlainNodeId {
        self.candidate
    }

    /// Whether the last probe found that the candidate applied nothing.
    pub fn is_stalled(&self) -> bool {
        self.last_advanced_at < self.last_probe_at
    }

    fn start_round(round: u32, ctx: &Context, m: &Measured) -> Self {
        Self {
            candidate: ctx.candidate,
            candidate_generation: m.progress.generation,
            versions: ctx.versions,
            round,
            round_target: m.tail,
            round_start_lag: m.lag,
            last_probe_at: m.progress.updated_at,
            last_probe_applied: m.applied,
            last_advanced_at: m.progress.updated_at,
        }
    }
}

/// The candidate's progress with every value the state machine needs known.
struct Measured<'a> {
    progress: &'a ReplicaProgress,
    tail: Lsn,
    applied: Lsn,
    lag: u64,
}

impl<'a> Measured<'a> {
    fn new(progress: Option<&'a ReplicaProgress>, tail: Option<Lsn>) -> Option<Self> {
        let progress = progress?;
        Some(Self {
            progress,
            tail: tail?,
            applied: progress.applied?,
            lag: progress.lag?,
        })
    }
}

/// Drops the deferral, if there is one, and logs why.
pub fn drop_deferral(
    deferral: &mut Option<Deferral>,
    partition_id: PartitionId,
    kind: HandoverKind,
    leader: Option<PlainNodeId>,
    reason: DropReason,
) {
    if let Some(dropped) = deferral.take() {
        info!(
            %partition_id,
            leader = %OrUnknown(leader),
            candidate = %dropped.candidate,
            round = dropped.round,
            %reason,
            "Dropped deferred {kind}"
        );
    }
}

/// Runs one pass of the deferral state machine for `ctx.candidate`, whose progress is
/// `progress`. Starts a deferral at the first check if the candidate is not close enough, and
/// clears it when it proceeds.
///
/// A different candidate than the deferral's (a reconfiguration whose candidate dropped out or
/// was overtaken by a member that is close enough) ends the current round.
pub fn evaluate(
    deferral: &mut Option<Deferral>,
    ctx: &Context,
    progress: Option<&ReplicaProgress>,
    options: &HandoverOptions,
) -> Decision {
    let measured = Measured::new(progress, ctx.tail);

    if let Some(d) = deferral.as_ref() {
        let reason = if d.versions != ctx.versions {
            Some(DropReason::ConfigurationChanged)
        } else if d.candidate == ctx.candidate
            && progress.is_some_and(|p| p.generation != d.candidate_generation)
        {
            Some(DropReason::CandidateRestarted)
        } else if measured.is_none() {
            Some(DropReason::LagUnknown)
        } else {
            None
        };
        if let Some(reason) = reason {
            drop_deferral(
                deferral,
                ctx.partition_id,
                ctx.kind,
                Some(ctx.leader),
                reason,
            );
        }
    }

    let decision = match (deferral.as_mut(), measured) {
        (_, None) => Decision::Proceed(ProceedReason::LagUnknown),
        (None, Some(m)) => {
            if m.progress.is_close_enough(options.max_catch_up_time) {
                Decision::Proceed(ProceedReason::CloseEnoughAtFirstCheck)
            } else {
                let d = Deferral::start_round(1, ctx, &m);
                info!(
                    partition_id = %ctx.partition_id,
                    leader = %ctx.leader,
                    candidate = %ctx.candidate,
                    lag = m.lag,
                    rate = %Rate(m.progress.rate.map(|r| r.smoothed)),
                    catch_up_time = %Secs(m.progress.catch_up_time()),
                    threshold = %Secs(Some(options.max_catch_up_time)),
                    round = d.round,
                    max_rounds = options.max_attempts.get(),
                    round_target_lsn = %d.round_target,
                    "Deferring {}: candidate is not close enough to the log tail",
                    ctx.kind
                );
                *deferral = Some(d);
                Decision::Defer
            }
        }
        (Some(d), Some(m)) => advance(d, ctx, &m, options),
    };

    if let Decision::Proceed(reason) = decision {
        *deferral = None;
        info!(
            partition_id = %ctx.partition_id,
            leader = %ctx.leader,
            candidate = %ctx.candidate,
            %reason,
            lag = %OrUnknown(progress.and_then(|p| p.lag)),
            catch_up_time = %Secs(progress.and_then(|p| p.catch_up_time())),
            "Proceeding with {}",
            ctx.kind
        );
    }
    decision
}

/// One pass of an existing deferral whose candidate's progress is known.
fn advance(
    d: &mut Deferral,
    ctx: &Context,
    m: &Measured<'_>,
    options: &HandoverOptions,
) -> Decision {
    let max_rounds = options.max_attempts.get();
    let close_enough = m.progress.is_close_enough(options.max_catch_up_time);
    let candidate_changed = d.candidate != ctx.candidate;

    // A round ends when the candidate applied the log up to the round's target, or when a
    // reconfiguration switched to a different candidate. A candidate that fell further behind
    // during the round still gets all rounds.
    if candidate_changed || m.applied.next() >= d.round_target {
        let decision = if close_enough {
            Decision::Proceed(ProceedReason::CloseEnough)
        } else if d.round >= max_rounds {
            Decision::Proceed(ProceedReason::AttemptsUsedUp)
        } else {
            Decision::Defer
        };
        info!(
            partition_id = %ctx.partition_id,
            leader = %ctx.leader,
            candidate = %ctx.candidate,
            round = d.round,
            max_rounds,
            lag = m.lag,
            round_start_lag = d.round_start_lag,
            catch_up_time = %Secs(m.progress.catch_up_time()),
            outcome = %decision,
            "Catch-up round of deferred {} ended{}",
            ctx.kind,
            fmt::from_fn(|f| if candidate_changed {
                write!(f, ": candidate changed from {}", d.candidate)
            } else {
                Ok(())
            })
        );
        if decision == Decision::Defer {
            *d = Deferral::start_round(d.round + 1, ctx, m);
            info!(
                partition_id = %ctx.partition_id,
                leader = %ctx.leader,
                candidate = %ctx.candidate,
                lag = m.lag,
                rate = %Rate(m.progress.rate.map(|r| r.smoothed)),
                catch_up_time = %Secs(m.progress.catch_up_time()),
                threshold = %Secs(Some(options.max_catch_up_time)),
                round = d.round,
                max_rounds,
                round_target_lsn = %d.round_target,
                "Starting next catch-up round of deferred {}",
                ctx.kind
            );
        }
        return decision;
    }

    if m.progress.updated_at < d.last_probe_at + options.probe_interval {
        return Decision::Defer;
    }

    // Probe
    let applied_nothing = m.applied <= d.last_probe_applied && m.lag > 0;
    let decision = if close_enough {
        Decision::Proceed(ProceedReason::CloseEnough)
    } else if applied_nothing {
        Decision::Hold
    } else {
        Decision::Defer
    };

    if decision == Decision::Hold {
        warn!(
            partition_id = %ctx.partition_id,
            leader = %ctx.leader,
            candidate = %ctx.candidate,
            lag = m.lag,
            since_last_advanced = %Secs(Some(m.progress.updated_at.duration_since(d.last_advanced_at))),
            "Holding deferred {}: candidate applied nothing since the last probe; the current leader keeps leading",
            ctx.kind
        );
    } else {
        info!(
            partition_id = %ctx.partition_id,
            leader = %ctx.leader,
            candidate = %ctx.candidate,
            round = d.round,
            lag = m.lag,
            round_start_lag = d.round_start_lag,
            rate_sample = %Rate(m.progress.rate.map(|r| r.sample)),
            rate = %Rate(m.progress.rate.map(|r| r.smoothed)),
            catch_up_time = %Secs(m.progress.catch_up_time()),
            outcome = %decision,
            "Probed candidate of deferred {}",
            ctx.kind
        );
    }

    if m.applied > d.last_probe_applied {
        d.last_advanced_at = m.progress.updated_at;
    }
    d.last_probe_at = m.progress.updated_at;
    d.last_probe_applied = m.applied;
    decision
}

/// Displays an optional value, `unknown` if it is missing.
pub struct OrUnknown<T>(pub Option<T>);

impl<T: fmt::Display> fmt::Display for OrUnknown<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(value) => value.fmt(f),
            None => f.write_str("unknown"),
        }
    }
}

/// Displays an optional duration in seconds.
struct Secs(Option<Duration>);

impl fmt::Display for Secs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(duration) => write!(f, "{:.1}s", duration.as_secs_f64()),
            None => f.write_str("unknown"),
        }
    }
}

/// Displays an optional rate in records per second.
struct Rate(Option<f64>);

impl fmt::Display for Rate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(rate) => write!(f, "{rate:.0}/s"),
            None => f.write_str("unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use restate_types::cluster::cluster_state::AliveNode;

    use super::*;

    const N1: PlainNodeId = PlainNodeId::new(1);
    const N2: PlainNodeId = PlainNodeId::new(2);

    fn options(max_attempts: u32) -> HandoverOptions {
        HandoverOptions {
            policy: LeaderHandoverPolicy::CatchUpTime,
            max_catch_up_time: Duration::from_secs(3),
            probe_interval: Duration::from_secs(10),
            max_attempts: NonZeroU32::new(max_attempts).unwrap(),
        }
    }

    fn secs(secs: u64) -> MillisSinceEpoch {
        MillisSinceEpoch::new(1_000_000 + secs * 1000)
    }

    /// Drives the state machine for candidate N2 of a leader switch away from N1.
    struct Driver {
        deferral: Option<Deferral>,
        options: HandoverOptions,
    }

    impl Driver {
        fn new(max_attempts: u32) -> Self {
            Self {
                deferral: None,
                options: options(max_attempts),
            }
        }

        /// One pass at `at` seconds in which the candidate applied up to `applied` and the log
        /// tail is `tail` (the next free LSN). Every pass is evaluated twice, like the scheduler
        /// does, and the second evaluation must not change anything.
        fn pass(&mut self, at: u64, applied: u64, tail: u64, rate: Option<f64>) -> Decision {
            let ctx = Context {
                partition_id: PartitionId::MIN,
                kind: HandoverKind::LeaderSwitch,
                leader: N1,
                candidate: N2,
                versions: (Version::MIN, None),
                tail: Some(Lsn::new(tail)),
            };
            let progress = ReplicaProgress {
                generation: N2.with_generation(1),
                updated_at: secs(at),
                applied: Some(Lsn::new(applied)),
                lag: lag(Some(Lsn::new(tail)), Some(Lsn::new(applied))),
                rate: rate.map(|rate| ReplayRate {
                    sample: rate,
                    smoothed: rate,
                }),
            };
            let decision = evaluate(&mut self.deferral, &ctx, Some(&progress), &self.options);
            if !decision.is_proceed() {
                let round = self.round();
                assert_eq!(
                    evaluate(&mut self.deferral, &ctx, Some(&progress), &self.options),
                    Decision::Defer
                );
                assert_eq!(self.round(), round);
            }
            decision
        }

        fn round(&self) -> Option<u32> {
            self.deferral.as_ref().map(|d| d.round)
        }
    }

    #[test]
    fn replay_rate_sampling() {
        let partition_id = PartitionId::MIN;
        let members: NodeSet = [N1].into_iter().collect();
        let probe_interval = Duration::from_secs(10);
        let mut rates = ReplayRates::default();
        let mut sample = |generation: u32, at: u64, applied: u64| {
            let mut legacy = LegacyClusterState::empty();
            let status = PartitionProcessorStatus {
                updated_at: secs(at),
                last_applied_log_lsn: Some(Lsn::new(applied)),
                ..Default::default()
            };
            legacy.nodes.insert(
                N1,
                NodeState::Alive(AliveNode {
                    last_heartbeat_at: secs(at),
                    generational_node_id: N1.with_generation(generation),
                    partitions: [(partition_id, status)].into_iter().collect(),
                    uptime: Duration::ZERO,
                }),
            );
            rates.sample(partition_id, &members, None, &legacy, probe_interval);
            rates.get(N1, N1.with_generation(generation))
        };

        // the first sample and samples less than a probe interval later give no rate
        assert_eq!(sample(1, 0, 1_000), None);
        assert_eq!(sample(1, 5, 2_000), None);
        // the rate uses the status timestamps, so repeating a status changes nothing
        let first = Some(ReplayRate {
            sample: 1_000.0,
            smoothed: 1_000.0,
        });
        assert_eq!(sample(1, 10, 11_000), first);
        assert_eq!(sample(1, 10, 11_000), first);
        // smoothing weighs the newest sample with 0.5
        assert_eq!(
            sample(1, 20, 41_000),
            Some(ReplayRate {
                sample: 3_000.0,
                smoothed: 2_000.0,
            })
        );
        // a new generation or an applied LSN that went backwards start over
        assert_eq!(sample(2, 30, 51_000), None);
        assert_eq!(sample(2, 40, 61_000).map(|r| r.smoothed), Some(1_000.0));
        assert_eq!(sample(2, 50, 10_000), None);
        // replicas that left the membership are forgotten
        rates.sample(
            partition_id,
            &NodeSet::default(),
            None,
            &LegacyClusterState::empty(),
            probe_interval,
        );
        assert!(rates.0.is_empty());
    }

    #[test]
    fn first_check() {
        // idle partition: no lag and no rate
        let mut driver = Driver::new(5);
        assert_eq!(
            driver.pass(0, 100, 101, Some(0.0)),
            Decision::Proceed(ProceedReason::CloseEnoughAtFirstCheck)
        );
        // 2_000 records behind at 1_000 records/s is below the 3 s threshold
        assert_eq!(
            driver.pass(0, 100, 2_101, Some(1_000.0)),
            Decision::Proceed(ProceedReason::CloseEnoughAtFirstCheck)
        );
        // without a rate, a replica that is behind is not close enough
        assert_eq!(driver.pass(0, 100, 2_101, None), Decision::Defer);
        assert_eq!(driver.round(), Some(1));

        // an unknown lag does not defer, and drops a running deferral
        let ctx = Context {
            partition_id: PartitionId::MIN,
            kind: HandoverKind::LeaderSwitch,
            leader: N1,
            candidate: N2,
            versions: (Version::MIN, None),
            tail: None,
        };
        assert_eq!(
            evaluate(&mut driver.deferral, &ctx, None, &driver.options),
            Decision::Proceed(ProceedReason::LagUnknown)
        );
        assert!(driver.deferral.is_none());
    }

    #[test]
    fn rounds_until_close_enough_or_attempts_used_up() {
        // 100_000 records behind, replaying 1_000 records/s faster than the log grows
        let mut driver = Driver::new(5);
        assert_eq!(driver.pass(0, 0, 100_001, Some(2_000.0)), Decision::Defer);
        assert_eq!(
            driver.pass(5, 10_000, 105_001, Some(2_000.0)),
            Decision::Defer
        );
        // probe: lag shrank from 100_000 to 90_000
        assert_eq!(
            driver.pass(10, 20_000, 110_001, Some(2_000.0)),
            Decision::Defer
        );
        assert_eq!(driver.round(), Some(1));
        // round 1 ends once the candidate reached the tail of the round's start
        assert_eq!(
            driver.pass(100, 100_000, 150_001, Some(2_000.0)),
            Decision::Defer
        );
        assert_eq!(driver.round(), Some(2));
        // the round end reset the probe, so no probe 1 s later
        assert_eq!(
            driver.pass(101, 100_100, 151_001, Some(2_000.0)),
            Decision::Defer
        );
        // a probe can proceed in the middle of a round
        assert_eq!(
            driver.pass(110, 147_000, 152_001, Some(2_000.0)),
            Decision::Proceed(ProceedReason::CloseEnough)
        );
        assert!(driver.deferral.is_none());

        // with two attempts, the switch is forced at the end of round 2
        let mut driver = Driver::new(2);
        assert_eq!(driver.pass(0, 0, 100_001, None), Decision::Defer);
        assert_eq!(driver.pass(9, 100_000, 150_001, None), Decision::Defer);
        assert_eq!(driver.round(), Some(2));
        assert_eq!(
            driver.pass(18, 150_000, 160_001, None),
            Decision::Proceed(ProceedReason::AttemptsUsedUp)
        );
    }

    #[test]
    fn growing_lag_and_hold() {
        // a candidate that falls further behind keeps deferring at probes and round ends, until
        // the attempts are used up
        let mut driver = Driver::new(2);
        assert_eq!(driver.pass(0, 0, 100_001, None), Decision::Defer);
        // probe: 110k records behind, more than the 100k at the round's start
        assert_eq!(
            driver.pass(10, 50_000, 160_001, Some(5_000.0)),
            Decision::Defer
        );
        // round 1 ends 120k records behind
        assert_eq!(
            driver.pass(20, 100_000, 220_001, Some(5_000.0)),
            Decision::Defer
        );
        assert_eq!(driver.round(), Some(2));
        assert_eq!(
            driver.pass(30, 160_000, 280_001, Some(6_000.0)),
            Decision::Defer
        );
        assert_eq!(
            driver.pass(44, 220_000, 340_001, Some(6_000.0)),
            Decision::Proceed(ProceedReason::AttemptsUsedUp)
        );

        // a candidate that applies nothing holds the handover
        let mut driver = Driver::new(5);
        assert_eq!(driver.pass(0, 0, 100_001, None), Decision::Defer);
        assert_eq!(driver.pass(10, 0, 110_001, Some(0.0)), Decision::Hold);
        assert_eq!(driver.pass(20, 0, 120_001, Some(0.0)), Decision::Hold);
        assert_eq!(driver.round(), Some(1));
        // once it applies again, it keeps deferring although it fell behind
        assert_eq!(
            driver.pass(30, 30_000, 140_001, Some(1_000.0)),
            Decision::Defer
        );
        assert_eq!(driver.round(), Some(1));
    }
}
