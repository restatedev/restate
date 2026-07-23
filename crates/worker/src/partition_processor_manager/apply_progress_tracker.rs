// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Detects a partition processor whose apply loop is lagging with no progress and no legitimate
//! busy work in progress ("apply-stall"), or whose loop has stopped scheduling entirely
//! ("loop-dead"), and drives the sticky quarantine signal that both downgrades the reported
//! status and eventually restarts the processor.
//!
//! This module is deliberately synchronous and clock-driven by the caller (the
//! `PartitionProcessorManager`): every type here is plain data plus pure transition functions, so
//! the whole state machine is unit-testable without spawning a processor or touching Bifrost. The
//! manager is responsible for turning [`TrackerEffect::IssueProbe`] into an actual
//! `ConsistentRead` tail probe and [`TrackerEffect::Bail`] into a `processor_state.stop()` call.
//!
//! Two independent "episode kinds" can quarantine a processor, each with its own clear proof
//! (never conflated -- see the module's design doc, DEFECTB):
//! - [`QuarantineEpisode::ApplyStall`]: born only from authoritative confirmed lag (a
//!   `ConsistentRead` probe, or -- from Stage 3 -- a committed `AnnounceLeader` marker that never
//!   applied). Clears only once the applied LSN passes `bail_lsn`.
//! - [`QuarantineEpisode::LoopDead`]: born from a stale-`InSelect` heartbeat (the loop has
//!   stopped scheduling). Clears as soon as the heartbeat is fresh again, regardless of LSN --
//!   a caught-up idle processor that restarts cleanly may never see a new LSN to clear against.
//!
//! `ApplyStall` always dominates `LoopDead`: a loop-dead sample never overwrites an active
//! `ApplyStall` episode, but confirmed lag may promote an existing `LoopDead` episode to
//! `ApplyStall`.

use std::time::Duration;

use tokio::time::Instant;
use ulid::Ulid;

use restate_types::cluster::cluster_state::ReplayStatus;
use restate_types::config::StallDetectionOptions;
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::time::MillisSinceEpoch;

use crate::partition::LoopPhase;

/// The manager's classification of a partition processor's apply loop for one tick, derived from
/// a [`HeartbeatView`] sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopState {
    /// Parked in `select`, and the heartbeat is fresh: genuinely waiting for work.
    ReaderIdle,
    /// Applying a batch or running a control operation, of any age. Busy is never itself
    /// evidence of a stall -- long-running legitimate work (e.g. leader init) looks the same.
    Busy,
    /// Parked in `select`, heartbeat neither fresh nor stale enough to call dead. Treated like a
    /// non-idle sample for detection purposes (never arms suspicion, never proves loop-death).
    Stale,
    /// Parked in `select` with a heartbeat stale enough that the loop provably cannot be
    /// running (see `hard_grace`).
    LoopDead,
}

/// Tracks one partition processor's [`LoopHeartbeat`](crate::partition::processor::LoopHeartbeat)
/// on the manager side: turns a raw `(phase, counter)` sample into a [`LoopState`] by locally
/// timestamping counter changes (never encoding an `Instant` into the shared atomic).
///
/// `(InSelect, 0)` is both the real `LoopHeartbeat`'s initial value *and* this view's
/// construction-time placeholder, so a freshly-constructed view (e.g. right after an incarnation
/// reset) must not be read as "just observed a fresh beat" -- that would be synthetic freshness
/// from having never actually sampled the new incarnation yet, not evidence of anything. Finding
/// 2 (DEFECTB review): `has_observed_change` gates that distinction.
#[derive(Debug, Clone, Copy)]
pub struct HeartbeatView {
    last_phase: LoopPhase,
    last_counter: u64,
    changed_at: Instant,
    has_observed_change: bool,
}

impl HeartbeatView {
    pub fn new(now: Instant) -> Self {
        Self {
            last_phase: LoopPhase::InSelect,
            last_counter: 0,
            changed_at: now,
            has_observed_change: false,
        }
    }

    /// Feed a fresh `(phase, counter)` sample taken from the processor's `LoopHeartbeat`.
    pub fn observe(&mut self, sample: (LoopPhase, u64), now: Instant) {
        if sample.1 != self.last_counter {
            self.changed_at = now;
            self.has_observed_change = true;
        }
        self.last_phase = sample.0;
        self.last_counter = sample.1;
    }

    pub fn loop_state(&self, now: Instant, cfg: &StallDetectionOptions) -> LoopState {
        let age = now.saturating_duration_since(self.changed_at);
        match self.last_phase {
            LoopPhase::ApplyingBatch | LoopPhase::ControlOp => LoopState::Busy,
            // Never report ReaderIdle from the construction-time placeholder alone -- only once a
            // genuine counter change has been observed from this incarnation. Staying at the
            // placeholder for `hard_grace` is still sound evidence of loop-death (the loop never
            // scheduled even once), so LoopDead does not need this gate.
            LoopPhase::InSelect if self.has_observed_change && age <= cfg.heartbeat_fresh() => {
                LoopState::ReaderIdle
            }
            LoopPhase::InSelect if age >= cfg.hard_grace() => LoopState::LoopDead,
            LoopPhase::InSelect => LoopState::Stale,
        }
    }

    pub fn phase(&self) -> LoopPhase {
        self.last_phase
    }

    pub fn phase_age(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.changed_at)
    }

    /// The raw `(phase, counter)` this view last observed. Lets a caller detect "has anything
    /// happened since this specific sample" by comparing against a fresh raw read, independent of
    /// `loop_state`'s freshness/gating logic (finding 4, DEFECTB re-review).
    pub fn last_sample(&self) -> (LoopPhase, u64) {
        (self.last_phase, self.last_counter)
    }
}

/// A single tail observation, fed by both the 1s `Fast` poller and the mandatory `ConsistentRead`
/// sweep. `last_consistent_attempt_at` is the A5 fairness input: it advances on every attempt
/// (success, error, *and* timeout), so a permanently-failing partition cannot monopolize the
/// single node-wide probe slot by "never succeeding" -- see
/// [`pick_next_consistent_read_sweep_target`].
#[derive(Debug, Clone, Copy)]
pub struct TailObservation {
    lsn: Lsn,
    observed_at: Instant,
    last_consistent_attempt_at: Instant,
}

impl TailObservation {
    pub fn new(lsn: Lsn, now: Instant) -> Self {
        Self {
            lsn,
            observed_at: now,
            last_consistent_attempt_at: now,
        }
    }

    pub fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// A `Fast` (possibly stale-cached) tail observation. May only arm suspicion, never confirm
    /// it -- see the module doc and the tracker's evidence rule.
    pub fn observe_fast(&mut self, lsn: Lsn, now: Instant) {
        if lsn > self.lsn {
            self.lsn = lsn;
        }
        self.observed_at = now;
    }

    /// A successful `ConsistentRead` observation: authoritative evidence. This is what reveals
    /// lag that a frozen `Fast` cache would otherwise hide forever (see the module doc) --
    /// `self.lsn` is monotonic across both observation kinds, so a sweep result that's higher
    /// than anything `Fast` has reported immediately becomes the new known tail.
    pub fn observe_consistent(&mut self, lsn: Lsn, now: Instant) {
        if lsn > self.lsn {
            self.lsn = lsn;
        }
        self.observed_at = now;
        self.last_consistent_attempt_at = now;
    }

    /// A `ConsistentRead` attempt that errored or timed out: advances the fairness clock (A5)
    /// without changing the known tail or its freshness.
    pub fn mark_consistent_attempt(&mut self, now: Instant) {
        self.last_consistent_attempt_at = now;
    }

    pub fn is_fresh(&self, now: Instant, tail_ttl: Duration) -> bool {
        now.saturating_duration_since(self.observed_at) <= tail_ttl
    }
}

/// A5: selects the running partition with the oldest `last_consistent_attempt_at` -- round-robin
/// over *attempts*, not successes, so a partition whose probes always error or time out still
/// yields its slot to others within a bounded number of sweep rounds.
pub fn pick_next_consistent_read_sweep_target<'a, Id: Copy + 'a>(
    observations: impl IntoIterator<Item = (Id, &'a TailObservation)>,
) -> Option<Id> {
    observations
        .into_iter()
        .min_by_key(|(_, obs)| obs.last_consistent_attempt_at)
        .map(|(id, _)| id)
}

/// A snapshot of everything the tracker needs to evaluate one partition for one tick.
#[derive(Debug, Clone, Copy)]
pub struct TickInput {
    pub last_applied_lsn: Option<Lsn>,
    pub replay_status: ReplayStatus,
    pub loop_state: LoopState,
    /// The freshest tail observation, if one exists within `tail_ttl`; `None` if stale or absent.
    /// May come from either `Fast` or `ConsistentRead` -- either is enough to *arm* suspicion
    /// (Healthy -> Suspect). Only an explicit, active `ConsistentRead` probe result
    /// (`on_probe_result`) can ever *confirm* it, which is what actually creates an
    /// `ApplyStall` episode -- so this field's provenance doesn't need tracking here.
    pub fresh_tail: Option<Lsn>,
}

pub enum ProbeResult {
    /// The probe completed and observed this tail. Whether it's still ahead of the last applied
    /// LSN (still lagging) or not (the reader had already caught up) is decided by
    /// `on_probe_result` re-reading `last_known_lsn` at completion time, not carried here.
    Confirmed {
        tail_lsn: Lsn,
    },
    Failed,
}

/// What the manager must do in response to a tracker transition. The tracker never performs I/O
/// itself -- the manager owns spawning probes and calling `processor_state.stop()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackerEffect {
    None,
    /// Issue one `ConsistentRead` probe tagged with this incarnation and nonce.
    IssueProbe {
        incarnation: Ulid,
        nonce: u64,
    },
    /// Call `processor_state.stop()` now. The tracker has already updated its own quarantine and
    /// restart-history bookkeeping.
    Bail,
}

#[derive(Debug, Clone, Copy)]
enum Detect {
    Healthy,
    Suspect {
        since: Instant,
        next_probe_at: Instant,
        probe_backoff: Duration,
    },
    Confirming {
        since: Instant,
        incarnation: Ulid,
        nonce: u64,
        /// Carried forward from the `Suspect` state that spawned this probe so a failed probe
        /// can double it (capped) without needing separate bookkeeping on `TrackerEntry`.
        probe_backoff: Duration,
    },
    Observing {
        since: Instant,
        until: Instant,
    },
    Stalled {
        since: Instant,
    },
}

/// The sticky, generation-surviving quarantine signal. Not reset by an incarnation change --
/// only cleared by its own kind's clear proof (see the module doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuarantineEpisode {
    ApplyStall {
        since: MillisSinceEpoch,
        bail_lsn: Lsn,
        bails: u32,
    },
    LoopDead {
        since: MillisSinceEpoch,
        bails: u32,
    },
}

impl QuarantineEpisode {
    pub fn since(&self) -> MillisSinceEpoch {
        match self {
            QuarantineEpisode::ApplyStall { since, .. } => *since,
            QuarantineEpisode::LoopDead { since, .. } => *since,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RestartHistory {
    count: u32,
    bail_lsn: Option<Lsn>,
    next_permitted_bail_at: Instant,
    next_backoff: Duration,
}

impl RestartHistory {
    fn fresh(now: Instant, cfg: &StallDetectionOptions) -> Self {
        Self {
            count: 0,
            bail_lsn: None,
            next_permitted_bail_at: now,
            next_backoff: cfg.bail_backoff_base(),
        }
    }
}

/// Per-partition apply-progress state, owned by the `PartitionProcessorManager`. See the module
/// doc for the overall design.
#[derive(Debug, Clone, Copy)]
pub struct TrackerEntry {
    incarnation: Ulid,
    last_known_lsn: Option<Lsn>,
    last_progress_at: Instant,
    /// Finding 1 (DEFECTB review): when the *current, unbroken* streak of reader-idle + lagging
    /// samples began, while `detect == Healthy`. Reset to `None` by any Busy/Stale sample or by
    /// the tail no longer looking ahead -- `last_progress_at` alone is not enough, since it only
    /// advances on real progress and can be arbitrarily old after a long legitimate `ApplyingBatch`,
    /// which would let a single idle+lagging sample satisfy `>= grace` immediately.
    reader_idle_since: Option<Instant>,
    detect: Detect,
    quarantine: Option<QuarantineEpisode>,
    restart_history: RestartHistory,
    next_nonce: u64,
}

impl TrackerEntry {
    pub fn new(incarnation: Ulid, now: Instant, cfg: &StallDetectionOptions) -> Self {
        Self {
            incarnation,
            last_known_lsn: None,
            last_progress_at: now,
            reader_idle_since: None,
            detect: Detect::Healthy,
            quarantine: None,
            restart_history: RestartHistory::fresh(now, cfg),
            next_nonce: 0,
        }
    }

    pub fn quarantine(&self) -> Option<QuarantineEpisode> {
        self.quarantine
    }

    pub fn is_quarantined(&self) -> bool {
        self.quarantine.is_some()
    }

    pub fn last_known_lsn(&self) -> Option<Lsn> {
        self.last_known_lsn
    }

    /// Rule 8: a new processor incarnation started. Progress and live detection reset (fresh
    /// grace period); the sticky quarantine and restart history are retained; any outstanding
    /// probe is implicitly orphaned because its tag will no longer match.
    pub fn on_incarnation_started(&mut self, now: Instant, incarnation: Ulid) {
        self.incarnation = incarnation;
        self.last_known_lsn = None;
        self.last_progress_at = now;
        self.reader_idle_since = None;
        self.detect = Detect::Healthy;
    }

    /// Evaluate one sample (status watch, tail observation, and heartbeat, all resolved to a
    /// point-in-time view by the caller). Called from the manager's `tracker_tick` and whenever a
    /// fresh observation arrives.
    pub fn on_sample(
        &mut self,
        now: Instant,
        input: TickInput,
        cfg: &StallDetectionOptions,
    ) -> TrackerEffect {
        // C2/rule 10: a LoopDead episode clears on any fresh heartbeat, regardless of LSN -- a
        // caught-up idle processor may never see new work to prove progress against. Checked
        // unconditionally, ahead of the progress branch below: after an incarnation reset,
        // `last_known_lsn` is `None`, so the very first sample always looks like "progress" (rule
        // 1) even when the LSN is unchanged from before the restart -- that must not suppress
        // this clear.
        if matches!(self.quarantine, Some(QuarantineEpisode::LoopDead { .. }))
            && input.loop_state == LoopState::ReaderIdle
        {
            self.quarantine = None;
        }

        // Rule 1: progress dominates every other transition.
        if let Some(lsn) = input.last_applied_lsn
            && self.last_known_lsn.is_none_or(|known| lsn > known)
        {
            self.last_known_lsn = Some(lsn);
            self.last_progress_at = now;
            self.reader_idle_since = None;
            self.detect = Detect::Healthy;
            if let Some(QuarantineEpisode::ApplyStall { bail_lsn, .. }) = self.quarantine
                && lsn > bail_lsn
            {
                self.quarantine = None;
            }
            if let Some(bail_lsn) = self.restart_history.bail_lsn
                && lsn > bail_lsn
            {
                self.restart_history = RestartHistory::fresh(now, cfg);
            }
            return TrackerEffect::None;
        }

        // A3: a loop-dead observation is independent evidence, evaluated before (and without
        // disturbing the kind of) any active ApplyStall episode.
        if input.loop_state == LoopState::LoopDead {
            return self.handle_loop_dead_sample(now);
        }

        self.advance_lag_detection(now, input, cfg)
    }

    fn handle_loop_dead_sample(&mut self, now: Instant) -> TrackerEffect {
        if matches!(self.quarantine, Some(QuarantineEpisode::ApplyStall { .. })) {
            // A3: never let a loop-dead sample overwrite an active ApplyStall episode's kind.
            // It's still independent grounds to (re-)attempt a bail, subject to the same spacing.
            return self.propose_bail(now);
        }

        let (since, bails) = match self.quarantine {
            Some(QuarantineEpisode::LoopDead { since, bails }) => (since, bails),
            _ => (MillisSinceEpoch::now(), 0),
        };
        self.quarantine = Some(QuarantineEpisode::LoopDead { since, bails });
        self.propose_bail(now)
    }

    fn advance_lag_detection(
        &mut self,
        now: Instant,
        input: TickInput,
        cfg: &StallDetectionOptions,
    ) -> TrackerEffect {
        let idle = input.loop_state == LoopState::ReaderIdle;
        let lagging = |known: Option<Lsn>| {
            input
                .fresh_tail
                .zip(known)
                .is_some_and(|(tail, known)| tail.prev() > known)
        };

        match self.detect {
            Detect::Healthy => {
                let eligible_replay = matches!(
                    input.replay_status,
                    ReplayStatus::Active | ReplayStatus::CatchingUp
                );
                let idle_and_lagging = idle
                    && eligible_replay
                    && self.last_known_lsn.is_some()
                    && lagging(self.last_known_lsn);

                if !idle_and_lagging {
                    // Finding 1: any Busy/Stale sample, or the tail no longer looking ahead,
                    // resets the streak -- a single idle+lagging sample right after a long
                    // legitimate `ApplyingBatch` must not satisfy the grace window instantly.
                    self.reader_idle_since = None;
                    return TrackerEffect::None;
                }

                let since = *self.reader_idle_since.get_or_insert(now);
                if now.saturating_duration_since(since) >= cfg.grace() {
                    self.detect = Detect::Suspect {
                        since: now,
                        next_probe_at: now,
                        probe_backoff: cfg.probe_backoff_base(),
                    };
                }
                TrackerEffect::None
            }
            Detect::Suspect {
                since,
                next_probe_at,
                probe_backoff,
            } => {
                if !idle || !lagging(self.last_known_lsn) {
                    // Rule 2: any Busy/stale sample, or the tail no longer looking ahead (stale
                    // observation expiry included), restarts the window from Healthy. Clear the
                    // idle streak too, so a stale timestamp from *before* Suspect was entered
                    // can't let a subsequent idle+lagging sample skip the grace window (finding 1).
                    self.reader_idle_since = None;
                    self.detect = Detect::Healthy;
                    return TrackerEffect::None;
                }
                if now >= next_probe_at {
                    let nonce = self.next_nonce;
                    self.next_nonce += 1;
                    self.detect = Detect::Confirming {
                        since,
                        incarnation: self.incarnation,
                        nonce,
                        probe_backoff,
                    };
                    TrackerEffect::IssueProbe {
                        incarnation: self.incarnation,
                        nonce,
                    }
                } else {
                    // Keep the same backoff/next_probe_at; nothing to do yet.
                    TrackerEffect::None
                }
            }
            Detect::Confirming { .. } => {
                // Only probe completion (`on_probe_result`) drives this state forward.
                TrackerEffect::None
            }
            Detect::Observing { since, until } => {
                if !idle {
                    // Rule 5: Busy during the recovery window means the reader resumed on its
                    // own; give it a fresh probe opportunity rather than declaring it stalled.
                    self.detect = Detect::Suspect {
                        since: now,
                        next_probe_at: now,
                        probe_backoff: cfg.probe_backoff_base(),
                    };
                    return TrackerEffect::None;
                }
                if now >= until {
                    self.detect = Detect::Stalled { since };
                    self.enter_stalled();
                }
                TrackerEffect::None
            }
            Detect::Stalled { since } => {
                if !idle {
                    // Rule 6: Stalled suspension -- demote, quarantine stays (only rule 1 clears
                    // ApplyStall).
                    self.detect = Detect::Suspect {
                        since: now,
                        next_probe_at: now,
                        probe_backoff: cfg.probe_backoff_base(),
                    };
                    return TrackerEffect::None;
                }
                let bail_deadline =
                    (since + cfg.bail_grace()).max(self.restart_history.next_permitted_bail_at);
                if now >= bail_deadline {
                    if lagging(self.last_known_lsn) {
                        self.propose_bail(now)
                    } else {
                        // Revalidation failed at the moment we'd act: demote instead of bailing
                        // on stale evidence (guardrail: never act without confirmed lag).
                        self.detect = Detect::Suspect {
                            since: now,
                            next_probe_at: now,
                            probe_backoff: cfg.probe_backoff_base(),
                        };
                        TrackerEffect::None
                    }
                } else {
                    TrackerEffect::None
                }
            }
        }
    }

    /// Rule 5: Observing -> Stalled. Creates/extends the ApplyStall episode; this and Stage 3's
    /// `AnnounceNotApplied` path (A2) are the only two legitimate ApplyStall birth sites.
    fn enter_stalled(&mut self) {
        let bail_lsn = self.last_known_lsn.unwrap_or(Lsn::INVALID);
        self.quarantine = Some(match self.quarantine {
            // A3: promote an existing LoopDead episode rather than losing its history.
            Some(QuarantineEpisode::LoopDead { bails, .. })
            | Some(QuarantineEpisode::ApplyStall { bails, .. }) => QuarantineEpisode::ApplyStall {
                since: MillisSinceEpoch::now(),
                bail_lsn,
                bails,
            },
            None => QuarantineEpisode::ApplyStall {
                since: MillisSinceEpoch::now(),
                bail_lsn,
                bails: 0,
            },
        });
    }

    /// Rule 3/4: feed a completed `ConsistentRead` probe result. Discards results whose tag no
    /// longer matches the current incarnation/nonce (superseded or orphaned by an incarnation
    /// change).
    pub fn on_probe_result(
        &mut self,
        now: Instant,
        incarnation: Ulid,
        nonce: u64,
        result: ProbeResult,
        cfg: &StallDetectionOptions,
    ) {
        let Detect::Confirming {
            since,
            incarnation: expected_incarnation,
            nonce: expected_nonce,
            probe_backoff,
        } = self.detect
        else {
            return;
        };
        if incarnation != expected_incarnation || nonce != expected_nonce {
            return;
        }

        match result {
            ProbeResult::Failed => {
                // Rule 3: stay Suspect, backing off (capped) before the next probe attempt.
                self.detect = Detect::Suspect {
                    since,
                    next_probe_at: now + probe_backoff,
                    probe_backoff: (probe_backoff * 2).min(cfg.probe_backoff_cap()),
                };
            }
            ProbeResult::Confirmed { tail_lsn } => {
                if self
                    .last_known_lsn
                    .is_none_or(|known| tail_lsn.prev() > known)
                {
                    // Rule 4: the probe may itself have healed the reader; give it a bounded
                    // recovery window before declaring Stalled.
                    self.detect = Detect::Observing {
                        since,
                        until: now + cfg.recovery_window(),
                    };
                } else {
                    self.detect = Detect::Healthy;
                }
            }
        }
    }

    /// Rule 7 / the loop-dead bail path: proposes a bail if the spacing
    /// (`next_permitted_bail_at`) allows one. Read-only -- does **not** mutate any state,
    /// including `next_permitted_bail_at` itself. The manager must revalidate reader-idle (and,
    /// for an `ApplyStall` episode, confirmed lag) immediately before calling `stop()` (finding 4,
    /// DEFECTB review): [`Self::commit_bail`] if revalidation still holds, or nothing otherwise --
    /// the tracker's state is untouched either way, so the next tick re-evaluates from scratch.
    fn propose_bail(&self, now: Instant) -> TrackerEffect {
        if now < self.restart_history.next_permitted_bail_at {
            TrackerEffect::None
        } else {
            TrackerEffect::Bail
        }
    }

    /// Commits a bail the manager has freshly revalidated immediately before calling `stop()`
    /// (finding 4). Bumps the active quarantine episode's `bails` count (creating an `ApplyStall`
    /// episode if none was active yet) and the restart-history backoff.
    pub fn commit_bail(&mut self, now: Instant, bail_lsn: Lsn, cfg: &StallDetectionOptions) {
        self.quarantine = Some(match self.quarantine {
            Some(QuarantineEpisode::ApplyStall { since, bails, .. }) => {
                QuarantineEpisode::ApplyStall {
                    since,
                    bail_lsn,
                    bails: bails + 1,
                }
            }
            Some(QuarantineEpisode::LoopDead { since, bails }) => QuarantineEpisode::LoopDead {
                since,
                bails: bails + 1,
            },
            None => QuarantineEpisode::ApplyStall {
                since: MillisSinceEpoch::now(),
                bail_lsn,
                bails: 1,
            },
        });

        self.restart_history.count += 1;
        self.restart_history.bail_lsn = Some(bail_lsn);
        self.restart_history.next_permitted_bail_at = now + self.restart_history.next_backoff;
        self.restart_history.next_backoff =
            (self.restart_history.next_backoff * 2).min(cfg.bail_backoff_cap());
    }

    /// A2/A4 (Option D): records a Candidate-watchdog bail. A committed-but-unapplied
    /// `AnnounceLeader` marker is itself authoritative lag evidence -- a second legitimate birth
    /// site for `ApplyStall`, so this always creates or promotes to `ApplyStall` (unlike a
    /// loop-dead bail, which preserves whatever episode kind was already active). Uses the exact
    /// FSM-captured `bail_lsn` the caller hands in, never a possibly-stale tracker sample (A4).
    ///
    /// Unlike [`Self::try_bail`], there is no `next_permitted_bail_at` gate here: the partition
    /// processor has already exited by the time this runs (the watchdog's deadline firing *is*
    /// the bail), so this only records the episode and restart-history bookkeeping for the
    /// manager's restart-delay decision and any future bail.
    pub fn on_announce_not_applied(
        &mut self,
        now: Instant,
        bail_lsn: Lsn,
        cfg: &StallDetectionOptions,
    ) {
        let bails = match self.quarantine {
            Some(QuarantineEpisode::ApplyStall { bails, .. }) => bails,
            Some(QuarantineEpisode::LoopDead { bails, .. }) => bails,
            None => 0,
        };
        let since = match self.quarantine {
            Some(QuarantineEpisode::ApplyStall { since, .. }) => since,
            _ => MillisSinceEpoch::now(),
        };
        self.quarantine = Some(QuarantineEpisode::ApplyStall {
            since,
            bail_lsn,
            bails: bails + 1,
        });

        self.restart_history.count += 1;
        self.restart_history.bail_lsn = Some(bail_lsn);
        self.restart_history.next_permitted_bail_at = now + self.restart_history.next_backoff;
        self.restart_history.next_backoff =
            (self.restart_history.next_backoff * 2).min(cfg.bail_backoff_cap());
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use ulid::Ulid;

    use super::*;

    fn cfg() -> StallDetectionOptions {
        // The production defaults happen to already be small/distinguishable enough to drive
        // deterministically in tests; used directly rather than duplicating them here.
        StallDetectionOptions::default()
    }

    fn idle_lagging(lsn_ahead_of: Lsn) -> TickInput {
        TickInput {
            last_applied_lsn: Some(lsn_ahead_of),
            replay_status: ReplayStatus::Active,
            loop_state: LoopState::ReaderIdle,
            fresh_tail: Some(lsn_ahead_of.next().next()),
        }
    }

    fn busy_sample(last_applied: Lsn) -> TickInput {
        TickInput {
            last_applied_lsn: Some(last_applied),
            replay_status: ReplayStatus::Active,
            loop_state: LoopState::Busy,
            fresh_tail: None,
        }
    }

    fn loop_dead_sample(last_applied: Option<Lsn>) -> TickInput {
        TickInput {
            last_applied_lsn: last_applied,
            replay_status: ReplayStatus::Active,
            loop_state: LoopState::LoopDead,
            fresh_tail: None,
        }
    }

    /// Full-table state machine test: Healthy -> Suspect (grace) -> Confirming (probe) ->
    /// Observing (recovery window) -> Stalled -> bail, plus the ApplyStall clear proof (progress
    /// past bail_lsn), the "any Busy/stale sample restarts the window" rule, and probe
    /// single-flight/backoff (stale nonce discarded, failure backs off).
    #[test]
    fn tracker_state_machine_unit() {
        let cfg = cfg();
        let t0 = Instant::now();
        let incarnation = Ulid::new();
        let mut tracker = TrackerEntry::new(incarnation, t0, &cfg);

        // Starting point: some initial progress at LSN 10.
        let effect = tracker.on_sample(
            t0,
            TickInput {
                last_applied_lsn: Some(Lsn::new(10)),
                replay_status: ReplayStatus::Active,
                loop_state: LoopState::ReaderIdle,
                fresh_tail: Some(Lsn::new(11)),
            },
            &cfg,
        );
        assert_eq!(effect, TrackerEffect::None);
        assert_eq!(tracker.last_known_lsn(), Some(Lsn::new(10)));

        // A Busy sample midway through what would otherwise be a grace window must not arm
        // suspicion (rule 2: "any Busy/stale sample restarts it").
        let mid_grace = t0 + Duration::from_secs(15);
        let effect = tracker.on_sample(mid_grace, busy_sample(Lsn::new(10)), &cfg);
        assert_eq!(effect, TrackerEffect::None);
        assert!(!tracker.is_quarantined());

        // Finding 1 (DEFECTB review): a *single* idle+lagging sample taken long after the last
        // progress must not immediately arm suspicion -- the grace window requires a continuous
        // idle+lagging streak, timed from when that streak actually started, not from
        // `last_progress_at` (which the preceding long Busy period left arbitrarily stale).
        let idle_streak_starts = mid_grace + Duration::from_secs(1);
        let effect = tracker.on_sample(idle_streak_starts, idle_lagging(Lsn::new(10)), &cfg);
        assert_eq!(effect, TrackerEffect::None);
        assert!(
            matches!(tracker.detect, Detect::Healthy),
            "one idle+lagging sample right after a long Busy period must not skip the grace window"
        );

        // Now sit reader-idle + lagging continuously for a full grace period from when the streak
        // actually started.
        let grace_elapsed = idle_streak_starts + cfg.grace() + Duration::from_secs(1);
        let effect = tracker.on_sample(grace_elapsed, idle_lagging(Lsn::new(10)), &cfg);
        assert_eq!(effect, TrackerEffect::None);
        assert!(matches!(tracker.detect, Detect::Suspect { .. }));

        // Suspect -> Confirming: issues exactly one probe.
        let probe_time = grace_elapsed + Duration::from_millis(1);
        let effect = tracker.on_sample(probe_time, idle_lagging(Lsn::new(10)), &cfg);
        let TrackerEffect::IssueProbe {
            incarnation: probe_incarnation,
            nonce,
        } = effect
        else {
            panic!("expected IssueProbe, got {effect:?}");
        };
        assert_eq!(probe_incarnation, incarnation);

        // A stale-tagged probe result (wrong nonce) must be discarded, not drive a transition.
        tracker.on_probe_result(
            probe_time,
            incarnation,
            nonce + 1000,
            ProbeResult::Confirmed {
                tail_lsn: Lsn::new(12),
            },
            &cfg,
        );
        assert!(matches!(tracker.detect, Detect::Confirming { .. }));

        // A failed probe backs off and stays Suspect.
        tracker.on_probe_result(probe_time, incarnation, nonce, ProbeResult::Failed, &cfg);
        let Detect::Suspect {
            next_probe_at,
            probe_backoff,
            ..
        } = tracker.detect
        else {
            panic!("expected Suspect after a failed probe");
        };
        assert_eq!(next_probe_at, probe_time + cfg.probe_backoff_base());
        assert_eq!(probe_backoff, cfg.probe_backoff_base() * 2);

        // Retry the probe after backoff; this time confirm the lag.
        let retry_time = next_probe_at;
        let effect = tracker.on_sample(retry_time, idle_lagging(Lsn::new(10)), &cfg);
        let TrackerEffect::IssueProbe {
            nonce: retry_nonce, ..
        } = effect
        else {
            panic!("expected a retried probe, got {effect:?}");
        };
        tracker.on_probe_result(
            retry_time,
            incarnation,
            retry_nonce,
            ProbeResult::Confirmed {
                tail_lsn: Lsn::new(12),
            },
            &cfg,
        );
        let Detect::Observing { until, .. } = tracker.detect else {
            panic!("expected Observing after a confirmed probe");
        };
        assert_eq!(until, retry_time + cfg.recovery_window());

        // Still idle, no progress once the recovery window elapses -> Stalled, quarantined.
        let stalled_time = until;
        let effect = tracker.on_sample(stalled_time, idle_lagging(Lsn::new(10)), &cfg);
        assert_eq!(effect, TrackerEffect::None);
        assert!(matches!(tracker.detect, Detect::Stalled { .. }));
        let Some(QuarantineEpisode::ApplyStall {
            bail_lsn, bails, ..
        }) = tracker.quarantine()
        else {
            panic!("expected an ApplyStall quarantine episode");
        };
        assert_eq!(bail_lsn, Lsn::new(10));
        assert_eq!(bails, 0);

        // Bail fires once bail_grace has elapsed from the Stalled entry. `on_sample` only
        // *proposes* the bail (finding 4, DEFECTB review); the manager must revalidate and then
        // call `commit_bail` immediately before actually stopping the processor.
        let bail_time = stalled_time + cfg.bail_grace() + Duration::from_secs(1);
        let effect = tracker.on_sample(bail_time, idle_lagging(Lsn::new(10)), &cfg);
        assert_eq!(effect, TrackerEffect::Bail);
        tracker.commit_bail(bail_time, Lsn::new(10), &cfg);
        let Some(QuarantineEpisode::ApplyStall {
            bail_lsn, bails, ..
        }) = tracker.quarantine()
        else {
            panic!("expected the ApplyStall episode to persist across the bail");
        };
        assert_eq!(bail_lsn, Lsn::new(10));
        assert_eq!(bails, 1);

        // Simulate the incarnation change after restart: fresh grace, quarantine retained.
        let restart_time = bail_time + Duration::from_millis(1);
        let new_incarnation = Ulid::new();
        tracker.on_incarnation_started(restart_time, new_incarnation);
        assert!(
            tracker.is_quarantined(),
            "quarantine must survive a restart"
        );
        assert_eq!(
            tracker.last_known_lsn(),
            None,
            "progress resets on incarnation change"
        );

        // Real progress (past bail_lsn) is the only thing that clears an ApplyStall episode.
        let progress_time = restart_time + Duration::from_secs(1);
        let effect = tracker.on_sample(
            progress_time,
            TickInput {
                last_applied_lsn: Some(Lsn::new(11)),
                replay_status: ReplayStatus::CatchingUp,
                loop_state: LoopState::ReaderIdle,
                fresh_tail: Some(Lsn::new(13)),
            },
            &cfg,
        );
        assert_eq!(effect, TrackerEffect::None);
        assert!(
            !tracker.is_quarantined(),
            "progress past bail_lsn must clear the ApplyStall episode"
        );
    }

    /// C2/rule 10: a caught-up idle processor that goes loop-dead, is bailed, and restarts with a
    /// fresh heartbeat and no new records must have its quarantine cleared on heartbeat recovery
    /// alone -- there may be no new LSN to prove progress against.
    #[test]
    fn loop_dead_idle_clears_on_restart() {
        let cfg = cfg();
        let t0 = Instant::now();
        let incarnation = Ulid::new();
        let mut tracker = TrackerEntry::new(incarnation, t0, &cfg);

        // Establish a known LSN so bail_lsn / progress checks have something to compare against.
        tracker.on_sample(
            t0,
            TickInput {
                last_applied_lsn: Some(Lsn::new(5)),
                replay_status: ReplayStatus::Active,
                loop_state: LoopState::ReaderIdle,
                fresh_tail: Some(Lsn::new(6)),
            },
            &cfg,
        );

        let dead_time = t0 + cfg.hard_grace() + Duration::from_secs(1);
        let effect = tracker.on_sample(dead_time, loop_dead_sample(Some(Lsn::new(5))), &cfg);
        assert_eq!(effect, TrackerEffect::Bail);
        assert!(matches!(
            tracker.quarantine(),
            Some(QuarantineEpisode::LoopDead { .. })
        ));

        // New incarnation after restart; no new records arrive (idle at tail).
        let restart_time = dead_time + Duration::from_millis(1);
        let new_incarnation = Ulid::new();
        tracker.on_incarnation_started(restart_time, new_incarnation);
        assert!(
            tracker.is_quarantined(),
            "quarantine must survive the restart"
        );

        let idle_after_restart = restart_time + Duration::from_secs(1);
        let effect = tracker.on_sample(
            idle_after_restart,
            TickInput {
                last_applied_lsn: Some(Lsn::new(5)), // unchanged: nothing new to apply
                replay_status: ReplayStatus::Active,
                loop_state: LoopState::ReaderIdle,
                fresh_tail: Some(Lsn::new(6)),
            },
            &cfg,
        );
        assert_eq!(effect, TrackerEffect::None);
        assert!(
            !tracker.is_quarantined(),
            "a fresh heartbeat alone must clear a LoopDead episode, with no LSN progress required"
        );
    }

    /// A3: confirmed lag must promote an existing LoopDead episode rather than being blocked by
    /// it, and a subsequent loop-dead sample must never overwrite an active ApplyStall episode's
    /// kind (only its bail bookkeeping).
    #[test]
    fn apply_stall_dominates_loop_dead() {
        let cfg = cfg();
        let t0 = Instant::now();
        let incarnation = Ulid::new();
        let mut tracker = TrackerEntry::new(incarnation, t0, &cfg);
        tracker.on_sample(
            t0,
            TickInput {
                last_applied_lsn: Some(Lsn::new(10)),
                replay_status: ReplayStatus::Active,
                loop_state: LoopState::ReaderIdle,
                fresh_tail: Some(Lsn::new(11)),
            },
            &cfg,
        );

        // Force a LoopDead episode first.
        let dead_time = t0 + cfg.hard_grace() + Duration::from_secs(1);
        tracker.on_sample(dead_time, loop_dead_sample(Some(Lsn::new(10))), &cfg);
        assert!(matches!(
            tracker.quarantine(),
            Some(QuarantineEpisode::LoopDead { .. })
        ));

        // Manually drive the tracker into Stalled (bypassing the full grace/probe timeline is not
        // possible via the public API, so directly assert the promotion behavior of
        // `enter_stalled`, which is what rule 5 / the Observing->Stalled transition calls).
        tracker.detect = Detect::Observing {
            since: dead_time,
            until: dead_time,
        };
        let recovery_time = dead_time + Duration::from_secs(1);
        tracker.on_sample(recovery_time, idle_lagging(Lsn::new(10)), &cfg);
        assert!(
            matches!(
                tracker.quarantine(),
                Some(QuarantineEpisode::ApplyStall { .. })
            ),
            "confirmed lag must promote LoopDead to ApplyStall"
        );

        // A subsequent loop-dead sample must not revert the episode kind back to LoopDead.
        let later_dead_time = recovery_time + cfg.hard_grace() + Duration::from_secs(1);
        tracker.on_sample(later_dead_time, loop_dead_sample(Some(Lsn::new(10))), &cfg);
        assert!(
            matches!(
                tracker.quarantine(),
                Some(QuarantineEpisode::ApplyStall { .. })
            ),
            "a loop-dead sample must never overwrite an active ApplyStall episode's kind"
        );
    }

    /// A5: the sweep must round-robin by *attempt* time, not success time, so a partition whose
    /// `ConsistentRead` probes always error or time out cannot starve the others of the single
    /// node-wide probe slot.
    #[test]
    fn sweep_fairness_under_persistent_failure() {
        let t0 = Instant::now();
        let mut observations: BTreeMap<u16, TailObservation> = (0..4)
            .map(|id| (id, TailObservation::new(Lsn::new(1), t0)))
            .collect();

        let mut picks_per_partition: BTreeMap<u16, u32> = BTreeMap::new();
        let mut now = t0;
        for round in 0u64..40 {
            let picked = pick_next_consistent_read_sweep_target(
                observations.iter().map(|(&id, obs)| (id, obs)),
            )
            .expect("at least one partition must be picked");
            *picks_per_partition.entry(picked).or_default() += 1;
            now += Duration::from_secs(5);

            let obs = observations.get_mut(&picked).unwrap();
            if picked == 0 {
                // Partition 0 always errors/times out: only its attempt clock advances.
                obs.mark_consistent_attempt(now);
            } else {
                obs.observe_consistent(Lsn::new(2 + round), now);
            }
        }

        // Every partition, including the permanently-failing one, must have been picked a
        // roughly even number of times -- none may be starved by the always-failing partition
        // monopolizing the slot, and the always-failing partition must not itself be starved by
        // the others.
        assert_eq!(
            picks_per_partition.len(),
            4,
            "every partition must get a turn"
        );
        for (&id, &picks) in &picks_per_partition {
            assert!(
                picks >= 8,
                "partition {id} was picked only {picks} times out of 40 rounds across 4 partitions"
            );
        }
    }

    #[test]
    fn heartbeat_view_loop_state_classification() {
        let cfg = cfg();
        let t0 = Instant::now();
        let mut view = HeartbeatView::new(t0);

        // A fresh InSelect beat is reader-idle.
        view.observe((LoopPhase::InSelect, 1), t0);
        assert_eq!(view.loop_state(t0, &cfg), LoopState::ReaderIdle);

        // The same beat, well past heartbeat_fresh but short of hard_grace, is neither idle nor
        // dead -- the ambiguous "Stale" zone that must not arm suspicion nor prove loop-death.
        let mid = t0 + cfg.heartbeat_fresh() + Duration::from_secs(1);
        assert_eq!(view.loop_state(mid, &cfg), LoopState::Stale);

        // Once the same beat is old enough, it proves loop-death.
        let dead = t0 + cfg.hard_grace() + Duration::from_secs(1);
        assert_eq!(view.loop_state(dead, &cfg), LoopState::LoopDead);

        // A busy phase is Busy regardless of age.
        view.observe((LoopPhase::ApplyingBatch, 2), t0);
        assert_eq!(view.loop_state(dead, &cfg), LoopState::Busy);
    }

    /// Finding 2 (DEFECTB review): a freshly-constructed `HeartbeatView` -- e.g. right after an
    /// incarnation reset -- starts at the same `(InSelect, 0)` value the real `LoopHeartbeat`
    /// itself starts at. That must never be read as "just observed a fresh beat": no beat has
    /// actually been observed yet, so it must classify as `Stale` (can't confirm idle or dead),
    /// not `ReaderIdle`, until a genuine counter change is seen. Eventually staying at the
    /// placeholder for `hard_grace` is still sound loop-death evidence.
    #[test]
    fn heartbeat_view_default_sample_is_not_reader_idle() {
        let cfg = cfg();
        let t0 = Instant::now();
        let view = HeartbeatView::new(t0);

        // Immediately at construction: not ReaderIdle (no beat observed yet).
        assert_eq!(view.loop_state(t0, &cfg), LoopState::Stale);

        // Still not ReaderIdle even once "fresh" by age alone -- there's still no observed beat.
        let still_fresh_by_age = t0 + cfg.heartbeat_fresh() / 2;
        assert_eq!(view.loop_state(still_fresh_by_age, &cfg), LoopState::Stale);

        // But staying at the placeholder for hard_grace is still genuine loop-death evidence:
        // the loop never scheduled even once.
        let dead = t0 + cfg.hard_grace() + Duration::from_secs(1);
        assert_eq!(view.loop_state(dead, &cfg), LoopState::LoopDead);
    }

    /// Finding 2: the default sample must not clear a `LoopDead` quarantine. Only a *genuinely
    /// observed* fresh beat from the new incarnation may clear it.
    #[test]
    fn loop_dead_default_heartbeat_sample_does_not_clear_quarantine() {
        let cfg = cfg();
        let t0 = Instant::now();
        let incarnation = Ulid::new();
        let mut tracker = TrackerEntry::new(incarnation, t0, &cfg);

        // Establish a known LSN so the loop-dead sample below isn't itself mistaken for progress
        // (a fresh tracker's `last_known_lsn` is `None`, and rule 1 treats any `Some(lsn)` as
        // progress against `None`).
        tracker.on_sample(
            t0,
            TickInput {
                last_applied_lsn: Some(Lsn::new(5)),
                replay_status: ReplayStatus::Active,
                loop_state: LoopState::ReaderIdle,
                fresh_tail: Some(Lsn::new(6)),
            },
            &cfg,
        );

        // Force a LoopDead episode.
        let dead_time = t0 + cfg.hard_grace() + Duration::from_secs(1);
        tracker.on_sample(dead_time, loop_dead_sample(Some(Lsn::new(5))), &cfg);
        assert!(matches!(
            tracker.quarantine(),
            Some(QuarantineEpisode::LoopDead { .. })
        ));

        // Simulate the restart: a fresh `HeartbeatView` for the new incarnation, sampled
        // immediately -- this is the exact `(InSelect, 0)` construction-time placeholder, not a
        // real observed beat.
        let restart_time = dead_time + Duration::from_millis(1);
        tracker.on_incarnation_started(restart_time, Ulid::new());
        let mut fresh_view = HeartbeatView::new(restart_time);
        let placeholder_loop_state = fresh_view.loop_state(restart_time, &cfg);
        assert_eq!(
            placeholder_loop_state,
            LoopState::Stale,
            "sanity: the construction-time placeholder must not classify as ReaderIdle"
        );

        let effect = tracker.on_sample(
            restart_time,
            TickInput {
                last_applied_lsn: Some(Lsn::new(5)),
                replay_status: ReplayStatus::Active,
                loop_state: placeholder_loop_state,
                fresh_tail: Some(Lsn::new(6)),
            },
            &cfg,
        );
        assert_eq!(effect, TrackerEffect::None);
        assert!(
            tracker.is_quarantined(),
            "the default (InSelect, 0) sample must not clear a LoopDead quarantine"
        );

        // Once a genuine beat is observed (counter actually changes) and it's fresh, the
        // quarantine clears as before.
        fresh_view.observe((LoopPhase::InSelect, 1), restart_time);
        let real_loop_state = fresh_view.loop_state(restart_time, &cfg);
        assert_eq!(real_loop_state, LoopState::ReaderIdle);
        let effect = tracker.on_sample(
            restart_time,
            TickInput {
                last_applied_lsn: Some(Lsn::new(5)),
                replay_status: ReplayStatus::Active,
                loop_state: real_loop_state,
                fresh_tail: Some(Lsn::new(6)),
            },
            &cfg,
        );
        assert_eq!(effect, TrackerEffect::None);
        assert!(
            !tracker.is_quarantined(),
            "a genuinely observed fresh beat must still clear the LoopDead quarantine"
        );
    }

    /// Finding 4 (DEFECTB review): `on_sample` returning `TrackerEffect::Bail` is only a
    /// *proposal* -- nothing about the bail (the quarantine's `bails` count, the restart-history
    /// backoff) is committed until the caller explicitly calls `commit_bail`, which the manager
    /// only does after freshly revalidating reader-idle/lag immediately before `stop()`. If the
    /// manager declines to commit, the tracker's state is untouched and the next sample can
    /// propose again.
    #[test]
    fn bail_is_a_proposal_until_committed() {
        let cfg = cfg();
        let t0 = Instant::now();
        let incarnation = Ulid::new();
        let mut tracker = TrackerEntry::new(incarnation, t0, &cfg);
        tracker.on_sample(
            t0,
            TickInput {
                last_applied_lsn: Some(Lsn::new(5)),
                replay_status: ReplayStatus::Active,
                loop_state: LoopState::ReaderIdle,
                fresh_tail: Some(Lsn::new(6)),
            },
            &cfg,
        );

        let dead_time = t0 + cfg.hard_grace() + Duration::from_secs(1);
        let effect = tracker.on_sample(dead_time, loop_dead_sample(Some(Lsn::new(5))), &cfg);
        assert_eq!(effect, TrackerEffect::Bail);
        // The episode itself is established eagerly (it's evidence, not an action), but the bail
        // itself has not been committed yet.
        let Some(QuarantineEpisode::LoopDead { bails, .. }) = tracker.quarantine() else {
            panic!("expected a LoopDead episode");
        };
        assert_eq!(
            bails, 0,
            "commit_bail was never called -- bails must not be bumped yet"
        );

        // The manager declines to commit (revalidation failed): calling on_sample again with the
        // same evidence proposes again, since next_permitted_bail_at was never advanced.
        let effect = tracker.on_sample(dead_time, loop_dead_sample(Some(Lsn::new(5))), &cfg);
        assert_eq!(effect, TrackerEffect::Bail);

        // Now the manager revalidates successfully and commits.
        tracker.commit_bail(dead_time, Lsn::new(5), &cfg);
        let Some(QuarantineEpisode::LoopDead { bails, .. }) = tracker.quarantine() else {
            panic!("expected a LoopDead episode");
        };
        assert_eq!(bails, 1);
    }
}
