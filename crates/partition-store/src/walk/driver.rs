// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The background driver: runs walks between the partition processor's turns, a
//! slice at a time, through the processor's own transaction (see [`engine`]).
//!
//! **Order.** One walk at a time, the started walks in ascending ID. Opening the
//! partition started them (see `registry::reconcile`), so the driver only runs
//! what is already durable.
//!
//! **Pacing.** A slice occupies the processor while it runs, so the driver limits
//! the share of time slices take: a slice that occupied `d` defers the next by
//! `d * (1 - share) / share`, with a share of 10% when the processor had other
//! work since the previous slice and 50% otherwise. A slice that overran its
//! target therefore defers the next one in proportion. The keys per slice adapt
//! to the time a key took, aiming at a target duration between fixed bounds, and
//! the slices' byte limits always apply. The optional throttling caps the keys
//! examined with a token bucket, one token per key: a slice waits until the
//! bucket holds all its keys, up to the burst. For example, with a 2 ms target
//! and keys taking 8 µs each, slices take 250 keys; under load, one runs every
//! 20 ms or so. Rests shorter than the timer's millisecond resolution round up,
//! which only lowers the share.
//!
//! **Failures.** A slice that fails before its commit is recorded on its walk,
//! and the driver retries after an exponential backoff, while the partition
//! keeps serving and the indexes stay Backfilling. A failed commit has an
//! unknown outcome: [`WalkDriver::step`] returns it, and the processor stops; its
//! reopen reloads the records.
//!
//! **Snapshots.** While a snapshot export holds the transaction's commits back,
//! the driver does nothing and checks again shortly after.

use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use gardal::{Limit, LocalTokenBucket, TokioClock, TryConsumeError};
use tokio::time::{Instant, Sleep};
use tracing::{info, warn};

use restate_storage_api::StorageError;
use restate_types::config::StorageOptions;

use crate::faults::{self, FaultPoint};
use crate::{PartitionDb, PartitionStore, PartitionStoreTransaction};

use super::engine::{self, Slice, SliceBudget, SliceError};

/// What the background drivers of a partition did, for diagnostics and tests.
/// Kept with the physical partition: across reopenings of the same one, reset
/// when it is recreated. Counts charged steps only: a step whose commit failed,
/// or that was dropped, is not charged.
#[derive(Debug, Default)]
pub(crate) struct WalkStats {
    busy_steps: AtomicU64,
    quiet_steps: AtomicU64,
    examined: AtomicU64,
    occupied_nanos: AtomicU64,
    fence_skips: AtomicU64,
    failures: AtomicU64,
}

/// A copy of a partition's [`WalkStats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalkStatsSnapshot {
    /// Slices charged after foreground work, at the lower share.
    pub busy_steps: u64,
    /// Slices charged at the quiet share.
    pub quiet_steps: u64,
    /// Source entries the slices examined.
    pub examined: u64,
    /// Time the slices occupied the processor.
    pub occupied: Duration,
    /// Steps skipped while a snapshot export held commits back.
    pub fence_skips: u64,
    /// Failed slices.
    pub failures: u64,
}

impl WalkStats {
    pub(crate) fn snapshot(&self) -> WalkStatsSnapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        WalkStatsSnapshot {
            busy_steps: load(&self.busy_steps),
            quiet_steps: load(&self.quiet_steps),
            examined: load(&self.examined),
            occupied: Duration::from_nanos(load(&self.occupied_nanos)),
            fence_skips: load(&self.fence_skips),
            failures: load(&self.failures),
        }
    }
}

fn add(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// The driver's constants.
#[derive(Debug, Clone)]
pub(crate) struct Tuning {
    /// The share of time slices take after foreground work.
    pub(crate) busy_share: f64,
    /// The share of time slices take otherwise.
    pub(crate) quiet_share: f64,
    /// The duration slices aim at.
    pub(crate) target: Duration,
    pub(crate) min_keys: usize,
    /// Keys per slice at most, within the slices' byte limits.
    pub(crate) max_keys: usize,
    /// The first retry delay, doubling up to `max_backoff`.
    pub(crate) backoff: Duration,
    pub(crate) max_backoff: Duration,
    /// How soon to check again while commits are held back.
    pub(crate) fence_recheck: Duration,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            busy_share: 0.1,
            quiet_share: 0.5,
            target: Duration::from_millis(2),
            min_keys: 16,
            max_keys: 1_000,
            backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(300),
            fence_recheck: Duration::from_millis(100),
        }
    }
}

/// Runs a partition's walks in the background; see the module documentation.
///
/// The processor awaits [`Self::ready`] alongside its other work and then calls
/// [`Self::step`] with its transaction, and calls [`Self::note_foreground`] when
/// it handles other work.
pub struct WalkDriver {
    db: PartitionDb,
    /// When the next step is due; `None` when there is nothing to do.
    due: Option<Instant>,
    /// When the rest after the latest work ends.
    rested: Instant,
    sleep: Pin<Box<Sleep>>,
    pacing: Pacing,
    /// Consecutive failures, for the backoff.
    failures: u32,
    busy: bool,
    /// The resumption of a step paused at [`FaultPoint::WalkStep`].
    held: Option<faults::Resume>,
}

impl PartitionStore {
    /// What the background drivers of this partition did on its physical
    /// partition; see [`WalkStats`].
    pub fn walk_stats(&self) -> WalkStatsSnapshot {
        self.partition_db().walk_stats().snapshot()
    }

    /// The background driver of this partition's walks, under `options`.
    pub fn walk_driver(&self, options: &StorageOptions) -> WalkDriver {
        let db = self.partition_db().clone();
        let started = self.maintained_indexes().walks().entries().len();
        let pending = started > 0;
        if pending && options.disable_background_migrations {
            info!(
                started,
                "Background migrations are disabled; the pending index and statistics backfill waits"
            );
        } else if pending {
            info!(
                started,
                "Backfilling indexes and statistics in the background"
            );
        }
        let now = Instant::now();
        WalkDriver {
            db,
            due: (pending && !options.disable_background_migrations).then_some(now),
            rested: now,
            sleep: Box::pin(tokio::time::sleep_until(now)),
            pacing: Pacing::new(
                Tuning::default(),
                options
                    .background_migrations_throttling
                    .clone()
                    .map(Limit::from),
            ),
            failures: 0,
            busy: false,
            held: None,
        }
    }
}

impl WalkDriver {
    /// Resolves once a [`Self::step`] is due, and never while there is nothing to
    /// do. Cancellation safe: awaiting it again resumes the same wait.
    pub async fn ready(&mut self) {
        let Some(due) = self.due else {
            // Only signals: resumes right away.
            let _ = self.db.faults().arrive(FaultPoint::WalksIdle);
            return std::future::pending().await;
        };
        if self.sleep.deadline() != due {
            self.sleep.as_mut().reset(due);
        }
        self.sleep.as_mut().await;
        self.hold().await;
    }

    /// Holds a due step while a fault pauses it at [`FaultPoint::WalkStep`], across
    /// cancelled waits.
    async fn hold(&mut self) {
        if self.held.is_none() {
            self.held = self.db.faults().arrive(FaultPoint::WalkStep);
        }
        if let Some(held) = &mut self.held {
            let _ = held.await;
            self.held = None;
        }
    }

    /// Notes that the processor handled other work, or has some waiting, which
    /// lowers the share of time the next slice may take.
    pub fn note_foreground(&mut self) {
        self.busy = true;
    }

    /// Does what is due, if anything: a slice of the current walk. `tx` must have
    /// no pending writes. Fails only with a failed commit, after which the
    /// processor must stop.
    ///
    /// Not cancellation safe: a step dropped during its commit or before its
    /// publication may have committed its native write while `tx` still carries
    /// the previous maintained set. Discard `tx` and this driver then, and reopen
    /// the partition before writing to it again; the reopen reloads the committed
    /// records.
    pub async fn step(
        &mut self,
        tx: &mut PartitionStoreTransaction<'_>,
    ) -> Result<(), StorageError> {
        let started = Instant::now();
        if self.due.is_none_or(|due| due > started) {
            return Ok(());
        }
        if tx.commits_held() {
            add(&self.db.walk_stats().fence_skips, 1);
            self.due = Some(started + self.pacing.tuning.fence_recheck);
            return Ok(());
        }
        match tx
            .maintained_indexes()
            .walks()
            .entries()
            .iter()
            .map(|w| w.id)
            .min()
        {
            Some(walk) => self.slice(tx, walk, started).await,
            None => {
                self.due = None;
                Ok(())
            }
        }
    }

    async fn slice(
        &mut self,
        tx: &mut PartitionStoreTransaction<'_>,
        walk: u64,
        started: Instant,
    ) -> Result<(), StorageError> {
        let keys = match self.pacing.keys(started) {
            Ok(keys) => keys,
            Err(refilled) => {
                self.due = Some(refilled.max(self.rested));
                return Ok(());
            }
        };
        let result = engine::run_counted_slice(tx, &self.db, walk, SliceBudget::from(keys)).await;
        let (examined, failure) = match result {
            Ok((slice, examined)) => {
                if slice == Slice::Exhausted {
                    info!(walk, "Backfilled the indexes of the walk; they are Ready");
                }
                (examined, None)
            }
            Err((SliceError::Commit(err), _)) => return Err(err),
            Err((SliceError::Staging(err), examined)) => {
                // Records it on the committed state, which the failure left as it was.
                let error = engine::describe(&err);
                let work = started.elapsed();
                match engine::record_failure(tx, &self.db, walk, &error, work).await {
                    Err(SliceError::Commit(err)) => return Err(err),
                    Err(SliceError::Staging(record)) => {
                        warn!(walk, "Failed to record a failed walk slice: {record}");
                    }
                    Ok(()) => {}
                }
                (examined, Some(error))
            }
        };
        self.charge(started, examined, failure);
        if tx.maintained_indexes().walks().entries().is_empty() {
            self.due = None;
        }
        Ok(())
    }

    /// Charges work that started at `started` and ends now, examining `examined`
    /// source keys, and schedules the next step once its rest, or after a
    /// `failure` its backoff too, has passed since the work ended.
    fn charge(&mut self, started: Instant, examined: usize, failure: Option<String>) {
        let ended = Instant::now();
        let busy = std::mem::take(&mut self.busy);
        let stats = self.db.walk_stats();
        add(
            if busy {
                &stats.busy_steps
            } else {
                &stats.quiet_steps
            },
            1,
        );
        add(&stats.examined, examined as u64);
        add(
            &stats.occupied_nanos,
            nanos(ended.saturating_duration_since(started)),
        );
        if failure.is_some() {
            add(&stats.failures, 1);
        }
        self.rested = self.pacing.charge(started, ended, examined, busy);
        self.due = Some(match failure {
            None => {
                self.failures = 0;
                self.rested
            }
            Some(error) => {
                warn!("Background index and statistics backfill failed, retrying: {error}");
                let retry = self.pacing.retry(ended, self.failures);
                self.failures = self.failures.saturating_add(1);
                retry.max(self.rested)
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn tuning_mut(&mut self) -> &mut Tuning {
        &mut self.pacing.tuning
    }

    #[cfg(test)]
    pub(crate) fn due(&self) -> Option<Instant> {
        self.due
    }
}

/// The keys of the next slice and when it is due.
struct Pacing {
    tuning: Tuning,
    keys: usize,
    /// A moving average of the time a key takes.
    per_key: Option<Duration>,
    /// The optional throttling, one token per key.
    bucket: Option<LocalTokenBucket<TokioClock>>,
    /// The tokens the latest slice took, to return those it did not examine.
    granted: u32,
}

impl Pacing {
    fn new(tuning: Tuning, limit: Option<Limit>) -> Self {
        Self {
            keys: tuning.min_keys,
            tuning,
            per_key: None,
            bucket: limit.map(|limit| LocalTokenBucket::new(limit, TokioClock)),
            granted: 0,
        }
    }

    /// The keys the next slice may examine, or, under throttling, when the
    /// bucket holds them.
    fn keys(&mut self, now: Instant) -> Result<usize, Instant> {
        let Some(bucket) = &self.bucket else {
            return Ok(self.keys);
        };
        // Waits for the whole slice, up to the burst, rather than a key at a
        // time: every slice commits, and timers cannot wait less than a
        // millisecond anyway.
        let keys = u32::try_from(self.keys)
            .unwrap_or(u32::MAX)
            .min(bucket.limit().burst().get());
        let keys = NonZeroU32::new(keys).unwrap_or(NonZeroU32::MIN);
        match bucket.try_consume(keys) {
            Ok(_) => {
                self.granted = keys.get();
                Ok(keys.get() as usize)
            }
            Err(TryConsumeError::RateLimited(limited)) => Err(now + limited.earliest_retry_after()),
            Err(TryConsumeError::ExceededBurstCapacity(_)) => {
                unreachable!("requested keys are clamped to the bucket's burst capacity")
            }
        }
    }

    /// Charges work from `started` to `ended` that examined `examined` keys,
    /// adapting the keys of the next slice, and returns when the rest after it
    /// ends: the work takes the share of the time from its start to then.
    fn charge(&mut self, started: Instant, ended: Instant, examined: usize, busy: bool) -> Instant {
        let occupied = ended.saturating_duration_since(started);
        let granted = std::mem::take(&mut self.granted);
        if let Some(bucket) = &self.bucket {
            let unused = granted.saturating_sub(u32::try_from(examined).unwrap_or(u32::MAX));
            if unused > 0 {
                bucket.add_tokens(unused);
            }
        }
        if let Ok(examined) = u32::try_from(examined)
            && examined > 0
        {
            let sample = occupied / examined;
            let per_key = self
                .per_key
                .map_or(sample, |average| (average * 7 + sample * 3) / 10);
            self.per_key = Some(per_key);
            let keys = self.tuning.target.as_nanos() / per_key.as_nanos().max(1);
            self.keys = usize::try_from(keys)
                .unwrap_or(usize::MAX)
                .clamp(self.tuning.min_keys, self.tuning.max_keys);
        }
        let share = if busy {
            self.tuning.busy_share
        } else {
            self.tuning.quiet_share
        };
        ended + occupied.mul_f64((1.0 - share) / share)
    }

    /// When to retry after `failures` earlier consecutive failures, the latest
    /// ending at `ended`.
    fn retry(&self, ended: Instant, failures: u32) -> Instant {
        ended
            + self
                .tuning
                .backoff
                .saturating_mul(1 << failures.min(16))
                .min(self.tuning.max_backoff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[restate_core::test(start_paused = true)]
    async fn pacing_adapts_keys_rests_after_work_and_limits_the_rate() {
        let t0 = Instant::now();
        let at = |micros: u64| t0 + Duration::from_micros(micros);
        let mut pacing = Pacing::new(Tuning::default(), None);
        assert_eq!(pacing.keys(t0), Ok(16), "starts at the lower bound");
        // 8 µs per key aims 2 ms slices at 250 keys. The rest starts when the work
        // ends: nine times the work under load, as long as the work when quiet.
        assert_eq!(pacing.charge(t0, at(128), 16, true), at(128 + 1_152));
        assert_eq!(pacing.keys(t0), Ok(250));
        assert_eq!(pacing.charge(at(5_000), at(7_000), 250, false), at(9_000));
        // Work that overran rests in proportion, and the average moves towards
        // its time per key, within bounds.
        assert_eq!(
            pacing.charge(t0, at(50_000), 250, true),
            at(50_000 + 450_000)
        );
        assert_eq!(pacing.keys(t0), Ok(30));
        pacing.charge(t0, at(1_000_000), 1, true);
        assert_eq!(pacing.keys(t0), Ok(16));
        let mut fast = Pacing::new(Tuning::default(), None);
        fast.charge(t0, at(1), 1_000, false);
        assert_eq!(fast.keys(t0), Ok(1_000));
        // Work that examined nothing still rests, and adapts nothing.
        assert_eq!(fast.charge(t0, at(1_000), 0, false), at(2_000));
        assert_eq!(fast.keys(t0), Ok(1_000));
        // Retries wait from the end of the failed work: 1 s, doubling, up to 5 min.
        assert_eq!(pacing.retry(at(300), 0), at(300) + Duration::from_secs(1));
        assert_eq!(pacing.retry(at(300), 3), at(300) + Duration::from_secs(8));
        assert_eq!(
            pacing.retry(at(300), 40),
            at(300) + Duration::from_secs(300)
        );

        // At 100 keys/s with the default burst of a second's worth, a slice takes
        // at most the burst, and waits until the bucket holds all its keys. Keys
        // it did not examine return to the bucket.
        let rate = NonZeroU32::new(100).unwrap();
        let mut limited = Pacing::new(Tuning::default(), Some(Limit::per_second(rate)));
        limited.keys = 1_000;
        assert_eq!(limited.keys(t0), Ok(100), "capped at the burst");
        limited.charge(t0, at(1_000), 60, true);
        let Err(refilled) = limited.keys(t0) else {
            panic!("the bucket holds 40 keys");
        };
        assert!((refilled - t0).abs_diff(Duration::from_millis(600)) < Duration::from_micros(1));
        // A millisecond late, as timers are, past the float clock's rounding.
        tokio::time::advance(Duration::from_millis(601)).await;
        assert_eq!(limited.keys(Instant::now()), Ok(100));
    }
}
