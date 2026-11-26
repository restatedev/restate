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
//! **Order.** Slices take turns between the started walks, by ID: after a slice
//! of walk 3, the next goes to the lowest started walk above 3, or else the
//! lowest. A walk waiting out its retry backoff is skipped. Before any slice, the
//! waiting jobs whose wait is over start a pass, together in one commit (see
//! [`engine::start_passes`]). A waiting job's wall-clock eligibility becomes a
//! deadline on this process's clock when the driver first sees it, at most the
//! longest re-check wait away, so a clock jump can't strand it. With nothing
//! runnable, the driver sleeps until the earliest retry or eligibility, and idles
//! when there is neither.
//!
//! **Pacing.** A slice occupies the processor while it runs, so the driver
//! limits the share of time slices take: a slice that occupied `d` defers the
//! next by `d * (1 - share) / share`, with a share of 10% when the processor
//! had other work since the previous slice and 50% otherwise. A slice that
//! overran its target therefore defers the next one in proportion. The keys a
//! slice aims at adapt to the time a key took, aiming at a target duration,
//! between 16 keys and `worker.storage.background-migrations-max-slice-entries`
//! (1,000 by default); with a cap below 16, slices aim at the cap. A slice can
//! take fewer: it reads at most 4 MiB of source and stages at most the command
//! batches' write budget, `worker.max-command-batch-bytes`, apart from its
//! first key, which always goes in, and the walk can run out of keys. The
//! optional throttling caps the keys examined with a token bucket, one token
//! per key: a slice waits until the bucket holds all its keys, up to the burst.
//! For example, with a 2 ms target and keys taking 8 µs each, slices take 250
//! keys; under load, one runs every 20 ms or so. Rests shorter than the timer's
//! millisecond resolution round up, which only lowers the share.
//!
//! **Failures.** A slice that fails before its commit is recorded on its walk's
//! jobs, and that walk retries after its own exponential backoff while the other
//! walks keep their turns, the partition keeps serving and the walk's indexes
//! stay Backfilling, or its migration in progress. A pass that fails to start likewise defers only its jobs, behind
//! a backoff of its own; started walks keep their turns meanwhile. A failed commit has an
//! unknown outcome: [`WalkDriver::step`] returns it, and the processor stops; its
//! reopen reloads the records.
//!
//! **Snapshots.** While a snapshot export holds the transaction's commits back,
//! the driver does nothing and checks again shortly after.
//!
//! **What it waits for.** The driver publishes the scheduling constraints it
//! last observed (a [`DriverState`]) on the physical partition, where
//! `restate.cluster.migration_jobs` reads them as `blocked_by`, separately from
//! the records. A backoff or the throttling carries its end, on this process's
//! monotonic clock, so it disappears once over, even before the driver's next
//! step. A snapshot export is only
//! observed at a due step: it shows from the step that found commits held back
//! until the next one that doesn't. Dropping the driver withdraws its state, so
//! a partition without a running processor shows none; a driver never withdraws
//! another's. The driver publishes only when a constraint changed, so slices
//! that change nothing take no lock.

use std::collections::HashMap;
use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use gardal::{Limit, LocalTokenBucket, TokioClock, TryConsumeError};
use tokio::time::{Instant, Sleep};
use tracing::{info, warn};

use restate_clock::WallClock;
use restate_storage_api::StorageError;
use restate_types::config::{Configuration, StorageOptions};

use crate::faults::{self, FaultPoint};
use crate::job::{MigrationJobStatus, RECHECK_CAP_MS};
use crate::registry::{IndexStatus, PublishedRegistry};
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

/// The scheduling constraints a partition's walk driver last observed; see the
/// module documentation. Times are on the driver's monotonic clock.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DriverState {
    /// The driver runs no walks: `disable-background-migrations` is set.
    pub disabled: bool,
    /// The driver's latest due step found commits held back for a snapshot
    /// export.
    pub snapshot_export: bool,
    /// Until when the throttling's token bucket can't cover the next slice.
    pub throttled_until: Option<Instant>,
    /// The walks waiting out a retry backoff after failed slices, by ID, with
    /// when it ends.
    pub backoff_walks: Vec<(u64, Instant)>,
    /// The indexes whose next pass start waits out a backoff after a failed
    /// start, by ID, with when it ends.
    pub backoff_starts: Vec<(u32, Instant)>,
    /// When each waiting job, by index ID, may start its next pass, as the
    /// driver scheduled it.
    pub eligible_at: Vec<(u32, Instant)>,
}

/// The [`DriverState`] published on a physical partition, with the driver that
/// published it: a driver only withdraws its own.
#[derive(Debug, Default)]
pub(crate) struct PublishedDriverState(restate_platform::sync::Mutex<Option<(u64, DriverState)>>);

impl PublishedDriverState {
    /// The state of the running driver, if any.
    pub(crate) fn get(&self) -> Option<DriverState> {
        self.0.lock().as_ref().map(|(_, state)| state.clone())
    }

    fn publish(&self, driver: u64, state: DriverState) {
        *self.0.lock() = Some((driver, state));
    }

    fn withdraw(&self, driver: u64) {
        let mut published = self.0.lock();
        if published
            .as_ref()
            .is_some_and(|(owner, _)| *owner == driver)
        {
            *published = None;
        }
    }
}

/// Identifies each driver of this process, for [`PublishedDriverState`].
static NEXT_DRIVER: AtomicU64 = AtomicU64::new(0);

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

impl Tuning {
    /// The default tuning with at most `max_keys` keys per slice. The adaptive
    /// size stays between 16 keys, or `max_keys` if that is lower, and `max_keys`.
    fn with_max_keys(max_keys: NonZeroUsize) -> Self {
        let defaults = Self::default();
        Self {
            min_keys: defaults.min_keys.min(max_keys.get()),
            max_keys: max_keys.get(),
            ..defaults
        }
    }
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
    /// The walk of the latest slice: the next slice goes to the next walk by ID.
    last_walk: Option<u64>,
    /// The walks whose latest slice failed: their consecutive failures, and when
    /// they may retry.
    backoffs: HashMap<u64, (u32, Instant)>,
    /// When each waiting job, by index ID, may start its next pass.
    eligible_at: HashMap<u32, Instant>,
    /// Consecutive failed pass starts, for their backoff.
    start_failures: u32,
    /// The indexes whose latest pass start failed, until one succeeds or they
    /// stop waiting.
    failed_starts: Vec<u32>,
    busy: bool,
    /// The resumption of a step paused at [`FaultPoint::WalkStep`].
    held: Option<faults::Resume>,
    /// This driver's identity, for the state it publishes.
    id: u64,
    /// Whether `disable-background-migrations` is set.
    disabled: bool,
    /// Whether the latest due step found commits held back.
    fenced: bool,
    /// When the token bucket covers the next slice, while it can't yet.
    throttled_until: Option<Instant>,
    /// Whether a constraint changed since the driver last published them.
    dirty: bool,
    /// Whether the driver waits for the processor to ask for the indexes over
    /// the vqueue entries ([`PartitionDb::request_vqueue_indexes`]): only on a
    /// store opened with indexes enabled, fenced, and not yet fully migrated to
    /// vqueues. Cleared once they are registered; every other driver never
    /// looks at the request, so its wait costs nothing.
    pub(crate) awaiting_vqueues: bool,
}

impl Drop for WalkDriver {
    fn drop(&mut self) {
        self.db.shared_registry().driver_state().withdraw(self.id);
    }
}

impl PartitionStore {
    /// What the background drivers of this partition did on its physical
    /// partition; see [`WalkStats`].
    pub fn walk_stats(&self) -> WalkStatsSnapshot {
        self.partition_db()
            .shared_registry()
            .walk_stats()
            .snapshot()
    }

    /// The background driver of this partition's walks, under `options`.
    pub fn walk_driver(&self, options: &StorageOptions) -> WalkDriver {
        let db = self.partition_db().clone();
        let active = self.published_registry();
        let started = active.walks().entries().len();
        let waiting = active
            .jobs()
            .entries()
            .iter()
            .any(|job| job.status == MigrationJobStatus::Waiting);
        let pending = started > 0 || waiting;
        if pending && options.disable_background_migrations {
            info!(
                started,
                "Background migrations are disabled; pending walks wait"
            );
        } else if pending {
            info!(started, "Running migration walks in the background");
        }
        // The same facts the open-time reconcile decides on: only such a store
        // registers the indexes over the vqueue entries later, when the processor
        // applies the migration.
        let awaiting_vqueues = Configuration::pinned()
            .common
            .experimental
            .is_indexes_enabled()
            && self.storage_features().is_registry
            && !crate::fsm_table::get_state_machine_features_from_partition_db(&db)
                .is_ok_and(|features| features.vqueues);
        let now = Instant::now();
        let mut driver = WalkDriver {
            db,
            due: (pending && !options.disable_background_migrations).then_some(now),
            rested: now,
            sleep: Box::pin(tokio::time::sleep_until(now)),
            pacing: Pacing::new(
                Tuning::with_max_keys(options.background_migrations_max_slice_entries),
                options
                    .background_migrations_throttling
                    .clone()
                    .map(Limit::from),
            ),
            last_walk: None,
            backoffs: HashMap::new(),
            eligible_at: HashMap::new(),
            start_failures: 0,
            failed_starts: Vec::new(),
            busy: false,
            held: None,
            id: NEXT_DRIVER.fetch_add(1, Ordering::Relaxed),
            disabled: options.disable_background_migrations,
            fenced: false,
            throttled_until: None,
            dirty: true,
            awaiting_vqueues,
        };
        driver.publish();
        driver
    }
}

impl WalkDriver {
    /// Resolves once a [`Self::step`] is due, and never while there is nothing to
    /// do. Cancellation safe: awaiting it again resumes the same wait.
    pub async fn ready(&mut self) {
        if self.awaiting_vqueues {
            return self.ready_or_vqueues_requested().await;
        }
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

    /// [`Self::ready`] of a driver awaiting the vqueues migration: the processor
    /// asking for the indexes over the vqueue entries
    /// ([`PartitionDb::request_vqueue_indexes`]) also makes a step due at once.
    /// An event, so the processor's loop checks nothing per command batch.
    async fn ready_or_vqueues_requested(&mut self) {
        // The request's flag is set before its wake-up, so a wake-up consumed by
        // a cancelled wait still leaves the flag for the next one.
        let requested = self.db.shared_registry().vqueue_indexes_wakeup();
        if self.db.shared_registry().vqueue_indexes_requested() {
            // A step that found commits held back kept the request and is due
            // again at its fence recheck: wait for that, not for the request.
            if self.fenced
                && let Some(due) = self.due
            {
                if self.sleep.deadline() != due {
                    self.sleep.as_mut().reset(due);
                }
                self.sleep.as_mut().await;
            }
            return;
        }
        let Some(due) = self.due else {
            // Only signals: resumes right away.
            let _ = self.db.faults().arrive(FaultPoint::WalksIdle);
            return requested.await;
        };
        if self.sleep.deadline() != due {
            self.sleep.as_mut().reset(due);
        }
        tokio::select! {
            () = self.sleep.as_mut() => {}
            () = requested => return,
        }
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

    /// Does what is due, if anything: starting the passes whose wait is over, or
    /// else a slice of the next walk (see the module documentation). `tx` must have
    /// no pending writes. A slice stages at most `write_bytes`, apart from its
    /// first key; the processor passes its command batches' write budget,
    /// `worker.max-command-batch-bytes`. Fails only with a failed commit, after
    /// which the processor must stop.
    ///
    /// Not cancellation safe: a step dropped during its commit or before its
    /// publication may have committed its native write while `tx` still carries
    /// the previous maintained set. Discard `tx` and this driver then, and reopen
    /// the partition before writing to it again; the reopen reloads the committed
    /// records.
    pub async fn step(
        &mut self,
        tx: &mut PartitionStoreTransaction<'_>,
        write_bytes: usize,
    ) -> Result<(), StorageError> {
        let result = self.do_step(tx, write_bytes).await;
        self.publish();
        result
    }

    async fn do_step(
        &mut self,
        tx: &mut PartitionStoreTransaction<'_>,
        write_bytes: usize,
    ) -> Result<(), StorageError> {
        let started = Instant::now();
        if self.awaiting_vqueues && self.db.shared_registry().vqueue_indexes_requested() {
            // Registering writes and flushes the registry records outside `tx`, so
            // it waits while commits are held back, for example during a snapshot
            // export, like a slice does below: the request stays, and the step is
            // due again at the fence recheck.
            let fenced = tx.commits_held();
            if fenced != self.fenced {
                self.fenced = fenced;
                self.dirty = true;
            }
            if fenced {
                add(&self.db.shared_registry().walk_stats().fence_skips, 1);
                self.due = Some(started + self.pacing.tuning.fence_recheck);
                return Ok(());
            }
            self.db.shared_registry().take_vqueue_indexes_request();
            let mut store = PartitionStore::from(self.db.clone());
            let registered = store.register_vqueue_indexes(tx).await?;
            if registered {
                info!("Registered the indexes over the vqueue entries after the migration");
                // Registered and the walk scheduled: back to waiting at no cost.
                self.awaiting_vqueues = false;
                self.dirty = true;
            }
            // A fence above may have left its recheck as the deadline. With
            // background migrations disabled nothing else is due, registered or
            // not; enabled, the walk just registered starts now.
            if self.disabled {
                self.due = None;
            } else if registered {
                self.due = Some(started);
            }
            return Ok(());
        }
        if self.due.is_none_or(|due| due > started) {
            return Ok(());
        }
        // Before the fence check: the published state then tells when each waiting
        // job may start, also while a fence holds every start back.
        let active = tx.published_registry();
        let eligible = self.eligible(&active, started);
        let fenced = tx.commits_held();
        if fenced != self.fenced {
            self.fenced = fenced;
            self.dirty = true;
        }
        if fenced {
            add(&self.db.shared_registry().walk_stats().fence_skips, 1);
            self.due = Some(started + self.pacing.tuning.fence_recheck);
            return Ok(());
        }
        if !eligible.is_empty() {
            return self.start_passes(tx, &eligible, started).await;
        }
        match self.next_walk(&active, started) {
            Some(walk) => self.slice(tx, walk, started, write_bytes).await,
            None => {
                self.due = self.wake_up();
                Ok(())
            }
        }
    }

    /// The waiting jobs, by index ID, whose wait is over at `now`. Gives each
    /// waiting job first seen a deadline on this process's clock: its wall-clock
    /// eligibility, at most [`RECHECK_CAP_MS`] away.
    fn eligible(&mut self, active: &PublishedRegistry, now: Instant) -> Vec<u32> {
        let wall = WallClock::now_ms().as_u64();
        let waiting: Vec<_> = active
            .jobs()
            .entries()
            .iter()
            .filter(|job| job.status == MigrationJobStatus::Waiting)
            .filter(|job| {
                active
                    .registry()
                    .get(job.id().as_u32())
                    .is_some_and(|index| {
                        index.state == IndexStatus::Backfilling && index.walk.is_none()
                    })
            })
            .map(|job| (job.id().as_u32(), job.next_eligible_at))
            .collect();
        let known = self.eligible_at.len();
        self.eligible_at
            .retain(|id, _| waiting.iter().any(|(waiting, _)| waiting == id));
        let mut changed = self.eligible_at.len() != known;
        let failed = self.failed_starts.len();
        self.failed_starts
            .retain(|id| waiting.iter().any(|(waiting, _)| waiting == id));
        changed |= self.failed_starts.len() != failed;
        for (id, at) in &waiting {
            self.eligible_at.entry(*id).or_insert_with(|| {
                changed = true;
                let wait = at.map_or(0, |at| at.saturating_sub(wall).min(RECHECK_CAP_MS));
                now + Duration::from_millis(wait)
            });
        }
        self.dirty |= changed;
        waiting
            .into_iter()
            .map(|(id, _)| id)
            .filter(|id| self.eligible_at.get(id).is_some_and(|at| *at <= now))
            .collect()
    }

    /// Publishes the constraints observed so far, if any changed since the last
    /// time.
    fn publish(&mut self) {
        if !std::mem::take(&mut self.dirty) {
            return;
        }
        let mut backoff_walks: Vec<_> = self
            .backoffs
            .iter()
            .map(|(walk, (_, retry))| (*walk, *retry))
            .collect();
        backoff_walks.sort_unstable();
        let backoff_starts = self
            .failed_starts
            .iter()
            .filter_map(|id| Some((*id, *self.eligible_at.get(id)?)))
            .collect();
        let mut eligible_at: Vec<_> = self.eligible_at.iter().map(|(id, at)| (*id, *at)).collect();
        eligible_at.sort_unstable();
        let state = DriverState {
            disabled: self.disabled,
            snapshot_export: self.fenced,
            throttled_until: self.throttled_until,
            backoff_walks,
            backoff_starts,
            eligible_at,
        };
        self.db
            .shared_registry()
            .driver_state()
            .publish(self.id, state);
    }

    /// The next walk to take a slice at `now`: the lowest started walk after the
    /// latest one, or else the lowest, skipping those in backoff.
    fn next_walk(&mut self, active: &PublishedRegistry, now: Instant) -> Option<u64> {
        let walks = active.walks().entries();
        let backoffs = self.backoffs.len();
        self.backoffs
            .retain(|id, _| walks.iter().any(|walk| walk.id == *id));
        self.dirty |= self.backoffs.len() != backoffs;
        let mut runnable: Vec<_> = walks
            .iter()
            .map(|walk| walk.id)
            .filter(|id| self.backoffs.get(id).is_none_or(|(_, retry)| *retry <= now))
            .collect();
        runnable.sort_unstable();
        let after = self.last_walk.unwrap_or(0);
        runnable
            .iter()
            .find(|id| **id > after)
            .or_else(|| runnable.first())
            .copied()
    }

    /// When something next becomes runnable: the earliest retry or eligibility.
    fn wake_up(&self) -> Option<Instant> {
        self.backoffs
            .values()
            .map(|(_, retry)| *retry)
            .chain(self.eligible_at.values().copied())
            .min()
    }

    /// Starts a pass for the waiting jobs `ids` in one commit. A failed start
    /// defers only those jobs, behind a backoff of its own, so started walks keep
    /// their turns; the deferred start counts towards the next wake-up.
    async fn start_passes(
        &mut self,
        tx: &mut PartitionStoreTransaction<'_>,
        ids: &[u32],
        started: Instant,
    ) -> Result<(), StorageError> {
        match engine::start_passes(tx, &self.db, ids).await {
            Ok(walk) => {
                info!(?walk, indexes = ?ids, "Started another backfill pass of indexes");
                self.start_failures = 0;
                let failed = self.failed_starts.len();
                self.failed_starts.retain(|id| !ids.contains(id));
                self.dirty |= self.failed_starts.len() != failed;
            }
            Err(SliceError::Commit(err)) => return Err(err),
            Err(SliceError::Staging(err)) => {
                warn!(indexes = ?ids, "Failed to start a backfill pass, retrying: {err}");
                let retry = self.pacing.retry(Instant::now(), self.start_failures);
                self.start_failures = self.start_failures.saturating_add(1);
                for id in ids {
                    self.eligible_at.insert(*id, retry);
                }
                for id in ids {
                    if !self.failed_starts.contains(id) {
                        self.failed_starts.push(*id);
                    }
                }
                self.dirty = true;
                // Like a failed slice, the failed start shows on the jobs.
                let error = engine::describe(&err);
                let work = started.elapsed();
                match engine::record_job_failures(tx, &self.db, ids, &error, work).await {
                    Err(SliceError::Commit(err)) => return Err(err),
                    Err(SliceError::Staging(record)) => {
                        warn!(indexes = ?ids, "Failed to record a failed pass start: {record}");
                    }
                    Ok(()) => {}
                }
            }
        }
        self.due = Some(started.max(self.rested));
        Ok(())
    }

    async fn slice(
        &mut self,
        tx: &mut PartitionStoreTransaction<'_>,
        walk: u64,
        started: Instant,
        write_bytes: usize,
    ) -> Result<(), StorageError> {
        let keys = match self.pacing.keys(started) {
            Ok(keys) => {
                if self.throttled_until.take().is_some() {
                    self.dirty = true;
                }
                keys
            }
            Err(refilled) => {
                self.throttled_until = Some(refilled);
                self.dirty = true;
                // A waiting job may become eligible before the bucket refills;
                // starting its pass needs no tokens.
                let refilled = refilled.max(self.rested);
                self.due = Some(
                    self.eligible_at
                        .values()
                        .copied()
                        .fold(refilled, Instant::min),
                );
                return Ok(());
            }
        };
        let budget = SliceBudget::new(keys, write_bytes);
        let result = engine::run_counted_slice(tx, &self.db, walk, budget).await;
        let (examined, failure) = match result {
            Ok((slice, examined)) => {
                if slice == Slice::Exhausted {
                    info!(walk, "A migration walk ended its pass");
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
        self.charge(walk, started, examined, failure);
        Ok(())
    }

    /// Charges `walk`'s work that started at `started` and ends now, examining
    /// `examined` source keys, and schedules the next step once its rest has
    /// passed since the work ended. After a `failure`, `walk` itself also waits
    /// out its backoff.
    fn charge(&mut self, walk: u64, started: Instant, examined: usize, failure: Option<String>) {
        let ended = Instant::now();
        let busy = std::mem::take(&mut self.busy);
        let stats = self.db.shared_registry().walk_stats();
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
        self.last_walk = Some(walk);
        match failure {
            None => {
                if self.backoffs.remove(&walk).is_some() {
                    self.dirty = true;
                }
            }
            Some(error) => {
                warn!(walk, "A migration walk failed, retrying: {error}");
                let failures = self
                    .backoffs
                    .get(&walk)
                    .map_or(0, |(failures, _)| *failures);
                let retry = self.pacing.retry(ended, failures);
                self.backoffs
                    .insert(walk, (failures.saturating_add(1), retry));
                self.dirty = true;
            }
        }
        self.due = Some(self.rested);
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
        // The configured cap bounds the adaptation: below 16, slices aim at exactly
        // the cap, also when keys are slow; above 1,000, fast keys reach it.
        let cap = |keys| Tuning::with_max_keys(NonZeroUsize::new(keys).unwrap());
        let mut small = Pacing::new(cap(4), None);
        assert_eq!(small.keys(t0), Ok(4));
        small.charge(t0, at(1_000_000), 4, true);
        assert_eq!(small.keys(t0), Ok(4));
        let mut large = Pacing::new(cap(5_000), None);
        assert_eq!(large.keys(t0), Ok(16));
        large.charge(t0, at(1), 1_000, false);
        assert_eq!(large.keys(t0), Ok(5_000));
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
