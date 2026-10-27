// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod completion;
#[cfg(test)]
mod lifecycle_tests;
mod tracking;

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use slotmap::SlotMap;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use restate_core::io::{FlightTracker, IoExecutor, OwnedLease, TrackedExecutor};
use restate_core::{ShutdownError, TaskCenter, TaskKind, cancellation_watcher};
use restate_futures_util::overdue::OverdueLoggingExt;
use restate_platform::sync::Mutex;
use restate_types::config::{CommonOptions, Configuration};
use restate_util_string::ReString;

use self::completion::{CloseGuard, OpenGuard, ShutdownGuard};
use self::tracking::{Status, TrackedDb};
use crate::background::{ReadyStorageTask, SinglePoolExecutor, StorageTask};
use crate::db_env::check_memory_limit;
use crate::{
    DbEnv, DbName, DbSpec, Priority, RocksAccess, RocksDb, RocksDbReadPerfGuard, RocksError,
    StorageTaskKind, metric_definitions,
};

static DB_MANAGER: OnceLock<RocksDbManager> = OnceLock::new();

slotmap::new_key_type! {
    /// Identifies one database incarnation within its originating manager.
    /// Copying or discarding a reference does not change the incarnation's state.
    pub struct DbRef;
}

#[derive(Default)]
struct State {
    tracked: SlotMap<DbRef, TrackedDb>,
    by_path: HashMap<PathBuf, DbRef>,
}

impl State {
    /// Validate replacement and transfer any owner hold. The caller inserts the
    /// new incarnation before unlocking; no active lifetime may be replaced.
    fn remove_previous(
        &mut self,
        path: &Path,
        owner: Option<DbRef>,
    ) -> Result<Option<ReString>, RocksError> {
        let previous = self.by_path.get(path).copied();
        if let Some(owner) = owner {
            let entry = self.tracked.get(owner).ok_or(RocksError::NotRegistered)?;
            if entry.path != path {
                return Err(RocksError::PathMismatch {
                    expected: entry.path.clone(),
                    actual: path.to_path_buf(),
                });
            }
            if !entry.is_held() {
                return Err(RocksError::NotHeld);
            }
            debug_assert_eq!(previous, Some(owner));
        } else if previous.is_some_and(|key| self.tracked[key].is_held()) {
            return Err(RocksError::Reserved);
        }
        if previous.is_some_and(|key| !self.tracked[key].is_closed()) {
            return Err(RocksError::AlreadyOpen);
        }
        Ok(previous.and_then(|key| {
            let entry = self.tracked.remove(key).expect("current incarnation");
            match entry.status {
                Status::Held { reason, .. } => Some(reason),
                _ => None,
            }
        }))
    }
}

#[derive(Clone, derive_more::Debug)]
#[debug("RocksDbManager")]
pub struct RocksDbManager(Arc<ManagerInner>);

#[derive(Clone, derive_more::Debug)]
#[debug("WeakRocksDbManager")]
pub struct WeakRocksDbManager(Weak<ManagerInner>);

impl WeakRocksDbManager {
    pub fn upgrade(&self) -> Option<RocksDbManager> {
        self.0.upgrade().map(RocksDbManager)
    }
}

/// Path fencing, monitoring, configuration propagation, and fallback
/// cleanup for databases owned by other components. The manager retains unclaimed
/// open results, then tracks handed-off databases weakly through destruction.
struct ManagerInner {
    // Inspect and transition incarnation metadata under this lock. Native work,
    // callbacks, notifications, and final database drops happen outside it.
    state: Mutex<State>,
    shutting_down: AtomicBool,
    background_work: Arc<FlightTracker>,
    // Admission is controlled by the incarnation protocol. Final-reference
    // cleanup remains possible for externally pinned databases after shutdown.
    lifecycle_work: Arc<FlightTracker>,
    high_pri_pool: SinglePoolExecutor,
    low_pri_pool: SinglePoolExecutor,
    // Keep at the end of the struct to ensure it's dropped last
    db_env: DbEnv,
}

impl RocksDbManager {
    #[track_caller]
    pub fn get() -> &'static RocksDbManager {
        DB_MANAGER.get().expect("DBManager not initialized")
    }

    pub fn maybe_get() -> Option<&'static RocksDbManager> {
        DB_MANAGER.get()
    }

    pub fn new(opts: &CommonOptions) -> RocksDbManager {
        metric_definitions::describe_metrics();
        let mut db_env = DbEnv::from_common_options(opts).expect("rocksdb env is created");

        // Initialize the process-wide pools only once at manager startup.
        // Initial sizes only: RocksDB grows these pools on database open to fit
        // max_background_flushes and max_background_compactions, never shrinking.
        if let Some(env) = &mut db_env.native_env {
            env.set_high_priority_background_threads(2);
            env.set_low_priority_background_threads(1);
        }

        // Create our own storage thread pools
        let high_pri_pool = SinglePoolExecutor::new(
            "rs:io-hi".to_owned(),
            opts.storage_high_priority_bg_threads().into(),
        );
        let low_pri_pool = SinglePoolExecutor::new(
            "rs:io-lo".to_owned(),
            opts.storage_low_priority_bg_threads().into(),
        );

        let manager = RocksDbManager(Arc::new(ManagerInner {
            db_env,
            state: Mutex::default(),
            shutting_down: AtomicBool::new(false),
            background_work: Arc::default(),
            lifecycle_work: Arc::default(),
            high_pri_pool,
            low_pri_pool,
        }));

        // Start db monitoring.
        TaskCenter::spawn(
            TaskKind::SystemService,
            "db-manager",
            DbWatchdog::run(manager.clone()),
        )
        .expect("run db watchdog");

        manager
    }

    /// Create a new instance of the database manager. This should not be executed concurrently,
    /// only run it once on program startup.
    ///
    /// Must run in task_center scope.
    pub fn init() -> &'static Self {
        // best-effort, it doesn't make concurrent access safe, but it's better than nothing.
        if let Some(manager) = DB_MANAGER.get() {
            return manager;
        }
        let manager = Self::new(&Configuration::pinned().common);
        DB_MANAGER.set(manager).expect("DBManager initialized once");
        Self::get()
    }

    pub fn downgrade(&self) -> WeakRocksDbManager {
        WeakRocksDbManager(Arc::downgrade(&self.0))
    }

    /// Returns the first live database with this reporting name. Names need not
    /// be unique across paths and do not identify an incarnation.
    pub fn get_db(&self, name: DbName) -> Option<Arc<RocksDb>> {
        let guard = self.0.state.lock();
        guard
            .tracked
            .values()
            .filter(|entry| entry.name == name)
            .find_map(TrackedDb::handle)
    }

    /// Returns the current tracked incarnation for the supplied path in any
    /// lifecycle state, or None if the path is untracked. Uses the path as supplied
    /// to opening or holding, without resolving filesystem aliases.
    pub fn get_db_ref(&self, path: &Path) -> Option<DbRef> {
        self.0.state.lock().by_path.get(path).copied()
    }

    /// Default shared resources, controlled by this manager's configuration watcher.
    /// Clone and customize the bundle to override or opt out of selected resources.
    pub fn db_env(&self) -> &DbEnv {
        &self.0.db_env
    }

    /// Synchronously admit and dispatch an open using the manager's resources.
    /// Only an absent or Closed path is accepted. Uses the supplied DbSpec path
    /// directly; callers coordinate filesystem aliases.
    ///
    /// Returns the fresh incarnation and its completion future. Dropping that
    /// future leaves the open and its unclaimed result accessible through
    /// [`Self::get_db_by_ref`]. Shutdown disposes of unclaimed results.
    pub fn open_db(
        &self,
        db_spec: DbSpec,
    ) -> Result<
        (
            DbRef,
            impl Future<Output = Result<Arc<RocksDb>, RocksError>> + Send + 'static + use<>,
        ),
        RocksError,
    > {
        self.open_db_with_env(db_spec, self.db_env().clone())
    }

    /// Opens with exactly the supplied resources, without filling absent fields
    /// from the default environment. Shared default resources still observe updates
    /// from this manager; custom resources are not enrolled in its resource tuning.
    /// Uses the same supplied-path fencing as [`Self::open_db`].
    pub fn open_db_with_env(
        &self,
        db_spec: DbSpec,
        env: DbEnv,
    ) -> Result<
        (
            DbRef,
            impl Future<Output = Result<Arc<RocksDb>, RocksError>> + Send + 'static + use<>,
        ),
        RocksError,
    > {
        self.open_db_inner(db_spec, env, None)
    }

    /// Reopen a physically closed, owner-held incarnation. Acceptance transfers
    /// its hold to the returned fresh DbRef, including on native failure or
    /// cancellation of the completion future. The caller must retain the new ref.
    pub fn reopen_db(
        &self,
        tracked: DbRef,
        spec: DbSpec,
    ) -> Result<
        (
            DbRef,
            impl Future<Output = Result<Arc<RocksDb>, RocksError>> + Send + 'static + use<>,
        ),
        RocksError,
    > {
        self.reopen_db_with_env(tracked, spec, self.db_env().clone())
    }

    /// Reopen an owner-held incarnation using exactly the supplied resources.
    /// The supplied path must match the incarnation identified by `tracked`.
    pub fn reopen_db_with_env(
        &self,
        tracked: DbRef,
        spec: DbSpec,
        env: DbEnv,
    ) -> Result<
        (
            DbRef,
            impl Future<Output = Result<Arc<RocksDb>, RocksError>> + Send + 'static + use<>,
        ),
        RocksError,
    > {
        self.open_db_inner(spec, env, Some(tracked))
    }

    fn open_db_inner(
        &self,
        spec: DbSpec,
        env: DbEnv,
        owner: Option<DbRef>,
    ) -> Result<
        (
            DbRef,
            impl Future<Output = Result<Arc<RocksDb>, RocksError>> + Send + 'static + use<>,
        ),
        RocksError,
    > {
        let open = self.admit_open(spec, env, owner)?;
        let tracked = open.tracked;
        // Capture this incarnation's completion before dispatch can fail and a
        // competing path-based open can replace the resulting Closed entry.
        let opening = self.get_db_by_ref(tracked);
        let task = StorageTask::default()
            .kind(StorageTaskKind::OpenDb)
            .op(move || open.run())
            .build()
            .expect("valid open task");
        tokio::task::spawn_blocking(task.into_runner());
        Ok((tracked, opening))
    }

    fn admit_open(
        &self,
        spec: DbSpec,
        env: DbEnv,
        owner: Option<DbRef>,
    ) -> Result<OpenGuard, RocksError> {
        let mut state = self.0.state.lock();
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err(ShutdownError.into());
        }
        let hold_after_close = state.remove_previous(&spec.path, owner)?;
        let (completion, result) = watch::channel(None);
        let entry = TrackedDb::opening(
            &spec,
            Status::Opening {
                since: Instant::now(),
            },
            hold_after_close,
            result,
        );
        let closed = entry.closed.clone();
        let tracked = state.tracked.insert(entry);
        state.by_path.insert(spec.path.clone(), tracked);
        // Admission, hold transfer, and accounting are atomic with
        // sealing. Dispatch does not depend on polling the returned future.
        Ok(OpenGuard::new(
            self.clone(),
            tracked,
            closed,
            spec,
            env,
            self.track_lifecycle(),
            completion,
        ))
    }

    /// Obtain a live database or await its accepted managed or remote open. A
    /// managed result is claimed on first retrieval; handed-off databases are
    /// upgraded from their weak handles. Dropping this future does not cancel
    /// opening or the owner's responsibility to finalize remote work.
    ///
    /// The reference belongs to this manager and never follows a replacement at
    /// the same path. Existing waiters retain an open failure after replacement.
    pub fn get_db_by_ref(
        &self,
        tracked: DbRef,
    ) -> impl Future<Output = Result<Arc<RocksDb>, RocksError>> + Send + 'static + use<> {
        let completion = self
            .0
            .state
            .lock()
            .tracked
            .get(tracked)
            .map(|entry| entry.open_result.clone())
            .ok_or(RocksError::NotRegistered);
        let manager = self.clone();
        async move {
            if let Some(mut completion) = completion? {
                let result = completion
                    .wait_for(Option::is_some)
                    .await
                    .map_err(|_| ShutdownError)?
                    .as_ref()
                    .expect("completed open")
                    .clone();
                result?;
            }
            let (db, accepted) = {
                let mut state = manager.0.state.lock();
                if manager.0.shutting_down.load(Ordering::Acquire) {
                    return Err(ShutdownError.into());
                }
                state
                    .tracked
                    .get_mut(tracked)
                    .ok_or(RocksError::NotRegistered)?
                    .claim()?
            };
            // Dropping a token can wake shutdown waiters. Keep it outside the lock.
            drop(accepted);
            Ok(db)
        }
    }

    fn open_native(&self, spec: DbSpec, env: DbEnv) -> Result<RocksAccess, RocksError> {
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err(ShutdownError.into());
        }
        let _perf = RocksDbReadPerfGuard::new("open-db");
        // todo: add create_if_missing to DbSpec and make directory and
        // native database creation follow that policy.
        RocksAccess::open_db(spec, env)
    }

    fn publish_open(
        &self,
        tracked: DbRef,
        db: Arc<RocksDb>,
        accepted: OwnedLease,
    ) -> Result<(), RocksError> {
        let mut state = self.0.state.lock();
        let entry = if self.0.shutting_down.load(Ordering::Acquire) {
            Err(ShutdownError.into())
        } else {
            state
                .tracked
                .get_mut(tracked)
                .ok_or(RocksError::NotRegistered)
        };
        match entry {
            Ok(entry) => {
                let name = db.name().clone();
                let elapsed = entry.opened(db, accepted);
                drop(state);
                debug!(db = %name, ?tracked, duration = ?elapsed, "RocksDB open completed");
                Ok(())
            }
            Err(error) => {
                drop(state);
                drop(db);
                drop(accepted);
                Err(error)
            }
        }
    }

    /// Book a fresh incarnation for owner-driven native opening. The owner must
    /// retain this manager and report the result through [`Self::register_db`],
    /// including failures before dispatch. Until then, the path stays fenced and
    /// shutdown waits. Discarding the ref or cancelling a waiter changes nothing.
    pub fn remote_open_db(&self, spec: &DbSpec) -> Result<DbRef, RocksError> {
        self.remote_open_db_inner(spec, None)
    }

    /// Transfer a physically closed owner's hold to a fresh remote incarnation.
    /// The caller must retain the returned ref even if native opening fails.
    pub fn remote_reopen_db(&self, tracked: DbRef, spec: &DbSpec) -> Result<DbRef, RocksError> {
        self.remote_open_db_inner(spec, Some(tracked))
    }

    fn remote_open_db_inner(
        &self,
        spec: &DbSpec,
        owner: Option<DbRef>,
    ) -> Result<DbRef, RocksError> {
        let mut state = self.0.state.lock();
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err(ShutdownError.into());
        }
        let hold_after_close = state.remove_previous(&spec.path, owner)?;
        let (completion, result) = watch::channel(None);
        let entry = TrackedDb::opening(
            spec,
            Status::RemoteOpen {
                since: Instant::now(),
                completion,
                accepted: self.track_lifecycle(),
            },
            hold_after_close,
            result,
        );
        let tracked = state.tracked.insert(entry);
        state.by_path.insert(spec.path.clone(), tracked);
        Ok(tracked)
    }

    /// Finalize the exact remote incarnation with its native open result. Call
    /// this on the owner's blocking executor: rejected native payloads are shut
    /// down and destroyed here, outside the manager lock. Invalid refs or paths
    /// leave the booked incarnation unchanged.
    ///
    /// An error result certifies that native work and resource disposal have
    /// finished (or never started). Cancellation or unwinding alone does not
    /// certify this. Failure preserves an owner hold, if present.
    ///
    /// Finalization remains accepted during shutdown, but a successful native
    /// result is disposed of instead of handed off. A successful handoff becomes
    /// the owner's responsibility, just like a claimed manager-driven open.
    pub fn register_db(
        &self,
        tracked: DbRef,
        result: Result<RocksAccess, RocksError>,
    ) -> Result<Arc<RocksDb>, RocksError> {
        let mut state = self.0.state.lock();
        let entry = state
            .tracked
            .get_mut(tracked)
            .ok_or(RocksError::NotRegistered)
            .and_then(|entry| {
                if !matches!(entry.status, Status::RemoteOpen { .. }) {
                    return Err(RocksError::NotRemoteOpen);
                }
                if let Ok(native) = &result
                    && native.path() != &entry.path
                {
                    return Err(RocksError::PathMismatch {
                        expected: entry.path.clone(),
                        actual: native.path().clone(),
                    });
                }
                Ok(entry)
            });
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                let disposal = result.is_ok().then(|| self.track_lifecycle());
                drop(state);
                if let Ok(native) = result {
                    native.shutdown();
                    drop(native);
                }
                drop(disposal);
                return Err(error);
            }
        };
        let (completion, accepted) = entry.take_remote_open();
        let result = match result {
            Ok(native) => {
                // Bind the lifetime once, before any Arc can leave this call.
                let db = RocksDb::from_access(self.clone(), native, tracked, entry.closed.clone());
                if self.0.shutting_down.load(Ordering::Acquire) {
                    drop(state);
                    // Final-reference cleanup takes over accounting before the
                    // remote admission token is released below.
                    drop(db);
                    Err(ShutdownError.into())
                } else {
                    entry.name = db.name().clone();
                    let elapsed = entry.registered(&db);
                    drop(state);
                    debug!(db = %db.name(), ?tracked, duration = ?elapsed, "Remote RocksDB open completed");
                    Ok(db)
                }
            }
            Err(error) => {
                let closed = entry.closed.clone();
                drop(state);
                self.finish_close(tracked, closed);
                Err(error)
            }
        };
        completion.send_replace(Some(result.as_ref().map(|_| ()).map_err(Clone::clone)));
        drop(accepted);
        result
    }

    /// Book an absent or physically closed path for owner maintenance. No native
    /// work is started. The hold survives discarded refs until explicit release
    /// or transfer to a fresh incarnation by [`Self::reopen_db`] or
    /// [`Self::remote_reopen_db`].
    pub fn hold_db(
        &self,
        name: DbName,
        path: PathBuf,
        reason: ReString,
    ) -> Result<DbRef, RocksError> {
        let mut state = self.0.state.lock();
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err(ShutdownError.into());
        }
        state.remove_previous(&path, None)?;
        let tracked = state
            .tracked
            .insert(TrackedDb::held(name, path.clone(), reason));
        state.by_path.insert(path, tracked);
        Ok(tracked)
    }

    /// Hold an existing incarnation across physical close, deletion, and reopen.
    /// This keeps no native pin and is allowed during shutdown. It does not make
    /// an active lifetime eligible for release or replacement.
    pub fn hold_db_by_ref(&self, tracked: DbRef, reason: ReString) -> Result<(), RocksError> {
        let mut state = self.0.state.lock();
        let entry = state
            .tracked
            .get_mut(tracked)
            .ok_or(RocksError::NotRegistered)?;
        if entry.is_held() {
            return Err(RocksError::Reserved);
        }
        if let Status::Closed { since } = entry.status {
            entry.status = Status::Held { since, reason };
        } else {
            entry.hold_after_close = Some(reason);
        }
        Ok(())
    }

    /// Release an owner hold only after native work and physical close complete.
    pub fn release_db(&self, tracked: DbRef) -> Result<(), RocksError> {
        let mut state = self.0.state.lock();
        let entry = state
            .tracked
            .get_mut(tracked)
            .ok_or(RocksError::NotRegistered)?;
        if !entry.is_held() {
            return Err(RocksError::NotHeld);
        }
        let Status::Held { since, .. } = entry.status else {
            return Err(RocksError::AlreadyOpen);
        };
        entry.status = Status::Closed { since };
        Ok(())
    }

    /// Observe this incarnation's physical completion without pinning native
    /// resources, releasing its hold, or following later replacements.
    pub fn closed(
        &self,
        tracked: DbRef,
    ) -> Result<impl Future<Output = ()> + Send + 'static + use<>, RocksError> {
        Ok(self
            .0
            .state
            .lock()
            .tracked
            .get(tracked)
            .ok_or(RocksError::NotRegistered)?
            .closed
            .clone()
            .cancelled_owned())
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn reset(&self) -> anyhow::Result<()> {
        self.shutdown().await;
        let mut state = self.0.state.lock();
        anyhow::ensure!(
            state
                .tracked
                .values()
                .all(|entry| entry.is_closed() && !entry.is_held()),
            "cannot reset RocksDB manager while database lifetimes or owner holds remain active"
        );
        // Clear without replacing the SlotMap, preserving stale-key rejection.
        state.tracked.clear();
        state.by_path.clear();
        self.0.background_work.reset();
        self.0.shutting_down.store(false, Ordering::Release);
        Ok(())
    }

    /// Occupy every worker in a pool until the returned senders are dropped.
    /// Used to deterministically test queued work and cancelled result waiters.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn pause_background(&self, priority: Priority) -> Vec<std::sync::mpsc::Sender<()>> {
        let pool = match priority {
            Priority::High => &self.0.high_pri_pool,
            Priority::Low => &self.0.low_pri_pool,
        };
        let mut resumes = Vec::with_capacity(pool.max_count());
        for _ in 0..pool.max_count() {
            let (resume, paused) = std::sync::mpsc::channel();
            let (started, reached) = tokio::sync::oneshot::channel();
            resumes.push(resume);
            pool.dispatch_fn(priority, move || {
                let _ = started.send(());
                let _ = paused.recv();
            })
            .expect("test pool accepts work");
            reached.await.expect("background worker started");
        }
        resumes
    }

    /// Capacity of the default shared WBM, excluding independently supplied WBMs.
    pub fn get_total_write_buffer_capacity(&self) -> u64 {
        self.db_env()
            .write_buffer_manager
            .as_ref()
            .map_or(0, |manager| manager.get_buffer_size() as u64)
    }

    /// Usage of the default shared WBM, excluding independently supplied WBMs.
    pub fn get_total_write_buffer_usage(&self) -> u64 {
        self.db_env()
            .write_buffer_manager
            .as_ref()
            .map_or(0, |manager| manager.get_usage() as u64)
    }

    /// Returns aggregated memory usage for the selected databases and their supplied
    /// caches. Shared caches are counted once, using their full usage rather than
    /// attributing entries to individual databases. An empty filter also includes
    /// the manager's default cache, even when no database currently uses it.
    pub fn get_memory_usage_stats(
        &self,
        filter: &[DbName],
    ) -> Result<rocksdb::perf::MemoryUsage, RocksError> {
        // for safety.
        // keep databases alive while we are aggregating memory usage
        let pinned = self.get_all_dbs();
        let mut builder = rocksdb::perf::MemoryUsageBuilder::new()?;
        if filter.is_empty()
            && let Some(cache) = &self.db_env().cache
        {
            builder.add_cache(cache);
        }

        for db in &pinned {
            if !filter.is_empty() && !filter.contains(db.name()) {
                continue;
            }
            builder.add_db(db.inner().as_raw_db());
            if let Some(cache) = &db.inner().db_env().cache {
                // The native builder deduplicates shared caches.
                builder.add_cache(cache);
            }
        }

        Ok(builder.build()?)
    }

    /// Returns aggregated memory usage for all databases if filter is empty
    pub fn get_db_memory_usage_stats(
        &self,
        db: &Arc<RocksDb>,
    ) -> Result<rocksdb::perf::MemoryUsage, RocksError> {
        let mut builder = rocksdb::perf::MemoryUsageBuilder::new()?;
        builder.add_db(db.inner().as_raw_db());
        Ok(builder.build()?)
    }

    pub fn get_all_dbs(&self) -> Vec<Arc<RocksDb>> {
        let guard = self.0.state.lock();
        guard
            .tracked
            .values()
            .filter_map(TrackedDb::handle)
            .collect()
    }

    /// Closes the database and waits for completion.
    pub(crate) async fn close_db(&self, db: Arc<RocksDb>) -> Result<(), Arc<RocksDb>> {
        let db = Arc::try_unwrap(db)?;
        let closed = db.closed().log_slow_after(
            Duration::from_secs(5),
            tracing::Level::WARN,
            format!("Closing RocksDB database '{}'", db.name()),
        );
        // Drop transfers the native payload to cleanup. The observer owns no
        // database reference and remains valid if its waiter is cancelled.
        drop(db);
        closed.await;
        Ok(())
    }

    /// Final-reference cleanup addresses only the originating incarnation.
    pub(crate) fn drop_db(&self, db: RocksAccess, closed: CancellationToken, registration: DbRef) {
        let active = self.track_lifecycle();
        let needs_shutdown = {
            let mut state = self.0.state.lock();
            state
                .tracked
                .get_mut(registration)
                .is_none_or(TrackedDb::begin_close)
        };
        let cleanup = CloseGuard::new(
            self.clone(),
            db,
            closed,
            registration,
            needs_shutdown,
            active,
        );
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::spawn_blocking(move || drop(cleanup));
        } else {
            // Also supports final-reference cleanup outside a Tokio runtime.
            drop(cleanup);
        }
    }

    fn finish_close(&self, registration: DbRef, closed: CancellationToken) {
        let completed = {
            let mut state = self.0.state.lock();
            state
                .tracked
                .get_mut(registration)
                .map(|entry| (entry.name.clone(), entry.finish_close()))
        };
        if let Some((name, elapsed)) = completed {
            debug!(db = %name, tracked = ?registration, duration = ?elapsed, "RocksDB lifetime completed");
        }
        // Observers may run user code. Native destruction and metadata updates
        // must both finish before waking them, without holding the manager lock.
        closed.cancel();
    }

    fn finish_shutdown(&self, tracked: DbRef, completed: bool) {
        if let Some(entry) = self.0.state.lock().tracked.get_mut(tracked) {
            entry.finish_shutdown(completed);
        }
    }

    fn track_lifecycle(&self) -> OwnedLease {
        self.0
            .lifecycle_work
            .try_acquire()
            .expect("lifecycle admission is controlled by the manager")
            .into_owned()
    }

    fn seal_opens(&self) {
        let unclaimed: Vec<_> = {
            let mut state = self.0.state.lock();
            self.0.shutting_down.store(true, Ordering::Release);
            state
                .tracked
                .values_mut()
                .filter_map(TrackedDb::take_unclaimed)
                .collect()
        };
        // Dispose before waiting on admission tokens, including results whose
        // original future was dropped. Database drops re-enter the manager.
        drop(unclaimed);
    }

    async fn wait_for_cleanup(&self) {
        self.0.lifecycle_work.wait_idle().await;
        // Native opening, preparation, or destruction may unwind without a known
        // outcome. Keep those lifetimes pending until physical completion.
        let pending: Vec<_> = self
            .0
            .state
            .lock()
            .tracked
            .values()
            .filter_map(TrackedDb::pending_cleanup)
            .collect();
        for closed in pending {
            closed.cancelled().await;
        }
    }

    /// Seal new opens and background I/O, drain accepted work, and perform fallback
    /// preparatory shutdown. Databases still pinned by their owners remain Open;
    /// their physical completion is observed separately via [`RocksDb::closed`].
    /// Interrupted preparation keeps shutdown pending until physical close.
    pub async fn shutdown(&self) {
        self.seal_opens();
        self.0.background_work.seal();
        self.0.background_work.drain().await;
        self.wait_for_cleanup().await;
        let pending: Vec<_> = self
            .0
            .state
            .lock()
            .tracked
            .values_mut()
            .filter_map(|entry| {
                let db = entry.claim_shutdown()?;
                // Publish the claim and its accounting under the same lock.
                Some(ShutdownGuard::new(db, self.track_lifecycle()))
            })
            .collect();
        for shutdown in pending {
            tokio::task::spawn_blocking(move || drop(shutdown));
        }
        self.wait_for_cleanup().await;
        if let Some(env) = &self.db_env().native_env {
            env.clone().join_all_threads();
        }
        info!("Rocksdb manager shutdown tasks completed");
    }

    /// Emergency shutdown is ongoing, this will ensure rocksdb's wal is fsynced.
    pub fn on_ungraceful_shutdown(&self) {
        let Some(guard) = self.0.state.try_lock_for(Duration::from_secs(1)) else {
            eprintln!("[rocksdb] couldn't acquire registry lock to flush in time");
            return;
        };
        let databases: Vec<_> = guard
            .tracked
            .values()
            .filter_map(TrackedDb::handle)
            .collect();
        drop(guard);
        // WAL first
        for db in &databases {
            let name = db.name();
            if let Err(e) = db.db.flush_wal(true) {
                eprintln!("[rocksdb] failed to flush WAL of {name}: {e}");
            } else {
                eprintln!("[rocksdb] flushed WAL of {name}");
            }
        }

        // Best effort normal shutdown
        for db in &databases {
            db.db.shutdown();
        }

        if !databases.is_empty() {
            eprintln!("[rocksdb] flushed all!");
        }
    }

    /// Spawn a rocksdb blocking operation in the background
    pub(crate) async fn async_spawn<OP, R>(
        &self,
        task: ReadyStorageTask<OP>,
    ) -> Result<R, ShutdownError>
    where
        OP: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err(ShutdownError);
        }

        self.async_spawn_unchecked(task).await
    }

    /// Allows work after node shutdown is requested, until manager shutdown seals
    /// background I/O. Every accepted task still participates in that drain.
    pub(crate) async fn async_spawn_unchecked<OP, R>(
        &self,
        task: ReadyStorageTask<OP>,
    ) -> Result<R, ShutdownError>
    where
        OP: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.executor(task.priority)
            .spawn(task)?
            .await
            .map_err(|_| ShutdownError)
    }

    #[allow(dead_code)]
    pub(crate) fn spawn<OP>(&self, task: ReadyStorageTask<OP>) -> Result<(), ShutdownError>
    where
        OP: FnOnce() + Send + 'static,
    {
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err(ShutdownError);
        }
        self.spawn_unchecked(task)
    }

    pub(crate) fn spawn_unchecked<OP>(
        &self,
        task: ReadyStorageTask<OP>,
    ) -> Result<(), ShutdownError>
    where
        OP: FnOnce() + Send + 'static,
    {
        self.executor(task.priority).dispatch(task)
    }

    fn executor(&self, priority: Priority) -> TrackedExecutor<'_, SinglePoolExecutor> {
        let pool = match priority {
            Priority::High => &self.0.high_pri_pool,
            Priority::Low => &self.0.low_pri_pool,
        };
        TrackedExecutor::new(pool, &self.0.background_work)
    }
}

struct DbWatchdog {
    manager: RocksDbManager,
    current_common_opts: CommonOptions,
}

impl DbWatchdog {
    pub async fn run(manager: RocksDbManager) -> anyhow::Result<()> {
        let prev_opts = Configuration::pinned().common.clone();
        let mut watchdog = Self {
            manager,
            current_common_opts: prev_opts,
        };

        let shutdown_watch = cancellation_watcher();
        tokio::pin!(shutdown_watch);

        let config_watch = Configuration::watcher();
        tokio::pin!(config_watch);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_watch => {
                    // Shutdown requested.
                    watchdog.manager.seal_opens();
                    break;
                }
                _ = config_watch.changed() => {
                    watchdog.on_config_update();
                }
            }
        }

        Ok(())
    }

    fn on_config_update(&mut self) {
        // ignore if in shutdown
        if self.manager.0.shutting_down.load(Ordering::Acquire) {
            info!("Ignoring config update as we are shutting down");
            return;
        }
        let new_common_opts = &Configuration::pinned().common;

        // Memory budget changed?
        if new_common_opts.rocksdb_total_memory_size()
            != self.current_common_opts.rocksdb_total_memory_size()
        {
            warn!(
                old = %self.current_common_opts.rocksdb_total_memory_size(),
                new = %new_common_opts.rocksdb_total_memory_size(),
                "[config update] Setting rocksdb total memory limit to {}",
                new_common_opts.rocksdb_total_memory_size()
            );
            check_memory_limit(new_common_opts);
            if let Some(cache) = &self.manager.db_env().cache {
                cache
                    .clone()
                    .set_capacity(new_common_opts.rocksdb_total_memory_size().as_usize());
            }
            if let Some(manager) = &self.manager.db_env().write_buffer_manager {
                manager.set_buffer_size(new_common_opts.rocksdb_total_memtables_size().as_usize());
            }
        }

        // update memtable total memory
        if new_common_opts.rocksdb_total_memtables_size()
            != self.current_common_opts.rocksdb_total_memtables_size()
        {
            warn!(
                old = %self.current_common_opts.rocksdb_total_memtables_size(),
                new = %new_common_opts.rocksdb_total_memtables_size(),
                "[config update] Setting rocksdb total memtables size limit to {}",
                new_common_opts.rocksdb_total_memtables_size()
            );
            if let Some(manager) = &self.manager.db_env().write_buffer_manager {
                manager.set_buffer_size(new_common_opts.rocksdb_total_memtables_size().as_usize());
            }
        }

        // Databases choose to react to config updates as they see fit.
        // e.g. set write_buffer_size
        // Native callbacks and final reference drops run outside registry locks.
        for db in self.manager.get_all_dbs() {
            db.note_config_update();
        }

        self.current_common_opts = new_common_opts.clone();
    }
}

static_assertions::assert_impl_all!(RocksDbManager: Send, Sync);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use restate_core::TaskCenterBuilder;
    use restate_platform::sync::Mutex;
    use restate_types::config::{Configuration, node_dir};
    use restate_types::protobuf::common::DatabaseKind;

    use super::{DbWatchdog, RocksDbManager};
    use crate::configuration::{DbConfigurator, create_default_db_options};
    use crate::{DbEnv, DbName, DbSpecBuilder, RocksAccess, RocksDb};

    #[derive(Clone, Default)]
    struct ReleaseOnConfigUpdate {
        owner: Arc<Mutex<Option<Arc<RocksDb>>>>,
    }

    impl DbConfigurator for ReleaseOnConfigUpdate {
        fn get_db_options(&self, db_name: &DbName, env: &DbEnv) -> rocksdb::Options {
            create_default_db_options(env, db_name)
        }

        fn note_config_update(&self, _db: &RocksAccess) {
            // On regression, retain the external owner so the test can report the
            // failure instead of deadlocking when the watchdog drops its Arc.
            if RocksDbManager::get().0.state.try_lock().is_some() {
                // Leave the watchdog holding the last strong reference.
                self.owner.lock().take();
            }
        }
    }

    struct MemoryStatsConfigurator;

    impl DbConfigurator for MemoryStatsConfigurator {
        fn get_db_options(&self, db_name: &DbName, env: &DbEnv) -> rocksdb::Options {
            create_default_db_options(env, db_name)
        }
    }

    #[tokio::test]
    async fn memory_stats_filter_and_deduplicate_supplied_caches() -> anyhow::Result<()> {
        let task_center = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()?
            .into_handle();
        let manager = task_center.run_sync(RocksDbManager::init);
        let tempdir = restate_test_util::tempdir()?;
        let custom_cache = rocksdb::Cache::new_lru_cache(1024 * 1024);
        // Avoid WBM cache reservations changing while measuring block-cache usage.
        let default_env = manager.db_env().clone().with_write_buffer_manager(None);
        let custom_env = default_env.clone().with_cache(Some(custom_cache.clone()));
        let mut databases = Vec::new();
        let mut names = Vec::new();
        for (index, env) in [
            default_env.clone(),
            default_env,
            custom_env.clone(),
            custom_env,
        ]
        .into_iter()
        .enumerate()
        {
            let name = DbName::new(&format!("memory-stats-{index}"));
            let spec = DbSpecBuilder::new(
                name.clone(),
                DatabaseKind::PartitionStore,
                tempdir.path().join(name.as_str()),
                MemoryStatsConfigurator,
            )
            .cf_patterns(Vec::new())
            .build()?;
            let (_, opening) = manager.open_db_with_env(spec, env)?;
            let db = opening.await?;
            let raw = db.inner().as_raw_db();
            raw.put(b"key", [42; 4096])?;
            raw.flush()?;
            assert!(raw.get(b"key")?.is_some());
            names.push(name);
            databases.push(db);
        }

        let default_usage = manager.db_env().cache.as_ref().unwrap().get_usage() as u64;
        let custom_usage = custom_cache.get_usage() as u64;
        assert!(default_usage > 0);
        assert!(custom_usage > 0);
        assert_eq!(
            manager
                .get_memory_usage_stats(&[])?
                .approximate_cache_total(),
            default_usage + custom_usage,
        );
        assert_eq!(
            manager
                .get_memory_usage_stats(&names[..2])?
                .approximate_cache_total(),
            default_usage,
        );
        assert_eq!(
            manager
                .get_memory_usage_stats(&names[2..])?
                .approximate_cache_total(),
            custom_usage,
        );
        assert_eq!(
            manager
                .get_memory_usage_stats(&names[2..3])?
                .approximate_cache_total(),
            custom_usage,
        );
        assert_eq!(
            manager
                .get_memory_usage_stats(&[DbName::new("missing")])?
                .approximate_cache_total(),
            0,
        );

        for db in databases {
            db.close().await.expect("all database users released");
        }
        task_center.shutdown_node("test completed", 0).await;
        manager.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn config_update_can_release_last_database_reference() -> anyhow::Result<()> {
        let task_center = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()?
            .into_handle();
        let manager = task_center.run_sync(RocksDbManager::init);
        let mut owners = Vec::new();
        for name in ["config-update-first", "config-update-second"] {
            let configurator = ReleaseOnConfigUpdate::default();
            let spec = DbSpecBuilder::new(
                DbName::new(name),
                DatabaseKind::PartitionStore,
                node_dir().join(name),
                configurator.clone(),
            )
            .cf_patterns(Vec::new())
            .build()?;
            let (_, opening) = manager.open_db(spec)?;
            let db = opening.await?;
            *configurator.owner.lock() = Some(db);
            owners.push(configurator.owner);
        }

        let mut watchdog = DbWatchdog {
            manager: manager.clone(),
            current_common_opts: Configuration::pinned().common.clone(),
        };
        watchdog.on_config_update();

        let released_in_callback = owners.iter().all(|owner| owner.lock().is_none());
        let databases_released = manager.get_all_dbs().is_empty();
        // Also release retained owners on regression, after the registry lock is gone.
        for owner in owners {
            owner.lock().take();
        }
        task_center.shutdown_node("test completed", 0).await;
        manager.shutdown().await;

        assert!(
            released_in_callback,
            "config callbacks must run without the database registry lock"
        );
        assert!(
            databases_released,
            "the watchdog must release its final database references before returning"
        );
        Ok(())
    }
}
