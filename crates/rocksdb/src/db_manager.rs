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
use std::time::Duration;

use slotmap::SlotMap;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use restate_core::io::{FlightTracker, IoExecutor, OwnedLease};
use restate_core::{ShutdownError, TaskCenter, TaskKind, cancellation_watcher};
use restate_platform::sync::Mutex;
use restate_types::config::{CommonOptions, Configuration};

use self::completion::{CloseGuard, OpenGuard, ShutdownGuard};
use self::tracking::TrackedDb;
use crate::background::{GlobalExecutor, SinglePoolExecutor, StorageTask};
use crate::db_env::check_memory_limit;
use crate::{
    DbCleanup, DbEnv, DbName, DbRegistration, DbSpec, Priority, RocksAccess, RocksDb, RocksError,
    StorageTaskKind, metric_definitions,
};

static DB_MANAGER: OnceLock<RocksDbManager> = OnceLock::new();

slotmap::new_key_type! {
    /// Identifies one lifetime of a database at a path, from open until close (an
    /// "incarnation"), within the manager that issued it. Opening the same path
    /// again creates a new `DbRef`; after that, the old one is reported as
    /// `NotRegistered`. Copying or dropping a `DbRef` has no effect on the database.
    pub struct DbRef;
}

#[derive(Default)]
struct State {
    tracked: SlotMap<DbRef, TrackedDb>,
    by_path: HashMap<PathBuf, DbRef>,
}

impl State {
    /// Forgets the previous record for `path`, which must be closed (`AlreadyOpen`
    /// otherwise). The caller must insert the new record before releasing the lock,
    /// so no other open can take the path in between.
    fn remove_previous(&mut self, path: &Path) -> Result<(), RocksError> {
        if let Some(previous) = self.by_path.get(path).copied() {
            if !self.tracked[previous].is_closed() {
                return Err(RocksError::AlreadyOpen);
            }
            self.tracked.remove(previous);
        }
        Ok(())
    }
}

/// Tracks the RocksDB databases opened on this node, from opening until their
/// native resources are released.
///
/// The manager makes sure a path is open at most once, applies memory and
/// configuration updates, and closes databases on its own lifecycle thread pool
/// when their last handle is dropped. It keeps a successful open's result until
/// someone collects it, then only a weak handle. It doesn't stop components from
/// using the raw RocksDB handle directly.
///
/// Cloning is cheap; clones share the same state.
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

/// State shared by all clones of a [`RocksDbManager`].
struct ManagerInner {
    // Records of all databases. Hold this lock only to read or update records:
    // RocksDB calls, callbacks, waking waiters, and dropping a database (which can
    // call back into the manager) all happen after releasing it.
    state: Mutex<State>,
    shutting_down: AtomicBool,
    // Counts in-flight opens, shutdown flushes, and closes. Never sealed: a database
    // still referenced elsewhere can be dropped after shutdown, and its close must
    // still be accepted and counted.
    lifecycle_tracker: Arc<FlightTracker>,
    executor: GlobalExecutor,
    lifecycle_executor: SinglePoolExecutor,
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

    /// Creates a manager that runs regular I/O on `executor` and opens and closes
    /// databases on its own `rs:io-lifecycle` pool. `executor` may be shared with
    /// other managers; [`Self::shutdown`] only stops this manager's databases.
    ///
    /// Unlike [`Self::init`], the manager isn't installed as the global one.
    /// Must run inside a TaskCenter, since it starts the configuration watchdog.
    pub fn new(opts: &CommonOptions, executor: GlobalExecutor) -> RocksDbManager {
        metric_definitions::describe_metrics();
        let mut db_env = DbEnv::from_common_options(opts).expect("rocksdb env is created");

        // Initialize the process-wide pools only once at manager startup.
        // Initial sizes only: RocksDB grows these pools on database open to fit
        // max_background_flushes and max_background_compactions, never shrinking.
        if let Some(env) = &mut db_env.native_env {
            env.set_high_priority_background_threads(2);
            env.set_low_priority_background_threads(1);
        }

        // Open, flush on shutdown, and close databases on their own threads, so this
        // doesn't wait behind regular I/O or block Tokio, and one slow database doesn't
        // hold up the others. The pool is never shut down, so databases dropped after
        // shutdown can still be closed.
        let lifecycle_executor = SinglePoolExecutor::new(
            "rs:io-lifecycle".to_owned(),
            opts.storage_lifecycle_threads().get(),
        );

        let manager = RocksDbManager(Arc::new(ManagerInner {
            db_env,
            state: Mutex::default(),
            shutting_down: AtomicBool::new(false),
            lifecycle_tracker: Arc::default(),
            executor,
            lifecycle_executor,
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
        let config = Configuration::pinned();
        let executor = GlobalExecutor::from_options(&config.common);
        let manager = Self::new(&config.common, executor);
        // Dependencies of test crates do not necessarily enable test-util.
        // Registration is unconditional; only shutdown_for_test runs finalizers.
        {
            let manager = manager.clone();
            TaskCenter::register_test_finalizer(Box::pin(async move {
                manager.shutdown().await;
                let state = manager.0.state.lock();
                for (tracked, entry) in &state.tracked {
                    assert!(
                        entry.is_closed(),
                        "database lifetime survived test teardown: {tracked:?} ({})",
                        entry.name,
                    );
                }
            }));
        }
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

    /// Returns the [`DbRef`] of the latest database opened at `path`, whatever its
    /// state (opening, open, or closed), or `None` if the manager hasn't seen this
    /// path. The path is compared exactly as given to [`Self::open_db`]; a symlink or
    /// other alias for the same directory doesn't match.
    pub fn get_db_ref(&self, path: &Path) -> Option<DbRef> {
        self.0.state.lock().by_path.get(path).copied()
    }

    /// Default shared resources, controlled by this manager's configuration watcher.
    /// Clone and customize the bundle to override or opt out of selected resources.
    pub fn db_env(&self) -> &DbEnv {
        &self.0.db_env
    }

    /// The `rs:io-lifecycle` pool that opens and closes this manager's databases.
    /// Other components can share it for lifecycle work that must not wait behind
    /// regular storage I/O, such as creating or importing partition stores. That
    /// work competes with opening and closing databases for the same threads.
    pub fn lifecycle_executor(&self) -> &SinglePoolExecutor {
        &self.0.lifecycle_executor
    }

    /// Starts opening a database with the manager's shared resources and returns
    /// right away, with the new database's [`DbRef`] and a future for the result.
    ///
    /// ```ignore
    /// let (_, opening) = manager.open_db(spec)?;
    /// let db = opening.await?;
    /// ```
    ///
    /// Fails with `AlreadyOpen` if a database at the same path is still opening, open,
    /// or closing, and with `Shutdown` once the manager is shutting down. The path is
    /// used exactly as given; the manager can't tell that two different paths point
    /// to the same directory.
    ///
    /// The open runs even if the returned future is dropped. The result can then be
    /// collected with [`Self::get_db_by_ref`]. At shutdown, the manager closes
    /// databases that nobody collected.
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
    /// Otherwise behaves like [`Self::open_db`].
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
        let open = self.admit_open(db_spec, env)?;
        let tracked = open.tracked;
        // Get the result future before dispatching. If dispatch fails, the record is
        // closed and another open could replace it right away, losing this result.
        let opening = self.get_db_by_ref(tracked);
        let task = StorageTask::default()
            .kind(StorageTaskKind::OpenDb)
            .op(move || open.run())
            .build()
            .expect("valid open task");
        self.0.lifecycle_executor.dispatch(task)?;
        Ok((tracked, opening))
    }

    fn admit_open(&self, spec: DbSpec, env: DbEnv) -> Result<OpenGuard, RocksError> {
        let mut state = self.0.state.lock();
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err(ShutdownError.into());
        }
        state.remove_previous(&spec.path)?;
        let (completion, result) = watch::channel(None);
        let entry = TrackedDb::opening(&spec, result);
        let closed = entry.closed.clone();
        let tracked = state.tracked.insert(entry);
        state.by_path.insert(spec.path.clone(), tracked);
        // Booking the path and taking the lifecycle lease happen under the same lock
        // as shutdown's check, so shutdown either rejects this open or waits for it.
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

    /// Returns the database for `tracked`, waiting for its open to finish if needed.
    /// The first call after a successful open takes the database from the manager.
    /// Later calls get it only while someone else still holds an `Arc`, and fail with
    /// `NotOpen` otherwise. Dropping this future doesn't cancel the open.
    ///
    /// A `DbRef` always means the same incarnation. After the path is reopened, the
    /// old ref fails with `NotRegistered`, while callers that were already waiting
    /// still see the old open's error if it failed.
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
            let mut completion = completion?;
            let result = completion
                .wait_for(Option::is_some)
                .await
                .map_err(|_| ShutdownError)?
                .as_ref()
                .expect("completed open")
                .clone();
            result?;
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
            // Releasing the lease can wake shutdown, so do it after releasing the lock.
            drop(accepted);
            Ok(db)
        }
    }

    fn prepare_open(
        &self,
        spec: DbSpec,
        env: DbEnv,
    ) -> Result<crate::rock_access::PreparedOpen, RocksError> {
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err(ShutdownError.into());
        }
        // todo: add create_if_missing to DbSpec and make directory and
        // native database creation follow that policy.
        RocksAccess::prepare_open(spec, env)
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

    fn bind_db(
        &self,
        native: RocksAccess,
        tracked: DbRef,
        closed: CancellationToken,
    ) -> Arc<RocksDb> {
        let mut db = RocksDb::new(native, self.0.executor.clone());
        db.registration = Some(DbRegistration {
            manager: self.clone(),
            tracked,
            closed,
        });
        Arc::new(db)
    }

    /// Returns a future that completes once this incarnation is fully closed and its
    /// native resources are released. It doesn't keep the database open, and it
    /// doesn't follow a later reopen of the same path.
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
            state.tracked.values().all(TrackedDb::is_closed),
            "cannot reset RocksDB manager while database lifetimes remain active"
        );
        // Clear the SlotMap instead of replacing it, so old `DbRef`s are still rejected.
        state.tracked.clear();
        state.by_path.clear();
        self.0.shutting_down.store(false, Ordering::Release);
        Ok(())
    }

    /// Occupy every worker in a pool until the returned senders are dropped.
    /// Used to deterministically test queued work and cancelled result waiters.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn pause_background(&self, priority: Priority) -> Vec<std::sync::mpsc::Sender<()>> {
        let pool = &self.0.executor;
        let mut resumes = Vec::with_capacity(pool.max_count(priority));
        for _ in 0..pool.max_count(priority) {
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

    /// Occupy every lifecycle worker after all previously queued work has completed.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn pause_lifecycle(&self) -> Vec<std::sync::mpsc::Sender<()>> {
        let pool = &self.0.lifecycle_executor;
        let mut resumes = Vec::with_capacity(pool.max_count());
        for _ in 0..pool.max_count() {
            let (resume, paused) = std::sync::mpsc::channel();
            let (started, reached) = tokio::sync::oneshot::channel();
            resumes.push(resume);
            pool.dispatch_fn(Priority::Low, move || {
                let _ = started.send(());
                let _ = paused.recv();
            })
            .expect("lifecycle pool accepts work");
            reached.await.expect("lifecycle worker started");
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

    /// Called when the last `Arc<RocksDb>` of a managed database is dropped. Closes it
    /// on the lifecycle pool, which releases memory and file handles; the data on
    /// disk stays.
    pub(crate) fn close_db(&self, mut cleanup: DbCleanup) {
        let registration = cleanup
            .registration
            .as_ref()
            .expect("registered database")
            .tracked;
        let active = self.track_lifecycle();
        let needs_shutdown = {
            let mut state = self.0.state.lock();
            state
                .tracked
                .get_mut(registration)
                .is_none_or(TrackedDb::begin_close)
        };
        cleanup.needs_shutdown = needs_shutdown;
        let cleanup = CloseGuard::new(cleanup, active);
        self.0
            .lifecycle_executor
            .dispatch_fn(Priority::Low, move || drop(cleanup))
            .expect("lifecycle executor remains available through native cleanup");
    }

    pub(crate) fn finish_close(&self, registration: DbRef, closed: CancellationToken) {
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
        // Wake `closed` waiters last: after the native database is destroyed and the
        // record says closed, and without the manager lock held, since a waiter may call
        // back into the manager right away.
        closed.cancel();
    }

    fn finish_shutdown(&self, tracked: DbRef, completed: bool) {
        if let Some(entry) = self.0.state.lock().tracked.get_mut(tracked) {
            entry.finish_shutdown(completed);
        }
    }

    pub(crate) fn track_lifecycle(&self) -> OwnedLease {
        self.0
            .lifecycle_tracker
            .try_acquire()
            .expect("lifecycle admission is controlled by the manager")
            .into_owned()
    }

    pub(crate) fn dispatch_cleanup(&self, task: impl FnOnce() + Send + 'static) {
        // Tuple fields drop in order, so the task's captures are released before the
        // lease, even if the job is dropped without running.
        let cleanup = (task, self.track_lifecycle());
        self.0
            .lifecycle_executor
            .dispatch_fn(Priority::Low, move || {
                let (task, _active) = cleanup;
                task();
            })
            .expect("lifecycle executor remains available through native cleanup");
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
        // Drop the databases nobody collected, including those whose open future was
        // dropped, before waiting for in-flight work: each drop queues that database's
        // close. Do it outside the lock, since dropping a database calls back into the
        // manager.
        drop(unclaimed);
    }

    async fn wait_for_cleanup(&self) {
        self.0.lifecycle_tracker.wait_idle().await;
        // Wait for databases that are still opening or closing, and for any whose flush
        // panicked: their `closed` signal fires only once the native database is gone.
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

    /// Stops new opens and new database work, waits for accepted work to finish,
    /// then flushes the WAL and the `flush_on_shutdown` column families of every
    /// database that is still open.
    ///
    /// Databases still referenced elsewhere stay open; use [`RocksDb::closed`] to
    /// wait until they are actually closed. If a flush panics, shutdown keeps
    /// waiting until that database is closed.
    pub async fn shutdown(&self) {
        self.seal_opens();
        let databases = self.get_all_dbs();
        for db in &databases {
            db.seal();
        }
        for db in &databases {
            db.drain().await;
        }
        drop(databases);
        self.wait_for_cleanup().await;
        let pending: Vec<_> = self
            .0
            .state
            .lock()
            .tracked
            .values_mut()
            .filter_map(|entry| {
                let db = entry.claim_shutdown()?;
                // Claim the flush and take its lease under the same lock, so a concurrent
                // shutdown neither flushes twice nor misses this work.
                Some(ShutdownGuard::new(db, self.track_lifecycle()))
            })
            .collect();
        for shutdown in pending {
            self.0
                .lifecycle_executor
                .dispatch_fn(Priority::Low, move || drop(shutdown))
                .expect("lifecycle executor remains available through native cleanup");
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
            .filter_map(TrackedDb::unprepared_handle)
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
        // get_all_dbs() releases the lock before returning, so config callbacks and
        // dropping a last `Arc` here (which closes that database) don't run under it.
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
