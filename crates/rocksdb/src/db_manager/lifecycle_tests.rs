// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Wake, Waker};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::timeout;

use restate_core::TaskCenterBuilder;
use restate_core::io::{AdmissionClosed, IoExecutor, IoOp};
use restate_platform::sync::Mutex;

use super::{DbRef, ManagerInner, RocksDbManager, WeakRocksDbManager};
use crate::background::StorageTask;
use crate::close_tests::{DropBarrier, db_spec};
use crate::configuration::{DbConfigurator, create_default_db_options};
use crate::{
    CfNameMatch, CfPrefixPattern, DbEnv, DbName, DbSpec, Priority, RocksAccess, RocksError,
    StorageTaskKind,
};

const DEADLINE: Duration = Duration::from_secs(10);

#[tokio::test]
async fn child_gates_are_sealed_and_drained_with_the_database() -> anyhow::Result<()> {
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let global = task_center.run_sync(RocksDbManager::init);
    let manager = owned_manager(global);
    let (_, opening) = manager.open_db(db_spec("child-gates", None))?;
    let db = opening.await?;
    let (_, opening) = manager.open_db(db_spec("scope-neighbor", None))?;
    let neighbor = opening.await?;
    timeout(DEADLINE, db.gate.wait_idle()).await?;

    let first = db.gate().child();
    let second = db.gate().child();
    first.seal();
    assert!(first.try_acquire().is_none());
    // Work in a child gate counts only there, not in the database's own gate.
    let lease = second.try_acquire().unwrap();
    assert!(db.gate.is_idle());
    drop(lease);
    db.prepare_in(&second, StorageTaskKind::MultiGet, |_| ())
        .call()?;
    // In debug builds, using a gate from another database panics.
    if cfg!(debug_assertions) {
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                neighbor
                    .prepare_in(&second, StorageTaskKind::MultiGet, |_| ())
                    .call()
            }))
            .is_err()
        );
    }

    let pause = manager.pause_background(Priority::High).await;
    let mut batch = rocksdb::WriteBatchWithIndex::new(0, true);
    batch.put(b"scoped-write", b"value");
    let mut write = Box::pin(db.write_batch_with_index(
        "scoped-write",
        Priority::High,
        crate::IoMode::AlwaysBackground,
        rocksdb::WriteOptions::default(),
        batch,
        second.try_acquire().unwrap(),
    ));
    assert!(
        write
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(
        db.gate.is_idle(),
        "a write with a lease from a child gate takes none from the database's gate"
    );
    drop(write);
    drop(second); // The queued lease must still be found through the database's gate.

    let running = db.gate().child();
    let (started, reached) = oneshot::channel();
    let (resume, paused) = mpsc::channel::<()>();
    let observer = db.executor().low_pri().spawn(
        db.prepare_in(&running, StorageTaskKind::MultiGet, move |_db| {
            let _ = started.send(());
            let _ = paused.recv();
        })
        .background(),
    )?;
    timeout(DEADLINE, reached).await??;
    drop(observer);
    drop(running);
    assert!(db.gate.is_idle());

    // Create child gates while another thread seals the database. Whether a gate
    // was created before or after sealing, it must end up sealed.
    let raced = std::thread::scope(|threads| {
        let create = threads.spawn(|| (0..32).map(|_| db.gate().child()).collect::<Vec<_>>());
        db.seal();
        create.join().unwrap()
    });
    for gate in raced {
        assert!(gate.try_acquire().is_none());
    }
    assert!(db.gate().child().try_acquire().is_none());
    assert!(neighbor.gate().child().try_acquire().is_some());
    let mut drain = Box::pin(db.drain());
    assert!(
        drain
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(drain); // Cancellation must not remove the queued child gate from draining.
    drop(pause);
    let mut shutdown = Box::pin(manager.shutdown());
    assert!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(shutdown);
    drop(resume);
    timeout(DEADLINE, manager.shutdown()).await?;
    assert!(db.inner().as_raw_db().get(b"scoped-write")?.is_none());

    drop(first);
    db.close().await.expect("scopes do not pin the database");
    neighbor.close().await.expect("no native pins");
    task_center.shutdown_node("test completed", 0).await;
    Ok(())
}

#[tokio::test]
async fn database_admission_is_independent_of_other_databases() -> anyhow::Result<()> {
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let global = task_center.run_sync(RocksDbManager::init);
    let manager = owned_manager(global);

    let executor = crate::GlobalExecutor::new(
        crate::SinglePoolExecutor::new("test-high", 1),
        crate::SinglePoolExecutor::new("test-low", 1),
    );
    let spec = db_spec("standalone", None);
    let env = manager.db_env().clone();
    let native = tokio::task::spawn_blocking(move || RocksAccess::open_db(spec, env)).await??;
    let db = Arc::new(crate::RocksDb::new(native, executor.clone()));
    let constructed_closed = db.closed();
    assert_eq!(db.registration(), None);
    db.clone().open_cf(crate::CfName::new("retired")).await?;
    // The result can arrive before the worker releases its lease, so wait for the
    // gate to go idle.
    timeout(DEADLINE, db.gate.wait_idle()).await?;
    let discarded = db.prepare(StorageTaskKind::MultiGet, |_| {
        panic!("an undispatched operation must not execute");
    });
    assert!(db.gate.is_idle());
    drop(discarded);
    assert!(db.gate.is_idle());
    let gate_refs = Arc::strong_count(&db.gate);
    db.prepare(StorageTaskKind::MultiGet, move |db| {
        assert!(!db.gate.is_idle());
        assert_eq!(Arc::strong_count(&db.gate), gate_refs);
    })
    .call()?;
    assert!(db.gate.is_idle());
    let discarded = db
        .prepare(StorageTaskKind::MultiGet, |_| {
            panic!("a discarded task must not execute");
        })
        .background()
        .into_task()?;
    assert!(!db.gate.is_idle());
    drop(discarded);
    assert!(db.gate.is_idle());
    let prepared = db
        .prepare(StorageTaskKind::OpenColumnFamily, |db| {
            db.inner().open_cf(crate::CfName::new("external"))
        })
        .background();
    assert!(db.gate.is_idle());
    let prepared = prepared.into_task()?;
    let unadmitted = db.prepare(StorageTaskKind::MultiGet, |_| {
        panic!("sealing before submission must reject execution");
    });

    let spec = db_spec("standalone-neighbor", None);
    let env = manager.db_env().clone();
    let native = tokio::task::spawn_blocking(move || RocksAccess::open_db(spec, env)).await??;
    let neighbor = Arc::new(crate::RocksDb::new(native, executor.clone()));

    let (started, reached) = oneshot::channel();
    let (resume, paused) = mpsc::channel::<()>();
    let mut read = Box::pin(db.clone().run_background_read_op(
        "database-drain",
        StorageTaskKind::MultiGet,
        Priority::High,
        move |_| {
            let _ = started.send(());
            let _ = paused.recv();
        },
    ));
    assert_pending(read.as_mut());
    timeout(DEADLINE, reached).await??;
    drop(read);
    let mut drained = Box::pin(db.drain());
    assert_pending(drained.as_mut());
    // Draining one database doesn't affect another database on the same pools.
    neighbor.clone().flush_wal(true).await?;
    drop(resume);
    assert_pending(drained.as_mut());
    // `prepared` took its lease in into_task() before sealing, so it still runs and
    // drain() waits for it.
    let external = crate::SinglePoolExecutor::new("external-storage", 1);
    external.spawn_fn("test", prepared)?.await??;
    timeout(DEADLINE, drained).await?;
    // `unadmitted` was prepared but never submitted, so it didn't hold up the drain.
    // Submitting it now fails.
    assert_eq!(
        external.dispatch(unadmitted.background()),
        Err(AdmissionClosed)
    );
    assert_eq!(
        db.prepare(StorageTaskKind::MultiGet, |_| ()).call(),
        Err(AdmissionClosed)
    );
    assert!(matches!(
        external.spawn(db.prepare(StorageTaskKind::MultiGet, |_| ()).background()),
        Err(AdmissionClosed)
    ));
    assert!(matches!(
        db.clone().flush_wal(true).await,
        Err(RocksError::AdmissionClosed(AdmissionClosed))
    ));
    for mode in [
        crate::IoMode::AllowBlockingIO,
        crate::IoMode::OnlyIfNonBlocking,
        crate::IoMode::AlwaysBackground,
        crate::IoMode::Default,
    ] {
        assert!(
            db.write_batch(
                "sealed-db",
                Priority::High,
                mode,
                rocksdb::WriteOptions::default(),
                rocksdb::WriteBatch::default()
            )
            .await
            .is_err()
        );
    }
    // The other database still accepts work.
    neighbor.clone().flush_wal(true).await?;
    // The flush's result can arrive before its lease is released. Wait for idle
    // first, so the check below only sees what the rejected work left behind.
    timeout(DEADLINE, neighbor.gate.wait_idle()).await?;
    neighbor.seal();
    let rejected = neighbor.prepare(StorageTaskKind::MultiGet, |_| {
        panic!("a rejected operation must not execute");
    });
    assert_eq!(
        executor.low_pri().dispatch(rejected.background()),
        Err(AdmissionClosed)
    );
    assert!(neighbor.clone().flush_wal(true).await.is_err());
    assert!(neighbor.gate.is_idle());
    // Deleting a column family still works after sealing, on any executor.
    external
        .spawn(db.clone().prepare_drop_cf(crate::CfName::new("external")))?
        .await??;
    assert!(db.inner().cf_handle("external").is_none());

    // drop_cf() also works after sealing, and dropping its future doesn't cancel
    // the deletion.
    drop(db.clone().drop_cf(crate::CfName::new("retired")));
    lifecycle_queue_drained(&manager).await?;
    assert!(db.inner().cf_handle("retired").is_none());

    // A sealed standalone database still closes normally.
    db.close().await.expect("last pin");
    timeout(DEADLINE, constructed_closed).await?;
    neighbor.close().await.expect("last pin");
    manager.shutdown().await;
    global.shutdown().await;
    Ok(())
}

// A manager that isn't the global one and has no config watchdog holding a
// clone of it, using the global manager's thread pools.
fn owned_manager(global: &RocksDbManager) -> RocksDbManager {
    RocksDbManager(Arc::new(ManagerInner {
        state: Mutex::default(),
        shutting_down: AtomicBool::new(false),
        lifecycle_gate: Default::default(),
        executor: global.0.executor.clone(),
        lifecycle_executor: global.0.lifecycle_executor.clone(),
        db_env: global.db_env().clone(),
    }))
}

fn with_single_blocking_thread(
    future: impl Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?
        .block_on(future)
}

async fn pause_lifecycle(manager: &RocksDbManager) -> Vec<mpsc::Sender<()>> {
    manager.pause_lifecycle().await
}

async fn lifecycle_queue_drained(manager: &RocksDbManager) -> anyhow::Result<()> {
    executor_drained(&manager.0.lifecycle_executor).await
}

async fn pools_drained(executor: &crate::GlobalExecutor) -> anyhow::Result<()> {
    let (tx, rx) = oneshot::channel();
    executor.barrier(move || {
        let _ = tx.send(());
    });
    timeout(DEADLINE, rx).await??;
    Ok(())
}

async fn executor_drained(executor: &impl IoExecutor) -> anyhow::Result<()> {
    let (tx, rx) = oneshot::channel();
    executor.barrier(move || {
        let _ = tx.send(());
    });
    timeout(DEADLINE, rx).await??;
    Ok(())
}

async fn reach_gate(
    mut opening: Pin<&mut impl Future>,
    reached: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    tokio::select! {
        _ = &mut opening => panic!("open completed before its barrier"),
        result = timeout(DEADLINE, reached) => { result??; }
    }
    Ok(())
}

fn assert_pending(future: Pin<&mut impl Future>) {
    assert!(
        future
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

struct CloseWake {
    manager: WeakRocksDbManager,
    tracked: DbRef,
    observed_closed: AtomicBool,
}

impl Wake for CloseWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let manager = self.manager.upgrade().expect("cleanup retains the manager");
        // Use try_lock: if a regression wakes us while the lock is held, record that
        // instead of deadlocking.
        let state = manager.0.state.try_lock();
        self.observed_closed.store(
            state.is_some_and(|state| state.tracked[self.tracked].is_closed()),
            Ordering::Release,
        );
    }
}

struct OpenBarrier {
    started: oneshot::Sender<()>,
    resume: mpsc::Receiver<()>,
}

struct Configurator {
    opening: Mutex<Option<OpenBarrier>>,
    closing: Mutex<Option<DropBarrier>>,
    opens: Arc<AtomicUsize>,
    fail: bool,
}

impl DbConfigurator for Configurator {
    fn get_db_options(&self, name: &DbName, env: &DbEnv) -> rocksdb::Options {
        self.opens.fetch_add(1, Ordering::Relaxed);
        if let Some(barrier) = self.opening.lock().take() {
            let _ = barrier.started.send(());
            let _ = barrier.resume.recv();
        }
        let mut options = create_default_db_options(env, name);
        options.create_if_missing(!self.fail);
        if let Some(barrier) = self.closing.lock().take() {
            options.add_event_listener(barrier);
        }
        options
    }
}

struct PanickingConfigurator;

impl DbConfigurator for PanickingConfigurator {
    fn get_db_options(&self, _: &DbName, _: &DbEnv) -> rocksdb::Options {
        panic!("interrupted database configuration");
    }
}

impl crate::configuration::CfConfigurator for PanickingConfigurator {
    fn get_cf_options(&self, _: &DbName, _: &str, _: &DbEnv) -> rocksdb::Options {
        panic!("interrupted column-family configuration");
    }
}

#[derive(Clone, Copy)]
enum OpenStage {
    Queued,
    Running,
    Buffered,
}

// Lets a test pause a database inside its native open and inside its native
// destruction, and resume each. Dropping a resume sender also resumes, so a
// failed assertion doesn't hang the test.
struct Gates {
    open_reached: oneshot::Receiver<()>,
    open_resume: mpsc::Sender<()>,
    close_reached: oneshot::Receiver<()>,
    close_resume: mpsc::Sender<()>,
    finished: Arc<AtomicBool>,
    opens: Arc<AtomicUsize>,
}

fn gated_spec(name: &str, fail: bool) -> (DbSpec, Gates) {
    let (open_started, open_reached) = oneshot::channel();
    let (open_resume, resume) = mpsc::channel();
    let (close_started, close_reached) = oneshot::channel();
    let (close_resume, paused) = mpsc::channel();
    let finished = Arc::new(AtomicBool::new(false));
    let opens = Arc::new(AtomicUsize::new(0));
    let mut spec = db_spec(name, None);
    spec.db_configurator = Box::new(Configurator {
        opening: Mutex::new(Some(OpenBarrier {
            started: open_started,
            resume,
        })),
        closing: Mutex::new(Some(DropBarrier {
            started: Some(close_started),
            resume: Mutex::new(paused),
            finished: finished.clone(),
        })),
        opens: opens.clone(),
        fail,
    });
    (
        spec,
        Gates {
            open_reached,
            open_resume,
            close_reached,
            close_resume,
            finished,
            opens,
        },
    )
}

#[tokio::test]
async fn open_guards_finalize_discarded_jobs_and_interrupted_preparation() -> anyhow::Result<()> {
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let global = task_center.run_sync(RocksDbManager::init);
    let manager = owned_manager(global);

    let (spec, gates) = gated_spec("discarded-open", false);
    let path = spec.path.clone();
    let open = manager.admit_open(spec, manager.db_env().clone())?;
    let tracked = open.tracked;
    let opening = manager.get_db_by_ref(tracked);
    let closed = manager.0.state.lock().tracked[tracked].closed.clone();
    // Drop the open job without running it. Its guard must first release everything
    // it captured, then mark the database closed and wake waiters.
    let disposal = tokio::task::spawn_blocking(move || drop(open));
    timeout(DEADLINE, gates.close_reached).await??;
    assert_eq!(gates.opens.load(Ordering::Relaxed), 0);
    assert_eq!(manager.get_db_ref(&path), Some(tracked));
    assert!(!manager.0.state.lock().tracked[tracked].is_closed());
    assert!(!closed.is_cancelled());
    assert!(!manager.0.lifecycle_gate.is_idle());
    assert!(manager.0.state.try_lock().is_some());
    drop(gates.close_resume);
    timeout(DEADLINE, disposal).await??;
    assert!(closed.is_cancelled());
    assert!(gates.finished.load(Ordering::Acquire));
    assert!(manager.0.lifecycle_gate.is_idle());
    let (replacement, reopened) = manager.open_db(db_spec("discarded-open", None))?;
    assert_ne!(replacement, tracked);
    let db = reopened.await?;
    assert!(matches!(opening.await, Err(RocksError::Shutdown(_))));
    assert_eq!(manager.get_db_ref(&path), Some(replacement));
    db.close().await.expect("no retained database pins");

    for cf in [false, true] {
        let mut spec = db_spec("interrupted-open", None);
        if cf {
            spec.cf_patterns.insert(
                0,
                (
                    Box::new(CfPrefixPattern::ANY),
                    Box::new(PanickingConfigurator),
                ),
            );
        } else {
            spec.db_configurator = Box::new(PanickingConfigurator);
        }
        let (tracked, opening) = manager.open_db(spec)?;
        drop(opening);
        let opening = manager.get_db_by_ref(tracked);
        let closed = manager.closed(tracked)?;
        assert!(matches!(
            timeout(DEADLINE, opening).await?,
            Err(RocksError::Shutdown(_))
        ));
        timeout(DEADLINE, closed).await?;
        let (_, opening) = manager.open_db(db_spec("interrupted-open", None))?;
        let db = timeout(DEADLINE, opening).await??;
        timeout(DEADLINE, db.close()).await?.expect("last pin");
    }
    timeout(DEADLINE, manager.shutdown()).await?;
    global.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn close_guard_releases_native_resources_before_completing_on_unwind() -> anyhow::Result<()> {
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let manager = task_center.run_sync(RocksDbManager::init);
    let (spec, gates) = gated_spec("unwound-close", false);
    drop(gates.open_resume);
    let (_, opening) = manager.open_db(spec)?;
    let db = opening.await?;
    db.inner().as_raw_db().put(b"key", b"value")?;
    let closed = db.closed.clone();
    // Drop the last `Arc` on a thread outside Tokio that panics. The close must still
    // go to the lifecycle pool, and that thread finishing doesn't mean the database
    // is closed yet.
    let cleanup = std::thread::spawn(move || {
        let _db = db;
        panic!("interrupted cleanup dispatch");
    });
    timeout(DEADLINE, gates.close_reached).await??;
    assert!(!closed.is_cancelled());
    assert!(!gates.finished.load(Ordering::Acquire));
    assert!(!manager.0.lifecycle_gate.is_idle());
    assert!(manager.0.state.try_lock().is_some());
    drop(gates.close_resume);
    assert!(cleanup.join().is_err());
    timeout(DEADLINE, closed.cancelled()).await?;
    lifecycle_queue_drained(manager).await?;
    assert!(closed.is_cancelled());
    assert!(gates.finished.load(Ordering::Acquire));
    assert!(manager.0.lifecycle_gate.is_idle());
    let (_, opening) = manager.open_db(db_spec("unwound-close", None))?;
    let db = opening.await?;
    assert_eq!(
        db.inner().as_raw_db().get(b"key")?.as_deref(),
        Some(b"value".as_slice())
    );
    db.close().await.expect("no retained database pins");
    manager.shutdown().await;
    Ok(())
}

#[test]
fn lifecycle_work_survives_runtime_shutdown_and_busy_global_executor() -> anyhow::Result<()> {
    with_single_blocking_thread(async {
        let task_center = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()?
            .into_handle();
        let global = task_center.run_sync(RocksDbManager::init);

        for queued in [true, false] {
            let mut manager = owned_manager(global);
            // Give this manager its own shared backend so blocking it does not
            // affect the singleton or the next iteration.
            let executor = crate::GlobalExecutor::new(
                crate::SinglePoolExecutor::new("test-high", 1),
                crate::SinglePoolExecutor::new("test-low", 1),
            );
            Arc::get_mut(&mut manager.0)
                .expect("unshared manager")
                .executor = executor.clone();
            let high = manager.pause_background(Priority::High).await;
            let low = manager.pause_background(Priority::Low).await;
            let mut spec = db_spec(
                if queued {
                    "discarded-shutdown"
                } else {
                    "rejected-shutdown"
                },
                None,
            );
            spec.flush_on_shutdown.push(Box::new(CfPrefixPattern::ANY));
            let (tracked, opening) = manager.open_db(spec)?;
            let db = opening.await?;
            assert!(
                manager.0.state.lock().tracked[tracked]
                    .unprepared_handle()
                    .is_some()
            );
            assert!(db.gate.is_idle());
            let native = db.inner().as_raw_db();
            native.put(b"key", b"value")?;
            assert_eq!(
                native.property_int_value(rocksdb::properties::NUM_ENTRIES_ACTIVE_MEM_TABLE)?,
                Some(1)
            );

            let runtime = tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(1)
                .build()?;
            let handle = runtime.handle().clone();
            let mut resume = None;
            let runtime = if queued {
                let (sender, paused) = mpsc::channel::<()>();
                let (started, reached) = oneshot::channel();
                runtime.spawn_blocking(move || {
                    let _ = started.send(());
                    let _ = paused.recv();
                });
                reached.await?;
                resume = Some(sender);
                Some(runtime)
            } else {
                runtime.shutdown_background();
                None
            };

            // Shutdown flushes run on the lifecycle pool, so they make progress while the
            // regular I/O pools are blocked and the runtime is busy or gone, even after this
            // shutdown future is dropped.
            let paused_lifecycle = pause_lifecycle(&manager).await;
            let mut shutdown = Box::pin(manager.shutdown());
            {
                let _entered = handle.enter();
                let polled = shutdown
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
                if queued {
                    assert!(polled.is_pending());
                }
            }
            drop(shutdown);
            if let Some(runtime) = runtime {
                runtime.shutdown_background();
            }
            drop(resume);
            drop(paused_lifecycle);

            timeout(DEADLINE, manager.shutdown()).await?;
            {
                let state = manager.0.state.lock();
                assert!(state.tracked[tracked].handle().is_some());
                assert!(state.tracked[tracked].unprepared_handle().is_none());
            }
            assert_eq!(
                native.property_int_value(rocksdb::properties::NUM_ENTRIES_ACTIVE_MEM_TABLE)?,
                Some(0)
            );
            assert!(
                !db.closed.is_cancelled(),
                "preparation is not physical close"
            );
            assert_eq!(Arc::strong_count(&db), 1);
            assert!(manager.0.lifecycle_gate.is_idle());
            db.close().await.expect("owner retains the last pin");
            drop((high, low));
            pools_drained(&executor).await?;
        }
        global.shutdown().await;
        Ok(())
    })
}

#[tokio::test]
async fn independent_database_lifecycle_progresses_while_one_worker_is_blocked()
-> anyhow::Result<()> {
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let global = task_center.run_sync(RocksDbManager::init);
    let manager = owned_manager(global);
    let (resume, paused) = mpsc::channel::<()>();
    let (started, reached) = oneshot::channel();
    manager.0.lifecycle_executor.dispatch_fn("test", move || {
        let _ = started.send(());
        let _ = paused.recv();
    })?;
    timeout(DEADLINE, reached).await??;

    let (_, opening) = manager.open_db(db_spec("independent-lifecycle", None))?;
    let db = timeout(DEADLINE, opening).await??;
    timeout(DEADLINE, manager.shutdown()).await?;
    timeout(DEADLINE, db.close())
        .await?
        .expect("last owner pin");
    drop(resume);
    global.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn shutdown_drains_queued_and_running_io_before_preparing_databases() -> anyhow::Result<()> {
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let global = task_center.run_sync(RocksDbManager::init);

    for (index, priority) in [Priority::High, Priority::Low].into_iter().enumerate() {
        let manager = owned_manager(global);
        let mut spec = db_spec(&format!("drained-background-{index}"), None);
        spec.flush_on_shutdown.push(Box::new(CfPrefixPattern::ANY));
        let (_, opening) = manager.open_db(spec)?;
        let db = opening.await?;
        db.inner().as_raw_db().put(b"key", b"value")?;
        let paused_pool = manager.pause_background(priority).await;
        let (started, reached) = oneshot::channel();
        let (resume, paused) = mpsc::channel::<()>();
        let finished = Arc::new(AtomicBool::new(false));
        let task = StorageTask::default()
            .kind(StorageTaskKind::MultiGet)
            .op({
                let db = db.clone();
                let finished = finished.clone();
                move || {
                    let _ = started.send(());
                    let _ = paused.recv();
                    assert_eq!(
                        db.inner().as_raw_db().get(b"key").unwrap().as_deref(),
                        Some(b"value".as_slice())
                    );
                    finished.store(true, Ordering::Release);
                }
            })
            .build()?;
        if index == 0 {
            let mut observer = Box::pin(db.tracked_executor(priority).spawn(task)?);
            assert_pending(observer.as_mut());
            drop(observer);
        } else {
            db.tracked_executor(priority).dispatch(task)?;
        }

        let mut shutdown = Box::pin(manager.shutdown());
        assert_pending(shutdown.as_mut());
        assert!(!db.gate.is_idle());
        assert_eq!(
            db.inner()
                .as_raw_db()
                .property_int_value(rocksdb::properties::NUM_ENTRIES_ACTIVE_MEM_TABLE)?,
            Some(1)
        );
        let rejected = || {
            StorageTask::default()
                .kind(StorageTaskKind::FlushWal)
                .op(|| panic!("late background work must be rejected"))
                .build()
                .unwrap()
        };
        assert!(db.tracked_executor(priority).dispatch(rejected()).is_err());
        assert!(db.tracked_executor(priority).spawn(rejected()).is_err());

        drop(paused_pool);
        if index == 0 {
            assert!(timeout(DEADLINE, reached).await?.is_err());
        } else {
            timeout(DEADLINE, reached).await??;
            assert_pending(shutdown.as_mut());
            assert_eq!(
                db.inner()
                    .as_raw_db()
                    .property_int_value(rocksdb::properties::NUM_ENTRIES_ACTIVE_MEM_TABLE)?,
                Some(1)
            );
        }
        drop(resume);
        timeout(DEADLINE, shutdown).await?;
        assert_eq!(finished.load(Ordering::Acquire), index != 0);
        assert_eq!(
            db.inner()
                .as_raw_db()
                .property_int_value(rocksdb::properties::NUM_ENTRIES_ACTIVE_MEM_TABLE)?,
            Some(0)
        );
        assert!(
            !db.closed.is_cancelled(),
            "owner still holds the native database"
        );
        assert!(db.gate.is_idle());
        assert!(manager.0.lifecycle_gate.is_idle());
        // Shutting down this manager mustn't stop the pools it shares with the global one.
        timeout(
            DEADLINE,
            global.0.executor.pool(priority).spawn_fn("test", || ())?,
        )
        .await??;
        db.close().await.expect("owner holds the last pin");
    }
    global.shutdown().await;
    Ok(())
}

#[test]
fn interrupted_shutdown_preparation_remains_pending_until_native_close() -> anyhow::Result<()> {
    #[derive(Debug)]
    struct PanicOnceOnShutdown(Arc<AtomicUsize>);

    impl CfNameMatch for PanicOnceOnShutdown {
        fn cf_matches(&self, _: &str) -> bool {
            assert_ne!(
                self.0.fetch_add(1, Ordering::Relaxed),
                0,
                "interrupted shutdown preparation"
            );
            true
        }
    }

    with_single_blocking_thread(async {
        let task_center = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()?
            .into_handle();
        let global = task_center.run_sync(RocksDbManager::init);
        let manager = owned_manager(global);
        let attempts = Arc::new(AtomicUsize::new(0));
        let mut spec = db_spec("interrupted-shutdown", None);
        spec.flush_on_shutdown
            .push(Box::new(PanicOnceOnShutdown(attempts.clone())));
        let (_, opening) = manager.open_db(spec)?;
        let db = opening.await?;
        db.inner().as_raw_db().put(b"key", b"value")?;
        let closed = db.closed();

        let mut shutdown = Box::pin(manager.shutdown());
        assert_pending(shutdown.as_mut());
        lifecycle_queue_drained(&manager).await?;
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
        assert!(manager.0.lifecycle_gate.is_idle());
        assert!(!db.closed.is_cancelled());
        assert_pending(shutdown.as_mut());
        let mut concurrent_shutdown = Box::pin(manager.shutdown());
        assert_pending(concurrent_shutdown.as_mut());

        // The flush panicked, so it doesn't count as done: closing the database must
        // flush again first.
        drop(db);
        timeout(DEADLINE, closed).await?;
        timeout(DEADLINE, shutdown).await?;
        timeout(DEADLINE, concurrent_shutdown).await?;
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        assert!(manager.0.lifecycle_gate.is_idle());
        global.shutdown().await;
        Ok(())
    })
}

#[test]
fn cancelled_open_waiters_retain_retrievable_incarnations() -> anyhow::Result<()> {
    with_single_blocking_thread(async {
        let task_center = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()?
            .into_handle();
        let manager = task_center.run_sync(RocksDbManager::init);

        for (index, stage) in [OpenStage::Queued, OpenStage::Running, OpenStage::Buffered]
            .into_iter()
            .enumerate()
        {
            let name = format!("cancelled-open-{index}");
            let (spec, gates) = gated_spec(&name, false);
            let path = spec.path.clone();
            let paused = pause_lifecycle(manager).await;
            assert_eq!(manager.get_db_ref(&path), None);
            let (tracked, opening) = manager.open_db(spec)?;
            // open_db() books the path and queues the open before its future is polled.
            assert_eq!(manager.get_db_ref(&path), Some(tracked));
            assert!(!path.exists(), "queued opening does no filesystem work");
            let closed = manager.0.state.lock().tracked[tracked].closed.clone();
            let mut early_observer = Box::pin(manager.get_db_by_ref(tracked));
            assert_pending(early_observer.as_mut());
            drop(early_observer);
            assert!(matches!(
                manager.open_db(db_spec(&name, None)),
                Err(RocksError::AlreadyOpen)
            ));
            if matches!(stage, OpenStage::Queued) {
                // Dropping the future of a queued open doesn't cancel it.
                drop(opening);
                drop(paused);
                drop(gates.open_resume);
            } else {
                drop(paused);
                timeout(DEADLINE, gates.open_reached).await??;
                assert!(
                    manager.0.state.try_lock().is_some(),
                    "native open must not hold the registry lock"
                );
                if matches!(stage, OpenStage::Buffered) {
                    drop(gates.open_resume);
                    lifecycle_queue_drained(manager).await?;
                    assert!(
                        !manager.0.lifecycle_gate.is_idle(),
                        "an unclaimed open still needs handoff"
                    );
                    let db = manager.get_db(DbName::new(&name)).unwrap();
                    assert_eq!(db.registration(), Some(tracked));
                    assert_eq!(manager.get_all_dbs().len(), 1);
                    drop(db);
                    drop(opening);
                } else {
                    drop(opening);
                    drop(gates.open_resume);
                }
            }

            lifecycle_queue_drained(manager).await?;
            assert!(!closed.is_cancelled());
            assert!(!gates.finished.load(Ordering::Acquire));
            assert!(!manager.0.lifecycle_gate.is_idle());
            // Only the DbRef is left: the result can still be collected without the
            // original future or any other waiter.
            let (early, late) = timeout(DEADLINE, async {
                tokio::join!(
                    manager.get_db_by_ref(tracked),
                    manager.get_db_by_ref(tracked)
                )
            })
            .await?;
            let db = early?;
            let late = late?;
            assert!(Arc::ptr_eq(&db, &late));
            assert_eq!(db.registration(), Some(tracked));
            assert!(manager.0.lifecycle_gate.is_idle());
            drop(late);
            let unpolled_observer = manager.get_db_by_ref(tracked);
            assert_eq!(Arc::strong_count(&db), 1, "observers retain no database");
            let mut close = Box::pin(db.close());
            assert_pending(close.as_mut());
            timeout(DEADLINE, gates.close_reached).await??;
            assert!(!gates.finished.load(Ordering::Acquire));
            assert!(
                manager.0.state.try_lock().is_some(),
                "native destruction must not hold the registry lock"
            );
            assert!(!closed.is_cancelled());
            assert_eq!(manager.get_db_ref(&path), Some(tracked));
            assert!(matches!(
                manager.get_db_by_ref(tracked).await,
                Err(RocksError::NotOpen)
            ));
            assert!(matches!(
                manager.open_db(db_spec(&name, None)),
                Err(RocksError::AlreadyOpen)
            ));
            drop(gates.close_resume);
            timeout(DEADLINE, close).await?.expect("last database pin");
            assert!(gates.finished.load(Ordering::Acquire));
            assert!(closed.is_cancelled());
            assert_eq!(manager.get_db_ref(&path), Some(tracked));
            assert!(matches!(
                manager.get_db_by_ref(tracked).await,
                Err(RocksError::NotOpen)
            ));
            let (replacement, opening) = manager.open_db(db_spec(&name, None))?;
            assert_ne!(tracked, replacement);
            assert_eq!(manager.get_db_ref(&path), Some(replacement));
            assert!(matches!(
                manager.open_db(db_spec(&name, None)),
                Err(RocksError::AlreadyOpen)
            ));
            let db = timeout(DEADLINE, opening).await??;
            assert!(!manager.0.state.lock().tracked.contains_key(tracked));
            assert!(matches!(
                manager.get_db_by_ref(tracked).await,
                Err(RocksError::NotRegistered)
            ));
            assert!(matches!(
                unpolled_observer.await,
                Err(RocksError::NotRegistered)
            ));
            // A late close notification for the old DbRef must not affect the new database
            // at the same path.
            manager.finish_close(tracked, closed);
            assert!(!manager.0.state.lock().tracked[replacement].is_closed());
            assert!(!db.closed.is_cancelled());
            db.inner().as_raw_db().put(b"key", b"value")?;
            assert_eq!(
                db.inner().as_raw_db().get(b"key")?.as_deref(),
                Some(b"value".as_slice())
            );
            db.close().await.expect("no retained database pins");
        }
        manager.shutdown().await;
        Ok(())
    })
}

#[test]
fn shutdown_drains_opens_through_handoff_or_disposal() -> anyhow::Result<()> {
    with_single_blocking_thread(async {
        let task_center = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()?
            .into_handle();
        let manager = task_center.run_sync(RocksDbManager::init);

        for (index, stage) in [
            OpenStage::Queued,
            OpenStage::Running,
            OpenStage::Buffered,
            OpenStage::Buffered,
        ]
        .into_iter()
        .enumerate()
        {
            let name = format!("shutdown-open-{index}");
            let (spec, gates) = gated_spec(&name, false);
            let path = spec.path.clone();
            let paused = pause_lifecycle(manager).await;
            let (tracked, opening) = manager.open_db(spec)?;
            let mut opening = Box::pin(opening);
            assert_pending(opening.as_mut());
            assert_eq!(manager.get_db_ref(&path), Some(tracked));

            let mut paused = Some(paused);
            let mut open_resume = Some(gates.open_resume);
            if !matches!(stage, OpenStage::Queued) {
                drop(paused.take());
                timeout(DEADLINE, gates.open_reached).await??;
                if matches!(stage, OpenStage::Buffered) {
                    drop(open_resume.take());
                    lifecycle_queue_drained(manager).await?;
                }
            }
            let mut shutdown = Box::pin(manager.shutdown());
            assert_pending(shutdown.as_mut());
            assert!(matches!(
                manager.open_db(db_spec("sealed-open", None)),
                Err(RocksError::Shutdown(_))
            ));

            drop(paused);
            drop(open_resume);
            let mut opening = Some(opening);
            if index >= 3 || !matches!(stage, OpenStage::Buffered) {
                // Shutdown must close databases nobody collected, even if their open future
                // was dropped.
                drop(opening.take());
            }
            // A remaining open future is never polled during shutdown. It must not delay
            // closing the database or keep it open.
            timeout(DEADLINE, gates.close_reached).await??;
            assert_pending(shutdown.as_mut());
            assert!(!manager.0.state.lock().tracked[tracked].is_closed());
            assert!(manager.0.state.try_lock().is_some());
            assert_eq!(
                gates.opens.load(Ordering::Relaxed),
                usize::from(!matches!(stage, OpenStage::Queued))
            );
            drop(gates.close_resume);
            timeout(DEADLINE, shutdown).await?;
            assert!(gates.finished.load(Ordering::Acquire));
            assert!(manager.0.state.lock().tracked[tracked].is_closed());
            assert!(manager.0.lifecycle_gate.is_idle());
            if let Some(opening) = opening {
                assert!(matches!(
                    timeout(DEADLINE, opening).await?,
                    Err(RocksError::Shutdown(_))
                ));
            }
            manager.reset().await?;
            assert_eq!(manager.get_db_ref(&path), None);
        }
        manager.shutdown().await;
        Ok(())
    })
}

#[tokio::test]
async fn failed_open_releases_resources_before_releasing_its_path() -> anyhow::Result<()> {
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let manager = task_center.run_sync(RocksDbManager::init);
    let (spec, gates) = gated_spec("failed-open", true);
    let path = spec.path.clone();
    let (tracked, opening) = manager.open_db(spec)?;
    let mut opening = Box::pin(opening);
    assert_pending(opening.as_mut());
    assert_eq!(manager.get_db_ref(&path), Some(tracked));
    let retained_observer = manager.get_db_by_ref(tracked);
    timeout(DEADLINE, gates.open_reached).await??;
    drop(gates.open_resume);
    timeout(DEADLINE, gates.close_reached).await??;
    assert_pending(opening.as_mut());
    assert!(!manager.0.state.lock().tracked[tracked].is_closed());
    assert!(matches!(
        manager.open_db(db_spec("failed-open", None)),
        Err(RocksError::AlreadyOpen)
    ));
    drop(gates.close_resume);
    let failure = timeout(DEADLINE, opening).await?.unwrap_err();
    assert!(matches!(failure, RocksError::Other(_)));
    assert!(manager.0.state.lock().tracked[tracked].is_closed());
    assert!(gates.finished.load(Ordering::Acquire));
    let late_observer = manager.get_db_by_ref(tracked);
    let (replacement, opening) = manager.open_db(db_spec("failed-open", None))?;
    assert_ne!(replacement, tracked);
    let db = opening.await?;
    // Callers that were already waiting keep the original open error after the
    // path is reopened.
    assert_eq!(
        retained_observer.await.unwrap_err().to_string(),
        failure.to_string()
    );
    assert_eq!(
        late_observer.await.unwrap_err().to_string(),
        failure.to_string()
    );
    assert!(matches!(
        manager.get_db_by_ref(tracked).await,
        Err(RocksError::NotRegistered)
    ));
    db.close().await.expect("no retained database pins");
    manager.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn registration_uses_supplied_paths_and_first_matching_live_name() -> anyhow::Result<()> {
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let manager = task_center.run_sync(RocksDbManager::init);
    let spec = db_spec("registered-db", None);
    #[cfg(unix)]
    let spec = {
        let mut spec = spec;
        let parent_alias = spec.path.with_file_name("supplied-parent");
        tokio::fs::create_dir_all(spec.path.parent().unwrap()).await?;
        std::os::unix::fs::symlink(spec.path.parent().unwrap(), &parent_alias)?;
        spec.path = parent_alias.join(spec.path.file_name().unwrap());
        spec
    };
    let path = spec.path.clone();
    let (tracked, opening) = manager.open_db(spec)?;
    assert_eq!(manager.get_db_ref(&path), Some(tracked));
    let closed_before_open = manager.closed(tracked)?;
    let db = opening.await?;
    assert_eq!(db.registration(), Some(tracked));
    assert_eq!(manager.get_db_ref(&path), Some(tracked));
    assert_eq!(db.inner().path(), &path);
    assert!(Arc::ptr_eq(&manager.get_db_by_ref(tracked).await?, &db));
    assert!(Arc::ptr_eq(
        &manager.get_db(db.name().clone()).unwrap(),
        &db
    ));
    let mut alias = db_spec("different-reporting-name", None);
    alias.path = path.clone();
    assert!(matches!(
        manager.open_db(alias),
        Err(RocksError::AlreadyOpen)
    ));

    let mut spec = db_spec("distinct-path", None);
    spec.name = db.name().clone();
    let other_path = spec.path.clone();
    let (previous, opening) = manager.open_db(spec)?;
    let other = opening.await?;
    assert_eq!(manager.get_all_dbs().len(), 2);
    assert!(Arc::ptr_eq(
        &manager.get_db(db.name().clone()).unwrap(),
        &db
    ));
    other.close().await.expect("no retained database pins");
    let (replacement, opening) = manager.open_db(db_spec("distinct-path", None))?;
    let other = opening.await?;
    assert_ne!(replacement, previous);
    assert_eq!(manager.get_db_ref(&other_path), Some(replacement));
    assert!(matches!(
        manager.get_db_by_ref(previous).await,
        Err(RocksError::NotRegistered)
    ));
    other.close().await.expect("no retained database pins");
    assert!(Arc::ptr_eq(
        &manager.get_db(db.name().clone()).unwrap(),
        &db
    ));

    let closed = db.closed.clone();
    assert!(
        timeout(DEADLINE, manager.reset()).await?.is_err(),
        "reset cannot erase a live path fence"
    );
    assert!(!closed.is_cancelled());
    assert!(matches!(
        manager.open_db(db_spec("sealed-open", None)),
        Err(RocksError::Shutdown(_))
    ));
    db.close().await.expect("no retained database pins");
    timeout(DEADLINE, closed_before_open).await?;
    assert!(closed.is_cancelled());
    manager.reset().await?;
    assert_eq!(manager.get_db_ref(&path), None);
    let (_, opening) = manager.open_db(db_spec("registered-db", None))?;
    let reopened = opening.await?;
    assert_ne!(reopened.registration(), Some(tracked));
    manager.finish_close(tracked, closed);
    assert!(!reopened.closed.is_cancelled());
    reopened.close().await.expect("no retained database pins");
    manager.shutdown().await;
    Ok(())
}

#[test]
fn cleanup_retains_manager_and_notifies_outside_its_lock() -> anyhow::Result<()> {
    with_single_blocking_thread(async {
        let task_center = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()?
            .into_handle();
        let global = task_center.run_sync(RocksDbManager::init);
        // Use a non-global manager, so the global one doesn't keep it alive.
        let manager = owned_manager(global);
        let weak = manager.downgrade();
        let (spec, gates) = gated_spec("owned-manager", false);
        let (tracked, opening) = manager.open_db(spec)?;
        let mut opening = Box::pin(opening);
        reach_gate(opening.as_mut(), gates.open_reached).await?;
        drop(gates.open_resume);
        let db = timeout(DEADLINE, opening).await??;
        assert!(matches!(
            global.closed(tracked),
            Err(RocksError::NotRegistered)
        ));
        let observer = Arc::new(CloseWake {
            manager: weak.clone(),
            tracked,
            observed_closed: AtomicBool::new(false),
        });
        let waker = Waker::from(observer.clone());
        let mut closed = Box::pin(manager.closed(tracked)?);
        assert!(
            closed
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );

        drop(manager);
        assert!(weak.upgrade().is_some(), "the database retains its manager");
        drop(db);
        timeout(DEADLINE, gates.close_reached).await??;
        assert!(
            weak.upgrade().is_some(),
            "native cleanup retains its manager"
        );
        assert!(!observer.observed_closed.load(Ordering::Acquire));
        drop(gates.close_resume);
        lifecycle_queue_drained(global).await?;
        assert!(observer.observed_closed.load(Ordering::Acquire));
        assert!(gates.finished.load(Ordering::Acquire));
        assert!(
            weak.upgrade().is_none(),
            "ref and observer are resource-free"
        );
        timeout(DEADLINE, closed).await?;
        global.shutdown().await;
        Ok(())
    })
}
