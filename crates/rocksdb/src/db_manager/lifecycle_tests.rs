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

use restate_core::io::{AdmissionClosed, IoExecutor, IoOp};
use restate_core::{ShutdownError, TaskCenterBuilder};
use restate_platform::sync::Mutex;

use super::{DbRef, ManagerInner, RocksDbManager, Status, WeakRocksDbManager};
use crate::background::StorageTask;
use crate::close_tests::{DropBarrier, db_spec};
use crate::configuration::{DbConfigurator, create_default_db_options};
use crate::{
    CfNameMatch, CfPrefixPattern, DbEnv, DbName, DbSpec, Priority, RocksAccess, RocksError,
    StorageTaskKind,
};

const DEADLINE: Duration = Duration::from_secs(10);

#[tokio::test]
async fn registered_scopes_drain_without_shared_operation_admission() -> anyhow::Result<()> {
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let global = task_center.run_sync(RocksDbManager::init);
    let manager = owned_manager(global);
    let (_, opening) = manager.open_db(db_spec("registered-scopes", None))?;
    let db = opening.await?;
    let (_, opening) = manager.open_db(db_spec("scope-neighbor", None))?;
    let neighbor = opening.await?;
    timeout(DEADLINE, db.background_tracker.wait_idle()).await?;

    let first = db.register_scope();
    let second = db.register_scope();
    first.seal();
    assert!(first.try_acquire().is_none());
    // Neither acquisition nor execution needs the registry lock.
    {
        let _registry = db.admission_scopes.lock();
        let lease = second.try_acquire().unwrap();
        assert!(db.background_tracker.is_idle());
        drop(lease);
        second.prepare_op(&db, |_| ()).call()?;
    }
    // A foreign scope cannot authorize work against this database.
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            second.prepare_op(&neighbor, |_| ()).call()
        }))
        .is_err()
    );

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
        db.background_tracker.is_idle(),
        "scope writes do not acquire direct admission"
    );
    drop(write);
    drop(second); // The queued lease must remain discoverable through the weak registry.

    let running = db.register_scope();
    let (started, reached) = oneshot::channel();
    let (resume, paused) = mpsc::channel::<()>();
    let observer = db.executor().spawn(
        running
            .prepare_op(&db, move |_db| {
                let _ = started.send(());
                let _ = paused.recv();
            })
            .background("scoped-read", StorageTaskKind::MultiGet),
    )?;
    timeout(DEADLINE, reached).await??;
    drop(observer);
    drop(running);
    assert!(db.background_tracker.is_idle());

    // Cover registration on both sides of sealing and racing with it.
    let raced = std::thread::scope(|threads| {
        let register = threads.spawn(|| (0..32).map(|_| db.register_scope()).collect::<Vec<_>>());
        db.seal();
        register.join().unwrap()
    });
    for scope in raced {
        assert!(scope.try_acquire().is_none());
    }
    assert!(db.register_scope().try_acquire().is_none());
    assert!(neighbor.register_scope().try_acquire().is_some());
    let mut drain = Box::pin(db.drain());
    assert!(
        drain
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(drain); // Cancellation must not remove the queued scope from drainage.
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
async fn database_admission_is_independent_and_attachment_preserves_close_observers()
-> anyhow::Result<()> {
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let global = task_center.run_sync(RocksDbManager::init);
    let manager = owned_manager(global);

    for attached in [false, true] {
        let executor = crate::GlobalExecutor::new(
            threadpool::ThreadPool::new(1),
            threadpool::ThreadPool::new(1),
        );
        let spec = db_spec(
            if attached {
                "late-attachment"
            } else {
                "standalone"
            },
            None,
        );
        let reservation = attached
            .then(|| manager.remote_open_db(&spec))
            .transpose()?;
        let manager_closed = reservation
            .map(|tracked| manager.closed(tracked))
            .transpose()?;
        let env = manager.db_env().clone();
        let native = tokio::task::spawn_blocking(move || RocksAccess::open_db(spec, env)).await??;
        let db = crate::RocksDb::new(native, executor.clone());
        let constructed_closed = db.closed();
        assert_eq!(db.registration(), None);
        let db = if let Some(tracked) = reservation {
            manager.attach_db(tracked, db)?
        } else {
            Arc::new(db)
        };
        assert_eq!(db.registration(), reservation);
        db.clone().open_cf(crate::CfName::new("retired")).await?;
        // Result delivery can precede release of the executor's tracking lease.
        timeout(DEADLINE, db.background_tracker.wait_idle()).await?;
        let discarded = db.prepare_op(|_| {
            panic!("an undispatched operation must not execute");
        });
        assert!(db.background_tracker.is_idle());
        drop(discarded);
        assert!(db.background_tracker.is_idle());
        let tracker_refs = Arc::strong_count(&db.background_tracker);
        db.prepare_op(move |db| {
            assert!(!db.background_tracker.is_idle());
            assert_eq!(Arc::strong_count(&db.background_tracker), tracker_refs);
        })
        .call()?;
        assert!(db.background_tracker.is_idle());
        let discarded = db
            .prepare_op(|_| {
                panic!("a discarded task must not execute");
            })
            .background("discarded-task", StorageTaskKind::MultiGet)
            .into_task()?;
        assert!(!db.background_tracker.is_idle());
        drop(discarded);
        assert!(db.background_tracker.is_idle());
        let prepared = db
            .prepare_op(|db| db.inner().open_cf(crate::CfName::new("external")))
            .background("prepared", StorageTaskKind::OpenColumnFamily)
            .with_priority(Priority::High);
        assert_eq!(prepared.priority(), Priority::High);
        assert!(db.background_tracker.is_idle());
        let prepared = prepared.into_task()?;
        let unadmitted = db.prepare_op(|_| {
            panic!("sealing before submission must reject execution");
        });

        let spec = db_spec(
            if attached {
                "attached-neighbor"
            } else {
                "standalone-neighbor"
            },
            None,
        );
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
        // Draining one database does not seal its neighbor on the shared backend.
        neighbor.clone().flush_wal(true).await?;
        drop(resume);
        assert_pending(drained.as_mut());
        // Background preparation commits admission before sealing.
        let external = crate::SinglePoolExecutor::new("external-storage".to_owned(), 1);
        external.spawn_fn(Priority::High, prepared)?.await??;
        timeout(DEADLINE, drained).await?;
        // Retaining an operation does not prevent drainage, but submission now fails.
        assert_eq!(
            external.dispatch(unadmitted.background("unadmitted", StorageTaskKind::MultiGet)),
            Err(AdmissionClosed)
        );
        assert_eq!(db.prepare_op(|_| ()).call(), Err(AdmissionClosed));
        assert!(matches!(
            external.spawn(
                db.prepare_op(|_| ())
                    .background("sealed-spawn", StorageTaskKind::MultiGet)
            ),
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
        // A separate database on the same backend remains admissible.
        neighbor.clone().flush_wal(true).await?;
        // Result delivery can precede release of the successful flush's lease.
        // Establish idle before checking that rejected work leaves no flights.
        timeout(DEADLINE, neighbor.background_tracker.wait_idle()).await?;
        neighbor.seal();
        let rejected = neighbor.prepare_op(|_| {
            panic!("a rejected operation must not execute");
        });
        assert_eq!(
            executor.dispatch(rejected.background("rejected", StorageTaskKind::MultiGet)),
            Err(AdmissionClosed)
        );
        assert!(neighbor.clone().flush_wal(true).await.is_err());
        assert!(neighbor.background_tracker.is_idle());
        // Cleanup carries lifecycle accounting to the chosen executor and remains
        // possible after ordinary admission is sealed.
        external
            .spawn(db.clone().prepare_drop_cf(crate::CfName::new("external")))?
            .await??;
        assert!(db.inner().cf_handle("external").is_none());

        // CF cleanup uses the lifecycle lane after sealing, and
        // discarding its result cannot cancel the native deletion.
        drop(db.clone().drop_cf(crate::CfName::new("retired")));
        lifecycle_queue_drained(&manager).await?;
        assert!(db.inner().cf_handle("retired").is_none());

        // Both standalone cleanup and registered lifecycle dispatch survive closure
        // of ordinary I/O. Observers obtained before attachment remain valid.
        if let Some(tracked) = reservation {
            let db = Arc::try_unwrap(db).expect("last pin");
            assert!(matches!(
                manager.attach_db(tracked, db),
                Err(RocksError::AlreadyRegistered)
            ));
        } else {
            db.close().await.expect("last pin");
        }
        timeout(DEADLINE, constructed_closed).await?;
        if let Some(closed) = manager_closed {
            timeout(DEADLINE, closed).await?;
        }
        neighbor.close().await.expect("last pin");
    }
    manager.shutdown().await;
    global.shutdown().await;
    Ok(())
}

// An owned manager without a singleton or watchdog pin, sharing the test pools.
fn owned_manager(global: &RocksDbManager) -> RocksDbManager {
    RocksDbManager(Arc::new(ManagerInner {
        state: Mutex::default(),
        shutting_down: AtomicBool::new(false),
        lifecycle_tracker: Default::default(),
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

async fn blocking_queue_drained() -> anyhow::Result<()> {
    // These tests use one blocking worker: the marker runs after native opening
    // has finished sending its result, without polling the opening future.
    timeout(DEADLINE, tokio::task::spawn_blocking(|| ())).await??;
    Ok(())
}

async fn lifecycle_queue_drained(manager: &RocksDbManager) -> anyhow::Result<()> {
    executor_drained(&manager.0.lifecycle_executor).await
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
        // Do not block on a regression: report the lock still being held instead.
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

// Gates both the native-open callback and native-owned destruction. Dropping a
// resume sender also releases its gate if an assertion fails.
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

    for reserved in [false, true] {
        let name = if reserved {
            "discarded-reserved-open"
        } else {
            "discarded-open"
        };
        let owner = if reserved {
            let spec = db_spec(name, None);
            Some(manager.hold_db(spec.name, spec.path, "discarded open".into())?)
        } else {
            None
        };
        let (spec, gates) = gated_spec(name, false);
        let path = spec.path.clone();
        let open = manager.admit_open(spec, manager.db_env().clone(), owner)?;
        let tracked = open.tracked;
        let opening = manager.get_db_by_ref(tracked);
        let closed = manager.0.state.lock().tracked[tracked].closed.clone();
        // Discard the job before it starts. Its guard must release all captured
        // resources before closing the incarnation and notifying its observers.
        let disposal = tokio::task::spawn_blocking(move || drop(open));
        timeout(DEADLINE, gates.close_reached).await??;
        assert_eq!(gates.opens.load(Ordering::Relaxed), 0);
        assert_eq!(manager.get_db_ref(&path), Some(tracked));
        assert!(!manager.0.state.lock().tracked[tracked].is_closed());
        assert!(!closed.is_cancelled());
        assert!(!manager.0.lifecycle_tracker.is_idle());
        assert!(manager.0.state.try_lock().is_some());
        drop(gates.close_resume);
        timeout(DEADLINE, disposal).await??;
        assert!(closed.is_cancelled());
        assert!(gates.finished.load(Ordering::Acquire));
        assert_eq!(manager.0.state.lock().tracked[tracked].is_held(), reserved);
        assert!(manager.0.lifecycle_tracker.is_idle());
        if owner.is_some() {
            manager
                .release_db(tracked)
                .expect("discarded opening preserves owner authority");
        }
        let (replacement, reopened) = manager.open_db(db_spec(name, None))?;
        assert_ne!(replacement, tracked);
        let db = reopened.await?;
        assert!(matches!(opening.await, Err(RocksError::Shutdown(_))));
        assert_eq!(manager.get_db_ref(&path), Some(replacement));
        db.close().await.expect("no retained database pins");
    }

    for cf in [false, true] {
        for held in [false, true] {
            let mut spec = db_spec("interrupted-open", None);
            let owner = held
                .then(|| manager.hold_db(spec.name.clone(), spec.path.clone(), "owner".into()))
                .transpose()?;
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
            let tracked = if let Some(owner) = owner {
                let (tracked, opening) = manager.reopen_db(owner, spec)?;
                drop(opening);
                tracked
            } else {
                let (tracked, opening) = manager.open_db(spec)?;
                drop(opening);
                tracked
            };
            let opening = manager.get_db_by_ref(tracked);
            let closed = manager.closed(tracked)?;
            assert!(matches!(
                timeout(DEADLINE, opening).await?,
                Err(RocksError::Shutdown(_))
            ));
            timeout(DEADLINE, closed).await?;
            assert_eq!(manager.0.state.lock().tracked[tracked].is_held(), held);
            if held {
                assert!(manager.open_db(db_spec("interrupted-open", None)).is_err());
                manager.release_db(tracked)?;
            }
            let (_, opening) = manager.open_db(db_spec("interrupted-open", None))?;
            let db = timeout(DEADLINE, opening).await??;
            timeout(DEADLINE, db.close()).await?.expect("last pin");
        }
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
    let tracked = manager.remote_open_db(&spec)?;
    drop(gates.open_resume);
    let env = manager.db_env().clone();
    let db = tokio::task::spawn_blocking(move || {
        manager.register_db(tracked, RocksAccess::open_db(spec, env))
    })
    .await??;
    db.inner().as_raw_db().put(b"key", b"value")?;
    let closed = db.closed.clone();
    // Outside a runtime, unwinding still hands final-reference cleanup to the
    // lifecycle executor; caller completion is not native completion.
    let cleanup = std::thread::spawn(move || {
        let _db = db;
        panic!("interrupted cleanup dispatch");
    });
    timeout(DEADLINE, gates.close_reached).await??;
    assert!(!closed.is_cancelled());
    assert!(!gates.finished.load(Ordering::Acquire));
    assert!(!manager.0.lifecycle_tracker.is_idle());
    assert!(manager.0.state.try_lock().is_some());
    drop(gates.close_resume);
    assert!(cleanup.join().is_err());
    timeout(DEADLINE, closed.cancelled()).await?;
    lifecycle_queue_drained(manager).await?;
    assert!(closed.is_cancelled());
    assert!(gates.finished.load(Ordering::Acquire));
    assert!(manager.0.lifecycle_tracker.is_idle());
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
                threadpool::ThreadPool::new(1),
                threadpool::ThreadPool::new(1),
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
            assert!(db.background_tracker.is_idle());
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

            // Lifecycle work remains independent of the busy global executor
            // and the ambient runtime, including cancellation of this waiter.
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
            assert!(manager.0.lifecycle_tracker.is_idle());
            db.close().await.expect("owner retains the last pin");
            drop((high, low));
            executor_drained(&executor).await?;
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
    manager
        .0
        .lifecycle_executor
        .dispatch_fn(Priority::Low, move || {
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
            .priority(priority)
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
            let mut observer = Box::pin(db.tracked_executor().spawn(task)?);
            assert_pending(observer.as_mut());
            drop(observer);
        } else {
            db.tracked_executor().dispatch(task)?;
        }

        let mut shutdown = Box::pin(manager.shutdown());
        assert_pending(shutdown.as_mut());
        assert!(!db.background_tracker.is_idle());
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
        assert!(db.tracked_executor().dispatch(rejected()).is_err());
        assert!(db.tracked_executor().spawn(rejected()).is_err());

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
        assert!(db.background_tracker.is_idle());
        assert!(manager.0.lifecycle_tracker.is_idle());
        // Sealing this manager's scope must not close the shared backend.
        timeout(DEADLINE, global.0.executor.spawn_fn(priority, || ())?).await??;
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
        assert!(manager.0.lifecycle_tracker.is_idle());
        assert!(!db.closed.is_cancelled());
        assert_pending(shutdown.as_mut());
        let mut concurrent_shutdown = Box::pin(manager.shutdown());
        assert_pending(concurrent_shutdown.as_mut());

        // An unwound preparation is not Completed: final-reference cleanup must
        // perform it again before releasing the native database.
        drop(db);
        timeout(DEADLINE, closed).await?;
        timeout(DEADLINE, shutdown).await?;
        timeout(DEADLINE, concurrent_shutdown).await?;
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        assert!(manager.0.lifecycle_tracker.is_idle());
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
            // Admission and dispatch happen without polling the returned future.
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
                // Cancellation cannot erase an accepted, not-yet-executed opening.
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
                        !manager.0.lifecycle_tracker.is_idle(),
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
            assert!(!manager.0.lifecycle_tracker.is_idle());
            // Only the DbRef remains: neither the original completion future nor
            // another observer is required to keep the result retrievable.
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
            assert!(manager.0.lifecycle_tracker.is_idle());
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
            // A late notification addresses its old incarnation, never the path.
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
                // Shutdown must dispose unclaimed managed results even if their
                // original waiter has been cancelled.
                drop(opening.take());
            }
            // Any remaining waiter stays unpolled throughout shutdown. It must
            // neither block disposal nor pin the native database.
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
            assert!(manager.0.lifecycle_tracker.is_idle());
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
    // Existing observers keep the original native error after the path is replaced.
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
async fn remote_open_validates_refs_and_paths_and_preserves_failed_owner_holds()
-> anyhow::Result<()> {
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let manager = task_center.run_sync(RocksDbManager::init);
    let name = "remote-owner";
    let spec = db_spec(name, None);
    let path = spec.path.clone();
    let held = manager.hold_db(spec.name.clone(), path.clone(), "owner maintenance".into())?;
    timeout(DEADLINE, manager.closed(held)?).await?;
    assert!(!path.exists());
    assert!(matches!(
        manager.get_db_by_ref(held).await,
        Err(RocksError::NotOpen)
    ));
    assert!(matches!(
        manager.hold_db(spec.name.clone(), path.clone(), "competing owner".into()),
        Err(RocksError::Reserved)
    ));
    assert!(matches!(
        manager.remote_reopen_db(held, &db_spec("wrong-held-path", None)),
        Err(RocksError::PathMismatch { .. })
    ));

    let tracked = manager.remote_reopen_db(held, &spec)?;
    assert_ne!(tracked, held);
    assert_eq!(manager.get_db_ref(&path), Some(tracked));
    let mut closed = Box::pin(manager.closed(tracked)?);
    let mut retrieval = Box::pin(manager.get_db_by_ref(tracked));
    assert_pending(closed.as_mut());
    assert_pending(retrieval.as_mut());
    assert!(matches!(
        manager.register_db(held, Err(ShutdownError.into())),
        Err(RocksError::NotRegistered)
    ));
    assert!(matches!(
        manager.release_db(tracked),
        Err(RocksError::AlreadyOpen)
    ));
    assert!(matches!(
        manager.remote_open_db(&spec),
        Err(RocksError::Reserved)
    ));

    // A wrong native payload is destroyed before rejection returns. It cannot
    // complete the booked lifetime, and disposal must not hold the metadata lock.
    let (wrong_spec, gates) = gated_spec("wrong-remote-payload", false);
    drop(gates.open_resume);
    let env = manager.db_env().clone();
    let rejected = tokio::task::spawn_blocking(move || {
        manager.register_db(tracked, RocksAccess::open_db(wrong_spec, env))
    });
    timeout(DEADLINE, gates.close_reached).await??;
    assert!(manager.0.state.try_lock().is_some());
    assert!(matches!(
        manager.0.state.lock().tracked[tracked].status,
        Status::RemoteOpen { .. }
    ));
    assert_pending(closed.as_mut());
    assert_pending(retrieval.as_mut());
    assert!(!manager.0.lifecycle_tracker.is_idle());
    drop(gates.close_resume);
    assert!(matches!(
        timeout(DEADLINE, rejected).await??,
        Err(RocksError::PathMismatch { .. })
    ));
    assert!(gates.finished.load(Ordering::Acquire));

    // No native work was started for the booked path. The owner can now report
    // that outcome explicitly; neither dropping an observer nor rejection did it.
    drop(retrieval);
    let wake = Arc::new(CloseWake {
        manager: manager.downgrade(),
        tracked,
        observed_closed: AtomicBool::new(false),
    });
    let waker = Waker::from(wake.clone());
    assert!(
        closed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(matches!(
        manager.register_db(tracked, Err(ShutdownError.into())),
        Err(RocksError::Shutdown(_))
    ));
    timeout(DEADLINE, closed).await?;
    assert!(wake.observed_closed.load(Ordering::Acquire));
    assert!(manager.0.state.lock().tracked[tracked].is_held());

    let (spec, gates) = gated_spec(name, true);
    let failed = manager.remote_reopen_db(tracked, &spec)?;
    let retained_error = manager.get_db_by_ref(failed);
    let mut closed = Box::pin(manager.closed(failed)?);
    let env = manager.db_env().clone();
    let opening = tokio::task::spawn_blocking(move || {
        manager.register_db(failed, RocksAccess::open_db(spec, env))
    });
    timeout(DEADLINE, gates.open_reached).await??;
    drop(gates.open_resume);
    timeout(DEADLINE, gates.close_reached).await??;
    assert_pending(closed.as_mut());
    assert!(!manager.0.lifecycle_tracker.is_idle());
    assert!(matches!(
        manager.release_db(failed),
        Err(RocksError::AlreadyOpen)
    ));
    drop(gates.close_resume);
    let error = timeout(DEADLINE, opening).await??.unwrap_err();
    assert!(matches!(error, RocksError::Other(_)));
    timeout(DEADLINE, closed).await?;
    assert!(gates.finished.load(Ordering::Acquire));
    assert!(manager.0.lifecycle_tracker.is_idle());
    assert!(manager.0.state.lock().tracked[failed].is_held());

    let spec = db_spec(name, None);
    let replacement = manager.remote_reopen_db(failed, &spec)?;
    assert_ne!(replacement, failed);
    assert!(matches!(
        manager.closed(failed),
        Err(RocksError::NotRegistered)
    ));
    assert!(matches!(
        manager.register_db(failed, Err(ShutdownError.into())),
        Err(RocksError::NotRegistered)
    ));
    assert_eq!(
        retained_error.await.unwrap_err().to_string(),
        error.to_string()
    );
    let env = manager.db_env().clone();
    let db = tokio::task::spawn_blocking(move || {
        manager.register_db(replacement, RocksAccess::open_db(spec, env))
    })
    .await??;
    assert_eq!(db.registration(), Some(replacement));
    assert!(matches!(
        manager.register_db(replacement, Err(ShutdownError.into())),
        Err(RocksError::NotRemoteOpen)
    ));
    assert!(!db.closed.is_cancelled());
    db.close().await.expect("owner holds the last native pin");
    manager.release_db(replacement)?;
    assert!(matches!(
        manager.release_db(replacement),
        Err(RocksError::NotHeld)
    ));
    manager.shutdown().await;
    Ok(())
}

#[test]
fn remote_open_shutdown_waits_for_finalization_but_not_handed_off_handles() -> anyhow::Result<()> {
    with_single_blocking_thread(async {
        let task_center = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()?
            .into_handle();
        let manager = task_center.run_sync(RocksDbManager::init);

        let spec = db_spec("abandoned-remote-open", None);
        let tracked = manager.remote_open_db(&spec)?;
        assert!(matches!(
            manager.remote_open_db(&spec),
            Err(RocksError::AlreadyOpen)
        ));
        assert!(matches!(
            manager.open_db(db_spec("abandoned-remote-open", None)),
            Err(RocksError::AlreadyOpen)
        ));
        let observer = manager.get_db_by_ref(tracked);
        drop(observer);
        let mut shutdown = Box::pin(manager.shutdown());
        assert_pending(shutdown.as_mut());
        assert!(matches!(
            manager.remote_open_db(&spec),
            Err(RocksError::Shutdown(_))
        ));
        assert!(matches!(
            manager.hold_db(spec.name.clone(), spec.path.clone(), "shutdown".into()),
            Err(RocksError::Shutdown(_))
        ));
        assert!(!manager.0.lifecycle_tracker.is_idle());
        assert!(!manager.0.state.lock().tracked[tracked].is_closed());
        assert!(matches!(
            manager.register_db(tracked, Err(ShutdownError.into())),
            Err(RocksError::Shutdown(_))
        ));
        timeout(DEADLINE, shutdown).await?;
        manager.reset().await?;

        for fail in [false, true] {
            let (spec, gates) = gated_spec(
                if fail {
                    "remote-failed-shutdown"
                } else {
                    "remote-shutdown"
                },
                fail,
            );
            let tracked = manager.remote_open_db(&spec)?;
            manager.hold_db_by_ref(tracked, "shutdown maintenance".into())?;
            let observer = manager.get_db_by_ref(tracked);
            let mut closed = Box::pin(manager.closed(tracked)?);
            let env = manager.db_env().clone();
            let opening = tokio::task::spawn_blocking(move || {
                manager.register_db(tracked, RocksAccess::open_db(spec, env))
            });
            timeout(DEADLINE, gates.open_reached).await??;
            let mut shutdown = Box::pin(manager.shutdown());
            assert_pending(shutdown.as_mut());
            drop(gates.open_resume);
            timeout(DEADLINE, gates.close_reached).await??;
            assert_pending(shutdown.as_mut());
            assert_pending(closed.as_mut());
            assert!(manager.0.state.try_lock().is_some());
            assert!(!manager.0.lifecycle_tracker.is_idle());
            assert!(matches!(
                manager.release_db(tracked),
                Err(RocksError::AlreadyOpen)
            ));
            drop(gates.close_resume);
            let error = timeout(DEADLINE, opening).await??.unwrap_err();
            assert!(if fail {
                matches!(error, RocksError::Other(_))
            } else {
                matches!(error, RocksError::Shutdown(_))
            });
            assert_eq!(observer.await.unwrap_err().to_string(), error.to_string());
            timeout(DEADLINE, closed).await?;
            timeout(DEADLINE, shutdown).await?;
            assert!(gates.finished.load(Ordering::Acquire));
            assert!(manager.0.lifecycle_tracker.is_idle());
            assert!(
                manager.reset().await.is_err(),
                "shutdown preserves owner holds"
            );
            manager.release_db(tracked)?;
            manager.reset().await?;
        }

        // The owner's executor buffers a successful handoff. Unlike unfinished
        // remote work, this is an external native pin: shutdown prepares it but
        // cannot claim to have physically closed it.
        let spec = db_spec("remote-buffered-owner", None);
        let tracked = manager.remote_open_db(&spec)?;
        let mut closed = Box::pin(manager.closed(tracked)?);
        let env = manager.db_env().clone();
        let unpolled = tokio::task::spawn_blocking(move || {
            manager.register_db(tracked, RocksAccess::open_db(spec, env))
        });
        blocking_queue_drained().await?;
        assert!(manager.0.lifecycle_tracker.is_idle());
        timeout(DEADLINE, manager.shutdown()).await?;
        assert_pending(closed.as_mut());
        assert!(manager.reset().await.is_err());
        let db = unpolled.await??;
        assert_eq!(db.registration(), Some(tracked));
        db.close().await.expect("buffered result was the last pin");
        timeout(DEADLINE, closed).await?;
        manager.reset().await?;
        Ok(())
    })
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
    let tracked = manager.remote_open_db(&spec)?;
    assert!(manager.get_all_dbs().is_empty());
    assert_eq!(manager.get_db_ref(&path), Some(tracked));
    let closed_before_registration = manager.closed(tracked)?;
    let mut retrieval = Box::pin(manager.get_db_by_ref(tracked));
    assert_pending(retrieval.as_mut());
    let env = manager.db_env().clone();
    let db = tokio::task::spawn_blocking(move || {
        manager.register_db(tracked, RocksAccess::open_db(spec, env))
    })
    .await??;
    assert!(Arc::ptr_eq(&retrieval.await?, &db));
    assert!(matches!(
        manager.register_db(tracked, Err(ShutdownError.into())),
        Err(RocksError::NotRemoteOpen)
    ));
    assert_eq!(db.registration(), Some(tracked));
    assert_eq!(manager.get_db_ref(&path), Some(tracked));
    assert_eq!(manager.0.state.lock().tracked[tracked].path, path);
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
    let spec = db_spec("distinct-path", None);
    let replacement = manager.remote_open_db(&spec)?;
    let env = manager.db_env().clone();
    let other = tokio::task::spawn_blocking(move || {
        manager.register_db(replacement, RocksAccess::open_db(spec, env))
    })
    .await??;
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
        manager.remote_open_db(&db_spec("sealed-remote-open", None)),
        Err(RocksError::Shutdown(_))
    ));
    db.close().await.expect("no retained database pins");
    timeout(DEADLINE, closed_before_registration).await?;
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

#[tokio::test]
async fn owner_hold_survives_close_delete_and_failed_or_cancelled_reopen() -> anyhow::Result<()> {
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let manager = task_center.run_sync(RocksDbManager::init);
    let name = "owner-held-db";
    let (spec, gates) = gated_spec(name, false);
    let path = spec.path.clone();
    let (first, opening) = manager.open_db(spec)?;
    let mut opening = Box::pin(opening);
    reach_gate(opening.as_mut(), gates.open_reached).await?;
    drop(gates.open_resume);
    let db = timeout(DEADLINE, opening).await??;
    manager.hold_db_by_ref(first, "owner maintenance".into())?;
    let mut owner = first;
    let first_closed = db.closed.clone();
    assert_eq!(
        Arc::strong_count(&db),
        1,
        "a hold must not pin native resources"
    );
    assert!(matches!(
        manager.hold_db_by_ref(first, "competing hold".into()),
        Err(RocksError::Reserved)
    ));
    db.inner().as_raw_db().put(b"old", b"value")?;
    assert!(matches!(
        manager.release_db(owner),
        Err(RocksError::AlreadyOpen)
    ));

    let closed = manager.closed(owner)?;
    let mut close = Box::pin(db.close());
    assert_pending(close.as_mut());
    timeout(DEADLINE, gates.close_reached).await??;
    assert!(matches!(
        manager.open_db(db_spec(name, None)),
        Err(RocksError::Reserved)
    ));
    assert!(matches!(
        manager.reopen_db(owner, db_spec(name, None)),
        Err(RocksError::AlreadyOpen)
    ));
    assert_eq!(
        manager.get_db_ref(&path),
        Some(first),
        "rejection is not acceptance"
    );
    drop(close);
    drop(gates.close_resume);
    timeout(DEADLINE, closed).await?;
    assert!(manager.0.state.lock().tracked[first].is_closed());
    assert!(manager.0.state.lock().tracked[first].is_held());
    assert_eq!(manager.get_db_ref(&path), Some(first));
    assert!(gates.finished.load(Ordering::Acquire));

    assert!(matches!(
        manager.remote_open_db(&db_spec(name, None)),
        Err(RocksError::Reserved)
    ));
    assert_eq!(manager.get_db_ref(&path), Some(first));

    tokio::fs::remove_dir_all(&path).await?;
    assert!(!path.exists());
    assert!(matches!(
        manager.open_db(db_spec(name, None)),
        Err(RocksError::Reserved)
    ));
    assert!(
        !path.exists(),
        "a rejected open must not recreate the owner's directory"
    );
    assert!(matches!(
        manager.reopen_db(owner, db_spec("wrong-held-path", None)),
        Err(RocksError::PathMismatch { .. })
    ));

    // Native failure and cancellation of an accepted reopen both preserve the
    // owner hold, which transfers to the returned ref during admission.
    for fail in [true, false] {
        let (spec, gates) = gated_spec(name, fail);
        let previous = owner;
        let (tracked, opening) =
            manager.reopen_db_with_env(owner, spec, manager.db_env().clone())?;
        owner = tracked;
        assert_ne!(tracked, previous);
        assert_eq!(manager.get_db_ref(&path), Some(tracked));
        let mut opening = Box::pin(opening);
        reach_gate(opening.as_mut(), gates.open_reached).await?;
        if fail {
            drop(gates.open_resume);
            reach_gate(opening.as_mut(), gates.close_reached).await?;
            assert!(matches!(
                manager.open_db(db_spec(name, None)),
                Err(RocksError::Reserved)
            ));
            drop(gates.close_resume);
            assert!(timeout(DEADLINE, opening).await?.is_err());
        } else {
            drop(opening);
            let closed = manager.closed(owner)?;
            let mut retrieval = Box::pin(manager.get_db_by_ref(tracked));
            assert_pending(retrieval.as_mut());
            drop(gates.open_resume);
            let db = timeout(DEADLINE, retrieval).await??;
            assert_eq!(db.registration(), Some(tracked));
            assert_eq!(Arc::strong_count(&db), 1);
            drop(db);
            timeout(DEADLINE, gates.close_reached).await??;
            assert!(matches!(
                manager.open_db(db_spec(name, None)),
                Err(RocksError::Reserved)
            ));
            drop(gates.close_resume);
            timeout(DEADLINE, closed).await?;
        }
        assert_ne!(owner, previous);
        assert_eq!(manager.get_db_ref(&path), Some(owner));
        assert!(!manager.0.state.lock().tracked.contains_key(previous));
        assert!(matches!(
            manager.release_db(previous),
            Err(RocksError::NotRegistered)
        ));
        assert!(gates.finished.load(Ordering::Acquire));
        assert!(matches!(
            manager.open_db(db_spec(name, None)),
            Err(RocksError::Reserved)
        ));
    }

    let (tracked, opening) = manager.reopen_db(owner, db_spec(name, None))?;
    owner = tracked;
    let db = opening.await?;
    assert_eq!(db.inner().as_raw_db().get(b"old")?, None);
    assert_eq!(db.registration(), Some(owner));
    assert_ne!(owner, first);
    manager.finish_close(first, first_closed);
    assert!(!db.closed.is_cancelled());
    assert!(matches!(
        manager.open_db(db_spec(name, None)),
        Err(RocksError::Reserved)
    ));
    assert_eq!(Arc::strong_count(&db), 1);
    db.close().await.expect("owner is the last database pin");
    manager
        .release_db(owner)
        .expect("physical close allows explicit release");
    let (_, opening) = manager.open_db(db_spec(name, None))?;
    let db = opening.await?;

    manager.hold_db_by_ref(
        db.registration().expect("registered"),
        "discarded owner ref".into(),
    )?;
    db.close().await.expect("hold owns no database pin");
    assert!(matches!(
        manager.open_db(db_spec(name, None)),
        Err(RocksError::Reserved)
    ));
    timeout(DEADLINE, manager.shutdown()).await?;
    assert!(
        manager.reset().await.is_err(),
        "reset must retain abandoned owner holds"
    );
    manager.release_db(manager.get_db_ref(&path).expect("held incarnation"))?;
    manager.reset().await?;
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
        // Exercise owned manager handles without the global singleton pin.
        let manager = owned_manager(global);
        let weak = manager.downgrade();
        let (spec, gates) = gated_spec("owned-manager", false);
        let (tracked, opening) = manager.open_db(spec)?;
        let mut opening = Box::pin(opening);
        reach_gate(opening.as_mut(), gates.open_reached).await?;
        drop(gates.open_resume);
        let db = timeout(DEADLINE, opening).await??;
        assert!(matches!(
            global.hold_db_by_ref(tracked, "unknown ref".into()),
            Err(RocksError::NotRegistered)
        ));
        assert!(matches!(
            manager
                .clone()
                .register_db(tracked, Err(ShutdownError.into())),
            Err(RocksError::NotRemoteOpen)
        ));
        manager.hold_db_by_ref(tracked, "owner hold".into())?;
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
