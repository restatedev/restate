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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Waker};
use std::time::Duration;

use parking_lot::Mutex;
use rocksdb::event_listener::EventListener;
use tokio::sync::oneshot;
use tokio::time::timeout;

use restate_core::{TaskCenter, TaskCenterBuilder};
use restate_types::config::node_dir;
use restate_types::protobuf::common::DatabaseKind;

use crate::configuration::{
    CfConfigurator, DbConfigurator, create_default_cf_options, create_default_db_options,
};
use crate::{CfPrefixPattern, DbEnv, DbName, DbSpec, DbSpecBuilder, RocksDb, RocksDbManager};

// Owned by native options/the database, not by the completion observer. Blocking
// its destruction distinguishes physical release from preparatory shutdown work.
struct DropBarrier {
    started: Option<oneshot::Sender<()>>,
    resume: Mutex<mpsc::Receiver<()>>,
    finished: Arc<AtomicBool>,
}

impl EventListener for DropBarrier {}

impl Drop for DropBarrier {
    fn drop(&mut self) {
        if let Some(started) = self.started.take() {
            let _ = started.send(());
        }
        let _ = self.resume.get_mut().recv();
        self.finished.store(true, Ordering::Release);
    }
}

struct Configurator(Mutex<Option<DropBarrier>>);

impl DbConfigurator for Configurator {
    fn get_db_options(&self, name: &DbName, env: &DbEnv) -> rocksdb::Options {
        let mut options = create_default_db_options(env, name);
        if let Some(listener) = self.0.lock().take() {
            options.add_event_listener(listener);
        }
        options
    }
}

impl CfConfigurator for Configurator {
    fn get_cf_options(&self, _: &DbName, _: &str, env: &DbEnv) -> rocksdb::Options {
        create_default_cf_options(env.write_buffer_manager.as_ref())
    }
}

fn db_spec(name: &str, listener: Option<DropBarrier>) -> DbSpec {
    DbSpecBuilder::new(
        DbName::new(name),
        DatabaseKind::PartitionStore,
        node_dir().join(name),
        Configurator(Mutex::new(listener)),
    )
    .add_cf_pattern(CfPrefixPattern::ANY, Configurator(Mutex::new(None)))
    .build()
    .unwrap()
}

#[derive(Clone, Copy)]
enum CloseMode {
    Explicit,
    Background,
    Unregistered,
    Inline,
}

#[tokio::test]
async fn task_center_finalization_waits_for_native_close() -> anyhow::Result<()> {
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let (finalizing, finalization_started) = oneshot::channel();
    let (ordinary_shutdown, ordinary_shutdown_completed) = oneshot::channel();
    task_center.set_on_shutdown(Box::pin(async move {
        ordinary_shutdown.send(()).unwrap();
    }));
    task_center.run_sync(|| {
        TaskCenter::register_test_finalizer(Box::pin(async move {
            // Ordinary shutdown must finish before any finalizer runs.
            ordinary_shutdown_completed.await.unwrap();
            finalizing.send(()).unwrap();
        }));
    });
    let manager = task_center.run_sync(RocksDbManager::init);
    let (started, reached) = oneshot::channel();
    let (resume, paused) = mpsc::channel();
    let finished = Arc::new(AtomicBool::new(false));
    let spec = db_spec(
        "automatic-test-finalization",
        Some(DropBarrier {
            started: Some(started),
            resume: Mutex::new(paused),
            finished: finished.clone(),
        }),
    );
    let db = manager.open_db(spec).await?;
    let mut closed = Box::pin(db.closed());
    drop(db);
    timeout(Duration::from_secs(10), reached).await??;

    let mut shutdown = Box::pin(task_center.shutdown_for_test());
    tokio::select! {
        result = timeout(Duration::from_secs(10), finalization_started) => { result??; }
        _ = &mut shutdown => panic!("teardown returned while native destruction was blocked"),
    }
    assert!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(!finished.load(Ordering::Acquire));
    drop(resume);
    timeout(Duration::from_secs(10), shutdown).await?;
    assert!(finished.load(Ordering::Acquire));
    assert!(
        closed
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    Ok(())
}

#[tokio::test]
async fn native_close_survives_cancelled_waiters_and_releases_directory() -> anyhow::Result<()> {
    const DEADLINE: Duration = Duration::from_secs(10);
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let manager = task_center.run_sync(RocksDbManager::init);

    for (index, mode) in [
        CloseMode::Explicit,
        CloseMode::Background,
        CloseMode::Unregistered,
        CloseMode::Inline,
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("native-close-{index}");
        let (started, reached) = oneshot::channel();
        let (resume, paused) = mpsc::channel();
        let finished = Arc::new(AtomicBool::new(false));
        let spec = db_spec(
            &name,
            Some(DropBarrier {
                started: Some(started),
                resume: Mutex::new(paused),
                finished: finished.clone(),
            }),
        );
        let db = if matches!(mode, CloseMode::Unregistered) {
            // Native destruction must be dispatched even without a registry entry.
            RocksDb::open(manager, spec, manager.db_env().clone()).await?
        } else {
            manager.open_db(spec).await?
        };
        db.inner().as_raw_db().put(b"key", b"value")?;

        let mut closed = Box::pin(db.closed());
        let mut cancelled = Box::pin(db.closed());
        assert_eq!(Arc::strong_count(&db), 1, "observers must not pin the DB");
        assert!(
            cancelled
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );

        let mut close = None;
        let mut inline_drop = None;
        match mode {
            CloseMode::Explicit => {
                let retained = db.clone();
                let db = db.close().await.expect_err("a live pin prevents close");
                drop(retained);
                let mut waiter = Box::pin(db.close());
                assert!(
                    waiter
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                close = Some(waiter);
            }
            CloseMode::Background | CloseMode::Unregistered => {
                let retained = db.clone();
                drop(db);
                assert!(
                    closed
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                // This caller must return while native destruction is blocked.
                timeout(
                    DEADLINE,
                    tokio::task::spawn_blocking(move || drop(retained)),
                )
                .await??;
            }
            CloseMode::Inline => {
                // No ambient Tokio runtime: native destruction happens here.
                inline_drop = Some(std::thread::spawn(move || drop(db)));
            }
        }

        timeout(DEADLINE, reached).await??;
        assert!(!finished.load(Ordering::Acquire));
        assert!(
            closed
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "native destruction is still blocked"
        );
        drop(close);
        drop(cancelled);
        drop(resume);
        timeout(DEADLINE, closed).await?;
        assert!(finished.load(Ordering::Acquire));
        if let Some(thread) = inline_drop {
            thread.join().expect("inline cleanup succeeded");
        }

        // Physical completion releases the native directory lock and lets the
        // existing execution pools open the database again immediately.
        let reopened = manager.open_db(db_spec(&name, None)).await?;
        assert_eq!(
            reopened.inner().as_raw_db().get(b"key")?.as_deref(),
            Some(b"value".as_slice())
        );
        reopened.close().await.expect("no retained database pins");
    }

    task_center.shutdown_for_test().await;
    Ok(())
}
