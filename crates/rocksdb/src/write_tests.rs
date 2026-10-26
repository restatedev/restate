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
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak, mpsc};
use std::task::{Context, Wake, Waker};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::timeout;

use restate_core::TaskCenterBuilder;
use restate_platform::sync::Mutex;
use restate_types::config::node_dir;
use restate_types::protobuf::common::DatabaseKind;

use crate::admission::Admission;
use crate::configuration::{
    CfConfigurator, DbConfigurator, create_default_cf_options, create_default_db_options,
};
use crate::{
    CfPrefixPattern, DbName, DbSpecBuilder, IoMode, Priority, RocksAccess, RocksDb, RocksDbManager,
    WriteOperation,
};

struct Configurator;

impl DbConfigurator for Configurator {
    fn get_db_options(
        &self,
        name: &DbName,
        env: &rocksdb::Env,
        buffers: &rocksdb::WriteBufferManager,
        limiter: &rocksdb::RateLimiter,
    ) -> rocksdb::Options {
        create_default_db_options(env, name, buffers, limiter)
    }
}

impl CfConfigurator for Configurator {
    fn get_cf_options(
        &self,
        _: &DbName,
        _: &str,
        _: &rocksdb::Cache,
        buffers: &rocksdb::WriteBufferManager,
    ) -> rocksdb::Options {
        let mut opts = create_default_cf_options(Some(buffers));
        let mut blocks = rocksdb::BlockBasedOptions::default();
        blocks.disable_cache();
        opts.set_block_based_table_factory(&blocks);
        opts
    }
}

// Captured by the write closure: its destruction must precede lease completion.
struct DropProbe {
    admission: Weak<Admission>,
    released: Arc<AtomicBool>,
}

impl DropProbe {
    fn check_ownership(&self, owned: bool) {
        assert_eq!(self.admission.strong_count(), if owned { 2 } else { 1 });
    }
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        let admission = self.admission.upgrade().unwrap();
        assert!(
            pin!(admission.drain())
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        self.released.store(true, Ordering::Release);
    }
}

// Observe database ownership at the exact completion notification, rather than
// after the async waiter resumes (which could hide premature notification).
struct DbRefsOnWake {
    db: Weak<RocksDb>,
    completed: Mutex<Option<oneshot::Sender<usize>>>,
}

impl Wake for DbRefsOnWake {
    fn wake(self: Arc<Self>) {
        if let Some(completed) = self.completed.lock().take() {
            let _ = completed.send(self.db.strong_count());
        }
    }
}

#[tokio::test]
async fn write_leases_follow_execution_and_release_resources_before_completion()
-> anyhow::Result<()> {
    const DEADLINE: Duration = Duration::from_secs(10);
    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let manager = task_center.run_sync(RocksDbManager::init);
    let spec = DbSpecBuilder::new(
        DbName::new("write-leases"),
        DatabaseKind::PartitionStore,
        node_dir().join("write-leases"),
        Configurator,
    )
    .add_cf_pattern(CfPrefixPattern::ANY, Configurator)
    .build()?;
    let db = manager.open_db(spec).await?;

    // Obtain real errors through the binding's public API. A cache-only read of
    // flushed data with caching disabled deterministically returns Incomplete.
    db.inner().as_raw_db().put(b"retry-source", b"value")?;
    db.inner().as_raw_db().flush()?;
    let mut read = rocksdb::ReadOptions::default();
    read.set_read_tier(rocksdb::ReadTier::BlockCache);
    let retry = db
        .inner()
        .as_raw_db()
        .get_opt(b"retry-source", &read)
        .unwrap_err();
    assert_eq!(retry.kind(), rocksdb::ErrorKind::Incomplete);
    let terminal = db
        .inner()
        .as_raw_db()
        .set_options(&[("unknown-test-option", "1")])
        .unwrap_err();
    assert!(!crate::is_retryable_error(terminal.kind()));

    for mode in [
        IoMode::AllowBlockingIO,
        IoMode::OnlyIfNonBlocking,
        IoMode::Default,
    ] {
        for fail in [false, true] {
            let admission = Arc::new(Admission::default());
            let lease = admission.try_acquire().unwrap();
            admission.seal();
            let released = Arc::new(AtomicBool::new(false));
            let probe = DropProbe {
                admission: Arc::downgrade(&admission),
                released: released.clone(),
            };
            let terminal = terminal.clone();
            let mut batch = rocksdb::WriteBatchWithIndex::new(0, true);
            batch.put(b"inline", b"value");
            let write = WriteOperation {
                batch,
                options: rocksdb::WriteOptions::default(),
                write_op: move |db: &RocksAccess,
                                batch: &rocksdb::WriteBatchWithIndex,
                                opts: &rocksdb::WriteOptions| {
                    probe.check_ownership(false);
                    if fail {
                        Err(terminal.clone())
                    } else {
                        db.write_batch_with_index(batch, opts)
                    }
                },
                db: &db,
                lease: Some(lease),
            };
            let result = db
                .write_batch_internal("inline-lease", Priority::High, mode, write)
                .await;
            assert_eq!(result.is_err(), fail);
            assert!(released.load(Ordering::Acquire));
            admission.drain().await;
            assert_eq!(Arc::strong_count(&admission), 1);
        }
    }

    // Both dispatch branches must retain the lease while queued, then running,
    // even after the async result waiter has been destroyed.
    for mode in [IoMode::AlwaysBackground, IoMode::Default] {
        let pause = manager.pause_background(Priority::High).await;
        let admission = Arc::new(Admission::default());
        let lease = admission.try_acquire().unwrap();
        admission.seal(); // Default-mode promotion must work after sealing too.
        let released = Arc::new(AtomicBool::new(false));
        let probe = DropProbe {
            admission: Arc::downgrade(&admission),
            released: released.clone(),
        };
        let (started, reached) = oneshot::channel();
        let started = Mutex::new(Some(started));
        let (resume, paused) = mpsc::channel::<()>();
        let first = AtomicBool::new(true);
        let retry = retry.clone();
        let mut batch = rocksdb::WriteBatchWithIndex::new(0, true);
        batch.put(b"background", b"value");
        let write = WriteOperation {
            batch,
            options: rocksdb::WriteOptions::default(),
            write_op: move |db: &RocksAccess,
                            batch: &rocksdb::WriteBatchWithIndex,
                            opts: &rocksdb::WriteOptions| {
                if mode == IoMode::Default && first.swap(false, Ordering::Relaxed) {
                    probe.check_ownership(false);
                    return Err(retry.clone());
                }
                probe.check_ownership(true);
                let _ = started.lock().take().unwrap().send(());
                let _ = paused.recv();
                db.write_batch_with_index(batch, opts)
            },
            db: &db,
            lease: Some(lease),
        };
        let (completed, mut notification) = oneshot::channel();
        let observed = Arc::new(DbRefsOnWake {
            db: Arc::downgrade(&db),
            completed: Mutex::new(Some(completed)),
        });
        let waker = Waker::from(observed.clone());
        let mut drain = Box::pin(admission.drain());
        assert!(
            drain
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let mut write =
            Box::pin(db.write_batch_internal("background-lease", Priority::High, mode, write));
        assert!(
            write
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(write);
        assert!(!released.load(Ordering::Acquire));
        assert!(matches!(
            notification.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        drop(pause);
        timeout(DEADLINE, reached).await??;
        assert!(!released.load(Ordering::Acquire));
        assert!(
            drain
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(resume);
        // Register a second waiter so the ownership-observing waker stays installed.
        timeout(DEADLINE, admission.drain()).await?;
        assert_eq!(timeout(DEADLINE, notification).await??, 1);
        assert!(released.load(Ordering::Acquire));
        assert_eq!(
            db.inner().as_raw_db().get(b"background")?.as_deref(),
            Some(b"value".as_slice())
        );
    }

    // Exercise destruction without execution, a terminal error, and unwinding
    // through the same owned operation used by the background executor.
    for outcome in 0..3 {
        let admission = Arc::new(Admission::default());
        let lease = admission.try_acquire().unwrap();
        admission.seal();
        let released = Arc::new(AtomicBool::new(false));
        let probe = DropProbe {
            admission: Arc::downgrade(&admission),
            released: released.clone(),
        };
        let terminal = terminal.clone();
        let write = WriteOperation {
            batch: rocksdb::WriteBatchWithIndex::new(0, true),
            options: rocksdb::WriteOptions::default(),
            write_op: move |_: &RocksAccess,
                            _: &rocksdb::WriteBatchWithIndex,
                            _: &rocksdb::WriteOptions| {
                probe.check_ownership(true);
                assert_ne!(outcome, 2, "test write panic");
                Err(terminal.clone())
            },
            db: &db,
            lease: Some(lease),
        }
        .into_background();
        match outcome {
            0 => drop(write),
            1 => assert!(write.run("failed-write").is_err()),
            2 => assert!(catch_unwind(AssertUnwindSafe(|| write.run("panicking-write"))).is_err()),
            _ => unreachable!(),
        }
        assert!(released.load(Ordering::Acquire));
        admission.drain().await;
        assert_eq!(Arc::strong_count(&db), 1);
    }
    db.close().await.expect("all operation references released");
    task_center.shutdown_node("test completed", 0).await;
    manager.shutdown().await;
    Ok(())
}
