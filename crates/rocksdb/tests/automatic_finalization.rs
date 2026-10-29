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
use restate_rocksdb::configuration::{
    CfConfigurator, DbConfigurator, create_default_cf_options, create_default_db_options,
};
use restate_rocksdb::{CfPrefixPattern, DbEnv, DbName, DbSpecBuilder, RocksDbManager};
use restate_types::config::node_dir;
use restate_types::protobuf::common::DatabaseKind;

struct NativeDrop {
    reached: Option<oneshot::Sender<()>>,
    resume: Mutex<mpsc::Receiver<()>>,
    finished: Arc<AtomicBool>,
}

impl EventListener for NativeDrop {}

impl Drop for NativeDrop {
    fn drop(&mut self) {
        let _ = self.reached.take().unwrap().send(());
        let _ = self.resume.get_mut().recv();
        self.finished.store(true, Ordering::Release);
    }
}

struct Configurator(Mutex<Option<NativeDrop>>);

impl DbConfigurator for Configurator {
    fn get_db_options(&self, name: &DbName, env: &DbEnv) -> rocksdb::Options {
        let mut options = create_default_db_options(env, name);
        options.add_event_listener(self.0.lock().take().unwrap());
        options
    }
}

impl CfConfigurator for Configurator {
    fn get_cf_options(&self, _: &DbName, _: &str, env: &DbEnv) -> rocksdb::Options {
        create_default_cf_options(env.write_buffer_manager.as_ref())
    }
}

// An integration test compiles restate-rocksdb as a dependency (without
// cfg(test)). Run with default features to cover consumers without test-util.
#[tokio::test]
async fn dependency_build_registers_native_cleanup() -> anyhow::Result<()> {
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()?
        .into_handle();
    let (finalizing, finalization_started) = oneshot::channel();
    tc.run_sync(|| {
        TaskCenter::register_test_finalizer(Box::pin(async move {
            finalizing.send(()).unwrap();
        }));
    });
    let manager = tc.run_sync(RocksDbManager::init);
    let (reached, native_drop_started) = oneshot::channel();
    let (resume, paused) = mpsc::channel();
    let finished = Arc::new(AtomicBool::new(false));
    let spec = DbSpecBuilder::new(
        DbName::new("automatic-finalization"),
        DatabaseKind::PartitionStore,
        node_dir().join("automatic-finalization"),
        Configurator(Mutex::new(Some(NativeDrop {
            reached: Some(reached),
            resume: Mutex::new(paused),
            finished: finished.clone(),
        }))),
    )
    .add_cf_pattern(CfPrefixPattern::ANY, Configurator(Mutex::new(None)))
    .build()?;
    let (_, opening) = manager.open_db(spec)?;
    let db = opening.await?;
    drop(db);
    timeout(Duration::from_secs(10), native_drop_started).await??;
    let mut teardown = Box::pin(tc.shutdown_for_test());
    tokio::select! {
        result = timeout(Duration::from_secs(10), finalization_started) => { result??; }
        _ = &mut teardown => panic!("teardown skipped native cleanup"),
    }
    assert!(
        teardown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(!finished.load(Ordering::Acquire));
    drop(resume);
    timeout(Duration::from_secs(10), teardown).await?;
    assert!(finished.load(Ordering::Acquire));
    Ok(())
}
