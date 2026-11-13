// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::oneshot;

use restate_core::io::{FutureTask, IoExecutor, IoTask, IoTaskError};
use restate_platform::sync::Mutex;
use restate_rocksdb::SinglePoolExecutor;
use restate_storage_api::{Result, StorageError};

/// The IO pool owned by the query engines, separate from partition and database
/// IO. Create one pool at query-service startup and share its clones between
/// engines. Reads use their snapshot's admission, not the pool's lifetime.
///
/// Dropping a result releases unclaimed work immediately. Once a worker claims
/// it, cleanup belongs to that worker: cancellation wakes suspended work, but
/// cannot interrupt a blocking native call. Wait for the read snapshot's
/// completion before tearing down its resources.
#[derive(Clone)]
pub struct QueryIoExecutor {
    pool: SinglePoolExecutor,
}

impl QueryIoExecutor {
    /// Starts `threads` dedicated `rs:query-io` workers. Clones share this pool.
    pub fn new(threads: NonZeroUsize) -> Self {
        Self {
            pool: SinglePoolExecutor::new("rs:query-io", threads),
        }
    }

    /// Runs an admitted snapshot's task on this pool. Keeping this private
    /// prevents callers from bypassing the read capability.
    pub(super) fn spawn<O: Send + 'static>(
        &self,
        task: impl IoTask<O>,
    ) -> Result<impl Future<Output = Result<O>> + Send + 'static> {
        let pending = Pending(Arc::new(Mutex::new(Some(task))));
        let work = Arc::clone(&pending.0);
        let (mut tx, rx) = oneshot::channel();
        self.pool
            .dispatch_fn(
                "query-scan",
                FutureTask(move || async move {
                    let task = work.lock().take();
                    let Some(task) = task else { return };
                    // Receiver closure cancels a suspended task on its worker.
                    // Its lease is part of `task`, outside the execution future.
                    let output = tokio::select! {
                        biased;
                        _ = tx.closed() => return,
                        output = async move { task.run().await } => output,
                    };
                    let _ = tx.send(output);
                }),
            )
            .map_err(|_| StorageError::OperationalError)?;
        Ok(async move {
            let result = rx
                .await
                .map_err(|_| StorageError::Generic(IoTaskError.into()));
            drop(pending);
            result
        })
    }
}

/// The queue retains only this shared slot. A dropped result can take unclaimed
/// captures and admission back without needing a free worker. Drop outside the
/// mutex: destructors can perform their own synchronization.
struct Pending<T>(Arc<Mutex<Option<T>>>);

impl<T> Drop for Pending<T> {
    fn drop(&mut self) {
        let work = self.0.lock().take();
        drop(work);
    }
}
