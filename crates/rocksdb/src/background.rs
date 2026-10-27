// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;

use derive_builder::Builder;
use metrics::{Gauge, gauge, histogram};
use parking_lot::Mutex;

use restate_core::ShutdownError;
use restate_core::io::{IoExecutor, IoOp, IoTask};

use crate::metric_definitions::{STORAGE_BG_TASK_RUN_DURATION, STORAGE_BG_TASK_WAIT_DURATION};
use crate::{OP_TYPE, PRIORITY, Priority, STORAGE_BG_TASK_IN_FLIGHT};

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "kebab-case")]
pub enum StorageTaskKind {
    WriteBatch,
    OpenColumnFamily,
    ImportColumnFamily,
    ExportColumnFamily,
    FlushWal,
    FlushMemtables,
    Compaction,
    Shutdown,
    OpenDb,
    BackgroundIterator,
    MultiGet,
}

impl StorageTaskKind {
    pub fn as_static_str(&self) -> &'static str {
        self.into()
    }
}

#[derive(Builder)]
#[builder(pattern = "owned")]
#[builder(name = "StorageTask")]
pub struct ReadyStorageTask<OP> {
    op: OP,
    #[builder(default)]
    pub(crate) priority: Priority,
    /// required
    kind: StorageTaskKind,
    #[builder(setter(skip))]
    #[builder(default = "Instant::now()")]
    created_at: Instant,
}

struct InFlight(Gauge);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.decrement(1);
    }
}

impl<OP, R> ReadyStorageTask<OP>
where
    OP: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    fn run(self) -> R {
        let start = Instant::now();
        let labels = [(OP_TYPE, self.kind.as_static_str())];
        histogram!(STORAGE_BG_TASK_WAIT_DURATION, &labels).record(self.created_at.elapsed());
        let res = (self.op)();
        histogram!(STORAGE_BG_TASK_RUN_DURATION, &labels).record(start.elapsed());
        res
    }
}

impl<OP, R> IoOp for ReadyStorageTask<OP>
where
    OP: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    type Output = R;

    fn priority(&self) -> Priority {
        self.priority
    }

    fn into_task(self) -> impl IoTask<Self::Output> {
        let gauge = gauge!(STORAGE_BG_TASK_IN_FLIGHT,
         PRIORITY => self.priority.as_static_str(),
         OP_TYPE => self.kind.as_static_str(),
        );
        gauge.increment(1);
        // Keep the operation and guard together, releasing captured resources
        // before decrementing even if the prepared task is dropped without running.
        let task = (self, InFlight(gauge));

        move || {
            let (task, _in_flight) = task;
            task.run()
        }
    }

    fn call(self) -> Self::Output {
        (self.op)()
    }
}

#[derive(Clone)]
pub(crate) struct SinglePoolExecutor {
    pool: threadpool::ThreadPool,
}

impl SinglePoolExecutor {
    pub fn new(name: String, num_threads: usize) -> Self {
        let pool = threadpool::Builder::new()
            .thread_name(name)
            .num_threads(num_threads)
            .build();

        Self { pool }
    }
}

impl IoExecutor for SinglePoolExecutor {
    fn dispatch_fn(&self, _priority: Priority, task: impl IoTask<()>) -> Result<(), ShutdownError> {
        let span = tracing::Span::current();
        self.pool.execute(move || {
            span.in_scope(task);
        });
        Ok(())
    }
}

#[derive(Clone)]
pub struct GlobalExecutor(Arc<GlobalExecutorInner>);

impl GlobalExecutor {
    /// Create the shared priority pools using the configured storage thread counts.
    pub fn from_options(opts: &restate_types::config::CommonOptions) -> Self {
        Self::new(
            threadpool::Builder::new()
                .thread_name("rs:io-hi".to_owned())
                .num_threads(opts.storage_high_priority_bg_threads().into())
                .build(),
            threadpool::Builder::new()
                .thread_name("rs:io-lo".to_owned())
                .num_threads(opts.storage_low_priority_bg_threads().into())
                .build(),
        )
    }

    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn max_count(&self, priority: Priority) -> usize {
        match priority {
            Priority::High => self.0.high_pri_pool.max_count(),
            Priority::Low => self.0.low_pri_pool.max_count(),
        }
    }

    pub fn new(
        high_pri_pool: threadpool::ThreadPool,
        low_pri_pool: threadpool::ThreadPool,
    ) -> Self {
        Self(Arc::new(GlobalExecutorInner {
            shutting_down: Mutex::new(false),
            high_pri_pool,
            low_pri_pool,
        }))
    }

    /// Marks the executor as closed for new work. The executor will continue
    /// driving enqueued work.
    pub fn close(&self) {
        *self.0.shutting_down.lock() = true;
    }
}

struct GlobalExecutorInner {
    shutting_down: Mutex<bool>,
    high_pri_pool: threadpool::ThreadPool,
    low_pri_pool: threadpool::ThreadPool,
}

impl IoExecutor for GlobalExecutor {
    fn dispatch_fn(&self, priority: Priority, task: impl IoTask<()>) -> Result<(), ShutdownError> {
        let span = tracing::Span::current();
        // Serialize acceptance with close so accepted work is enqueued before
        // close returns. Rejected tasks are dropped after releasing the lock.
        let shutting_down = self.0.shutting_down.lock();
        if *shutting_down {
            drop(shutting_down);
            return Err(ShutdownError);
        }
        let pool = match priority {
            Priority::High => &self.0.high_pri_pool,
            Priority::Low => &self.0.low_pri_pool,
        };
        pool.execute(move || span.in_scope(task));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use restate_core::io::{FlightTracker, TrackedExecutor};

    use super::*;

    #[tokio::test]
    async fn global_pool_routes_priority_and_rejects_after_close() {
        let backend = GlobalExecutor::new(
            threadpool::Builder::new()
                .num_threads(1)
                .thread_name("test-high".to_owned())
                .build(),
            threadpool::Builder::new()
                .num_threads(1)
                .thread_name("test-low".to_owned())
                .build(),
        );
        let flights = Arc::new(FlightTracker::default());
        let executor = TrackedExecutor::new(&backend, &flights);
        let high = executor
            .spawn(
                StorageTask::default()
                    .kind(StorageTaskKind::MultiGet)
                    .priority(Priority::High)
                    .op(|| std::thread::current().name().unwrap().to_owned())
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let low = executor
            .spawn_fn(Priority::Low, || {
                std::thread::current().name().unwrap().to_owned()
            })
            .unwrap();
        backend.close();
        backend.close();

        assert_eq!(high.await.unwrap(), "test-high");
        assert_eq!(low.await.unwrap(), "test-low");
        for priority in [Priority::High, Priority::Low] {
            assert!(
                executor
                    .dispatch_fn(priority, || panic!("rejected task must not run"))
                    .is_err()
            );
        }
        flights.seal();
        flights.drain().await;
    }
}
