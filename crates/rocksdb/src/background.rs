// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Barrier};
use std::time::Instant;

use derive_builder::Builder;
use metrics::{Gauge, gauge, histogram};
use parking_lot::Mutex;

use restate_core::io::{AdmissionClosed, Gate, IoExecutor, IoOp, IoTask, TrackedTask};

use crate::metric_definitions::{STORAGE_BG_TASK_RUN_DURATION, STORAGE_BG_TASK_WAIT_DURATION};
use crate::{OP_TYPE, PRIORITY, Priority, STORAGE_BG_TASK_IN_FLIGHT};

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "kebab-case")]
pub enum StorageTaskKind {
    WriteBatch,
    OpenColumnFamily,
    ImportColumnFamily,
    DropColumnFamily,
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

/// An operation prepared with [`RocksDb::prepare_op`](crate::RocksDb::prepare_op)
/// or [`RocksDb::prepare_op_in`](crate::RocksDb::prepare_op_in) that hasn't
/// run yet. Run it on the current thread with [`Self::call`], or turn it into a
/// background task with [`Self::background`]. Nothing is admitted until then.
#[must_use = "execute, schedule, or explicitly drop the operation"]
pub struct StorageOp<'a, OP> {
    op: OP,
    gate: &'a Arc<Gate>,
}

impl<'a, OP, R> StorageOp<'a, OP>
where
    OP: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    pub(crate) fn new(op: OP, gate: &'a Arc<Gate>) -> Self {
        Self { op, gate }
    }

    /// Calls `make_guard` on the thread that runs the operation, right before it
    /// runs, and drops the guard right after, also if the operation panics. Use it
    /// for setup that must happen on that thread, like RocksDB perf counters. The
    /// guard doesn't need to be `Send`. If the operation is rejected or dropped
    /// without running, `make_guard` is never called. Calling `with_guard` again
    /// wraps the operation once more.
    ///
    /// # Examples
    ///
    /// Opt into RocksDB read metrics. Construct the guard inside the factory,
    /// rather than capturing an existing guard: RocksDB perf guards are thread-local.
    /// Use `RocksDbWritePerfGuard` instead for write operations.
    ///
    /// ```no_run
    /// use restate_rocksdb::{RocksDb, RocksDbReadPerfGuard};
    /// use std::sync::Arc;
    ///
    /// # fn example(db: &Arc<RocksDb>) -> Result<(), Box<dyn std::error::Error>> {
    /// let value = db
    ///     .prepare_op(|db| db.inner().as_raw_db().get(b"key"))
    ///     .with_guard(|| RocksDbReadPerfGuard::new("lookup"))
    ///     .call()??;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Inject latency before background execution. The factory can return `()`
    /// when no cleanup is needed. The delay happens on the worker, not at submission.
    ///
    /// ```no_run
    /// use restate_core::io::IoExecutor;
    /// use restate_rocksdb::{RocksDb, StorageTaskKind};
    /// use std::{sync::Arc, time::Duration};
    ///
    /// # fn example(db: &Arc<RocksDb>, executor: &impl IoExecutor) -> Result<(), Box<dyn std::error::Error>> {
    /// let task = db
    ///     .prepare_op(|db| db.inner().as_raw_db().get(b"key"))
    ///     .with_guard(|| std::thread::sleep(Duration::from_millis(50)))
    ///     .background("delayed-lookup", StorageTaskKind::MultiGet);
    /// let result = executor.spawn(task)?;
    /// // Keep `result` alive and await it; dropping it can skip queued work.
    /// // Use `executor.dispatch(task)` instead for fire-and-forget execution.
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Scope a test-only thread-local fault flag to execution, restoring its
    /// previous value even if the operation unwinds. The operation (or a helper
    /// it calls) can consult the flag to decide whether to inject a failure.
    ///
    /// ```no_run
    /// use restate_rocksdb::RocksDb;
    /// use std::{cell::Cell, sync::Arc};
    ///
    /// thread_local! {
    ///     static INJECT_FAILURE: Cell<bool> = const { Cell::new(false) };
    /// }
    /// struct RestoreFaultFlag(bool);
    /// impl Drop for RestoreFaultFlag {
    ///     fn drop(&mut self) {
    ///         INJECT_FAILURE.set(self.0);
    ///     }
    /// }
    ///
    /// # fn example(db: &Arc<RocksDb>) -> Result<(), Box<dyn std::error::Error>> {
    /// db.prepare_op(|_db| assert!(INJECT_FAILURE.get()))
    ///     .with_guard(|| RestoreFaultFlag(INJECT_FAILURE.replace(true)))
    ///     .call()?;
    /// assert!(!INJECT_FAILURE.get());
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_guard<G>(
        self,
        make_guard: impl FnOnce() -> G + Send + 'static,
    ) -> StorageOp<'a, impl FnOnce() -> R + Send + 'static> {
        let Self { op, gate } = self;
        StorageOp::new(
            move || {
                let _guard = make_guard();
                op()
            },
            gate,
        )
    }

    /// Runs the operation now, on the current thread. Fails with [`AdmissionClosed`]
    /// if the database (or scope) is sealed. Unlike background tasks, it records no
    /// task metrics or tracing span.
    pub fn call(self) -> Result<R, AdmissionClosed> {
        let lease = self.gate.try_acquire().ok_or(AdmissionClosed)?;
        Ok(TrackedTask::new(self.op, lease).run())
    }

    /// Turns this into a background task with a name and kind, used for metrics and
    /// tracing. Admission is still checked only when the task is submitted.
    pub fn background(self, name: &'static str, kind: StorageTaskKind) -> ReadyStorageTask<'a, OP> {
        StorageTask::default()
            .op(self.op)
            .gate(self.gate)
            .name(name)
            .kind(kind)
            .build()
            .unwrap()
    }
}

/// A storage operation ready for an executor, with a kind and priority for
/// metrics and scheduling. If it belongs to a gate, it's admitted only when
/// it runs (`call`) or is submitted (`into_task`), so building one doesn't delay
/// draining.
#[derive(Builder)]
#[builder(pattern = "owned")]
#[builder(name = "StorageTask")]
#[must_use = "dispatch, execute, or explicitly drop the prepared storage operation"]
pub struct ReadyStorageTask<'a, OP> {
    op: OP,
    #[builder(default, setter(strip_option))]
    gate: Option<&'a Arc<Gate>>,
    #[builder(default, setter(strip_option))]
    name: Option<&'static str>,
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

impl<OP, R> ReadyStorageTask<'_, OP>
where
    OP: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    /// Override the scheduling hint before preparing or dispatching the task.
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }
}

impl<OP, R> IoOp for ReadyStorageTask<'_, OP>
where
    OP: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    type Output = R;

    fn priority(&self) -> Priority {
        self.priority
    }

    fn into_task(self) -> Result<impl IoTask<Self::Output>, AdmissionClosed> {
        let lease = self
            .gate
            .map(|gate| {
                gate.try_acquire()
                    .map(|lease| lease.into_owned())
                    .ok_or(AdmissionClosed)
            })
            .transpose()?;
        let gauge = gauge!(STORAGE_BG_TASK_IN_FLIGHT,
         PRIORITY => self.priority.as_static_str(),
         OP_TYPE => self.kind.as_static_str(),
        );
        gauge.increment(1);
        // Bundle the operation with the in-flight gauge and the lease, so what it
        // captured is dropped before the gauge goes down and the lease is released,
        // even if the task is dropped without running.
        let Self {
            op,
            name,
            kind,
            created_at,
            ..
        } = self;
        let task = TrackedTask::new(
            move || {
                let _span = tracing::debug_span!("storage-task", name).entered();
                let start = Instant::now();
                let labels = [(OP_TYPE, kind.as_static_str())];
                histogram!(STORAGE_BG_TASK_WAIT_DURATION, &labels).record(created_at.elapsed());
                let res = op();
                histogram!(STORAGE_BG_TASK_RUN_DURATION, &labels).record(start.elapsed());
                res
            },
            (InFlight(gauge), lease),
        );
        Ok(move || task.run())
    }

    fn call(self) -> Result<Self::Output, AdmissionClosed> {
        let lease = self
            .gate
            .map(|gate| gate.try_acquire().ok_or(AdmissionClosed))
            .transpose()?;
        Ok(TrackedTask::new(self.op, lease).run())
    }
}

/// A single thread pool with its own threads, for work that must not wait behind
/// regular storage I/O. For example, the manager's `rs:io-lifecycle` pool opens
/// and closes databases. Clones share the same threads.
#[derive(Clone)]
pub struct SinglePoolExecutor(Arc<SinglePoolExecutorInner>);

struct SinglePoolExecutorInner {
    pool: threadpool::ThreadPool,
    barrier_submission: Mutex<()>,
}

impl SinglePoolExecutor {
    pub fn new(name: String, num_threads: usize) -> Self {
        let pool = threadpool::Builder::new()
            .thread_name(name)
            .num_threads(num_threads)
            .build();

        Self(Arc::new(SinglePoolExecutorInner {
            pool,
            barrier_submission: Mutex::new(()),
        }))
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn max_count(&self) -> usize {
        self.0.pool.max_count()
    }
}

impl IoExecutor for SinglePoolExecutor {
    fn barrier(&self, callback: impl FnOnce() + Send + 'static) {
        let _submission = self.0.barrier_submission.lock();
        pool_barrier(&[&self.0.pool], callback);
    }

    fn dispatch_fn(
        &self,
        _priority: Priority,
        task: impl IoTask<()>,
    ) -> Result<(), AdmissionClosed> {
        let span = tracing::Span::current();
        self.0.pool.execute(move || {
            span.in_scope(task);
        });
        Ok(())
    }
}

/// Two thread pools for regular storage I/O: one for [`Priority::High`] work and
/// one for [`Priority::Low`] work. [`Self::from_options`] creates the standard
/// `rs:io-hi` and `rs:io-lo` pools. Clones share the same threads, so several
/// managers can use one set of pools.
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
            barrier_submission: Mutex::new(()),
            high_pri_pool,
            low_pri_pool,
        }))
    }
}

struct GlobalExecutorInner {
    // Only barriers take this lock; regular dispatch only uses the pool's queue.
    barrier_submission: Mutex<()>,
    high_pri_pool: threadpool::ThreadPool,
    low_pri_pool: threadpool::ThreadPool,
}

impl IoExecutor for GlobalExecutor {
    fn barrier(&self, callback: impl FnOnce() + Send + 'static) {
        let _submission = self.0.barrier_submission.lock();
        pool_barrier(&[&self.0.high_pri_pool, &self.0.low_pri_pool], callback);
    }

    fn dispatch_fn(
        &self,
        priority: Priority,
        task: impl IoTask<()>,
    ) -> Result<(), AdmissionClosed> {
        let span = tracing::Span::current();
        let pool = match priority {
            Priority::High => &self.0.high_pri_pool,
            Priority::Low => &self.0.low_pri_pool,
        };
        pool.execute(move || span.in_scope(task));
        Ok(())
    }
}

fn pool_barrier(pools: &[&threadpool::ThreadPool], callback: impl FnOnce() + Send + 'static) {
    struct Canary<F> {
        workers: Barrier,
        callback: Mutex<Option<F>>,
    }

    // A barrier queues one canary task per worker, and each canary blocks until all
    // of them are running, which means all earlier work is done. If two barriers'
    // canaries were interleaved in the queue, every worker could end up blocked in
    // a half-complete barrier while its remaining canaries wait behind them. The
    // caller holds the submission lock while we queue the whole batch.
    let workers = pools.iter().map(|pool| pool.max_count()).sum();
    let canary = Arc::new(Canary {
        workers: Barrier::new(workers),
        callback: Mutex::new(Some(callback)),
    });
    for pool in pools {
        for _ in 0..pool.max_count() {
            let canary = canary.clone();
            pool.execute(move || {
                if canary.workers.wait().is_leader() {
                    let callback = canary.callback.lock().take().expect("one leader");
                    callback();
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::oneshot;

    use restate_core::io::{Gate, TrackedExecutor};

    use super::*;

    fn observe_barrier(executor: &impl IoExecutor) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        executor.barrier(move || {
            let _ = tx.send(());
        });
        rx
    }

    #[test]
    fn guards_are_execution_local_and_optional() {
        struct Guard {
            _local: Rc<()>,
            thread: std::thread::ThreadId,
            gate: Arc<Gate>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Guard {
            fn drop(&mut self) {
                assert_eq!(self.thread, std::thread::current().id());
                assert!(!self.gate.is_idle());
                self.drops.fetch_add(1, Ordering::Relaxed);
            }
        }

        let gate = Arc::new(Gate::default());
        let drops = Arc::new(AtomicUsize::new(0));
        let factory = {
            let gate = gate.clone();
            let drops = drops.clone();
            move || Guard {
                _local: Rc::new(()),
                thread: std::thread::current().id(),
                gate,
                drops,
            }
        };
        let op = StorageOp::new(|| 42, &gate).with_guard(factory.clone());
        assert!(gate.is_idle());
        assert_eq!(op.call().unwrap(), 42);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert!(gate.is_idle());

        let task = StorageOp::new(|| 43, &gate)
            .with_guard(factory.clone())
            .background("guarded", StorageTaskKind::MultiGet);
        assert_eq!(task.priority(), Priority::Low);
        assert!(gate.is_idle());
        let task = task.into_task().unwrap();
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(std::thread::spawn(task).join().unwrap(), 43);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        assert!(gate.is_idle());

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            StorageOp::new(|| panic!("operation panic"), &gate)
                .with_guard(factory)
                .call()
        }));
        assert!(result.is_err());
        assert_eq!(drops.load(Ordering::Relaxed), 3);
        assert!(gate.is_idle());

        let task = StorageOp::new(|| (), &gate)
            .with_guard(|| panic!("discard must not create a guard"))
            .background("discarded", StorageTaskKind::MultiGet)
            .into_task()
            .unwrap();
        drop(task);
        assert!(gate.is_idle());
        gate.seal();
        assert_eq!(
            StorageOp::new(|| (), &gate)
                .with_guard(|| panic!("rejection must not create a guard"))
                .call(),
            Err(AdmissionClosed)
        );
    }

    #[tokio::test]
    async fn global_pool_routes_priority_and_outlives_owner_admission() {
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
        let gate = Arc::new(Gate::default());
        let executor = TrackedExecutor::new(&backend, &gate);
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
        gate.seal();

        assert_eq!(high.await.unwrap(), "test-high");
        assert_eq!(low.await.unwrap(), "test-low");
        for priority in [Priority::High, Priority::Low] {
            assert!(
                executor
                    .dispatch_fn(priority, || panic!("rejected task must not run"))
                    .is_err()
            );
        }
        gate.drain().await;
        observe_barrier(&backend).await.unwrap();
        assert_eq!(
            backend
                .spawn_fn(Priority::Low, || 42)
                .unwrap()
                .await
                .unwrap(),
            42
        );
    }

    #[tokio::test]
    async fn global_barrier_covers_both_lanes_and_recovers_from_callback_panic() {
        use std::sync::mpsc;
        use std::time::Duration;

        let backend = GlobalExecutor::new(
            threadpool::ThreadPool::new(2),
            threadpool::ThreadPool::new(2),
        );
        let mut releases = Vec::new();
        for priority in [Priority::High, Priority::Low] {
            let (release, wait) = mpsc::channel::<()>();
            let (started, reached) = oneshot::channel();
            backend
                .dispatch_fn(priority, move || {
                    let _ = started.send(());
                    let _ = wait.recv();
                })
                .unwrap();
            reached.await.unwrap();
            releases.push(release);
        }
        let gate = Arc::new(Gate::default());
        gate.seal();
        let executor = TrackedExecutor::new(&backend, &gate);
        let mut completed = observe_barrier(&executor);
        assert!(matches!(
            completed.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        drop(releases.pop());
        assert!(matches!(
            completed.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        drop(releases);
        tokio::time::timeout(Duration::from_secs(10), completed)
            .await
            .unwrap()
            .unwrap();

        let (tx, rx) = oneshot::channel::<()>();
        executor.barrier(move || {
            let _tx = tx;
            panic!("callback panic must not strand the pool");
        });
        assert!(
            tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .unwrap()
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(10), observe_barrier(&executor))
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn worker_barriers_wait_for_all_workers_and_survive_cancelled_observers() {
        use std::sync::mpsc;
        use std::task::{Context, Poll, Waker};
        use std::time::Duration;

        let executor = SinglePoolExecutor::new("barrier-test".to_owned(), 3);
        let (started, reached) = oneshot::channel();
        let (resume, paused) = mpsc::channel::<()>();
        executor
            .dispatch_fn(Priority::Low, move || {
                started.send(()).unwrap();
                let _ = paused.recv();
            })
            .unwrap();
        reached.await.unwrap();

        let mut barrier = Box::pin(observe_barrier(&executor));
        tokio::time::timeout(Duration::from_secs(10), async {
            while executor.0.pool.active_count() != 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the other workers reached their canaries");
        assert!(matches!(
            barrier
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        // Dropping the barrier's receiver must not leave the workers already waiting in
        // it stuck.
        drop(barrier);
        let barriers = std::thread::scope(|scope| {
            let submissions: Vec<_> = (0..8)
                .map(|_| {
                    let executor = &executor;
                    scope.spawn(move || observe_barrier(executor))
                })
                .collect();
            submissions
                .into_iter()
                .map(|submission| submission.join().unwrap())
                .collect::<Vec<_>>()
        });
        drop(resume);
        tokio::time::timeout(Duration::from_secs(10), async {
            for barrier in barriers {
                barrier.await.unwrap();
            }
            assert_eq!(
                executor
                    .spawn_fn(Priority::Low, || 42)
                    .unwrap()
                    .await
                    .unwrap(),
                42
            );
        })
        .await
        .unwrap();
    }
}
