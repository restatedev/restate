// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use metrics::{Gauge, gauge, histogram};
use parking_lot::Mutex;

use restate_core::io::{AdmissionClosed, Gate, IoExecutor, IoOp, IoTask, TrackedTask};

use crate::metric_definitions::{STORAGE_BG_TASK_RUN_DURATION, STORAGE_BG_TASK_WAIT_DURATION};
use crate::{EXECUTOR, OP_TYPE, STORAGE_BG_TASK_IN_FLIGHT};

/// Which of [`GlobalExecutor`]'s pools runs a piece of work. `High` is for
/// latency-sensitive work.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "kebab-case")]
pub enum Priority {
    High,
    #[default]
    Low,
}

impl Priority {
    pub fn as_static_str(&self) -> &'static str {
        self.into()
    }
}

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
    CloseDb,
    BackgroundIterator,
    MultiGet,
}

impl StorageTaskKind {
    pub fn as_static_str(&self) -> &'static str {
        self.into()
    }
}

/// What a prepared [`StorageOp`] works on, such as the database or a scope.
///
/// A context stays borrowed for as long as it can: while the operation is
/// prepared, and while it runs on the calling thread with [`StorageOp::call`].
/// Nothing is cloned for that. Only when the operation becomes a task for a
/// background thread does [`Self::into_owned`] turn the context into something
/// the task can own, because a borrow can't move to another thread.
///
/// | Context | Prepared with | What a background task owns | What the work gets |
/// |---|---|---|---|
/// | `&C`, for a shared handle `C` | [`RocksDb::prepare`](crate::RocksDb::prepare) (the database), [`StorageOp::with_context`] | a clone of `C` | `&C` |
/// | a [`ReadIterator`](crate::reads::ReadIterator) | [`ReadIterator::prepare`](crate::reads::ReadIterator::prepare) | the same iterator, made owned, at the same position | `&mut ReadIterator<'_, _>` |
/// | `()` | operations that need nothing, such as deleting a column family | nothing | nothing |
///
/// A shared handle is a type such as `Arc<RocksDb>`, a `Scope`, or a
/// `ReadSnapshot`: its clones are cheap and point to the same thing. See
/// [`StorageOp`] for an example.
pub trait OpContext {
    /// What a background task holds instead of the borrowed context.
    type Owned: Send + 'static;

    /// Turns the context into what a background task holds.
    fn into_owned(self) -> Self::Owned;
}

/// The work of a [`StorageOp`].
///
/// The work gets the operation's context: borrowed when the operation runs on
/// the calling thread ([`Self::run`]), or owned when it runs on a background
/// thread ([`Self::run_owned`]). It sees the same thing either way, so one
/// closure serves both.
///
/// There is no need to implement it: pass a closure. Closures implement it for
/// every kind of context, and take what the table in [`OpContext`] lists:
/// `FnOnce(&C)` for a shared handle, `FnOnce(&mut ReadIterator<'_, H>)` for an
/// iterator, and `FnOnce()` for no context.
pub trait OpFn<C: OpContext>: Send + 'static {
    /// What the work returns.
    type Output;

    /// Runs the work with the borrowed context, on the calling thread.
    fn run(self, context: C) -> Self::Output;

    /// Runs the work with the owned context, on a background thread.
    fn run_owned(self, context: C::Owned) -> Self::Output;
}

impl<C: Clone + Send + Sync + 'static> OpContext for &C {
    type Owned = C;

    fn into_owned(self) -> C {
        self.clone()
    }
}

impl<'a, C, F, R> OpFn<&'a C> for F
where
    C: Clone + Send + Sync + 'static,
    F: FnOnce(&C) -> R + Send + 'static,
{
    type Output = R;

    fn run(self, context: &'a C) -> R {
        self(context)
    }

    fn run_owned(self, context: C) -> R {
        self(&context)
    }
}

impl OpContext for () {
    type Owned = ();

    fn into_owned(self) {}
}

impl<F, R> OpFn<()> for F
where
    F: FnOnce() -> R + Send + 'static,
{
    type Output = R;

    fn run(self, (): ()) -> R {
        self()
    }

    fn run_owned(self, (): ()) -> R {
        self()
    }
}

/// An operation on the database that has been prepared but hasn't run yet,
/// for example with [`RocksDb::prepare`](crate::RocksDb::prepare).
///
/// Run it on the current thread with [`Self::call`], or submit it to an
/// executor, which runs it on a background thread:
///
/// ```no_run
/// use std::sync::Arc;
///
/// use restate_core::io::IoExecutor;
/// use restate_rocksdb::{RocksDb, StorageTaskKind};
///
/// # async fn example(db: &Arc<RocksDb>) -> Result<(), Box<dyn std::error::Error>> {
/// // On this thread. The operation borrows `db`, so nothing is cloned.
/// let value = db
///     .prepare(StorageTaskKind::MultiGet, |db| db.inner().as_raw_db().get(b"key"))
///     .call()??;
///
/// // On a background thread. Submitting the operation clones the `Arc` once,
/// // for the task to own.
/// let lookup = db.prepare(StorageTaskKind::MultiGet, |db| db.inner().as_raw_db().get(b"key"));
/// let value = db.executor().low_pri().spawn(lookup)?.await??;
/// # Ok(())
/// # }
/// ```
///
/// Nothing is admitted until the operation runs or is submitted: that is when
/// it takes its lease from its gate. So a prepared operation doesn't hold up
/// draining, and if the gate is sealed first, it fails with
/// [`AdmissionClosed`]. The exception is
/// [`RocksDb::prepare_drop_cf`](crate::RocksDb::prepare_drop_cf), which takes no
/// lease, so that a column family can still be deleted after the database is
/// sealed.
///
/// `C` is the operation's context, what it works on, such as the database. It
/// stays borrowed until the operation moves to a background thread (see
/// [`OpContext`]). `F` is the work itself (see [`OpFn`]).
///
/// Its kind says what the operation does. When it runs in the background, the
/// kind labels the task's metrics and tracing span.
#[must_use = "execute, schedule, or explicitly drop the operation"]
pub struct StorageOp<'a, C, F> {
    // Borrowed until the operation moves to a background thread.
    context: C,
    op: F,
    // `None` for the few operations that must be accepted even after the
    // database is sealed, such as deleting a column family.
    gate: Option<&'a Arc<Gate>>,
    kind: StorageTaskKind,
}

impl<'a, F> StorageOp<'a, (), F> {
    // An operation without a context, counted in `gate`.
    #[cfg(test)]
    pub(crate) fn new(op: F, gate: &'a Arc<Gate>, kind: StorageTaskKind) -> Self {
        Self {
            context: (),
            op,
            gate: Some(gate),
            kind,
        }
    }
}

impl<F> StorageOp<'static, (), F> {
    /// An operation that takes no lease, so it is accepted even after the
    /// database is sealed. Only for work that must finish regardless, such as
    /// deleting a column family or opening a database that was already
    /// accounted for elsewhere.
    pub(crate) fn without_admission(op: F, kind: StorageTaskKind) -> Self {
        Self {
            context: (),
            op,
            gate: None,
            kind,
        }
    }
}

impl<'a, 'c, C, F> StorageOp<'a, &'c C, F>
where
    C: Clone + Send + Sync + 'static,
{
    /// Prepares an operation on `context`, a shared handle such as a `Scope` or
    /// a `ReadSnapshot`, counted in `gate`. Sealing and draining that gate then
    /// covers the operation.
    ///
    /// `op` gets the context by reference. The context is cloned only if the
    /// operation moves to a background thread (see [`OpContext`]). To keep
    /// reading with an iterator, use
    /// [`ReadIterator::prepare`](crate::reads::ReadIterator::prepare) instead.
    ///
    /// ```no_run
    /// use restate_rocksdb::reads::ColumnFamilies;
    /// use restate_rocksdb::{StorageOp, StorageTaskKind};
    /// use restate_util_lifecycle::Scope;
    ///
    /// # fn example<C: ColumnFamilies>(scope: &Scope<C>) -> Result<(), Box<dyn std::error::Error>> {
    /// // Runs on this thread, so the scope isn't cloned.
    /// let name = StorageOp::with_context(scope, scope.gate(), StorageTaskKind::MultiGet, |scope| {
    ///     scope.name().to_owned()
    /// })
    /// .call()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_context<R>(
        context: &'c C,
        gate: &'a Arc<Gate>,
        kind: StorageTaskKind,
        op: F,
    ) -> Self
    where
        F: FnOnce(&C) -> R + Send + 'static,
    {
        Self::from_parts(context, gate, kind, op)
    }
}

impl<'a, C: OpContext, F: OpFn<C>> StorageOp<'a, C, F> {
    // An operation on any kind of context, counted in `gate`.
    pub(crate) fn from_parts(
        context: C,
        gate: &'a Arc<Gate>,
        kind: StorageTaskKind,
        op: F,
    ) -> Self {
        Self {
            context,
            op,
            gate: Some(gate),
            kind,
        }
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
    /// use restate_rocksdb::{RocksDb, RocksDbReadPerfGuard, StorageTaskKind};
    /// use std::sync::Arc;
    ///
    /// # fn example(db: &Arc<RocksDb>) -> Result<(), Box<dyn std::error::Error>> {
    /// let value = db
    ///     .prepare(StorageTaskKind::MultiGet, |db| db.inner().as_raw_db().get(b"key"))
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
    /// let op = db
    ///     .prepare(StorageTaskKind::MultiGet, |db| db.inner().as_raw_db().get(b"key"))
    ///     .with_guard(|| std::thread::sleep(Duration::from_millis(50)));
    /// let result = executor.spawn(op)?;
    /// // Keep `result` alive and await it; dropping it can skip queued work.
    /// // Use `executor.dispatch(op)` instead for fire-and-forget execution.
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Scope a test-only thread-local fault flag to execution, restoring its
    /// previous value even if the operation unwinds. The operation (or a helper
    /// it calls) can consult the flag to decide whether to inject a failure.
    ///
    /// ```no_run
    /// use restate_rocksdb::{RocksDb, StorageTaskKind};
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
    /// db.prepare(StorageTaskKind::MultiGet, |_db| assert!(INJECT_FAILURE.get()))
    ///     .with_guard(|| RestoreFaultFlag(INJECT_FAILURE.replace(true)))
    ///     .call()?;
    /// assert!(!INJECT_FAILURE.get());
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_guard<G, M>(self, make_guard: M) -> StorageOp<'a, C, Guarded<F, M>>
    where
        M: FnOnce() -> G + Send + 'static,
    {
        let Self {
            context,
            op,
            gate,
            kind,
        } = self;
        StorageOp {
            context,
            op: Guarded { op, make_guard },
            gate,
            kind,
        }
    }

    /// Runs the operation now, on the current thread, with its context still
    /// borrowed. Fails with [`AdmissionClosed`] if the database (or scope) is
    /// sealed. Unlike background tasks, it records no task metrics or tracing
    /// span.
    pub fn call(self) -> Result<F::Output, AdmissionClosed> {
        let lease = self
            .gate
            .map(|gate| gate.try_acquire().ok_or(AdmissionClosed))
            .transpose()?;
        // `run` consumes the operation and its context, so both are dropped
        // before the lease is released.
        let output = self.op.run(self.context);
        drop(lease);
        Ok(output)
    }
}

/// The work of an operation returned by [`StorageOp::with_guard`]: the
/// original work, run while the guard exists.
///
/// It's a named type, rather than an `impl OpFn`, so that async code holding a
/// prepared operation stays `Send`. The compiler can't always prove that for
/// an opaque type that carries the lifetime of a borrowed context.
pub struct Guarded<F, M> {
    op: F,
    make_guard: M,
}

impl<C, F, G, M> OpFn<C> for Guarded<F, M>
where
    C: OpContext,
    F: OpFn<C>,
    M: FnOnce() -> G + Send + 'static,
{
    type Output = F::Output;

    fn run(self, context: C) -> F::Output {
        let _guard = (self.make_guard)();
        self.op.run(context)
    }

    fn run_owned(self, context: C::Owned) -> F::Output {
        let _guard = (self.make_guard)();
        self.op.run_owned(context)
    }
}

impl<C, F> IoOp for StorageOp<'_, C, F>
where
    C: OpContext,
    F: OpFn<C>,
    F::Output: Send + 'static,
{
    type Output = F::Output;

    fn operation(&self) -> &'static str {
        self.kind.as_static_str()
    }

    fn call(self) -> Result<F::Output, AdmissionClosed> {
        StorageOp::call(self)
    }

    fn into_task(self) -> Result<impl IoTask<F::Output>, AdmissionClosed> {
        let lease = self
            .gate
            .map(|gate| {
                gate.try_acquire()
                    .map(|lease| lease.into_owned())
                    .ok_or(AdmissionClosed)
            })
            .transpose()?;
        // Only now, and only once admitted, does the context become owned: the
        // task moves to another thread, where the borrow can't follow it.
        let context = self.context.into_owned();
        let op = self.op;
        // The lease is released after the operation and its context are dropped,
        // even if the task is dropped without running.
        Ok(TrackedTask::new(move || op.run_owned(context), lease))
    }
}

/// A thread pool for storage work. For example, the manager's `rs:io-lifecycle`
/// pool opens and closes databases, and [`GlobalExecutor`] is made of two of
/// them. Clones share the same threads.
///
/// It measures every task it runs: how many are submitted but not finished, how
/// long each waited from submission until it started, and how long it ran. The
/// metrics are labeled with the pool's name and the task's operation.
#[derive(Clone)]
pub struct SinglePoolExecutor(Arc<SinglePoolExecutorInner>);

struct SinglePoolExecutorInner {
    name: &'static str,
    pool: threadpool::ThreadPool,
    barrier_submission: Mutex<()>,
}

impl SinglePoolExecutor {
    /// Starts a pool of `num_threads` threads. `name` names the threads, and
    /// labels the metrics of the tasks that run on them.
    pub fn new(name: &'static str, num_threads: usize) -> Self {
        let pool = threadpool::Builder::new()
            .thread_name(name.to_owned())
            .num_threads(num_threads)
            .build();

        Self(Arc::new(SinglePoolExecutorInner {
            name,
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
        pool_barrier(&self.0.pool, callback);
    }

    fn dispatch_fn(
        &self,
        operation: &'static str,
        task: impl IoTask<()>,
    ) -> Result<(), AdmissionClosed> {
        let labels = [(EXECUTOR, self.0.name), (OP_TYPE, operation)];
        let in_flight = InFlight::new(gauge!(STORAGE_BG_TASK_IN_FLIGHT, &labels));
        let submitted_at = Instant::now();
        // The gauge goes down after what the task captured is dropped, whether the
        // task runs, is skipped, or is dropped from the queue.
        let task = TrackedTask::new(task, in_flight);
        let span = tracing::Span::current();
        self.0.pool.execute(move || {
            // A task that is no longer wanted is dropped without being measured:
            // it didn't run.
            if !task.is_wanted() {
                return;
            }
            // Run inside the submitter's span, which tells where the work came from.
            span.in_scope(|| {
                histogram!(STORAGE_BG_TASK_WAIT_DURATION, &labels).record(submitted_at.elapsed());
                let _span = tracing::debug_span!("storage-task", operation).entered();
                let started_at = Instant::now();
                task.run();
                histogram!(STORAGE_BG_TASK_RUN_DURATION, &labels).record(started_at.elapsed());
            });
        });
        Ok(())
    }
}

// Counts a task as in flight from creation until it is dropped.
struct InFlight(Gauge);

impl InFlight {
    fn new(gauge: Gauge) -> Self {
        gauge.increment(1);
        Self(gauge)
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.decrement(1);
    }
}

/// The two pools for regular storage I/O: one for latency-sensitive work
/// ([`Self::high_pri`]) and one for everything else ([`Self::low_pri`]).
/// [`Self::from_options`] creates the standard `rs:io-hi` and `rs:io-lo` pools.
/// Clones share the same threads, so several managers can use one set of pools.
///
/// It isn't an executor itself: work goes to one of its pools, which the caller
/// picks.
#[derive(Clone)]
pub struct GlobalExecutor {
    high_pri: SinglePoolExecutor,
    low_pri: SinglePoolExecutor,
}

impl GlobalExecutor {
    /// Creates the two pools with the configured numbers of threads.
    pub fn from_options(opts: &restate_types::config::CommonOptions) -> Self {
        Self::new(
            SinglePoolExecutor::new("rs:io-hi", opts.storage_high_priority_bg_threads().into()),
            SinglePoolExecutor::new("rs:io-lo", opts.storage_low_priority_bg_threads().into()),
        )
    }

    pub fn new(high_pri: SinglePoolExecutor, low_pri: SinglePoolExecutor) -> Self {
        Self { high_pri, low_pri }
    }

    /// The pool for latency-sensitive work.
    pub fn high_pri(&self) -> &SinglePoolExecutor {
        &self.high_pri
    }

    /// The pool for everything else.
    pub fn low_pri(&self) -> &SinglePoolExecutor {
        &self.low_pri
    }

    /// The pool for work of `priority`.
    pub fn pool(&self, priority: Priority) -> &SinglePoolExecutor {
        match priority {
            Priority::High => &self.high_pri,
            Priority::Low => &self.low_pri,
        }
    }

    /// Runs `callback` once all work submitted to either pool before this call
    /// has run or been dropped. See [`IoExecutor::barrier`].
    pub fn barrier(&self, callback: impl FnOnce() + Send + 'static) {
        // Each pool gets its own barrier. Whichever passes second runs `callback`.
        let pending = Arc::new(AtomicUsize::new(2));
        let callback = Arc::new(Mutex::new(Some(callback)));
        for pool in [&self.high_pri, &self.low_pri] {
            let pending = Arc::clone(&pending);
            let callback = Arc::clone(&callback);
            pool.barrier(move || {
                if pending.fetch_sub(1, Ordering::AcqRel) == 1 {
                    let callback = callback.lock().take().expect("runs once");
                    callback();
                }
            });
        }
    }
}

fn pool_barrier(pool: &threadpool::ThreadPool, callback: impl FnOnce() + Send + 'static) {
    struct Canary<F> {
        workers: Barrier,
        callback: Mutex<Option<F>>,
    }

    // A barrier queues one canary task per worker, and each canary blocks until all
    // of them are running, which means all earlier work is done. If two barriers'
    // canaries were interleaved in the queue, every worker could end up blocked in
    // a half-complete barrier while its remaining canaries wait behind them. The
    // caller holds the submission lock while we queue the whole batch.
    let workers = pool.max_count();
    let canary = Arc::new(Canary {
        workers: Barrier::new(workers),
        callback: Mutex::new(Some(callback)),
    });
    for _ in 0..workers {
        let canary = canary.clone();
        pool.execute(move || {
            if canary.workers.wait().is_leader() {
                let callback = canary.callback.lock().take().expect("one leader");
                callback();
            }
        });
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

    fn observe_global_barrier(executor: &GlobalExecutor) -> oneshot::Receiver<()> {
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
        let op =
            StorageOp::new(|| 42, &gate, StorageTaskKind::MultiGet).with_guard(factory.clone());
        assert!(gate.is_idle());
        assert_eq!(op.call().unwrap(), 42);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert!(gate.is_idle());

        let task =
            StorageOp::new(|| 43, &gate, StorageTaskKind::MultiGet).with_guard(factory.clone());
        assert!(gate.is_idle());
        let task = task.into_task().unwrap();
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(std::thread::spawn(move || task.run()).join().unwrap(), 43);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        assert!(gate.is_idle());

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            StorageOp::new(
                || panic!("operation panic"),
                &gate,
                StorageTaskKind::MultiGet,
            )
            .with_guard(factory)
            .call()
        }));
        assert!(result.is_err());
        assert_eq!(drops.load(Ordering::Relaxed), 3);
        assert!(gate.is_idle());

        let task = StorageOp::new(|| (), &gate, StorageTaskKind::MultiGet)
            .with_guard(|| panic!("discard must not create a guard"))
            .into_task()
            .unwrap();
        drop(task);
        assert!(gate.is_idle());
        gate.seal();
        assert_eq!(
            StorageOp::new(|| (), &gate, StorageTaskKind::MultiGet)
                .with_guard(|| panic!("rejection must not create a guard"))
                .call(),
            Err(AdmissionClosed)
        );
    }

    #[tokio::test]
    async fn global_executor_routes_work_to_its_pools_and_outlives_owner_admission() {
        let backend = GlobalExecutor::new(
            SinglePoolExecutor::new("test-high", 1),
            SinglePoolExecutor::new("test-low", 1),
        );
        let gate = Arc::new(Gate::default());
        let high_pri = TrackedExecutor::new(backend.high_pri(), &gate);
        let low_pri = TrackedExecutor::new(backend.pool(Priority::Low), &gate);
        let high = high_pri
            .spawn(StorageOp::without_admission(
                || std::thread::current().name().unwrap().to_owned(),
                StorageTaskKind::MultiGet,
            ))
            .unwrap();
        let low = low_pri
            .spawn_fn("test", || std::thread::current().name().unwrap().to_owned())
            .unwrap();
        gate.seal();

        assert_eq!(high.await.unwrap(), "test-high");
        assert_eq!(low.await.unwrap(), "test-low");
        for executor in [&high_pri, &low_pri] {
            assert!(
                executor
                    .dispatch_fn("test", || panic!("rejected task must not run"))
                    .is_err()
            );
        }
        gate.drain().await;
        observe_global_barrier(&backend).await.unwrap();
        assert_eq!(
            backend
                .low_pri()
                .spawn_fn("test", || 42)
                .unwrap()
                .await
                .unwrap(),
            42
        );
    }

    #[tokio::test]
    async fn global_barrier_covers_both_pools_and_recovers_from_callback_panic() {
        use std::sync::mpsc;
        use std::time::Duration;

        let backend = GlobalExecutor::new(
            SinglePoolExecutor::new("test-high", 2),
            SinglePoolExecutor::new("test-low", 2),
        );
        let mut releases = Vec::new();
        for priority in [Priority::High, Priority::Low] {
            let (release, wait) = mpsc::channel::<()>();
            let (started, reached) = oneshot::channel();
            backend
                .pool(priority)
                .dispatch_fn("test", move || {
                    let _ = started.send(());
                    let _ = wait.recv();
                })
                .unwrap();
            reached.await.unwrap();
            releases.push(release);
        }
        // The barrier passes only once the work in both pools is done.
        let mut completed = observe_global_barrier(&backend);
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
        backend.barrier(move || {
            let _tx = tx;
            panic!("callback panic must not strand the pools");
        });
        assert!(
            tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .unwrap()
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(10), observe_global_barrier(&backend))
            .await
            .unwrap()
            .unwrap();

        // A tracked executor forwards barriers to its pool, even once sealed.
        let gate = Arc::new(Gate::default());
        gate.seal();
        let executor = TrackedExecutor::new(backend.low_pri(), &gate);
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

        let executor = SinglePoolExecutor::new("barrier-test", 3);
        let (started, reached) = oneshot::channel();
        let (resume, paused) = mpsc::channel::<()>();
        executor
            .dispatch_fn("test", move || {
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
            assert_eq!(executor.spawn_fn("test", || 42).unwrap().await.unwrap(), 42);
        })
        .await
        .unwrap();
    }
}
