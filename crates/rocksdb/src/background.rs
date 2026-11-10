// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use futures::FutureExt;
use futures::executor::{LocalPool, LocalSpawner};
use futures::task::{LocalFutureObj, LocalSpawn};
use metrics::{Gauge, gauge, histogram};
use tracing::Instrument;

use restate_core::io::{AdmissionClosed, FutureTask, Gate, IoExecutor, IoOp, IoTask, TrackedTask};

use crate::RocksDbPerfExt;
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
    ReadProperties,
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

/// The work of a [`StorageOp`], as it runs on a storage worker with the owned
/// context. The future need not be `Send`: it stays on one worker.
///
/// There is no need to implement it. Sync work ([`SyncOpFn`]) also
/// implements it, as async work that finishes in a single poll, and
/// [`StorageOp::with_context_async`] takes async work.
pub trait OpFn<C: OpContext>: Send + 'static {
    /// What the work returns.
    type Output;

    /// Runs the work with the owned context, on a storage worker.
    fn run_async(self, context: C::Owned) -> impl Future<Output = Self::Output>;
}

/// The synchronous work of a [`StorageOp`], which can also run on the calling
/// thread, with [`StorageOp::call`].
///
/// The work gets the operation's context: borrowed when the operation runs on
/// the calling thread ([`Self::run`]), or owned when it runs on a storage
/// worker ([`OpFn::run_async`]). It sees the same thing either way, so one
/// closure serves both.
///
/// There is no need to implement it: pass a closure. Closures implement it for
/// every kind of context, and take what the table in [`OpContext`] lists:
/// `FnOnce(&C)` for a shared handle, `FnOnce(&mut ReadIterator<'_, H>)` for an
/// iterator, and `FnOnce()` for no context.
pub trait SyncOpFn<C: OpContext>: OpFn<C> {
    /// Runs the work with the borrowed context, on the calling thread.
    fn run(self, context: C) -> Self::Output;
}

impl<C: Clone + Send + Sync + 'static> OpContext for &C {
    type Owned = C;

    fn into_owned(self) -> C {
        self.clone()
    }
}

impl<'a, C, F, R> SyncOpFn<&'a C> for F
where
    C: Clone + Send + Sync + 'static,
    F: FnOnce(&C) -> R + Send + 'static,
{
    fn run(self, context: &'a C) -> R {
        self(context)
    }
}

impl<C, F, R> OpFn<&C> for F
where
    C: Clone + Send + Sync + 'static,
    F: FnOnce(&C) -> R + Send + 'static,
{
    type Output = R;

    async fn run_async(self, context: C) -> R {
        self(&context)
    }
}

impl OpContext for () {
    type Owned = ();

    fn into_owned(self) {}
}

impl<F, R> SyncOpFn<()> for F
where
    F: FnOnce() -> R + Send + 'static,
{
    fn run(self, (): ()) -> R {
        self()
    }
}

impl<F, R> OpFn<()> for F
where
    F: FnOnce() -> R + Send + 'static,
{
    type Output = R;

    async fn run_async(self, (): ()) -> R {
        self()
    }
}

/// An operation on the database that has been prepared but hasn't run yet,
/// for example with [`RocksDb::prepare`](crate::RocksDb::prepare).
///
/// Run it on the current thread with [`StorageOp::call`], or submit it to an
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
/// [`AdmissionClosed`]. The exceptions are
/// [`RocksDb::prepare_drop_cf`](crate::RocksDb::prepare_drop_cf), which takes no
/// lease, so that a column family can still be deleted after the database is
/// sealed, and an operation with [`LeasePolicy::StopOnSeal`], which takes its
/// lease when it starts on a worker.
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

impl<'a, 'c, C, F> StorageOp<'a, &'c C, Async<F>>
where
    C: Clone + Send + Sync + 'static,
{
    /// Like [`Self::with_context`], but for async work, such as a read that
    /// awaits a channel between lookups.
    ///
    /// Async work only runs on a storage worker, so the operation can't be
    /// called inline with [`StorageOp::call`]: submit it to an executor. The
    /// worker runs other tasks while it waits, so use
    /// [`StorageOp::with_read_perf`] rather than [`StorageOp::with_guard`] for
    /// RocksDB perf counters. Workers don't run a tokio runtime: the work can
    /// await channels, but not tokio timers, IO or `tokio::spawn`.
    ///
    /// ```no_run
    /// use restate_core::io::IoExecutor;
    /// use restate_rocksdb::reads::ColumnFamilies;
    /// use restate_rocksdb::{RocksDb, StorageOp, StorageTaskKind};
    /// use restate_util_lifecycle::Scope;
    ///
    /// # async fn example<C: ColumnFamilies>(db: &RocksDb, scope: &Scope<C>) -> Result<(), Box<dyn std::error::Error>> {
    /// let read = StorageOp::with_context_async(scope, scope.gate(), StorageTaskKind::MultiGet, async |scope| {
    ///     scope.name().to_owned()
    /// })
    /// .with_read_perf("lookup");
    /// let name = db.executor().low_pri().spawn(read)?.await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_context_async<R>(
        context: &'c C,
        gate: &'a Arc<Gate>,
        kind: StorageTaskKind,
        op: F,
    ) -> Self
    where
        F: AsyncFnOnce(&C) -> R + Send + 'static,
    {
        Self::from_parts(context, gate, kind, Async(op))
    }
}

impl<'a, C, F> StorageOp<'a, C, F> {
    // An operation on any kind of context, counted in `gate`, if any.
    pub(crate) fn from_parts(
        context: C,
        gate: impl Into<Option<&'a Arc<Gate>>>,
        kind: StorageTaskKind,
        op: F,
    ) -> Self {
        Self {
            context,
            op,
            gate: gate.into(),
            kind,
        }
    }
}

impl<'a, C: OpContext, F: OpFn<C>> StorageOp<'a, C, F> {
    /// Measures RocksDB read perf counters while the work runs, also across
    /// `.await` points, and reports them under `name` when it finishes.
    pub fn with_read_perf(self, name: &'static str) -> StorageOp<'a, C, ReadPerf<F>> {
        let Self {
            context,
            op,
            gate,
            kind,
        } = self;
        StorageOp {
            context,
            op: ReadPerf { op, name },
            gate,
            kind,
        }
    }

    /// Decides what sealing the operation's gate does once the operation is
    /// submitted (see [`LeasePolicy`]). The operation then returns
    /// [`AdmissionClosed`] if [`LeasePolicy::StopOnSeal`] stopped it.
    pub fn with_lease_policy(self, policy: LeasePolicy) -> StorageOp<'a, C, Sealable<F>> {
        let Self {
            context,
            op,
            gate,
            kind,
        } = self;
        let (gate, stop_on_seal) = match policy {
            // No lease at submission: the operation admits itself on the worker.
            LeasePolicy::StopOnSeal => (None, gate.map(Arc::clone)),
            LeasePolicy::WholeOperation => (gate, None),
        };
        StorageOp {
            context,
            op: Sealable { op, stop_on_seal },
            gate,
            kind,
        }
    }

    /// Passes the operation's result to `f`, on the thread that runs the
    /// operation, right after it. Use it for work that belongs with the result,
    /// such as taking ownership of files that the operation created, so that
    /// they are cleaned up even if nobody claims the result. If the operation is
    /// rejected or dropped without running, `f` is never called.
    ///
    /// For an operation that returns a `Result`, [`Self::map_ok`] changes only
    /// a successful result.
    pub fn map<U, M>(self, f: M) -> StorageOp<'a, C, Mapped<F, M>>
    where
        M: FnOnce(F::Output) -> U + Send + 'static,
    {
        let Self {
            context,
            op,
            gate,
            kind,
        } = self;
        StorageOp {
            context,
            op: Mapped { op, f },
            gate,
            kind,
        }
    }

    /// Like [`Self::map`], but only for a successful result: `f` changes the
    /// value inside `Ok`, and an error is passed on as it is.
    ///
    /// ```no_run
    /// use restate_rocksdb::{RocksDb, StorageTaskKind};
    /// use std::sync::Arc;
    ///
    /// # fn example(db: &Arc<RocksDb>) {
    /// // Measures the value on the worker, so only its length comes back.
    /// let op = db
    ///     .prepare(StorageTaskKind::MultiGet, |db| db.inner().as_raw_db().get(b"key"))
    ///     .map_ok(|value| value.map_or(0, |value| value.len()));
    /// # }
    /// ```
    pub fn map_ok<T, E, U, M>(self, f: M) -> StorageOp<'a, C, MappedOk<F, M>>
    where
        F: OpFn<C, Output = Result<T, E>>,
        M: FnOnce(T) -> U + Send + 'static,
    {
        let Self {
            context,
            op,
            gate,
            kind,
        } = self;
        StorageOp {
            context,
            op: MappedOk { op, f },
            gate,
            kind,
        }
    }
}

impl<'a, C: OpContext, F: SyncOpFn<C>> StorageOp<'a, C, F> {
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
}

/// The work of an operation returned by [`StorageOp::map`]: the original work,
/// followed by the function that changes its result.
pub struct Mapped<F, M> {
    op: F,
    f: M,
}

impl<C, F, M, U> SyncOpFn<C> for Mapped<F, M>
where
    C: OpContext,
    F: SyncOpFn<C>,
    M: FnOnce(F::Output) -> U + Send + 'static,
{
    fn run(self, context: C) -> U {
        (self.f)(self.op.run(context))
    }
}

impl<C, F, M, U> OpFn<C> for Mapped<F, M>
where
    C: OpContext,
    F: OpFn<C>,
    M: FnOnce(F::Output) -> U + Send + 'static,
{
    type Output = U;

    async fn run_async(self, context: C::Owned) -> U {
        (self.f)(self.op.run_async(context).await)
    }
}

/// The work of an operation returned by [`StorageOp::map_ok`]: the original
/// work, followed by the function that changes a successful result.
pub struct MappedOk<F, M> {
    op: F,
    f: M,
}

impl<C, F, M, T, E, U> SyncOpFn<C> for MappedOk<F, M>
where
    C: OpContext,
    F: SyncOpFn<C, Output = Result<T, E>>,
    M: FnOnce(T) -> U + Send + 'static,
{
    fn run(self, context: C) -> Result<U, E> {
        self.op.run(context).map(self.f)
    }
}

impl<C, F, M, T, E, U> OpFn<C> for MappedOk<F, M>
where
    C: OpContext,
    F: OpFn<C, Output = Result<T, E>>,
    M: FnOnce(T) -> U + Send + 'static,
{
    type Output = Result<U, E>;

    async fn run_async(self, context: C::Owned) -> Result<U, E> {
        self.op.run_async(context).await.map(self.f)
    }
}

/// The work of an operation returned by [`StorageOp::with_guard`]: the
/// original work, run while the guard exists.
///
/// It's a named type, rather than an `impl SyncOpFn`, so that async code holding a
/// prepared operation stays `Send`. The compiler can't always prove that for
/// an opaque type that carries the lifetime of a borrowed context.
pub struct Guarded<F, M> {
    op: F,
    make_guard: M,
}

impl<C, F, G, M> SyncOpFn<C> for Guarded<F, M>
where
    C: OpContext,
    F: SyncOpFn<C>,
    M: FnOnce() -> G + Send + 'static,
{
    fn run(self, context: C) -> F::Output {
        let _guard = (self.make_guard)();
        self.op.run(context)
    }
}

impl<C, F, G, M> OpFn<C> for Guarded<F, M>
where
    C: OpContext,
    F: OpFn<C>,
    M: FnOnce() -> G + Send + 'static,
{
    type Output = F::Output;

    async fn run_async(self, context: C::Owned) -> F::Output {
        let _guard = (self.make_guard)();
        self.op.run_async(context).await
    }
}

/// Async work, from [`StorageOp::with_context_async`].
pub struct Async<F>(F);

impl<C, F, R> OpFn<&C> for Async<F>
where
    C: Clone + Send + Sync + 'static,
    F: AsyncFnOnce(&C) -> R + Send + 'static,
{
    type Output = R;

    async fn run_async(self, context: C) -> R {
        (self.0)(&context).await
    }
}

/// The work of an operation returned by [`StorageOp::with_read_perf`]: the
/// original work, with its RocksDB read perf counters measured.
pub struct ReadPerf<F> {
    op: F,
    name: &'static str,
}

impl<C: OpContext, F: OpFn<C>> OpFn<C> for ReadPerf<F> {
    type Output = F::Output;

    async fn run_async(self, context: C::Owned) -> F::Output {
        self.op.run_async(context).with_read_perf(self.name).await
    }
}

/// What happens to a submitted operation once its gate is sealed. See
/// [`StorageOp::with_lease_policy`].
///
/// - [`Self::StopOnSeal`]: the operation stops early. Use it for reads that
///   are pointless once their owner shuts down, such as a query on a partition
///   that is closing.
/// - [`Self::WholeOperation`]: the operation runs to completion, and shutting
///   down waits for it. Use it for work that must not be cut short.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeasePolicy {
    /// The operation takes its lease when it starts on a worker, not when it's
    /// submitted, so a queued operation doesn't hold up draining. If the gate
    /// is sealed by then, it doesn't run. Once running, it stops as soon as the
    /// gate is sealed and it hits a yielding point.
    ///
    /// Stopping drops the operation and its context before the lease, and with
    /// them any lease the context holds, such as a
    /// [`ReadSnapshot`](crate::reads::ReadSnapshot)'s.
    StopOnSeal,
    /// The operation takes one lease when it's submitted and holds it until it
    /// ends and its context is dropped. Draining the gate waits for it.
    WholeOperation,
}

/// The work of an operation returned by [`StorageOp::with_lease_policy`].
pub struct Sealable<F> {
    op: F,
    // Set with `LeasePolicy::StopOnSeal`: the gate to admit through and watch.
    stop_on_seal: Option<Arc<Gate>>,
}

impl<C: OpContext, F: OpFn<C>> OpFn<C> for Sealable<F> {
    type Output = Result<F::Output, AdmissionClosed>;

    async fn run_async(self, context: C::Owned) -> Self::Output {
        let Some(gate) = self.stop_on_seal else {
            return Ok(self.op.run_async(context).await);
        };
        let lease = gate.try_acquire().ok_or(AdmissionClosed)?;
        let op = self.op;
        let output = tokio::select! {
            biased;
            _ = gate.wait_sealed() => Err(AdmissionClosed),
            // Lazy, so that the work doesn't start if the gate is already sealed.
            output = async move { op.run_async(context).await } => Ok(output),
        };
        // The work and its context are dropped by now, before the lease.
        drop(lease);
        output
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
        Ok(TrackedTask::new(
            FutureTask(move || op.run_async(context)),
            lease,
        ))
    }
}

impl<C: OpContext, F: SyncOpFn<C>> StorageOp<'_, C, F> {
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
    // Every worker takes tasks from this queue (see `Worker::run`).
    queue: async_channel::Sender<QueuedTask>,
    num_threads: NonZeroUsize,
}

impl SinglePoolExecutor {
    /// Starts a pool of `num_threads` threads. `name` names the threads, and
    /// labels the metrics of the tasks that run on them.
    pub fn new(name: &'static str, num_threads: NonZeroUsize) -> Self {
        let (queue, tasks) = async_channel::unbounded();
        for _ in 0..num_threads.get() {
            Worker::start(name, tasks.clone());
        }
        Self(Arc::new(SinglePoolExecutorInner {
            name,
            queue,
            num_threads,
        }))
    }

    /// The number of threads, and so of tasks that can run at once.
    pub fn max_count(&self) -> usize {
        self.0.num_threads.get()
    }

    // `task` builds the future on the worker that runs it, so the future need not
    // be `Send`.
    fn spawn<F: Future<Output = ()> + 'static>(&self, task: impl FnOnce() -> F + Send + 'static) {
        self.0
            .queue
            .try_send(Box::new(move || {
                // Unlike tokio, `LocalPool` doesn't contain a task's panic, which would
                // otherwise end the worker. The panic was already reported by the hook.
                let task = AssertUnwindSafe(task()).catch_unwind().map(drop);
                LocalFutureObj::new(Box::new(task))
            }))
            .expect("storage workers outlive the pool's handles");
    }
}

impl IoExecutor for SinglePoolExecutor {
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
        self.spawn(move || async move {
            // A task that is no longer wanted is dropped without being measured:
            // it didn't run.
            if !task.is_wanted() {
                return;
            }
            // Run inside the submitter's span, which tells where the work came from.
            async move {
                histogram!(STORAGE_BG_TASK_WAIT_DURATION, &labels).record(submitted_at.elapsed());
                async move {
                    let started_at = Instant::now();
                    task.run().await;
                    histogram!(STORAGE_BG_TASK_RUN_DURATION, &labels).record(started_at.elapsed());
                }
                .instrument(tracing::debug_span!("storage-task", operation))
                .await;
            }
            .instrument(span)
            .await;
        });
        Ok(())
    }
}

type QueuedTask = Box<dyn FnOnce() -> LocalFutureObj<'static, ()> + Send>;

struct Worker {
    spawner: LocalSpawner,
    queue: async_channel::Receiver<QueuedTask>,
}

impl Worker {
    // Runs tasks on a thread's own `LocalPool`, so they need not be `Send`. The
    // thread exits once all senders are dropped and the queued tasks are done.
    //
    // The worker deliberately doesn't run a tokio runtime. Most tasks are blocking
    // work that may call tokio's `blocking_*` methods (or drop values that do),
    // which panic on a thread driving a runtime. In turn, tasks can't use tokio's
    // timers, IO or `tokio::spawn`.
    fn start(name: &'static str, queue: async_channel::Receiver<QueuedTask>) {
        std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                let mut pool = LocalPool::new();
                let worker = Self {
                    spawner: pool.spawner(),
                    queue,
                };
                pool.run_until(worker.run());
                // Finish the tasks that are still running.
                pool.run();
            })
            .expect("storage worker thread starts");
    }

    async fn run(self) {
        while let Ok(task) = self.queue.recv().await {
            self.spawn(task);
            // Takes one task at a time, and the next only after this one has
            // finished or is suspended. A worker blocked in a task that never
            // suspends leaves the queue to the others instead of holding tasks
            // back behind it. A suspended task that blocks once woken doesn't
            // get this: the worker may already be waiting for the next task,
            // which then waits behind the blocked one.
            yield_now().await;
        }
    }

    fn spawn(&self, task: QueuedTask) {
        self.spawner
            .spawn_local_obj(task())
            .expect("the pool outlives its worker loop");
    }
}

// Lets the pool run its tasks before the worker loop continues.
pub(crate) async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if std::mem::replace(&mut yielded, true) {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
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
            SinglePoolExecutor::new("rs:io-hi", opts.storage_high_priority_bg_threads()),
            SinglePoolExecutor::new("rs:io-lo", opts.storage_low_priority_bg_threads()),
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
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use restate_core::io::{Gate, TrackedExecutor};

    use super::*;

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

        // `map` changes any result; `map_ok` only a successful one.
        let op = StorageOp::new(|| 42, &gate, StorageTaskKind::MultiGet).map(|n| n + 1);
        assert_eq!(op.call().unwrap(), 43);
        let op = StorageOp::new(|| Ok::<_, &str>(42), &gate, StorageTaskKind::MultiGet)
            .map_ok(|n| n + 1);
        assert_eq!(op.call().unwrap(), Ok(43));
        let op = StorageOp::new(|| Err::<i32, _>("failed"), &gate, StorageTaskKind::MultiGet)
            .map_ok(|n| n + 1);
        assert_eq!(op.call().unwrap(), Err("failed"));

        let task =
            StorageOp::new(|| 43, &gate, StorageTaskKind::MultiGet).with_guard(factory.clone());
        assert!(gate.is_idle());
        let task = task.into_task().unwrap();
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        let on_other_thread = move || futures::executor::block_on(task.run());
        assert_eq!(std::thread::spawn(on_other_thread).join().unwrap(), 43);
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
    async fn async_work_yields_the_worker_and_holds_admission_while_pending() {
        let executor = SinglePoolExecutor::new("test-async", NonZeroUsize::MIN);
        let gate = Arc::new(Gate::default());
        let context = "ctx".to_owned();
        let (tx, rx) = futures::channel::oneshot::channel::<u32>();
        let pending = IoExecutor::spawn(
            &executor,
            StorageOp::with_context_async(
                &context,
                &gate,
                StorageTaskKind::MultiGet,
                async move |context: &String| format!("{context}:{}", rx.await.unwrap()),
            )
            .with_read_perf("test"),
        )
        .unwrap();

        // The only worker runs other tasks while the first one waits.
        let other = IoExecutor::spawn(
            &executor,
            StorageOp::new(
                || std::thread::current().name().unwrap().to_owned(),
                &gate,
                StorageTaskKind::MultiGet,
            ),
        )
        .unwrap();
        assert_eq!(other.await.unwrap(), "test-async");
        assert!(!gate.is_idle());

        tx.send(7).unwrap();
        assert_eq!(pending.await.unwrap(), "ctx:7");
        // Draining returns once the finished task releases its lease.
        gate.seal();
        gate.drain().await;
    }

    #[tokio::test]
    async fn global_executor_routes_work_to_its_pools_and_outlives_owner_admission() {
        let backend = GlobalExecutor::new(
            SinglePoolExecutor::new("test-high", NonZeroUsize::MIN),
            SinglePoolExecutor::new("test-low", NonZeroUsize::MIN),
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
}
