// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use tokio::sync::oneshot;

use super::{Gate, OwnedLease};

/// The work was rejected because its scope or executor no longer accepts new
/// work, typically during shutdown.
///
/// The work never started, and anything it captured has already been dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("I/O admission is closed")]
pub struct AdmissionClosed;

/// Work that can move to an executor thread, blocking or async. The executor
/// calls [`Self::run`] on the thread that runs the task.
///
/// Whatever it captures stays alive until it has run or been dropped. Any
/// closure is a task that is always wanted; [`FutureTask`] wraps async work.
pub trait IoTask<O>: Send + 'static {
    /// Whether the task should still run. An executor asks right before it
    /// starts the task, and drops the task without running it if the answer is
    /// no. For example, the task that [`IoExecutor::spawn_fn`] submits is no
    /// longer wanted once its caller has dropped the result future.
    fn is_wanted(&self) -> bool {
        true
    }

    /// Runs the task. The executor calls this on the thread that will poll the
    /// returned future, so the future need not be `Send`.
    fn run(self) -> impl Future<Output = O>;
}

impl<O, F: FnOnce() -> O + Send + 'static> IoTask<O> for F {
    async fn run(self) -> O {
        self()
    }
}

/// Wraps a function that returns a future, so async work can be submitted as an
/// [`IoTask`]. The function is called on the executor thread, so the future need
/// not be `Send`.
pub struct FutureTask<F>(pub F);

impl<O, Fut, F> IoTask<O> for FutureTask<F>
where
    Fut: Future<Output = O>,
    F: FnOnce() -> Fut + Send + 'static,
{
    fn run(self) -> impl Future<Output = O> {
        (self.0)()
    }
}

/// Work that an [`IoExecutor`] can run: a label for its metrics, and a way to
/// become an [`IoTask`].
///
/// The operation itself may borrow (e.g. `&Db`) and need not be `Send`; only the
/// task returned by [`Self::into_task`] must be `Send + 'static`.
///
/// ```ignore
/// struct Flush<'a> {
///     db: &'a Arc<Db>,
///     gate: &'a Arc<Gate>,
/// }
///
/// impl IoOp for Flush<'_> {
///     type Output = Result<(), FlushError>;
///
///     fn operation(&self) -> &'static str {
///         "flush"
///     }
///
///     fn into_task(self) -> Result<impl IoTask<Self::Output>, AdmissionClosed> {
///         let lease = self.gate.try_acquire().ok_or(AdmissionClosed)?.into_owned();
///         let db = Arc::clone(self.db); // borrowed -> owned
///         Ok(TrackedTask::new(move || db.flush(), lease))
///     }
/// }
/// ```
pub trait IoOp: Sized {
    type Output: Send + 'static;

    /// What the operation does, such as `"flush"`. Executors label the metrics
    /// of its background task with it, so it should be short and come from a
    /// small, fixed set.
    fn operation(&self) -> &'static str;

    /// Prepares the operation for background execution and returns the task.
    ///
    /// This runs on the submitting thread, before the executor has accepted
    /// anything. It should:
    ///
    /// - Acquire admission (a lease) and move it into the task, or return
    ///   `AdmissionClosed`. The lease must stay alive until the task has run or
    ///   been dropped; [`TrackedTask`] takes care of that.
    /// - Turn borrowed resources into owned ones, e.g. clone an `Arc`.
    ///
    /// The executor measures the task (how long it waited and ran), so the task
    /// doesn't need to.
    ///
    /// The task may be rejected or dropped without ever running, so preparing it
    /// must not record an execution.
    fn into_task(self) -> Result<impl IoTask<Self::Output>, AdmissionClosed>;
}

/// An accepted task ended without delivering a result, e.g. it panicked or the
/// executor dropped it.
///
/// Unlike [`AdmissionClosed`], the task *was* accepted, and may have started.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("I/O task terminated without delivering its result")]
pub struct IoTaskError;

/// Runs blocking work on background threads.
///
/// Implementors provide [`Self::dispatch_fn`]; the other methods are helpers
/// built on top. Each task comes with the name of the
/// operation it performs (see [`IoOp::operation`]), which an executor can use to
/// label the metrics it records about the task. Submission is eager: a task is accepted or
/// rejected before the method returns, not when the returned future is first
/// polled. Returned futures don't borrow the executor.
///
/// The helpers differ in what happens if the caller loses interest. Work that
/// has already started is never interrupted.
///
/// | Method                     | Returns       | Result future dropped before the task starts |
/// |----------------------------|---------------|----------------------------------------------|
/// | `spawn` / `spawn_fn`       | result future | task is skipped                              |
/// | `spawn_detached`           | result future | task still runs, result is dropped           |
/// | `dispatch` / `dispatch_fn` | nothing       | n/a, task always runs                        |
///
/// ```ignore
/// // A read or write: no point doing it if the caller has gone away.
/// let value = executor.spawn(ReadOp { .. })?.await?;
///
/// // Dropping a column family: must finish even if the caller is cancelled.
/// executor.spawn_detached(DropCfOp { .. })?.await?;
///
/// // Opening a partition: the task publishes its outcome itself (e.g. on a
/// // watch channel), so no result future is needed.
/// executor.dispatch(OpenPartitionOp { .. })?;
/// ```
///
/// Getting a result back doesn't mean the work is fully finished: the worker
/// may still be dropping captured resources. Use a [`TrackedExecutor`] to know
/// when it is safe to shut down.
pub trait IoExecutor {
    /// Submits a fire-and-forget task.
    ///
    /// `Ok(())` means the task was accepted and will run exactly once, regardless
    /// of what happens to the caller, unless it is no longer wanted when it's
    /// about to start. On rejection, the task is dropped without running.
    ///
    /// Implementations must ask [`IoTask::is_wanted`] right before starting the
    /// task, and drop it without running it if the answer is no. That is how
    /// [`Self::spawn_fn`] skips work whose caller has gone away.
    ///
    /// Implementations with their own admission, like [`TrackedExecutor`], must
    /// synchronize acceptance with sealing and count an accepted task before
    /// handing it to a worker, so a drain cannot miss it.
    ///
    /// A scope may finish draining before this returns (e.g. a worker ran the
    /// task already), so after handing the task off, implementations must not
    /// touch anything that is shut down once a scope drains.
    fn dispatch_fn(
        &self,
        operation: &'static str,
        task: impl IoTask<()>,
    ) -> Result<(), AdmissionClosed>;

    /// Submits a task and returns a future for its result.
    ///
    /// Dropping the future means "I no longer need this". If the worker notices
    /// before starting, it drops the task without running it; otherwise the task
    /// runs to completion. The future need not be polled for the task to run.
    ///
    /// ```text
    /// let result = spawn_fn(task)?;  // accepted and queued
    /// drop(result);                  // not started yet: task is dropped, never runs
    ///                                // already running: task finishes, result is dropped
    /// ```
    ///
    /// Dropping the future doesn't free the task's captures right away; they live
    /// until a worker runs or drops the task.
    ///
    /// Beware of results that hold resources (a file handle, a snapshot): task
    /// accounting ends when the task returns, but the result can outlive it. An
    /// unclaimed result is dropped on the executor thread; a delivered one lives
    /// in the future until awaited or dropped. Give such results their own lease.
    fn spawn_fn<O: Send + 'static>(
        &self,
        operation: &'static str,
        task: impl IoTask<O>,
    ) -> Result<impl Future<Output = Result<O, IoTaskError>> + Send + 'static, AdmissionClosed>
    {
        let (tx, rx) = oneshot::channel();
        self.dispatch_fn(
            operation,
            SendResult {
                task,
                tx,
                skip_if_abandoned: true,
            },
        )?;
        Ok(async move { rx.await.map_err(|_| IoTaskError) })
    }

    /// Like [`Self::spawn_fn`], but for an [`IoOp`]: prepares it with
    /// [`IoOp::into_task`] and submits the task.
    ///
    /// Dropping the result future can skip queued work. If the work must run
    /// regardless, use [`Self::spawn_detached`] or [`Self::dispatch`].
    fn spawn<O>(
        &self,
        op: O,
    ) -> Result<
        impl Future<Output = Result<O::Output, IoTaskError>> + Send + 'static,
        AdmissionClosed,
    >
    where
        O: IoOp,
    {
        let operation = op.operation();
        self.spawn_fn(operation, op.into_task()?)
    }

    /// Like [`Self::spawn`], but the work runs even if the result future is
    /// dropped or never polled.
    ///
    /// Use it when the work must survive the caller, e.g. a write that must not
    /// be lost if the request is cancelled, but the caller still wants the result.
    /// Results are owned as described in [`Self::spawn_fn`].
    fn spawn_detached<O>(
        &self,
        op: O,
    ) -> Result<
        impl Future<Output = Result<O::Output, IoTaskError>> + Send + 'static,
        AdmissionClosed,
    >
    where
        O: IoOp,
    {
        let operation = op.operation();
        let (tx, rx) = oneshot::channel();
        self.dispatch_fn(
            operation,
            SendResult {
                task: op.into_task()?,
                tx,
                skip_if_abandoned: false,
            },
        )?;
        Ok(async move { rx.await.map_err(|_| IoTaskError) })
    }

    /// Like [`Self::dispatch_fn`], but for an [`IoOp`].
    ///
    /// No result channel is allocated: the output, including any error from the
    /// operation, is dropped on the executor thread. `Ok(())` means accepted,
    /// not done.
    fn dispatch<O>(&self, op: O) -> Result<(), AdmissionClosed>
    where
        O: IoOp,
    {
        let operation = op.operation();
        self.dispatch_fn(
            operation,
            DiscardResult {
                task: op.into_task()?,
                output: PhantomData,
            },
        )
    }
}

// Sends the result of `task` to the future that `spawn_fn` or `spawn_detached`
// returned.
struct SendResult<T, O> {
    task: T,
    tx: oneshot::Sender<O>,
    // Whether the task is no longer wanted once the caller drops the future.
    skip_if_abandoned: bool,
}

impl<T: IoTask<O>, O: Send + 'static> IoTask<()> for SendResult<T, O> {
    fn is_wanted(&self) -> bool {
        !(self.skip_if_abandoned && self.tx.is_closed()) && self.task.is_wanted()
    }

    async fn run(self) {
        let _ = self.tx.send(self.task.run().await);
    }
}

// Runs `task` for `dispatch`, and drops its result on the executor thread.
struct DiscardResult<T, O> {
    task: T,
    output: PhantomData<fn() -> O>,
}

impl<T: IoTask<O>, O: 'static> IoTask<()> for DiscardResult<T, O> {
    fn is_wanted(&self) -> bool {
        self.task.is_wanted()
    }

    async fn run(self) {
        drop(self.task.run().await);
    }
}

/// Wraps an executor so that everything submitted through it is counted by a
/// [`Gate`].
///
/// Each submission holds a lease from admission until the task has run or been
/// dropped. Once the gate is sealed, submissions fail with
/// [`AdmissionClosed`], and [`Gate::drain`] waits for accepted work.
///
/// Scopes sharing one backend are independent. For example, each partition on
/// a database can have its own gate while all use the same thread pools:
///
/// ```ignore
/// let p1 = TrackedExecutor::new(&pool, &p1_flights);
/// let p2 = TrackedExecutor::new(&pool, &p2_flights);
///
/// p1_flights.seal(); // closing partition 1
/// assert!(p1.dispatch(op1).is_err()); // AdmissionClosed
/// assert!(p2.dispatch(op2).is_ok());  // partition 2 is unaffected
/// p1_flights.drain().await;           // doesn't wait for partition 2's work
/// ```
///
/// The gate can also hold leases for work submitted elsewhere, or for results
/// that hold resources (see [`IoExecutor::spawn_fn`]).
pub struct TrackedExecutor<'a, E> {
    executor: &'a E,
    gate: &'a Arc<Gate>,
}

impl<'a, E: IoExecutor> TrackedExecutor<'a, E> {
    pub fn new(executor: &'a E, gate: &'a Arc<Gate>) -> Self {
        Self { executor, gate }
    }
}

impl<E: IoExecutor> IoExecutor for TrackedExecutor<'_, E> {
    fn dispatch_fn(
        &self,
        operation: &'static str,
        task: impl IoTask<()>,
    ) -> Result<(), AdmissionClosed> {
        let submission = self.gate.try_acquire().ok_or(AdmissionClosed)?;
        self.executor
            .dispatch_fn(operation, TrackedTask::new(task, submission.into_owned()))
    }
}

/// Wraps an executor and counts the tasks submitted through it, or through its
/// clones, until each has run or been dropped, so callers can wait until none
/// are left.
#[derive(Clone)]
pub struct IdleTracker<E> {
    executor: E,
    tasks: Arc<Gate>,
}

impl<E> IdleTracker<E> {
    pub fn new(executor: E) -> Self {
        Self {
            executor,
            tasks: Arc::default(),
        }
    }

    /// The wrapped executor. Tasks submitted to it directly are not counted.
    pub fn inner(&self) -> &E {
        &self.executor
    }

    /// Waits until every counted task has run or been dropped, including what it
    /// captured. Tasks submitted meanwhile are waited for too, so under a steady
    /// stream of new tasks, this may never return.
    pub async fn wait_idle(&self) {
        self.tasks.wait_idle().await;
    }
}

impl<E: IoExecutor> IoExecutor for IdleTracker<E> {
    fn dispatch_fn(
        &self,
        operation: &'static str,
        task: impl IoTask<()>,
    ) -> Result<(), AdmissionClosed> {
        // The gate is never sealed, so it only counts.
        TrackedExecutor::new(&self.executor, &self.tasks).dispatch_fn(operation, task)
    }
}

/// A task bundled with the lease that accounts for it, dropped in the right
/// order: the task's captures first, then the lease.
///
/// With the opposite order, `drain` could return while the task still holds,
/// say, a database handle. The order holds whether the task runs or is dropped
/// from the queue.
///
/// `L` is usually an [`OwnedLease`], but any guard works, e.g. a tuple like
/// `(in_flight_gauge, lease)`, or an `Option<OwnedLease>` when accounting is
/// optional.
///
/// With an owned guard, a tracked task is itself an [`IoTask`], and is wanted
/// as long as the task inside is.
pub struct TrackedTask<T, L = OwnedLease> {
    // Fields drop in declaration order, so `task` must come first.
    task: T,
    _execution: L,
}

impl<T, L> TrackedTask<T, L> {
    pub fn new(task: T, lease: L) -> Self {
        Self {
            task,
            _execution: lease,
        }
    }
}

impl<R, T: IoTask<R>, L: Send + 'static> IoTask<R> for TrackedTask<T, L> {
    fn is_wanted(&self) -> bool {
        self.task.is_wanted()
    }

    /// Runs the task, then releases the lease. The returned value outlives the
    /// lease, so a resource-bearing result needs its own.
    async fn run(self) -> R {
        let Self { task, _execution } = self;
        task.run().await
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::panic::AssertUnwindSafe;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};

    use futures::FutureExt;

    use super::*;

    // Queues tasks instead of running them, so each test decides exactly when a
    // task runs. Testing cancellation before a task starts then needs no sleeps.
    // A queued task builds its future when called, like a worker starting it.
    type QueuedTask = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()>>> + Send>;

    struct QueuedExecutor(mpsc::Sender<QueuedTask>);

    impl IoExecutor for QueuedExecutor {
        fn dispatch_fn(
            &self,
            _operation: &'static str,
            task: impl IoTask<()>,
        ) -> Result<(), AdmissionClosed> {
            // Asks whether the task is wanted when the test runs it, like a worker
            // about to start it.
            let run = move || -> Pin<Box<dyn Future<Output = ()>>> {
                Box::pin(async move {
                    if task.is_wanted() {
                        task.run().await;
                    }
                })
            };
            self.0.send(Box::new(run)).map_err(|_| AdmissionClosed)
        }
    }

    struct DropProbe(Arc<AtomicUsize>);

    #[tokio::test]
    async fn result_delivery_does_not_certify_execution_drainage() {
        use std::sync::Mutex;
        use std::task::{Context, Wake, Waker};
        use std::time::Duration;

        // `oneshot` wakes the receiver synchronously inside `send()`. Blocking in
        // that wake freezes the worker right after the result is sent but before
        // `TrackedTask` releases its lease, deterministically.
        struct PauseOnWake {
            reached: Mutex<Option<oneshot::Sender<()>>>,
            resume: Mutex<mpsc::Receiver<()>>,
        }

        impl Wake for PauseOnWake {
            fn wake(self: Arc<Self>) {
                if let Some(reached) = self.reached.lock().unwrap().take() {
                    let _ = reached.send(());
                    let _ = self.resume.lock().unwrap().recv();
                }
            }
        }

        let gate = Arc::new(Gate::default());
        let (tx, rx) = mpsc::channel();
        let backend = QueuedExecutor(tx);
        let executor = TrackedExecutor::new(&backend, &gate);
        let mut result = Box::pin(executor.spawn_fn("test", || 42).unwrap());
        let (reached, paused) = oneshot::channel();
        let (resume, wait) = mpsc::channel();
        let waker = Waker::from(Arc::new(PauseOnWake {
            reached: Mutex::new(Some(reached)),
            resume: Mutex::new(wait),
        }));
        assert!(
            result
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        gate.seal();
        let task = rx.try_recv().unwrap();
        let worker = std::thread::spawn(move || futures::executor::block_on(task()));
        tokio::time::timeout(Duration::from_secs(10), paused)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.await.unwrap(), 42);
        assert!(!gate.is_idle(), "a delivered result is not drainage");
        let mut drain = Box::pin(gate.drain());
        assert!(
            drain
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(resume);
        tokio::time::timeout(Duration::from_secs(10), drain)
            .await
            .unwrap();
        worker.join().unwrap();
        assert!(gate.is_idle());
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn tracked_dispatch_covers_submission_disposal_and_retained_results() {
        struct SealDuringDispatch(Arc<Gate>);

        impl IoExecutor for SealDuringDispatch {
            fn dispatch_fn(
                &self,
                _operation: &'static str,
                task: impl IoTask<()>,
            ) -> Result<(), AdmissionClosed> {
                self.0.seal();
                assert!(!self.0.is_idle(), "accepted task is counted before handoff");
                if task.is_wanted() {
                    futures::executor::block_on(task.run());
                }
                Ok(())
            }
        }

        let gate = Arc::new(Gate::default());
        let executor = SealDuringDispatch(gate.clone());
        TrackedExecutor::new(&executor, &gate)
            .dispatch_fn("test", || {})
            .unwrap();
        gate.drain().await;
        assert!(
            TrackedExecutor::new(&executor, &gate)
                .dispatch_fn("test", || panic!("sealed"))
                .is_err()
        );

        struct CapturedResource(Arc<Gate>);
        impl Drop for CapturedResource {
            fn drop(&mut self) {
                assert!(!self.0.is_idle(), "release resources before their flight");
            }
        }

        let gate = Arc::new(Gate::default());
        let (tx, rx) = mpsc::channel();
        let backend = QueuedExecutor(tx);
        let executor = TrackedExecutor::new(&backend, &gate);
        let resource = CapturedResource(gate.clone());
        let ran = Arc::new(AtomicUsize::new(0));
        let task_ran = ran.clone();
        let observer = executor
            .spawn_fn("test", move || {
                drop(resource);
                task_ran.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap();
        drop(observer);
        assert!(
            !gate.is_idle(),
            "observer cancellation does not complete work"
        );
        rx.try_recv().unwrap()().await;
        assert_eq!(
            ran.load(Ordering::Relaxed),
            0,
            "the lease wrapper keeps the task skippable"
        );
        gate.wait_idle().await;
        // `wait_idle` doesn't seal: new leases can still be acquired.
        let retained = gate.try_acquire().unwrap().into_owned();
        let observer = executor.spawn_fn("test", move || retained).unwrap();
        rx.try_recv().unwrap()().await;
        gate.seal();
        assert!(!gate.is_idle(), "buffered output owns its explicit flight");
        drop(observer);
        gate.drain().await;

        for execute in [false, true] {
            let gate = Arc::new(Gate::default());
            let executor = TrackedExecutor::new(&backend, &gate);
            let resource = CapturedResource(gate.clone());
            let observer = executor
                .spawn_fn("test", move || {
                    let _resource = resource;
                    panic!("interrupted task");
                })
                .unwrap();
            gate.seal();
            let task = rx.try_recv().unwrap();
            if execute {
                assert!(AssertUnwindSafe(task()).catch_unwind().await.is_err());
            } else {
                // A backend dropping the job must still release captures before
                // the lease, and the observer must see an error, not success.
                drop(task);
            }
            assert!(matches!(observer.await, Err(IoTaskError)));
            gate.drain().await;
        }

        let gate = Arc::new(Gate::default());
        let resource = CapturedResource(gate.clone());
        drop(rx);
        assert!(
            TrackedExecutor::new(&backend, &gate)
                .dispatch_fn("test", move || drop(resource))
                .is_err()
        );
        assert!(gate.is_idle(), "rejection disposes the complete task");
    }

    // This operation borrows non-Sync state and is neither Send nor 'static.
    struct BorrowedOp<'a> {
        preparations: &'a Cell<usize>,
        background_runs: &'a Arc<AtomicUsize>,
        drops: &'a Arc<AtomicUsize>,
    }

    impl IoOp for BorrowedOp<'_> {
        type Output = DropProbe;

        fn operation(&self) -> &'static str {
            "borrowed"
        }

        fn into_task(self) -> Result<impl IoTask<Self::Output>, AdmissionClosed> {
            self.preparations.set(self.preparations.get() + 1);
            let background_runs = self.background_runs.clone();
            let drops = self.drops.clone();
            Ok(move || {
                // Background-only instrumentation runs at task entry, not when
                // the operation is prepared for dispatch.
                background_runs.fetch_add(1, Ordering::Relaxed);
                DropProbe(drops)
            })
        }
    }

    #[tokio::test]
    async fn eager_submission_skips_abandoned_work_and_dispatch_runs_independently() {
        let background_runs = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();
        let observer = {
            let executor = QueuedExecutor(tx);
            let preparations = Cell::new(0);

            drop(
                BorrowedOp {
                    preparations: &preparations,
                    background_runs: &background_runs,
                    drops: &drops,
                }
                .into_task()
                .unwrap(),
            );
            assert_eq!(preparations.get(), 1);
            assert_eq!(background_runs.load(Ordering::Relaxed), 0);

            let observer = executor
                .spawn(BorrowedOp {
                    preparations: &preparations,
                    background_runs: &background_runs,
                    drops: &drops,
                })
                .unwrap();
            assert_eq!(preparations.get(), 2);
            assert_eq!(background_runs.load(Ordering::Relaxed), 0);
            observer
        }; // The observer outlives both the executor and the borrowed state.
        let task = rx
            .try_recv()
            .expect("submitted without polling the observer");
        drop(observer);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        task().await;
        assert_eq!(background_runs.load(Ordering::Relaxed), 0);
        assert_eq!(drops.load(Ordering::Relaxed), 0);

        let (tx, rx) = mpsc::channel();
        let executor = QueuedExecutor(tx);
        let preparations = Cell::new(0);

        let observer = executor
            .spawn(BorrowedOp {
                preparations: &preparations,
                background_runs: &background_runs,
                drops: &drops,
            })
            .unwrap();
        assert_eq!(background_runs.load(Ordering::Relaxed), 0);
        rx.try_recv().unwrap()().await;
        assert_eq!(background_runs.load(Ordering::Relaxed), 1);
        assert_eq!(drops.load(Ordering::Relaxed), 0, "result is buffered");
        drop(observer);
        assert_eq!(drops.load(Ordering::Relaxed), 1);

        let observer = executor
            .spawn_fn("test", || Err::<(), _>("operation failed"))
            .unwrap();
        rx.try_recv().unwrap()().await;
        assert_eq!(observer.await.unwrap(), Err("operation failed"));

        executor
            .dispatch(BorrowedOp {
                preparations: &preparations,
                background_runs: &background_runs,
                drops: &drops,
            })
            .unwrap();
        assert_eq!(preparations.get(), 2);
        assert_eq!(background_runs.load(Ordering::Relaxed), 1);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        rx.try_recv().unwrap()().await;
        assert_eq!(background_runs.load(Ordering::Relaxed), 2);
        assert_eq!(drops.load(Ordering::Relaxed), 2, "dispatch discards output");

        let observer = executor
            .spawn_detached(BorrowedOp {
                preparations: &preparations,
                background_runs: &background_runs,
                drops: &drops,
            })
            .unwrap();
        drop(observer);
        rx.try_recv().unwrap()().await;
        assert_eq!(background_runs.load(Ordering::Relaxed), 3);
        assert_eq!(
            drops.load(Ordering::Relaxed),
            3,
            "spawn_detached survives observer drop"
        );

        drop(rx);
        assert!(matches!(
            executor.spawn(BorrowedOp {
                preparations: &preparations,
                background_runs: &background_runs,
                drops: &drops,
            }),
            Err(AdmissionClosed)
        ));
        assert_eq!(preparations.get(), 4);
        assert_eq!(background_runs.load(Ordering::Relaxed), 3);
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn rejection_is_immediate_and_distinct_from_failure_after_acceptance() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();
        let executor = QueuedExecutor(tx);
        let captured = DropProbe(drops.clone());
        let observer = executor
            .spawn_fn("test", move || {
                let _captured = captured;
                panic!("execution failed after acceptance");
            })
            .unwrap();
        assert!(
            AssertUnwindSafe(rx.try_recv().unwrap()())
                .catch_unwind()
                .await
                .is_err()
        );
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert!(matches!(observer.await, Err(IoTaskError)));

        // Closing the test queue rejects further work: it never runs, and its
        // captures are dropped before `spawn_fn` returns.
        drop(rx);
        let runs = Arc::new(AtomicUsize::new(0));
        let task_runs = runs.clone();
        let captured = DropProbe(drops.clone());
        assert!(matches!(
            executor.spawn_fn("test", move || {
                let _captured = captured;
                task_runs.fetch_add(1, Ordering::Relaxed);
            }),
            Err(AdmissionClosed)
        ));
        assert_eq!(runs.load(Ordering::Relaxed), 0, "rejected work never runs");
        assert_eq!(drops.load(Ordering::Relaxed), 2, "rejection drops captures");
    }
}
