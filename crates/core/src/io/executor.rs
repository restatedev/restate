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
use std::sync::Arc;

use tokio::sync::oneshot;

use super::{FlightTracker, OwnedLease};
use crate::ShutdownError;

/// An owned blocking operation that can move to an executor thread.
/// Captured resources must remain alive until execution or disposal finishes.
pub trait IoTask<O>: FnOnce() -> O + Send + 'static {}

impl<O, T: FnOnce() -> O + Send + 'static + ?Sized> IoTask<O> for T {}

/// An operation that can execute inline or prepare an owned background task.
///
/// The operation itself may borrow resources and need not be `Send`. Only the
/// task returned by [`Self::into_task`] must be `Send + 'static`. Preparing that
/// task does not submit it; dropping it must release its captured resources.
pub trait IoOp: Sized {
    type Output: Send + 'static;

    /// Scheduling priority hint for background execution.
    ///
    /// Interpretation is executor-specific; implementations may ignore this hint.
    fn priority(&self) -> Priority {
        Priority::default()
    }

    /// Execute inline, consuming the operation, without background-task setup or
    /// instrumentation. Nonblocking probes and retry decisions belong to the
    /// operation layer, before this consuming call.
    fn call(self) -> Self::Output;

    /// Promote borrowed resources as needed and prepare background execution.
    ///
    /// This method runs on the submitting thread, before acceptance. Put code
    /// that requires actual background execution inside the returned task: for
    /// example, record queue-wait duration at its entry and execution duration
    /// around the operation. The executor invokes that task, not [`Self::call`].
    ///
    /// Preparing, rejecting, or dropping an unexecuted task must not record an
    /// execution. Preparation-time bookkeeping, such as an in-flight gauge,
    /// should use a drop guard so it is balanced even if the task never runs.
    fn into_task(self) -> impl IoTask<Self::Output>;
}

/// An accepted task terminated without delivering its result, for example by
/// panicking. This is distinct from rejecting a submission during shutdown.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("I/O task terminated without delivering its result")]
pub struct IoTaskError;

/// Eager submission of blocking work, independent of result observation.
///
/// Implementations provide one submission primitive, [`Self::dispatch_fn`].
/// The default helpers prepare operations and deliver results without borrowing
/// the executor in the returned future. Executor shutdown/drainage is a separate
/// responsibility: receiving a result does not prove executor or resource cleanup.
pub trait IoExecutor {
    /// Accept a task for execution before returning.
    ///
    /// `Ok(())` commits the executor to running the task exactly once. Submission
    /// must be synchronized with any admission seal, accounting for accepted work
    /// before dispatch. Rejection must release the task without executing it.
    /// Accepted work must remain independent of the submitting caller's lifetime.
    fn dispatch_fn(&self, priority: Priority, task: impl IoTask<()>) -> Result<(), ShutdownError>;

    /// Submit eagerly and return an owned result observer on acceptance.
    ///
    /// Dropping or never polling the observer does not cancel the task. An
    /// undeliverable result is dropped on the executor thread; an already-buffered
    /// result is owned by the observer until received or dropped. Resource-bearing
    /// results therefore need their own lifetime accounting beyond task execution.
    fn spawn_fn<O: Send + 'static>(
        &self,
        priority: Priority,
        task: impl IoTask<O>,
    ) -> Result<impl Future<Output = Result<O, IoTaskError>> + Send + 'static, ShutdownError> {
        let (tx, rx) = oneshot::channel();
        self.dispatch_fn(priority, move || {
            let result = task();
            let _ = tx.send(result);
        })?;
        Ok(async move { rx.await.map_err(|_| IoTaskError) })
    }

    /// Prepare and submit an operation, returning an owned result observer.
    /// Acceptance and cancellation have the same semantics as [`Self::spawn_fn`].
    fn spawn<O>(
        &self,
        op: O,
    ) -> Result<impl Future<Output = Result<O::Output, IoTaskError>> + Send + 'static, ShutdownError>
    where
        O: IoOp,
    {
        let priority = op.priority();
        self.spawn_fn(priority, op.into_task())
    }

    /// Prepare and submit an operation without allocating a result channel.
    /// Its output, including any operation error, is discarded on the executor
    /// thread. `Ok(())` reports acceptance, not execution completion.
    fn dispatch<O>(&self, op: O) -> Result<(), ShutdownError>
    where
        O: IoOp,
    {
        let priority = op.priority();
        let task = op.into_task();
        self.dispatch_fn(priority, move || drop(task()))
    }
}

/// A scheduling hint indicating whether an operation is latency sensitive.
///
/// Interpretation is up to the executor implementation. Executors may ignore this
/// hint; it does not guarantee execution order or scheduling preference.
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

/// An executor's submission view for one independently sealable work scope.
///
/// The scope counts admission through dispatch, execution and task disposal. It
/// can also hold explicit leases for external work or resource-bearing results.
/// Sharing a backend does not couple the admission of different scopes.
pub struct TrackedExecutor<'a, E> {
    executor: &'a E,
    flights: &'a Arc<FlightTracker>,
}

impl<'a, E: IoExecutor> TrackedExecutor<'a, E> {
    pub fn new(executor: &'a E, flights: &'a Arc<FlightTracker>) -> Self {
        Self { executor, flights }
    }
}

impl<E: IoExecutor> IoExecutor for TrackedExecutor<'_, E> {
    fn dispatch_fn(&self, priority: Priority, task: impl IoTask<()>) -> Result<(), ShutdownError> {
        let submission = self.flights.try_acquire().ok_or(ShutdownError)?;
        let task = TrackedTask {
            task,
            _execution: submission.fork_owned(),
        };
        // The borrowed participant remains until dispatch returns, even if the
        // backend executes or rejects the owned task before then.
        self.executor.dispatch_fn(priority, move || task.run())
    }
}

struct TrackedTask<T> {
    // Discarding a queued task releases its captures before its flight.
    task: T,
    _execution: OwnedLease,
}

impl<T: IoTask<()>> TrackedTask<T> {
    fn run(self) {
        let Self { task, _execution } = self;
        task();
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};

    use super::*;

    // Submission and execution are explicitly separated so no timing or sleeping
    // is needed to exercise cancellation before execution and before handoff.
    struct QueuedExecutor(mpsc::Sender<Box<dyn FnOnce() + Send>>);

    impl IoExecutor for QueuedExecutor {
        fn dispatch_fn(
            &self,
            _priority: Priority,
            task: impl IoTask<()>,
        ) -> Result<(), ShutdownError> {
            self.0.send(Box::new(task)).map_err(|_| ShutdownError)
        }
    }

    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn tracked_dispatch_covers_submission_disposal_and_retained_results() {
        struct SealDuringDispatch(Arc<FlightTracker>);

        impl IoExecutor for SealDuringDispatch {
            fn dispatch_fn(
                &self,
                priority: Priority,
                task: impl IoTask<()>,
            ) -> Result<(), ShutdownError> {
                assert_eq!(priority, Priority::High);
                self.0.seal();
                task();
                assert!(!self.0.is_idle(), "submission has not returned yet");
                Ok(())
            }
        }

        let flights = Arc::new(FlightTracker::default());
        let executor = SealDuringDispatch(flights.clone());
        TrackedExecutor::new(&executor, &flights)
            .dispatch_fn(Priority::High, || {})
            .unwrap();
        flights.drain().await;
        assert!(
            TrackedExecutor::new(&executor, &flights)
                .dispatch_fn(Priority::High, || panic!("sealed"))
                .is_err()
        );

        struct CapturedResource(Arc<FlightTracker>);
        impl Drop for CapturedResource {
            fn drop(&mut self) {
                assert!(!self.0.is_idle(), "release resources before their flight");
            }
        }

        let flights = Arc::new(FlightTracker::default());
        let (tx, rx) = mpsc::channel();
        let backend = QueuedExecutor(tx);
        let executor = TrackedExecutor::new(&backend, &flights);
        let resource = CapturedResource(flights.clone());
        let observer = executor
            .spawn_fn(Priority::Low, move || drop(resource))
            .unwrap();
        drop(observer);
        assert!(
            !flights.is_idle(),
            "observer cancellation does not complete work"
        );
        rx.try_recv().unwrap()();
        flights.wait_idle().await;
        // An idle observation does not seal the scope.
        let retained = flights.try_acquire().unwrap().into_owned();
        let observer = executor.spawn_fn(Priority::Low, move || retained).unwrap();
        rx.try_recv().unwrap()();
        flights.seal();
        assert!(
            !flights.is_idle(),
            "buffered output owns its explicit flight"
        );
        drop(observer);
        flights.drain().await;

        for execute in [false, true] {
            let flights = Arc::new(FlightTracker::default());
            let executor = TrackedExecutor::new(&backend, &flights);
            let resource = CapturedResource(flights.clone());
            let observer = executor
                .spawn_fn(Priority::Low, move || {
                    let _resource = resource;
                    panic!("interrupted task");
                })
                .unwrap();
            flights.seal();
            let task = rx.try_recv().unwrap();
            if execute {
                assert!(catch_unwind(AssertUnwindSafe(task)).is_err());
            } else {
                // A discarded backend job must still dispose captures before
                // accounting, without certifying operation success.
                drop(task);
            }
            assert!(matches!(observer.await, Err(IoTaskError)));
            flights.drain().await;
        }

        let flights = Arc::new(FlightTracker::default());
        let resource = CapturedResource(flights.clone());
        drop(rx);
        assert!(
            TrackedExecutor::new(&backend, &flights)
                .dispatch_fn(Priority::Low, move || drop(resource))
                .is_err()
        );
        assert!(flights.is_idle(), "rejection disposes the complete task");
    }

    // This operation borrows non-Sync state and is neither Send nor 'static.
    struct BorrowedOp<'a> {
        preparations: &'a Cell<usize>,
        background_runs: &'a Arc<AtomicUsize>,
        drops: &'a Arc<AtomicUsize>,
    }

    impl IoOp for BorrowedOp<'_> {
        type Output = DropProbe;

        fn call(self) -> Self::Output {
            DropProbe(self.drops.clone())
        }

        fn into_task(self) -> impl IoTask<Self::Output> {
            self.preparations.set(self.preparations.get() + 1);
            let background_runs = self.background_runs.clone();
            let drops = self.drops.clone();
            move || {
                // Background-only instrumentation runs at task entry, not when
                // the operation is called inline or prepared for dispatch.
                background_runs.fetch_add(1, Ordering::Relaxed);
                DropProbe(drops)
            }
        }
    }

    #[tokio::test]
    async fn eager_submission_promotes_borrows_and_survives_cancelled_observers() {
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
                .call(),
            );
            assert_eq!(preparations.get(), 0);
            assert_eq!(background_runs.load(Ordering::Relaxed), 0);
            assert_eq!(drops.load(Ordering::Relaxed), 1);

            drop(
                BorrowedOp {
                    preparations: &preparations,
                    background_runs: &background_runs,
                    drops: &drops,
                }
                .into_task(),
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
        }; // Neither the executor nor the operation's borrowed state is retained.
        let task = rx
            .try_recv()
            .expect("submitted without polling the observer");
        drop(observer);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        task();
        assert_eq!(background_runs.load(Ordering::Relaxed), 1);
        assert_eq!(drops.load(Ordering::Relaxed), 2);

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
        assert_eq!(background_runs.load(Ordering::Relaxed), 1);
        rx.try_recv().unwrap()();
        assert_eq!(background_runs.load(Ordering::Relaxed), 2);
        assert_eq!(drops.load(Ordering::Relaxed), 2, "result is buffered");
        drop(observer);
        assert_eq!(drops.load(Ordering::Relaxed), 3);

        let observer = executor
            .spawn_fn(Priority::default(), || Err::<(), _>("operation failed"))
            .unwrap();
        rx.try_recv().unwrap()();
        assert_eq!(observer.await.unwrap(), Err("operation failed"));

        executor
            .dispatch(BorrowedOp {
                preparations: &preparations,
                background_runs: &background_runs,
                drops: &drops,
            })
            .unwrap();
        assert_eq!(preparations.get(), 2);
        assert_eq!(background_runs.load(Ordering::Relaxed), 2);
        assert_eq!(drops.load(Ordering::Relaxed), 3);
        rx.try_recv().unwrap()();
        assert_eq!(background_runs.load(Ordering::Relaxed), 3);
        assert_eq!(drops.load(Ordering::Relaxed), 4, "dispatch discards output");

        drop(rx);
        assert!(matches!(
            executor.spawn(BorrowedOp {
                preparations: &preparations,
                background_runs: &background_runs,
                drops: &drops,
            }),
            Err(ShutdownError)
        ));
        assert_eq!(preparations.get(), 3);
        assert_eq!(background_runs.load(Ordering::Relaxed), 3);
        assert_eq!(drops.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn rejection_is_immediate_and_distinct_from_failure_after_acceptance() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();
        let executor = QueuedExecutor(tx);
        let captured = DropProbe(drops.clone());
        let observer = executor
            .spawn_fn(Priority::default(), move || {
                let _captured = captured;
                panic!("execution failed after acceptance");
            })
            .unwrap();
        assert!(catch_unwind(AssertUnwindSafe(rx.try_recv().unwrap())).is_err());
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert!(matches!(observer.await, Err(IoTaskError)));

        // Closing the test queue rejects further work without running it.
        drop(rx);
        let captured = DropProbe(drops.clone());
        assert!(matches!(
            executor.spawn_fn(Priority::default(), move || {
                drop(captured);
                panic!("rejected work must not execute");
            }),
            Err(ShutdownError)
        ));
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }
}
