// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Physical table reads for in-process readers, independent of logical schemas
//! and Arrow batches.
//!
//! A [`Table`] is a physical table as storage defines it: the **record** type it
//! yields, often bytes borrowed from the iterator, and the prepared **request**
//! type that describes one read, such as a key range with the encoded conditions
//! on it. A request is prepared once and executed in one of three ways:
//!
//! - [`BlockingScan`] runs the whole read on the caller's thread, calling back
//!   for every record; the caller owns scheduling and backpressure.
//! - [`CursorScan`] opens a [`ReadCursor`] driven on the caller's thread. It does
//!   bounded work per step and can move between work submissions.
//! - [`Scan`] runs the read on a query IO worker, lending records to an awaitable
//!   consumer so output waits do not block that worker.
//!
//! A caller driving its own cursor can interleave the read with other work:
//!
//! ```text
//! let mut cursor = scan.scan_cursor(request)?;
//! loop {
//!     match cursor.advance()? {
//!         CursorStep::Record => consume(cursor.current()?), // the next record
//!         CursorStep::Skipped => {}                          // a candidate that did not match
//!         CursorStep::Done => break,
//!     }
//!     // yield to other work here once the budget is spent
//! }
//! ```
//!
//! None of these interfaces knows SQL, logical columns or Arrow; a schema binding
//! ([`crate::SchemaBinding`]) turns the records into logical rows.
//! [`consume_cursor`] drives the bounded cursor with an awaitable typed
//! consumer on an IO worker, yielding between slices even if no records match.
//! [`Scan`] submits that work with the backend's admission and query IO executor.

use std::fmt::Debug;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::task::Poll;

use restate_storage_api::Result;
use restate_storage_api::scan::{RowConsumer, ScanOutcome};

/// A physical table: the records it yields and the prepared request that
/// describes one read of it.
///
/// Storage defines both types. A record may borrow the iterator's bytes or hold
/// decoded values. A request describes physical access only, with no SQL
/// expression or logical selection in it. A table needs no logical schema or
/// Arrow adapter to exist.
pub trait Table: 'static {
    type Record<'a>;
    /// Immutable preparation shared by independent executions. Cursor state is
    /// local to each execution, so reusing a request never reuses its position.
    type Scan: Send + Sync + 'static;
}

/// Executes a prepared read on the caller's thread, including blocking storage
/// I/O and record consumption. The caller owns scheduling and backpressure.
pub trait BlockingScan<T: Table>: Debug + Send + Sync + 'static {
    /// Records borrow storage bytes only for the callback. `Break(Ok(()))` stops
    /// successfully; callback and storage errors fail the read. All storage work
    /// and borrowed resources are finished when this method returns.
    fn scan_blocking(
        &self,
        request: Arc<T::Scan>,
        emit: impl for<'a> FnMut(T::Record<'a>) -> ControlFlow<Result<()>>,
    ) -> Result<()>;
}

/// The outcome of one bounded cursor step. A skipped physical candidate still
/// consumes work: callers can yield even when filtering produces no records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorStep {
    Record,
    Skipped,
    Done,
}

/// Caller-driven, blocking physical reads. A cursor owns its storage lifetime,
/// can move between work submissions, and never dispatches background work.
pub trait ReadCursor<T: Table>: Send + 'static {
    /// Positions at the next candidate. Each call does bounded storage work;
    /// it must not search indefinitely for a matching record.
    fn advance(&mut self) -> Result<CursorStep>;

    /// Borrows the record selected by `advance`. The borrow prevents advancing or
    /// moving the cursor until consumption finishes. Valid only after `Record`.
    fn current(&self) -> Result<T::Record<'_>>;
}

/// Opens an independently positioned cursor over a shared prepared request.
/// Opening, advancing, and reading may block on storage I/O. Scheduling,
/// cancellation-aware output waits, and work budgets belong to the caller.
pub trait CursorScan<T: Table>: Debug + Send + Sync + 'static {
    type Cursor: ReadCursor<T>;

    fn scan_cursor(&self, request: Arc<T::Scan>) -> Result<Self::Cursor>;
}

/// Runs prepared reads on a query-owned IO pool so a consumer can await output
/// room without blocking the calling thread. The backend supplies the read's
/// snapshot and permission to use storage; sharing a pool does not admit a read.
///
/// This physical interface lends storage records to typed consumers. The
/// crate-root [`crate::Scan`] interface serves Arrow batch streams for node-local
/// or global query sources.
pub trait Scan: Debug + Send + Sync + 'static {
    /// The query-owned pool used to run this backend's scans. Engines share it
    /// so storage work runs separately from partition and database IO.
    type Executor: Send + Sync + 'static;

    /// Queues the scan immediately, before the returned future is polled. An IO
    /// worker opens the cursor and lends records to the consumer. The future
    /// reports exhaustion, a consumer-requested stop, or failure.
    ///
    /// Dropping the future releases the scan's holders of the snapshot and its
    /// consumer immediately if no worker has claimed it. Otherwise it asks the
    /// worker to stop; a blocking storage call must return before cleanup runs.
    /// Await the backend's read-completion signal to know cleanup has finished.
    ///
    /// A returned consumer that still owns storage resources must also retain
    /// their snapshot and lease, so storage cannot close while they are in use.
    /// For example, a consumer keeping pinned lookup results must keep their
    /// owning snapshot too. Release those holders before waiting for completion.
    fn scan<T, C>(
        &self,
        executor: &Self::Executor,
        name: &'static str,
        request: Arc<T::Scan>,
        consumer: C,
    ) -> Result<impl Future<Output = Result<ScanOutcome<C>>> + Send + 'static>
    where
        T: Table,
        Self: CursorScan<T>,
        C: for<'a> RowConsumer<Item<'a> = T::Record<'a>>;
}

/// Work steps between cooperative yields: physical candidates (including skips)
/// in [`consume_cursor`], or materialization calls in [`crate::SchemaBinding::read`].
/// These budgets are independent; neither bounds elapsed time within one step.
pub const SCAN_SLICE: usize = 128;

// Suspend once even if all work completed immediately. The IO worker does not
// run a Tokio runtime, so this must not depend on a runtime-specific budget.
pub(crate) async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

/// Drives a physical cursor with an awaitable, borrowed-record consumer.
///
/// Run this future on the query's IO executor: advancing the cursor can block,
/// and the future need not be `Send`. Every [`SCAN_SLICE`] physical candidates,
/// including skipped candidates, it yields to the executor. This bounds visits
/// per slice, not elapsed time or the work inside one consumer call.
///
/// The caller owns admission and cancellation. It can race this future against
/// its cancellation observer; dropping the future releases the cursor and any
/// suspended consumer. Retain admission until that cleanup has finished. On
/// success, the cursor is released before returning the consumer; resources
/// owned by the returned consumer remain the caller's responsibility.
///
/// For example, a consumer can await room in its output channel while borrowing
/// the current record. The cursor cannot advance until that call returns. If
/// the consumer stops after reaching a limit, the result is `Stopped`, so the
/// caller does not open the next cursor in a sequence.
pub async fn consume_cursor<T, C>(
    mut cursor: impl ReadCursor<T>,
    mut consumer: C,
) -> Result<ScanOutcome<C>>
where
    T: Table,
    C: for<'a> RowConsumer<Item<'a> = T::Record<'a>>,
{
    let stopped: Result<bool> = async {
        loop {
            for _ in 0..SCAN_SLICE {
                match cursor.advance()? {
                    CursorStep::Record => {
                        if consumer.consume(cursor.current()?).await?.is_break() {
                            return Ok(true);
                        }
                    }
                    CursorStep::Skipped => {}
                    CursorStep::Done => return Ok(false),
                }
            }
            yield_now().await;
        }
    }
    .await;
    drop(cursor);
    if stopped? {
        Ok(ScanOutcome::Stopped(consumer))
    } else {
        Ok(ScanOutcome::Exhausted(consumer))
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use futures::FutureExt;
    use futures::channel::oneshot;

    use restate_storage_api::StorageError;

    use super::*;

    struct Numbers;

    impl Table for Numbers {
        type Record<'a> = &'a usize;
        type Scan = ();
    }

    struct Cursor {
        position: usize,
        skipped: usize,
        end: usize,
        steps: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }

    impl ReadCursor<Numbers> for Cursor {
        fn advance(&mut self) -> Result<CursorStep> {
            self.steps.fetch_add(1, Ordering::Relaxed);
            self.position += 1;
            Ok(if self.position > self.end {
                CursorStep::Done
            } else if self.position <= self.skipped {
                CursorStep::Skipped
            } else {
                CursorStep::Record
            })
        }

        fn current(&self) -> Result<&usize> {
            Ok(&self.position)
        }
    }

    impl Drop for Cursor {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }

    #[derive(Default)]
    struct Consumer {
        seen: Vec<usize>,
        stop: bool,
        fail: bool,
        wait: Option<oneshot::Receiver<()>>,
        dropped: DropFlag,
    }

    impl RowConsumer for Consumer {
        type Item<'a> = &'a usize;

        async fn consume(&mut self, row: Self::Item<'_>) -> Result<ControlFlow<()>> {
            // Both the row borrow and a non-Send local survive the output wait.
            let before = Rc::new(*row);
            if let Some(wait) = self.wait.take() {
                wait.await.unwrap();
            }
            assert_eq!(*before, *row);
            if self.fail {
                return Err(StorageError::DataIntegrityError);
            }
            self.seen.push(*row);
            Ok(if self.stop {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            })
        }
    }

    #[derive(Default)]
    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    fn numbers(skipped: usize, end: usize) -> Cursor {
        Cursor {
            position: 0,
            skipped,
            end,
            steps: Arc::default(),
            dropped: Arc::default(),
        }
    }

    #[test]
    fn skipped_candidates_yield_and_completion_preserves_its_reason() {
        let cursor = numbers(SCAN_SLICE, SCAN_SLICE + 2);
        let steps = Arc::clone(&cursor.steps);
        let dropped = Arc::clone(&cursor.dropped);
        let mut scan = Box::pin(consume_cursor::<Numbers, _>(cursor, Consumer::default()));
        assert!(scan.as_mut().now_or_never().is_none());
        assert_eq!(steps.load(Ordering::Relaxed), SCAN_SLICE);
        assert!(!dropped.load(Ordering::Relaxed));
        let outcome = futures::executor::block_on(scan).unwrap();
        assert!(dropped.load(Ordering::Relaxed));
        assert!(matches!(outcome, ScanOutcome::Exhausted(_)));
        assert_eq!(outcome.into_inner().seen, [SCAN_SLICE + 1, SCAN_SLICE + 2]);

        let cursor = numbers(0, 3);
        let steps = Arc::clone(&cursor.steps);
        let outcome = futures::executor::block_on(consume_cursor::<Numbers, _>(
            cursor,
            Consumer {
                stop: true,
                ..Consumer::default()
            },
        ))
        .unwrap();
        assert!(matches!(outcome, ScanOutcome::Stopped(_)));
        assert_eq!(outcome.into_inner().seen, [1]);
        assert_eq!(steps.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn suspended_borrows_resume_or_drop_and_errors_release_resources() {
        for cancel in [false, true] {
            let cursor = numbers(0, 1);
            let steps = Arc::clone(&cursor.steps);
            let cursor_dropped = Arc::clone(&cursor.dropped);
            let (release, wait) = oneshot::channel();
            let consumer = Consumer {
                wait: Some(wait),
                ..Consumer::default()
            };
            let consumer_dropped = Arc::clone(&consumer.dropped.0);
            let mut scan = Box::pin(consume_cursor::<Numbers, _>(cursor, consumer));
            assert!(scan.as_mut().now_or_never().is_none());
            assert_eq!(steps.load(Ordering::Relaxed), 1);
            assert!(!cursor_dropped.load(Ordering::Relaxed));
            if cancel {
                drop(scan);
            } else {
                release.send(()).unwrap();
                let outcome = futures::executor::block_on(scan).unwrap();
                assert!(cursor_dropped.load(Ordering::Relaxed));
                assert!(!consumer_dropped.load(Ordering::Relaxed));
                assert_eq!(outcome.into_inner().seen, [1]);
            }
            assert!(cursor_dropped.load(Ordering::Relaxed));
            assert!(consumer_dropped.load(Ordering::Relaxed));
        }

        let cursor = numbers(0, 2);
        let cursor_dropped = Arc::clone(&cursor.dropped);
        let consumer = Consumer {
            fail: true,
            ..Consumer::default()
        };
        let consumer_dropped = Arc::clone(&consumer.dropped.0);
        let result = futures::executor::block_on(consume_cursor::<Numbers, _>(cursor, consumer));
        assert!(matches!(result, Err(StorageError::DataIntegrityError)));
        assert!(cursor_dropped.load(Ordering::Relaxed));
        assert!(consumer_dropped.load(Ordering::Relaxed));
    }
}
