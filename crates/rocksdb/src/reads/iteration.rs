// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use restate_core::io::Gate;

use super::{OwnedReadIterator, ReadIterator, ReadSource};
use crate::background::{OpFn, yield_now};
use crate::{Cancellable, Cooperative, IterAction, RocksError, StorageOp, StorageTaskKind};

/// How many items an iteration reads in one slice: it then lets the thread's
/// other tasks run.
const STEPS_PER_SLICE: usize = 128;

/// Consumes an iterator's borrowed rows on a storage worker.
///
/// The iterator stays on the current row until [`Self::visit`] completes, so
/// the visitor can borrow its key and value across awaits without copying them.
/// Its futures need not be `Send`: they stay on the same worker. The iteration
/// hands the visitor back once it ends without an error.
pub trait IteratorVisitor: Send + Sized + 'static {
    type Error: Send + 'static;

    /// Processes a row and chooses where to move next. An error fails the
    /// iteration immediately. Gate cancellation can drop a suspended visit;
    /// any resources it holds must be released when its future is dropped.
    fn visit(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> impl Future<Output = Result<IterAction, Self::Error>>;
}

impl<F, E> IteratorVisitor for F
where
    F: AsyncFnMut(&[u8], &[u8]) -> Result<IterAction, E> + Send + 'static,
    E: Send + 'static,
{
    type Error = E;

    async fn visit(&mut self, key: &[u8], value: &[u8]) -> Result<IterAction, E> {
        self(key, value).await
    }
}

/// A successful iteration, including why it ended.
#[derive(Debug)]
pub enum IterationOutcome<V> {
    /// No rows remain within the iterator's bounds.
    Exhausted(V),
    /// The initial action or visitor requested [`IterAction::Stop`].
    Stopped(V),
}

impl<V> IterationOutcome<V> {
    /// Transforms the returned value without changing the completion reason.
    pub fn map<T>(self, f: impl FnOnce(V) -> T) -> IterationOutcome<T> {
        match self {
            Self::Exhausted(value) => IterationOutcome::Exhausted(f(value)),
            Self::Stopped(value) => IterationOutcome::Stopped(f(value)),
        }
    }
}

/// A failure while reading rows or running a visitor.
#[derive(Debug, thiserror::Error)]
pub enum IterationError<E> {
    /// The gate's owner requested early termination after admission.
    #[error("iteration cancelled")]
    Cancelled,
    #[error(transparent)]
    RocksDb(#[from] RocksError),
    #[error("iterator visitor failed: {0}")]
    Visitor(#[source] E),
}

/// How [`RocksDb::prepare_iteration`](crate::RocksDb::prepare_iteration) runs
/// an iteration.
#[derive(Clone)]
pub struct IterationOptions {
    /// The iterator's first move.
    pub initial_action: IterAction,
}

impl<'a, H> ReadIterator<'a, H>
where
    H: ReadSource + Send + Sync + 'static,
{
    /// Prepares running the iterator as one async operation, handing each item
    /// to `visitor`, which returns how to move the iterator next. Submit it to an
    /// executor, for example with [`IoExecutor::spawn`](restate_core::io::IoExecutor::spawn).
    ///
    /// Storage and visitor errors fail the operation directly. The iterator
    /// starts with `initial_action`.
    ///
    /// The visitor can wait, for example for room in a channel. The worker runs
    /// other tasks meanwhile, so a waiting iteration doesn't hold it. Every so
    /// many items, the iteration also lets the worker's other tasks run.
    ///
    /// The operation returns [`IterationOutcome::Exhausted`] when no rows remain,
    /// or [`IterationOutcome::Stopped`] for [`IterAction::Stop`]. Both contain the
    /// visitor. The iterator is dropped before either result is delivered.
    ///
    /// # Errors
    /// Preparing this operation does not acquire admission. Submission can fail
    /// with [`AdmissionClosed`](restate_core::io::AdmissionClosed) before the
    /// executor accepts it. After acceptance, [`IterationError::RocksDb`]
    /// reports a read failure, and [`IterationError::Visitor`] preserves the
    /// visitor's error. [`IterationError::Cancelled`] reports a gate owner's
    /// request to stop after admission. The executor's separate
    /// [`IoTaskError`](restate_core::io::IoTaskError) means accepted work ended
    /// without delivering its result, for example after a panic. Dropping a
    /// cancellable result requests worker cleanup without waiting for it.
    ///
    /// The iteration admits through `gate` at submission and holds its lease
    /// through cleanup. Sealing lets it finish; cancelling the gate stops it at
    /// a slice boundary or while its visitor is suspended. The visitor must
    /// be safe to drop while suspended. A slice bounds the number of visits,
    /// not elapsed time: native calls and synchronous visitor work cannot be
    /// interrupted.
    ///
    /// ```ignore
    /// let iteration = iterator
    ///     .prepare_iteration(gate, Some(IterAction::SeekToFirst), visitor)
    ///     .with_read_perf("scan");
    /// executor.spawn_cancellable(iteration)?.await?;
    /// ```
    ///
    /// # Panics
    /// In debug builds, panics if `gate` is outside the iterator's database.
    pub fn prepare_iteration<'g, V: IteratorVisitor>(
        self,
        gate: &'g Arc<Gate>,
        initial_action: Option<IterAction>,
        visitor: V,
    ) -> StorageOp<'g, Cooperative<Self>, Iteration<V>> {
        self.debug_assert_inside(gate);
        StorageOp::from_parts(
            Cooperative(self),
            gate,
            StorageTaskKind::BackgroundIterator,
            Iteration {
                visitor,
                initial_action,
            },
        )
    }
}

/// The work of an operation from [`ReadIterator::prepare_iteration`].
pub struct Iteration<V> {
    visitor: V,
    initial_action: Option<IterAction>,
}

impl<'a, H, V> OpFn<Cooperative<ReadIterator<'a, H>>> for Iteration<V>
where
    H: ReadSource + Send + Sync + 'static,
    V: IteratorVisitor,
{
    type Output = Result<IterationOutcome<V>, IterationError<V::Error>>;

    async fn run_async(
        mut self,
        Cancellable {
            context: mut iterator,
            cancelled,
        }: Cancellable<OwnedReadIterator<H>>,
    ) -> Self::Output {
        // Check cancellation before each poll, including after a slice yield
        // or while a visitor is suspended, rather than locking for every row.
        let completion = tokio::select! {
            biased;
            _ = cancelled.cancelled() => return Err(IterationError::Cancelled),
            completion = run(&mut iterator, &mut self.visitor, self.initial_action) => completion?,
        };
        Ok(completion.map(|()| self.visitor))
    }
}

/// Reads until the iterator is exhausted or the visitor stops it.
async fn run<H, V>(
    iterator: &mut OwnedReadIterator<H>,
    visitor: &mut V,
    initial_action: Option<IterAction>,
) -> Result<IterationOutcome<()>, IterationError<V::Error>>
where
    H: ReadSource,
    V: IteratorVisitor,
{
    if let Some(action) = initial_action
        && !apply(iterator, action)
    {
        return Ok(IterationOutcome::Stopped(()));
    }
    let mut steps = 0;
    while let Some((key, value)) = iterator.item()? {
        // Check the next row before yielding, so a finished iteration returns
        // without yielding once more.
        if steps == STEPS_PER_SLICE {
            yield_now().await;
            steps = 0;
        }
        let action = visitor
            .visit(key, value)
            .await
            .map_err(IterationError::Visitor)?;
        if !apply(iterator, action) {
            return Ok(IterationOutcome::Stopped(()));
        }
        steps += 1;
    }
    Ok(IterationOutcome::Exhausted(()))
}

/// Moves `iterator` as `action` says. Returns `false` for [`IterAction::Stop`].
fn apply<H>(iterator: &mut OwnedReadIterator<H>, action: IterAction) -> bool {
    match action {
        IterAction::Seek(b) => iterator.seek(b.as_ref()),
        IterAction::Prev => iterator.prev(),
        IterAction::Next => iterator.next(),
        IterAction::SeekToFirst => iterator.seek_to_first(),
        IterAction::SeekToLast => iterator.seek_to_last(),
        IterAction::Stop => return false,
    }
    true
}
