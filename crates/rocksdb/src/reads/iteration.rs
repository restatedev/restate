// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use restate_core::io::{AdmissionClosed, Gate, Lease};

use super::{OwnedReadIterator, ReadIterator, ReadSource};
use crate::background::{OpFn, yield_now};
use crate::{IterAction, RocksError, StorageOp, StorageTaskKind};

/// How many items an iteration reads in one slice: it then lets the thread's
/// other tasks run. With [`LeasePolicy::StopOnSeal`], a slice also shares one
/// lease across its reads.
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
    /// iteration immediately.
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

/// A failure while reading rows, admitting a slice, or running a visitor.
#[derive(Debug, thiserror::Error)]
pub enum IterationError<E> {
    #[error(transparent)]
    RocksDb(#[from] RocksError),
    /// The operation was accepted, but its gate closed before a read slice.
    #[error(transparent)]
    ScopeClosed(#[from] AdmissionClosed),
    #[error("iterator visitor failed: {0}")]
    Visitor(#[source] E),
}

/// What happens to an iteration from [`ReadIterator::prepare_iteration`] once
/// its gate is sealed,
/// which decides how long the iteration holds a lease from the gate:
///
/// - [`Self::StopOnSeal`]: the iteration stops early. Use it for reads that
///   are pointless once their owner shuts down, such as a query on a partition
///   that is closing.
/// - [`Self::WholeIteration`]: the iteration runs to completion, and shutting
///   down waits for it. Use it for reads that must not be cut short.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeasePolicy {
    /// The iteration stops once the gate is sealed, and resolves to
    /// [`IterationError::ScopeClosed`], distinguishing interruption from a
    /// complete iteration. This includes an iteration that hasn't started reading yet,
    /// if the gate was sealed already.
    ///
    /// The iteration holds a lease while it reads, and none while the callback
    /// waits (for example for room downstream) or yields, so draining
    /// the gate doesn't wait for a consumer. It takes a new lease to read again,
    /// and stops if the gate was sealed meanwhile. Until then, the iterator and
    /// the callback stay alive.
    StopOnSeal,
    /// The iteration runs to completion even if the gate is sealed meanwhile,
    /// and draining the gate waits until it's done.
    ///
    /// The iteration takes one lease when it's submitted, like any
    /// [`StorageOp`], and holds it until the iterator and the callback are
    /// dropped.
    WholeIteration,
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
    /// with [`AdmissionClosed`] before the executor accepts it. After acceptance,
    /// [`IterationError::ScopeClosed`] means a read slice could not acquire its
    /// lease, [`IterationError::RocksDb`] reports a read failure, and
    /// [`IterationError::Visitor`] preserves the visitor's error. The executor's
    /// separate [`IoTaskError`](restate_core::io::IoTaskError) means accepted work
    /// ended without delivering its result, for example after a panic.
    /// Dropping a cancellable result requests worker cleanup without waiting for it.
    ///
    /// The iteration admits through `gate`, and `lease_policy` decides what
    /// sealing it does (see [`LeasePolicy`]).
    ///
    /// ```ignore
    /// let iteration = iterator
    ///     .prepare_iteration(gate, LeasePolicy::StopOnSeal, Some(IterAction::SeekToFirst), visitor)
    ///     .with_read_perf("scan");
    /// executor.spawn_cancellable(iteration)?.await?;
    /// ```
    ///
    /// # Panics
    /// In debug builds, panics if `gate` is outside the iterator's database.
    pub fn prepare_iteration<'g, V: IteratorVisitor>(
        self,
        gate: &'g Arc<Gate>,
        lease_policy: LeasePolicy,
        initial_action: Option<IterAction>,
        visitor: V,
    ) -> StorageOp<'g, Self, Iteration<V>> {
        self.debug_assert_inside(gate);
        // `WholeIteration` is plain `StorageOp` admission: one lease for the
        // whole task. `StopOnSeal` takes no lease up front: the iteration takes
        // one per slice instead.
        let (op_gate, slice_gate) = match lease_policy {
            LeasePolicy::StopOnSeal => (None, Some(Arc::clone(gate))),
            LeasePolicy::WholeIteration => (Some(gate), None),
        };
        StorageOp::from_parts(
            self,
            op_gate,
            StorageTaskKind::BackgroundIterator,
            Iteration {
                visitor,
                initial_action,
                gate: slice_gate,
            },
        )
    }
}

/// The work of an operation from [`ReadIterator::prepare_iteration`].
pub struct Iteration<V> {
    visitor: V,
    initial_action: Option<IterAction>,
    // Set with `LeasePolicy::StopOnSeal`: the gate to admit each slice through.
    gate: Option<Arc<Gate>>,
}

impl<'a, H, V> OpFn<ReadIterator<'a, H>> for Iteration<V>
where
    H: ReadSource + Send + Sync + 'static,
    V: IteratorVisitor,
{
    type Output = Result<IterationOutcome<V>, IterationError<V::Error>>;

    async fn run_async(mut self, mut iterator: OwnedReadIterator<H>) -> Self::Output {
        let completion = run(
            &mut iterator,
            &mut self.visitor,
            self.initial_action,
            &self.gate,
        )
        .await?;
        Ok(completion.map(|()| self.visitor))
    }
}

/// Reads until the iterator is exhausted or the visitor stops it.
async fn run<H, V>(
    iterator: &mut OwnedReadIterator<H>,
    visitor: &mut V,
    initial_action: Option<IterAction>,
    gate: &Option<Arc<Gate>>,
) -> Result<IterationOutcome<()>, IterationError<V::Error>>
where
    H: ReadSource,
    V: IteratorVisitor,
{
    let mut lease = admit(gate)?;
    if let Some(action) = initial_action
        && !apply(iterator, action)
    {
        return Ok(IterationOutcome::Stopped(()));
    }
    let mut steps = 0;
    while let Some((key, value)) = iterator.item()? {
        // Check the next row before yielding so exhaustion and errors take
        // precedence over re-admission.
        if steps == STEPS_PER_SLICE {
            drop(lease);
            yield_now().await;
            lease = admit(gate)?;
            steps = 0;
        }
        let action = {
            let mut action = pin!(visitor.visit(key, value));
            match futures::poll!(action.as_mut()) {
                Poll::Ready(action) => action.map_err(IterationError::Visitor)?,
                // Release the lease while the callback waits, so draining
                // the gate doesn't wait for a consumer.
                Poll::Pending => {
                    drop(lease);
                    let action = action.await.map_err(IterationError::Visitor)?;
                    if matches!(action, IterAction::Stop) {
                        return Ok(IterationOutcome::Stopped(()));
                    }
                    lease = admit(gate)?;
                    action
                }
            }
        };
        if !apply(iterator, action) {
            return Ok(IterationOutcome::Stopped(()));
        }
        steps += 1;
    }
    Ok(IterationOutcome::Exhausted(()))
}

/// Takes the lease the iteration holds while it reads, with
/// [`LeasePolicy::StopOnSeal`].
fn admit(gate: &Option<Arc<Gate>>) -> Result<Option<Lease<'_>>, AdmissionClosed> {
    gate.as_ref()
        .map(|gate| gate.try_acquire().ok_or(AdmissionClosed))
        .transpose()
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
