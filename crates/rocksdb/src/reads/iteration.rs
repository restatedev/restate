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
    /// [`AdmissionClosed`], so the consumer can tell it apart from a complete
    /// iteration. This includes an iteration that hasn't started reading yet,
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
    /// to `on_item`, which returns how to move the iterator next. Submit it to an
    /// executor, for example with [`IoExecutor::spawn`](restate_core::io::IoExecutor::spawn).
    ///
    /// An `Err` item is the last one: the iteration ends after it, whatever
    /// `on_item` returns. Once the iterator is exhausted, `on_item` gets `None`,
    /// so it can finish up, e.g. flush what it buffered. It doesn't get `None` if
    /// it stopped the iteration itself, or after an `Err` item. The iterator
    /// starts with `initial_action`.
    ///
    /// `on_item` can wait, for example for room in a channel. The worker runs
    /// other tasks meanwhile, so a waiting iteration doesn't hold it. Every so
    /// many items, the iteration also lets the worker's other tasks run.
    ///
    /// The operation resolves to `Ok(())` once the iterator is exhausted or
    /// `on_item` returns [`IterAction::Stop`], and to [`AdmissionClosed`] if
    /// [`LeasePolicy::StopOnSeal`] stopped it. Either way, the iterator and
    /// `on_item` are dropped by then.
    ///
    /// The iteration admits through `gate`, and `lease_policy` decides what
    /// sealing it does (see [`LeasePolicy`]). With [`LeasePolicy::WholeIteration`],
    /// submitting fails if `gate` is sealed.
    ///
    /// A running iteration finishes even if the caller stops waiting for it.
    /// To cancel it then, also while `on_item` waits, make it
    /// [`abortable`](StorageOp::abortable).
    ///
    /// ```ignore
    /// let (iteration, _abort) = iterator
    ///     .prepare_iteration(gate, LeasePolicy::StopOnSeal, Some(IterAction::SeekToFirst), on_item)
    ///     .with_read_perf("scan")
    ///     .abortable();
    /// executor.spawn(iteration)?.await?;
    /// ```
    ///
    /// # Panics
    /// In debug builds, panics if `gate` is outside the iterator's database.
    pub fn prepare_iteration<'g, F>(
        self,
        gate: &'g Arc<Gate>,
        lease_policy: LeasePolicy,
        initial_action: Option<IterAction>,
        on_item: F,
    ) -> StorageOp<'g, Self, Iteration<F>>
    where
        F: AsyncFnMut(Option<Result<(&[u8], &[u8]), RocksError>>) -> IterAction + Send + 'static,
    {
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
                on_item,
                initial_action,
                gate: slice_gate,
            },
        )
    }
}

/// The work of an operation from [`ReadIterator::prepare_iteration`].
pub struct Iteration<F> {
    on_item: F,
    initial_action: Option<IterAction>,
    // Set with `LeasePolicy::StopOnSeal`: the gate to admit each slice through.
    gate: Option<Arc<Gate>>,
}

impl<'a, H, F> OpFn<ReadIterator<'a, H>> for Iteration<F>
where
    H: ReadSource + Send + Sync + 'static,
    F: AsyncFnMut(Option<Result<(&[u8], &[u8]), RocksError>>) -> IterAction + Send + 'static,
{
    type Output = Result<(), AdmissionClosed>;

    async fn run_async(mut self, mut iterator: OwnedReadIterator<H>) -> Self::Output {
        run(
            &mut iterator,
            &mut self.on_item,
            self.initial_action,
            &self.gate,
        )
        .await
    }
}

/// Runs until the iteration is over. Fails if a slice isn't admitted.
async fn run<H, F>(
    iterator: &mut OwnedReadIterator<H>,
    on_item: &mut F,
    initial_action: Option<IterAction>,
    gate: &Option<Arc<Gate>>,
) -> Result<(), AdmissionClosed>
where
    H: ReadSource,
    F: AsyncFnMut(Option<Result<(&[u8], &[u8]), RocksError>>) -> IterAction,
{
    let mut lease = admit(gate)?;
    if let Some(action) = initial_action
        && !apply(iterator, action)
    {
        return Ok(());
    }
    loop {
        for _ in 0..STEPS_PER_SLICE {
            let item = match iterator.item() {
                Ok(Some(item)) => Ok(item),
                // Exhausted: not on an entry, and no error to hand over. The
                // callback finishes up without the lease: it reads nothing.
                Ok(None) => {
                    drop(lease);
                    on_item(None).await;
                    return Ok(());
                }
                Err(error) => Err(error),
            };
            let last = item.is_err();
            let action = {
                let mut action = pin!(on_item(Some(item)));
                match futures::poll!(action.as_mut()) {
                    Poll::Ready(action) => action,
                    // Release the lease while the callback waits, so draining
                    // the gate doesn't wait for a consumer.
                    Poll::Pending => {
                        drop(lease);
                        let action = action.await;
                        lease = admit(gate)?;
                        action
                    }
                }
            };
            // An error is the last item.
            if last || !apply(iterator, action) {
                return Ok(());
            }
        }
        // Let the thread's other tasks run.
        drop(lease);
        yield_now().await;
        lease = admit(gate)?;
    }
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
