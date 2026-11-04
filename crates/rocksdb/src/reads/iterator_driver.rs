// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU16;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::mpsc::{OwnedPermit, Sender};

use restate_core::io::{AdmissionClosed, Gate, IoExecutor, IoTaskError, TrackedTask};

use crate::{IterAction, RocksDbReadPerfGuard, RocksError, StorageTaskKind};

use super::OwnedReadIterator;

/// The default number of items an [`IteratorDriver`] hands to its sink in one
/// slice, before it yields the executor's thread and gets rescheduled.
pub const DEFAULT_STEPS_PER_SLICE: NonZeroU16 = NonZeroU16::new(128).unwrap();

/// How long an [`IteratorDriver`] holds a lease from its gate, which decides
/// what happens to the iteration once the gate is sealed:
///
/// - [`Self::PerSlice`]: the iteration stops early. Use it for reads that
///   are pointless once their owner shuts down, such as a query on a partition
///   that is closing.
/// - [`Self::WholeIteration`]: the iteration runs to completion, and shutting
///   down waits for it. Use it for reads that must not be cut short.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeasePolicy {
    /// The iteration stops at its next slice once the gate is sealed: that
    /// slice is rejected, and the driver fails with [`IoTaskError`], so the
    /// consumer can tell it apart from a complete iteration. This includes the
    /// first slice, if the gate was sealed already.
    ///
    /// Each slice holds a lease from the gate while it's queued or running.
    /// Between slices, while the iterator waits for room in the sink, the
    /// iteration holds no lease, so draining the gate doesn't wait for it. If
    /// the gate is sealed meanwhile, the iterator and the sink are only dropped
    /// once the sink has room again and the next slice is rejected, or once
    /// the driver is dropped.
    PerSlice,
    /// The iteration runs to completion even if the gate is sealed meanwhile,
    /// and draining the gate waits until it's done.
    ///
    /// The iteration acquires one lease when it starts, and holds it until the
    /// iterator and the sink are dropped. Slices aren't checked against the
    /// gate.
    WholeIteration,
}

/// Receives the items an [`IteratorDriver`] reads. All methods are called on
/// the executor's threads and must not block.
pub trait IteratorSink {
    /// Room for one item, reserved by [`IteratorSink::poll_ready`].
    type Permit;

    /// Called before every [`IteratorSink::on_item`] call. Returns:
    /// 1. `Poll::Ready(Some(permit))`: the sink can take the next item.
    ///    [`IteratorSink::on_item`] is then called with that permit.
    /// 2. `Poll::Ready(None)`: the sink takes no more items, and the iteration
    ///    ends.
    /// 3. `Poll::Pending`: the sink is full for now. It must wake the waker in
    ///    `ctx` once it has room, which schedules the next slice.
    fn poll_ready(&mut self, ctx: &mut Context<'_>) -> Poll<Option<Self::Permit>>;

    /// Handles one item, and returns how to move the iterator next. An `Err`
    /// item is the last one: the iteration ends after it, whatever this returns.
    fn on_item(
        &mut self,
        item: Result<(&[u8], &[u8]), RocksError>,
        permit: Self::Permit,
    ) -> IterAction;
}

/// Any non-blocking FnMut can act as an iterator sink.
impl<T> IteratorSink for T
where
    T: FnMut(Result<(&[u8], &[u8]), RocksError>) -> IterAction,
{
    type Permit = ();

    fn poll_ready(&mut self, _ctx: &mut Context<'_>) -> Poll<Option<()>> {
        Poll::Ready(Some(()))
    }

    fn on_item(&mut self, item: Result<(&[u8], &[u8]), RocksError>, _permit: ()) -> IterAction {
        (self)(item)
    }
}

type ReserveFuture<T> = Pin<Box<dyn Future<Output = Result<OwnedPermit<T>, SendError<()>>> + Send>>;

enum ChannelState<T> {
    Idle(Sender<T>),
    /// Waiting for capacity; only entered when the channel is full.
    Acquiring(ReserveFuture<T>),
    /// The sender is lent out to an outstanding permit, or the channel is closed.
    Taken,
}

/// A sink that maps each item with a function and sends the result to a bounded
/// channel. While the channel is full, the iterator waits without holding an
/// executor thread. The iteration ends once the receiver is dropped.
pub struct ChannelSink<T, F> {
    on_item: F,
    state: ChannelState<T>,
}

impl<T, F> ChannelSink<T, F>
where
    F: FnMut(Result<(&[u8], &[u8]), RocksError>) -> (Option<T>, IterAction),
{
    /// `on_item` maps each item to a value to send, if any, and the next
    /// action for the iterator.
    pub fn new(tx: Sender<T>, on_item: F) -> Self {
        Self {
            on_item,
            state: ChannelState::Idle(tx),
        }
    }
}

impl<T, F> IteratorSink for ChannelSink<T, F>
where
    T: Send + 'static,
    F: FnMut(Result<(&[u8], &[u8]), RocksError>) -> (Option<T>, IterAction),
{
    type Permit = OwnedPermit<T>;

    fn poll_ready(&mut self, ctx: &mut Context<'_>) -> Poll<Option<Self::Permit>> {
        loop {
            match std::mem::replace(&mut self.state, ChannelState::Taken) {
                // Fast path: reserve without allocating while the channel has capacity.
                ChannelState::Idle(tx) => match tx.try_reserve_owned() {
                    Ok(permit) => return Poll::Ready(Some(permit)),
                    Err(TrySendError::Full(tx)) => {
                        self.state = ChannelState::Acquiring(Box::pin(tx.reserve_owned()));
                    }
                    Err(TrySendError::Closed(_)) => return Poll::Ready(None),
                },
                ChannelState::Acquiring(mut fut) => match fut.as_mut().poll(ctx) {
                    Poll::Ready(permit) => return Poll::Ready(permit.ok()),
                    Poll::Pending => {
                        self.state = ChannelState::Acquiring(fut);
                        return Poll::Pending;
                    }
                },
                // The channel is closed, or a permit is outstanding (a caller bug).
                ChannelState::Taken => return Poll::Ready(None),
            }
        }
    }

    fn on_item(
        &mut self,
        item: Result<(&[u8], &[u8]), RocksError>,
        permit: Self::Permit,
    ) -> IterAction {
        let (to_send, next_action) = (self.on_item)(item);

        let sender = if let Some(to_send) = to_send {
            permit.send(to_send)
        } else {
            permit.release()
        };
        self.state = ChannelState::Idle(sender);

        next_action
    }
}

/// Runs an iterator on an [`IoExecutor`] in slices, handing the items to a
/// sink. Each slice drives the iterator `steps_per_slice` steps. Resolves
/// once the iteration is over.
///
/// The iteration is a task whose executor is the [`IoExecutor`]: scheduling it
/// submits one slice, which hands a bounded number of items to the sink and then
/// yields to let other IO work run on the executor. If the sink is not ready
/// the iterator gets parked until the sink is ready again.
///
/// Resolves to `Ok(sink)` once the iterator is exhausted, the sink stops taking
/// items, or the sink returns [`IterAction::Stop`]. The iterator and the
/// iteration's lease, if any, are dropped by then. It fails with
/// [`IoTaskError`] if a slice panicked, or was dropped without running, e.g.
/// because the executor rejected it.
///
/// Dropping the driver cancels the iteration: no slice starts after that. The
/// iterator, the sink, and the iteration's lease, if any, are dropped soon
/// after, usually on the executor, not by the time the drop returns.
pub struct IteratorDriver<S> {
    task: async_task::FallibleTask<S>,
}

impl<S: IteratorSink + Send + 'static> IteratorDriver<S> {
    /// Starts running `iterator` on `executor`, handing the items to `sink`. The
    /// iterator starts with `initial_action`, and `name` labels the read perf
    /// metrics of each slice. Each slice hands at most `steps_per_slice` items
    /// to the sink before it yields the executor's thread.
    ///
    /// The iteration admits through the passed `gate`. `lease_policy` defines
    /// the interactions between the gate getting sealed and the driver (see [`LeasePolicy`]).
    ///
    /// # Errors
    /// With [`LeasePolicy::WholeIteration`], fails with [`AdmissionClosed`] if
    /// `gate` is sealed. With [`LeasePolicy::PerSlice`], admission is checked at
    /// each slice instead, so this doesn't fail.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn<R, E>(
        name: &'static str,
        iterator: OwnedReadIterator<R>,
        sink: S,
        mut initial_action: Option<IterAction>,
        steps_per_slice: NonZeroU16,
        gate: &Arc<Gate>,
        lease_policy: LeasePolicy,
        executor: E,
    ) -> Result<Self, AdmissionClosed>
    where
        R: Send + Sync + 'static,
        E: IoExecutor + Send + Sync + 'static,
    {
        // When the lease policy is `WholeIteration` we hold one lease in the beginning
        // and keep alive for the lifetime of the driver. If the policy is `PerSlice`,
        // every slice is admitted through the gate when its about to run.
        let (run_gate, iteration_lease) = match lease_policy {
            LeasePolicy::PerSlice => (Some(Arc::clone(gate)), None),
            LeasePolicy::WholeIteration => (
                None,
                Some(gate.try_acquire().ok_or(AdmissionClosed)?.into_owned()),
            ),
        };
        // Closure captures drop in an unspecified order, but tuple fields drop in
        // order: the lease goes last, after the iterator and the sink.
        let mut state = Some((iterator, sink, iteration_lease));
        let iteration = std::future::poll_fn(move |cx| {
            let (iterator, sink, _lease) = state.as_mut().expect("polled after completion");
            let _perf = RocksDbReadPerfGuard::new(name);
            match iterator_run_slice(iterator, sink, cx, initial_action.take(), steps_per_slice) {
                SliceResult::Stop => {}
                // The sink registered this task's waker, which schedules the
                // next slice once the sink has room.
                SliceResult::NotReady => return Poll::Pending,
                // Go to the back of the executor's queue, so other work gets a
                // turn.
                SliceResult::Yielded => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
            // Release the iterator, then the lease, and hand the sink back.
            let (iterator, sink, lease) = state.take().expect("polled after completion");
            drop(iterator);
            drop(lease);
            Poll::Ready(sink)
        });

        // Slices are scheduled from whichever thread wakes the task: the previous
        // slice (nesting each slice's span in the last one's) or the sink's
        // consumer. Dispatch them all under the span current at spawn instead.
        let span = tracing::Span::current();
        let schedule = move |runnable: async_task::Runnable| {
            // A rejected runnable is dropped right here, and the future in it
            // with it. The driver then fails with `IoTaskError`.
            let lease = match &run_gate {
                Some(gate) => match gate.try_acquire() {
                    Some(lease) => Some(lease.into_owned()),
                    None => return,
                },
                None => None,
            };
            // The lease is released after the slice, or after the runnable if the
            // executor drops it without running it.
            let _ = span.in_scope(|| {
                executor.dispatch_fn(
                    StorageTaskKind::BackgroundIterator.as_static_str(),
                    TrackedTask::new(
                        move || {
                            runnable.run();
                        },
                        lease,
                    ),
                )
            });
        };
        let (runnable, task) = async_task::spawn(iteration, schedule);
        runnable.schedule();
        Ok(Self {
            task: task.fallible(),
        })
    }
}

impl<S> Future for IteratorDriver<S> {
    type Output = Result<S, IoTaskError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.task)
            .poll(cx)
            .map(|done| done.ok_or(IoTaskError))
    }
}

/// How a slice ([`iterator_run_slice`]) ended.
#[derive(Copy, Clone, Debug)]
pub(super) enum SliceResult {
    /// Ran `steps_per_slice` steps; the iteration isn't over.
    Yielded,
    /// The sink is full, and will wake the waker once it has room.
    NotReady,
    /// The iteration is over.
    Stop,
}

/// Applies `initial_action`, if any, then hands up to `steps_per_slice` items to
/// `sink`. The sink is only asked for room once the iterator is known to have
/// an entry or an error for it, so an exhausted iterator stops even while the
/// sink is full.
pub(super) fn iterator_run_slice<H, S: IteratorSink>(
    iterator: &mut OwnedReadIterator<H>,
    sink: &mut S,
    cx: &mut Context<'_>,
    initial_action: Option<IterAction>,
    steps_per_slice: NonZeroU16,
) -> SliceResult {
    if let Some(action) = initial_action
        && !apply(iterator, action)
    {
        return SliceResult::Stop;
    }

    for _ in 0..steps_per_slice.get() {
        // Exhausted: not on an entry, and no error to hand to the sink.
        if !iterator.valid() && iterator.status().is_ok() {
            return SliceResult::Stop;
        }
        let permit = match sink.poll_ready(cx) {
            Poll::Ready(Some(p)) => p,
            Poll::Ready(None) => return SliceResult::Stop,
            Poll::Pending => return SliceResult::NotReady,
        };
        match iterator.item() {
            Ok(Some(item)) => {
                if !apply(iterator, sink.on_item(Ok(item), permit)) {
                    return SliceResult::Stop;
                }
            }
            // Unreachable after the check above.
            Ok(None) => return SliceResult::Stop,
            // An error is the last item.
            Err(e) => {
                sink.on_item(Err(e), permit);
                return SliceResult::Stop;
            }
        }
    }
    SliceResult::Yielded
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
