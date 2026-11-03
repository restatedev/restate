// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::task::{Context, Poll, Waker, ready};

use futures_util::FutureExt;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::mpsc::{OwnedPermit, Sender};

use restate_core::io::{IoExecutor, IoTaskError};

use crate::{IterAction, RocksDbReadPerfGuard, RocksError, StorageTaskKind};

use super::OwnedReadIterator;

/// The number of iterator ticks per scheduled run on the executor after which
/// the iterator needs to yield and get rescheduled on the executor.
const STEPS_PER_OP: u16 = 128;

/// Receives the items an [`IteratorDriver`] reads. Both methods are called on
/// the executor's threads and must not block.
pub trait IteratorSink {
    /// Room for one item, reserved by [`IteratorSink::poll_ready`].
    type Permit: Send;

    /// Called before every [`IteratorSink::on_item`] call. Returns:
    /// 1. `Poll::Ready(Some(permit))`: the sink can take the next item.
    ///    [`IteratorSink::on_item`] is then called with that permit.
    /// 2. `Poll::Ready(None)`: the sink takes no more items, and the iteration
    ///    ends.
    /// 3. `Poll::Pending`: the sink is full for now. It must wake the waker in
    ///    `ctx` once it has room, which schedules the next step.
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
                ChannelState::Acquiring(mut fut) => match fut.poll_unpin(ctx) {
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

/// Runs an iterator on an [`IoExecutor`], a few steps at a time, handing the
/// items to a sink. Resolves once the iteration is over.
///
/// The iteration is a task whose executor is the [`IoExecutor`]: scheduling it
/// submits one step. A step that leaves room in the sink submits the next one
/// itself, and a sink that was full submits it once it has room again, so the
/// iterator moves whether or not this future is polled.
///
/// Resolves to `Ok(())` once the iterator is exhausted, the sink stops taking
/// items, or the sink returns [`IterAction::Stop`]. It fails with
/// [`IoTaskError`] if a step panicked, or was dropped without running, e.g.
/// because the executor rejected it.
///
/// Dropping the driver cancels the iteration: no step starts after that. The
/// iterator, the sink, and the guard are dropped soon after, usually on the
/// executor, not by the time the drop returns.
pub struct IteratorDriver {
    task: async_task::FallibleTask<()>,
}

impl IteratorDriver {
    /// Starts running `iterator` on `executor`, handing the items to `sink`. The
    /// iterator starts with `initial_action`, and `name` labels the read perf
    /// metrics of each step.
    ///
    /// `guard` is dropped after the iterator and the sink, whether the iteration
    /// completes, fails, or is cancelled. Use it to hold a lease that must
    /// outlive both.
    pub fn spawn<R, S, E, G>(
        name: &'static str,
        iterator: OwnedReadIterator<R>,
        sink: S,
        mut initial_action: Option<IterAction>,
        executor: E,
        guard: G,
    ) -> Self
    where
        R: Send + Sync + 'static,
        S: IteratorSink + Send + 'static,
        E: IoExecutor + Send + Sync + 'static,
        G: Send + 'static,
    {
        // Closure captures drop in an unspecified order, but tuple fields drop in
        // order: the guard goes last, after the iterator and the sink.
        let mut state = (iterator, sink, guard);
        let iteration = std::future::poll_fn(move |cx| {
            let (iterator, sink, _guard) = &mut state;
            loop {
                let Some(permit) = ready!(sink.poll_ready(cx)) else {
                    return Poll::Ready(());
                };
                let _perf = RocksDbReadPerfGuard::new(name);
                match iterator_run_steps(iterator, sink, cx.waker(), permit, initial_action.take())
                {
                    StepResult::Stop => return Poll::Ready(()),
                    // The sink registered this task's waker, which schedules the
                    // next step once the sink has room. Polling it again here
                    // returns `Pending`, unless it already has room.
                    StepResult::NotReady => continue,
                    // Go to the back of the executor's queue, so other work gets a
                    // turn.
                    StepResult::Yielded => {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }
            }
        });

        let schedule = move |runnable: async_task::Runnable| {
            // A rejected runnable is dropped right here, and the future in it
            // with it. The driver then fails with `IoTaskError`.
            let _ = executor.dispatch_fn(
                StorageTaskKind::BackgroundIterator.as_static_str(),
                move || {
                    runnable.run();
                },
            );
        };
        let (runnable, task) = async_task::spawn(iteration, schedule);
        runnable.schedule();
        Self {
            task: task.fallible(),
        }
    }
}

impl Future for IteratorDriver {
    type Output = Result<(), IoTaskError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.task.poll_unpin(cx).map(|done| done.ok_or(IoTaskError))
    }
}

/// How a run of [`iterator_run_steps`] ended.
#[derive(Copy, Clone, Debug)]
pub(super) enum StepResult {
    /// Ran [`STEPS_PER_OP`] steps; the iteration isn't over.
    Yielded,
    /// The sink is full, and will wake the waker once it has room.
    NotReady,
    /// The iteration is over.
    Stop,
}

/// Applies `initial_action`, if any, then hands up to [`STEPS_PER_OP`] items to
/// `sink`, starting with the one `initial_permit` was reserved for.
pub(super) fn iterator_run_steps<H: 'static, S: IteratorSink>(
    iterator: &mut OwnedReadIterator<H>,
    sink: &mut S,
    waker: &Waker,
    initial_permit: S::Permit,
    initial_action: Option<IterAction>,
) -> StepResult {
    if let Some(action) = initial_action
        && !apply(iterator, action)
    {
        return StepResult::Stop;
    }
    let mut permit = initial_permit;

    for step in 1..=STEPS_PER_OP {
        let item = match iterator.item() {
            Ok(Some(i)) => i,
            Ok(None) => return StepResult::Stop,
            Err(e) => {
                sink.on_item(Err(e), permit);
                return StepResult::Stop;
            }
        };

        if !apply(iterator, sink.on_item(Ok(item), permit)) {
            return StepResult::Stop;
        }

        // Don't reserve past the last step: the unused permit would be dropped at
        // the end of the op, together with the sender it holds.
        if step == STEPS_PER_OP {
            break;
        }
        permit = match sink.poll_ready(&mut Context::from_waker(waker)) {
            Poll::Ready(Some(p)) => p,
            Poll::Ready(None) => return StepResult::Stop,
            Poll::Pending => return StepResult::NotReady,
        };
    }
    StepResult::Yielded
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
