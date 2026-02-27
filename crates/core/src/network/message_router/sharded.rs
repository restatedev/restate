// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Poll, ready};

use futures::Stream;
use metrics::{Counter, counter};
use tokio::sync::{mpsc, oneshot};

use restate_memory::{MemoryLease, MemoryPool};
use restate_types::net::Service;

use super::shard_map::Shards;
use super::{BackPressureMode, RawSender, ServiceStream};
use crate::network::metric_definitions::NETWORK_SERVICE_ACCEPTED_REQUEST_BYTES;
use crate::network::{RouterError, ShardRegistrationError, Verdict};

/// A handle to a sharded network service.
///
/// Sharded network services run independent shards for each 'sort-code'.
///
/// Owners of sharded services can create new shards via two mechanisms:
///   1. Upon receiving a message with a sort-code that is not registered yet, the service
///      owner will receive a [`ShardControlMessage::RegisterSortCode`] message on the [`ShardControlStream`]
///      with the sort-code in question. The service owner can then do the necessary setup
///      and return a [`ShardSender`] via [`ShardControlMessage::RegisterSortCode::decision`].
///   2. By calling [`ControlServiceShards::force_register_sort_code`]. This will register a new
///      shard immediately and unconditionally.
pub struct Sharded<S> {
    rx: ShardControlReceiver<S>,
    shards: Shards,
}

impl<S: Service> Sharded<S> {
    pub(crate) const fn new(rx: ShardControlReceiver<S>, shards: Shards) -> Self {
        Self { rx, shards }
    }

    /// Starts accepting messages and returns the control handles.
    ///
    /// Returns a pair of:
    /// - [`ShardControlStream`] — a stream of [`ShardControlMessage`] that the service owner
    ///   polls to handle on-demand shard registration requests from the message router. Each
    ///   message carries a sort-code and a [`ShardRegistrationDecision`] that the owner must
    ///   resolve (accept or fail).
    /// - [`ControlServiceShards`] — a handle for imperative shard management
    ///   (force-register / unregister) outside the request-driven flow.
    ///
    /// After this call, the message router will begin routing incoming messages to
    /// registered shards and will send registration requests for unknown sort-codes.
    pub fn start(self) -> (ShardControlStream<S>, ControlServiceShards<S>) {
        let Self { rx, shards } = self;
        let stream = rx.start();

        (stream, ControlServiceShards::new(shards))
    }

    /// Splits into the raw receiver and the shard control handle **without** starting.
    ///
    /// Use this when you need to defer calling
    /// [`ShardControlReceiver::start`] (e.g. to pass the receiver to another task
    /// that will start it later). Messages sent before `start()` is called will be
    /// rejected with [`RouterError::ServiceNotReady`].
    pub fn split(self) -> (ShardControlReceiver<S>, ControlServiceShards<S>) {
        (self.rx, ControlServiceShards::new(self.shards))
    }

    /// Takes ownership of the inner value, replacing `self` with a closed default.
    ///
    /// The returned [`Sharded`] retains the original receiver and shard map.
    /// `self` is left in a default (closed) state — any messages routed to it will
    /// be rejected.
    ///
    /// This is useful when a component needs to move the receiver into a subtask
    /// while keeping the field in the parent struct intact.
    pub fn take(&mut self) -> Sharded<S> {
        std::mem::take(self)
    }
}

impl<S: Service> Default for Sharded<S> {
    fn default() -> Self {
        Self::new(ShardControlReceiver::default(), Shards::default())
    }
}

/// A one-shot decision handle for an on-demand shard registration request.
///
/// The message router produces this handle when it encounters a sort-code that
/// has no registered shard yet. The service owner must resolve it by calling
/// either [`accept`](Self::accept) or [`fail`](Self::fail). Dropping the handle
/// without calling either method is equivalent to a failure — the router will
/// treat the registration as cancelled and may retry on the next message.
pub struct ShardRegistrationDecision<S> {
    inner: oneshot::Sender<Result<RawSender, ShardRegistrationError>>,
    _phantom: PhantomData<S>,
}

impl<S: Service> ShardRegistrationDecision<S> {
    pub(crate) const fn new(
        inner: oneshot::Sender<Result<RawSender, ShardRegistrationError>>,
    ) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }

    /// Accepts the registration and provides a [`ShardSender`] that the router
    /// will use to deliver subsequent messages with this sort-code.
    pub fn accept(self, sender: ShardSender<S>) {
        let _ = self.inner.send(Ok(sender.into_raw()));
    }

    /// Rejects the registration with a [`Verdict`].
    ///
    /// The verdict is propagated back to the caller:
    /// - For RPC calls, it is sent as the reply status.
    /// - For unary messages, it is translated into a routing error.
    pub fn fail(self, verdict: Verdict) {
        let _ = self
            .inner
            .send(Err(ShardRegistrationError::Verdict(verdict)));
    }
}

/// Imperative shard management handle for a sharded service.
///
/// This is the counterpart to the request-driven [`ShardControlStream`]: while
/// the stream lets the router ask the service to create shards on demand, this
/// handle lets the service proactively register or unregister shards at any
/// time (e.g. during startup to pre-warm known shards, or during shutdown to
/// tear them down).
///
/// Obtained from [`Sharded::start`] or [`Sharded::split`].
pub struct ControlServiceShards<S> {
    shards: Shards,
    _phantom: std::marker::PhantomData<S>,
}

impl<S: Service> ControlServiceShards<S> {
    pub(crate) fn new(shards: Shards) -> Self {
        Self {
            shards,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn force_register_sort_code(&self, sort_code: u64, sender: ShardSender<S>) {
        self.shards
            .force_register(S::TAG, sort_code, sender.into_raw());
    }

    pub fn force_unregister_sort_code(&self, sort_code: u64) {
        let _ = self.shards.remove(S::TAG, sort_code);
    }
}

/// A typed sender for delivering messages to a single shard of a sharded service.
///
/// Created via [`ShardSender::new`], which returns a `(ShardSender, ServiceStream)`
/// pair. The [`ServiceStream`] is the receiving end that the shard's worker task
/// polls for incoming messages.
///
/// A `ShardSender` is handed to the router — either through
/// [`ShardRegistrationDecision::accept`] (on-demand) or
/// [`ControlServiceShards::force_register_sort_code`] (imperative) — so the
/// router can deliver messages for the corresponding sort-code.
///
/// Cloning a `ShardSender` produces another handle to the **same** underlying
/// channel.
pub struct ShardSender<S> {
    raw_sender: RawSender,
    _phantom: PhantomData<S>,
}

impl<S: Service> ShardSender<S> {
    pub fn new() -> (Self, ServiceStream<S>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let raw_sender = RawSender(sender);
        (
            Self {
                raw_sender,
                _phantom: PhantomData,
            },
            ServiceStream {
                inner: receiver,
                _marker: std::marker::PhantomData,
            },
        )
    }

    pub(crate) fn into_raw(self) -> RawSender {
        self.raw_sender
    }

    /// Sends a service message directly through this shard sender.
    ///
    /// This is intended for use in tests and benchmarks where messages are sent
    /// directly to a loglet worker without going through the full message router.
    #[cfg(feature = "message-util")]
    pub fn send(&self, msg: super::ServiceMessage<S>) {
        use super::ServiceOp;
        let op = match msg {
            super::ServiceMessage::Rpc(i) => ServiceOp::CallRpc(i.into_raw_rpc()),
            super::ServiceMessage::Watch(i) => ServiceOp::Watch(i.into_raw_watch()),
            super::ServiceMessage::Unary(i) => ServiceOp::Unary(i.into_raw_unary()),
        };
        let _ = self.raw_sender.send(op);
    }
}

impl<S> Clone for ShardSender<S> {
    fn clone(&self) -> Self {
        Self {
            raw_sender: self.raw_sender.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S> std::fmt::Debug for ShardSender<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardSender").finish_non_exhaustive()
    }
}

/// A stream of shard control messages for a sharded service.
///
/// The service owner polls this stream (typically inside a `tokio::select!`
/// loop) to handle on-demand shard registration requests from the message
/// router. Each item is a [`ShardControlMessage`] that must be resolved before
/// the router can deliver messages for that sort-code.
///
/// The stream terminates (`None`) when the router side is dropped (e.g. during
/// shutdown).
pub struct ShardControlStream<S> {
    inner: mpsc::UnboundedReceiver<ShardControlOp>,
    _marker: std::marker::PhantomData<S>,
}

impl<S: Service> ShardControlStream<S> {
    fn new(receiver: mpsc::UnboundedReceiver<ShardControlOp>) -> Self {
        Self {
            inner: receiver,
            _marker: std::marker::PhantomData,
        }
    }

    /// Closes the receiving end of the service handler
    ///
    /// This prevents any further messages from being sent on the inner channel while
    /// still enabling the receiver to drain messages that are buffered.
    ///
    /// Peers that attempt to send RPC calls to this service will receive an error indicating that
    /// this service has been stopped.
    ///
    /// To guarantee no messages are dropped, after calling `close()`, you must
    /// receive all items from the stream until `None` is returned.
    pub fn close(&mut self) {
        self.inner.close();
    }

    /// Returns the number of messages in the queue.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if there are no messages in the queue.
    pub fn is_empty(&self) -> bool {
        self.inner.len() == 0
    }
}

impl<S: Service> Stream for ShardControlStream<S> {
    type Item = ShardControlMessage<S>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(self.inner.poll_recv(cx)) {
            Some(op) => match op {
                ShardControlOp::RegisterSortCode(sort_code, reply_port) => {
                    Poll::Ready(Some(ShardControlMessage::RegisterSortCode {
                        sort_code,
                        decision: ShardRegistrationDecision::new(reply_port),
                    }))
                }
            },
            None => Poll::Ready(None),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.inner.len(), None)
    }
}

/// A control message delivered on [`ShardControlStream`].
#[derive(derive_more::Debug)]
pub enum ShardControlMessage<S> {
    /// The router received a message for an unregistered sort-code and is asking
    /// the service owner to set up a shard for it. The owner must resolve the
    /// [`decision`](ShardControlMessage::RegisterSortCode::decision) by calling
    /// [`accept`](ShardRegistrationDecision::accept) or
    /// [`fail`](ShardRegistrationDecision::fail).
    RegisterSortCode {
        sort_code: u64,
        #[debug(skip)]
        decision: ShardRegistrationDecision<S>,
    },
}

pub struct ShardControlReceiver<S> {
    receiver: mpsc::UnboundedReceiver<ShardControlOp>,
    started: Arc<AtomicBool>,
    _marker: std::marker::PhantomData<S>,
}

impl<S: Service> ShardControlReceiver<S> {
    pub fn start(self) -> ShardControlStream<S> {
        self.started.store(true, Ordering::Relaxed);
        ShardControlStream::new(self.receiver)
    }
}

// creates a default closed receiver
impl<S: Service> Default for ShardControlReceiver<S> {
    fn default() -> Self {
        let (_tx, mut receiver) = mpsc::unbounded_channel();
        receiver.close();
        Self {
            receiver,
            started: Arc::new(AtomicBool::new(false)),
            _marker: std::marker::PhantomData,
        }
    }
}

pub(crate) struct ShardedSender {
    control_sender: mpsc::UnboundedSender<ShardControlOp>,
    started: Arc<AtomicBool>,
    pool: MemoryPool,
    backpressure: BackPressureMode,
    bytes_accepted: Counter,
}

impl ShardedSender {
    pub fn new<S: Service>(
        pool: MemoryPool,
        backpressure: BackPressureMode,
    ) -> (Self, ShardControlReceiver<S>) {
        let started = Arc::new(AtomicBool::new(false));
        let (control_sender, receiver) = mpsc::unbounded_channel();
        (
            Self {
                control_sender,
                started: started.clone(),
                pool,
                backpressure,
                bytes_accepted: counter!(NETWORK_SERVICE_ACCEPTED_REQUEST_BYTES, "target" => S::TAG.as_str_name()),
            },
            ShardControlReceiver {
                receiver,
                started,
                _marker: std::marker::PhantomData,
            },
        )
    }

    pub fn increment_bytes_accepted(&self, payload_size: usize) {
        self.bytes_accepted.increment(payload_size as u64);
    }

    pub async fn register_sort_code(
        &self,
        sort_code: u64,
    ) -> Result<RawSender, ShardRegistrationError> {
        let (reply_port, reply_rx) = oneshot::channel();
        self.control_sender
            .send(ShardControlOp::RegisterSortCode(sort_code, reply_port))
            .map_err(|_| RouterError::ServiceStopped)?;
        reply_rx
            .await
            .map_err(|_| ShardRegistrationError::Router(RouterError::ServiceStopped))?
    }

    pub async fn reserve(&self, payload_size: usize) -> Result<MemoryLease, RouterError> {
        if !self.started.load(Ordering::Relaxed) {
            return Err(RouterError::ServiceNotReady);
        }

        // Reserve memory based on backpressure mode
        match self.backpressure {
            BackPressureMode::PushBack => {
                // Wait for memory to become available
                Ok(self.pool.reserve(payload_size).await)
            }
            BackPressureMode::Lossy => {
                // Try to reserve immediately, fail if no capacity
                self.pool
                    .try_reserve(payload_size)
                    .ok_or(RouterError::CapacityExceeded)
            }
        }
    }
}

/// Internal messages for shard control messages
enum ShardControlOp {
    RegisterSortCode(
        u64,
        oneshot::Sender<Result<RawSender, ShardRegistrationError>>,
    ),
}
