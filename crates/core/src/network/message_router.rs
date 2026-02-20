// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod shard_map;
mod sharded;

// exports
pub use sharded::{
    ControlServiceShards, ShardControlMessage, ShardControlStream, ShardRegistrationDecision,
    ShardSender, Sharded,
};

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Poll, ready};

use ahash::HashMap;
use futures::Stream;
use metrics::{Counter, counter};

use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::StreamExt;
use tracing::{debug, instrument, trace, warn};

use restate_memory::{EstimatedMemorySize, MemoryLease, MemoryPool};
use restate_types::SharedString;
use restate_types::net::{Service, ServiceTag};

use self::shard_map::Shards;
use self::sharded::ShardedSender;
use super::incoming::{Incoming, RawRpc, RawUnary, RawWatch};
use super::{
    Connection, RawSvcRpc, RawSvcUnary, RawSvcWatch, ReplyEnvelope, RouterError,
    ShardRegistrationError, Verdict, WatchUpdateEnvelope,
};
use crate::network::metric_definitions::NETWORK_SERVICE_ACCEPTED_REQUEST_BYTES;
use crate::network::{PeerMetadataVersion, RpcReplyPort};
use crate::{ShutdownError, TaskCenter, TaskId, TaskKind, cancellation_token, network::protobuf};

/// Chooses the draining strategy for the service
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Drain {
    /// Gracefully drain all enqueued operations
    Graceful,
    /// Stop immediately without draining
    Immediate,
}

/// Service handler trait
///
/// Use this trait to write service handlers when using [`Buffered`] services.
/// It provides callbacks for various life-cycle events of the service and allows you
/// to handle messages sent to the service.
#[allow(unused)]
pub trait Handler: Send + 'static {
    type Service: Service;
    /// Called immediately before the service starts serving requests
    ///
    /// It's guaranteed that we have not started serving any requests yet
    fn on_start(&mut self) -> impl Future<Output = ()> + Send {
        debug!("Handler for {} is starting", Self::Service::TAG);
        std::future::ready(())
    }
    /// Called when cancellation is requested
    ///
    /// Based on the return value, the handler can choose to gracefully drain all enqueued
    /// operations or stop immediately.
    ///
    /// Default is [`Drain::Graceful`].
    fn on_drain(&mut self) -> impl Future<Output = Drain> + Send {
        debug!("Handler for {} is draining", Self::Service::TAG);
        std::future::ready(Drain::Graceful)
    }
    /// Called when the handler has finished draining and is about to stop
    fn on_stop(&mut self) -> impl Future<Output = ()> + Send {
        debug!("Handler for {} has stopped", Self::Service::TAG);
        std::future::ready(())
    }
    /// Handle a unary messages
    fn on_unary(
        &mut self,
        message: Incoming<RawSvcUnary<Self::Service>>,
    ) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
    /// Handle a RPC request
    fn on_rpc(
        &mut self,
        message: Incoming<RawSvcRpc<Self::Service>>,
    ) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
    /// Handle a watch/subscription request
    fn on_watch(
        &mut self,
        message: Incoming<RawSvcWatch<Self::Service>>,
    ) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

#[derive(Default)]
pub struct MessageRouter {
    senders: HashMap<ServiceTag, ServiceSink>,
    shards: Shards,
}

impl MessageRouter {
    fn get_sink(&self, target: ServiceTag) -> Result<&ServiceSink, RouterError> {
        let Some(sender) = self.senders.get(&target) else {
            return Err(RouterError::ServiceNotFound);
        };
        Ok(sender)
    }

    async fn get_sharded_sender(
        &self,
        parent_sender: &ShardedSender,
        target: ServiceTag,
        sort_code: u64,
    ) -> Result<RawSender, ShardRegistrationError> {
        loop {
            match self.shards.maybe_register(target, sort_code) {
                shard_map::MaybeValue::Filled(raw_sender) => return Ok(raw_sender),
                shard_map::MaybeValue::Opening(wait_token) => {
                    wait_token.join().await;
                    // try again, hopefully next time we'll find the fully registered shard
                    // or we'll take over registration.
                    continue;
                }
                shard_map::MaybeValue::Register(registration_token) => {
                    // give the service, the receiver for this shard.
                    // shard was not found, should the service create it?
                    //
                    // Note that registration_token's drop will remove the entry from the
                    // shard_map unless we call done(). This means that if registration was
                    // cancelled (due to future being canceled). We'll detect that on the next
                    // attempt to create the shard and try again.
                    let raw_sender = parent_sender.register_sort_code(sort_code).await?;
                    if !registration_token.done(raw_sender.clone()) {
                        // race...
                        continue;
                    }
                    return Ok(raw_sender);
                }
            }
        }
    }

    /// Dispatches a [`ServiceOp`] to the appropriate sink (sharded or mono).
    ///
    /// For sharded sinks this handles the full lifecycle: resolve the shard sender
    /// (registering on demand if needed), send the message, and retry if the shard's
    /// channel was closed since it was last looked up.
    ///
    /// Returns `Ok(())` on success or a [`ShardRegistrationError`] if the shard
    /// registration was rejected (verdict) or the service is unreachable (router
    /// error). Callers map the verdict case according to their message type.
    async fn dispatch_op(
        &self,
        service_sink: &ServiceSink,
        target_service: ServiceTag,
        sort_code: Option<u64>,
        encoded_len: usize,
        op: ServiceOp,
    ) -> Result<(), ShardRegistrationError> {
        match service_sink {
            ServiceSink::Sharded(parent_sender) => {
                let sort_code = sort_code.expect("sort code must be set in sharded services");
                let mut op = op;
                loop {
                    let sender = self
                        .get_sharded_sender(parent_sender, target_service, sort_code)
                        .await?;
                    // if the shard sender is closed, we need to retry registering the shard
                    if let Err(mpsc::error::SendError(msg)) = sender.send(op) {
                        op = msg;
                        // remove this sender
                        self.shards
                            .remove_if_matches(target_service, sort_code, &sender);
                        // retry...
                        continue;
                    } else {
                        parent_sender.increment_bytes_accepted(encoded_len);
                        break;
                    }
                }
            }
            ServiceSink::Mono(sender) => {
                sender.send(op).map_err(|_| RouterError::ServiceStopped)?;
            }
        }
        Ok(())
    }

    pub async fn call_rpc(
        &self,
        header: protobuf::network::Header,
        rpc_call: protobuf::network::RpcCall,
        connection: &Connection,
    ) -> Result<oneshot::Receiver<ReplyEnvelope>, RouterError> {
        let target_service = rpc_call.service();
        trace!(
            peer = %connection.peer(),
            rpc_id = %rpc_call.id,
            "Received RPC call: {target_service}::{}",
            rpc_call.msg_type
        );
        let encoded_len = rpc_call.payload.len();
        let service_sink = self.get_sink(target_service)?;
        let sort_code = rpc_call.sort_code;
        let reservation = service_sink.reserve(encoded_len, sort_code).await?;

        let (reply_port, reply_rx) = RpcReplyPort::new();
        let raw_rpc = RawRpc {
            reply_port,
            payload: rpc_call.payload,
            sort_code,
            msg_type: rpc_call.msg_type,
            reservation,
        };
        let incoming = Incoming::new(
            connection.protocol_version,
            raw_rpc,
            connection.peer,
            PeerMetadataVersion::from(header),
        );

        let op = ServiceOp::CallRpc(incoming);
        match self
            .dispatch_op(service_sink, target_service, sort_code, encoded_len, op)
            .await
        {
            Ok(()) => Ok(reply_rx),
            Err(ShardRegistrationError::Router(err)) => Err(err),
            Err(ShardRegistrationError::Verdict(verdict)) => {
                Ok(RpcReplyPort::reply_with_verdict(verdict))
            }
        }
    }

    // WIP
    #[allow(unused)]
    pub async fn call_watch(
        &self,
        header: protobuf::network::Header,
        watch: protobuf::network::Watch,
        connection: &Connection,
    ) -> Result<watch::Receiver<WatchUpdateEnvelope>, RouterError> {
        let target_service = watch.service();
        trace!(
            "Received Watch request: {target_service}::{}",
            watch.msg_type
        );

        let service_sink = self.get_sink(target_service)?;
        let encoded_len = watch.payload.len();
        let sort_code = watch.sort_code;
        let reservation = service_sink.reserve(encoded_len, sort_code).await?;

        let (reply_port, reply_rx) = todo!();

        let incoming = Incoming::new(
            connection.protocol_version,
            RawWatch {
                payload: watch.payload,
                sort_code: watch.sort_code,
                msg_type: watch.msg_type,
                reservation,
                reply_port,
            },
            connection.peer(),
            PeerMetadataVersion::from(header),
        );

        let op = ServiceOp::Watch(incoming);
        match self
            .dispatch_op(service_sink, target_service, sort_code, encoded_len, op)
            .await
        {
            Ok(()) => Ok(reply_rx),
            Err(ShardRegistrationError::Router(err)) => Err(err),
            Err(ShardRegistrationError::Verdict(_verdict)) => {
                unimplemented!("Sharded watch replies are not yet implemented");
            }
        }
    }

    pub async fn call_unary(
        &self,
        header: protobuf::network::Header,
        unary: protobuf::network::Unary,
        connection: &Connection,
    ) -> Result<(), RouterError> {
        let target_service = unary.service();
        trace!("Received Unary call: {target_service}::{}", unary.msg_type);

        let service_sink = self.get_sink(target_service)?;
        let sort_code = unary.sort_code;
        let encoded_len = unary.payload.len();
        let reservation = service_sink.reserve(encoded_len, sort_code).await?;

        let incoming = Incoming::new(
            connection.protocol_version,
            RawUnary {
                payload: unary.payload,
                sort_code: unary.sort_code,
                msg_type: unary.msg_type,
                reservation,
            },
            connection.peer(),
            PeerMetadataVersion::from(header),
        );

        let op = ServiceOp::Unary(incoming);
        match self
            .dispatch_op(service_sink, target_service, sort_code, encoded_len, op)
            .await
        {
            Ok(()) => Ok(()),
            Err(ShardRegistrationError::Router(err)) => Err(err),
            Err(ShardRegistrationError::Verdict(_verdict)) => {
                // Since this is a unary message, we cannot reply with a verdict but let's
                // return a routing error instead. Errors are not shipped to the caller
                // but they are counted as errors in metrics.
                Err(RouterError::ServiceNotReady)
            }
        }
    }
}

pub struct MessageRouterBuilder {
    /// The default memory pool that's shared across services that don't specify an explicit pool.
    default_pool: MemoryPool,
    handlers: HashMap<ServiceTag, ServiceSink>,
    shards: Shards,
}

impl MessageRouterBuilder {
    pub fn with_default_pool(pool: MemoryPool) -> Self {
        Self {
            default_pool: pool,
            handlers: HashMap::default(),
            shards: Shards::default(),
        }
    }
}

/// Controls backpressure behavior when the memory pool is exhausted.
#[derive(Clone, Copy, Default, Debug)]
pub enum BackPressureMode {
    /// Waits asynchronously for memory to become available before sending.
    /// Use this when you want to slow down senders when the service is under pressure.
    #[default]
    PushBack,
    /// Immediately rejects messages when there's no capacity (load shedding).
    /// The sender will receive [`RouterError::CapacityExceeded`].
    /// Use this when you prefer to drop requests rather than queue them.
    Lossy,
}

impl MessageRouterBuilder {
    /// Registers a service with the specified memory pool and backpressure mode.
    ///
    /// The memory reservation is attached to each incoming message and should be
    /// held until processing is complete (e.g., until data is persisted to storage).
    ///
    /// # Backpressure Modes
    ///
    /// - [`BackPressureMode::PushBack`]: Waits for memory to become available.
    ///   Use when you want to slow down senders under pressure.
    /// - [`BackPressureMode::Lossy`]: Immediately rejects with [`RouterError::CapacityExceeded`].
    ///   Use when you prefer to drop requests rather than queue them.
    #[track_caller]
    #[must_use]
    pub fn register_service_with_pool<S: Service>(
        &mut self,
        pool: MemoryPool,
        backpressure: BackPressureMode,
    ) -> ServiceReceiver<S> {
        let (sender, receiver) = ServiceSender::new::<S>(pool, backpressure);
        if self
            .handlers
            .insert(S::TAG, ServiceSink::Mono(sender))
            .is_some()
        {
            panic!(
                "Handler for service {} has been registered already!",
                S::TAG
            );
        }
        receiver
    }

    /// Registers a service with the specified memory pool and backpressure mode.
    ///
    /// Backpressure mode is applied based on the shared memory pool that was supplied at the time
    /// of constructing the router.
    ///
    /// # Backpressure Modes
    ///
    /// - [`BackPressureMode::PushBack`]: Waits for memory to become available.
    ///   Use when you want to slow down senders under pressure.
    /// - [`BackPressureMode::Lossy`]: Immediately rejects with [`RouterError::CapacityExceeded`].
    ///   Use when you prefer to drop requests rather than queue them.
    #[track_caller]
    #[must_use]
    pub fn register_service<S: Service>(
        &mut self,
        backpressure: BackPressureMode,
    ) -> ServiceReceiver<S> {
        let (sender, receiver) = ServiceSender::new::<S>(self.default_pool.clone(), backpressure);
        if self
            .handlers
            .insert(S::TAG, ServiceSink::Mono(sender))
            .is_some()
        {
            panic!(
                "Handler for service {} has been registered already!",
                S::TAG
            );
        }
        receiver
    }

    /// Registers a sharded service using the router's default memory pool.
    ///
    /// Unlike [`register_service`](Self::register_service), a sharded service routes
    /// each incoming message to an independent shard identified by the message's
    /// sort-code. Shards are created on demand: the first message targeting an
    /// unknown sort-code triggers a [`ShardControlMessage::RegisterSortCode`] on the
    /// returned [`Sharded`]'s control stream, giving the service owner a chance to
    /// set up the shard and return a [`ShardSender`].  Subsequent messages with the
    /// same sort-code are delivered directly to that shard's channel.
    ///
    /// Shards can also be pre-registered imperatively via
    /// [`ControlServiceShards::force_register_sort_code`].
    ///
    /// # Backpressure Modes
    ///
    /// - [`BackPressureMode::PushBack`]: Waits for memory to become available.
    ///   Use when you want to slow down senders under pressure.
    /// - [`BackPressureMode::Lossy`]: Immediately rejects with [`RouterError::CapacityExceeded`].
    ///   Use when you prefer to drop requests rather than queue them.
    ///
    /// # Panics
    ///
    /// Panics if a handler for `S` has already been registered.
    #[track_caller]
    #[must_use]
    pub fn register_sharded_service<S: Service>(
        &mut self,
        backpressure: BackPressureMode,
    ) -> Sharded<S> {
        let (sender, receiver) = ShardedSender::new::<S>(self.default_pool.clone(), backpressure);

        if self
            .handlers
            .insert(S::TAG, ServiceSink::Sharded(sender))
            .is_some()
        {
            panic!(
                "Handler for service {} has been registered already!",
                S::TAG
            );
        }
        Sharded::new(receiver, self.shards.clone())
    }

    /// Registers a sharded service with a dedicated memory pool.
    ///
    /// Behaves identically to [`register_sharded_service`](Self::register_sharded_service)
    /// but uses the supplied `pool` instead of the router's default. This is useful
    /// when a service has distinct memory-budget requirements — for example, a data
    /// service that handles large payloads may need a larger or separately managed
    /// pool to avoid starving other services sharing the default pool.
    ///
    /// # Backpressure Modes
    ///
    /// - [`BackPressureMode::PushBack`]: Waits for memory to become available.
    ///   Use when you want to slow down senders under pressure.
    /// - [`BackPressureMode::Lossy`]: Immediately rejects with [`RouterError::CapacityExceeded`].
    ///   Use when you prefer to drop requests rather than queue them.
    ///
    /// # Panics
    ///
    /// Panics if a handler for `S` has already been registered.
    #[track_caller]
    #[must_use]
    pub fn register_sharded_service_with_pool<S: Service>(
        &mut self,
        pool: MemoryPool,
        backpressure: BackPressureMode,
    ) -> Sharded<S> {
        let (sender, receiver) = ShardedSender::new::<S>(pool, backpressure);

        if self
            .handlers
            .insert(S::TAG, ServiceSink::Sharded(sender))
            .is_some()
        {
            panic!(
                "Handler for service {} has been registered already!",
                S::TAG
            );
        }
        Sharded::new(receiver, self.shards.clone())
    }

    /// Subscribes to messages for a specific service using a dedicated memory pool.
    ///
    /// [`Buffered`] provides a batteries-included implementation of a processor
    /// task that manages the service lifecycle and exposes a convenient API for
    /// handling requests and unary messages. The buffered processor task is started
    /// when calling [`Buffered::start()`].
    #[track_caller]
    #[must_use]
    pub fn register_buffered_service_with_pool<S: Service>(
        &mut self,
        pool: MemoryPool,
        backpressure: BackPressureMode,
    ) -> Buffered<S> {
        let receiver = self.register_service_with_pool::<S>(pool, backpressure);
        Buffered::new(receiver)
    }

    /// Subscribes to messages for a specific service using the default memory pool.
    ///
    /// [`Buffered`] provides a batteries-included implementation of a processor
    /// task that manages the service lifecycle and exposes a convenient API for
    /// handling requests and unary messages. The buffered processor task is started
    /// when calling [`Buffered::start()`].
    #[track_caller]
    #[must_use]
    pub fn register_buffered_service<S: Service>(
        &mut self,
        backpressure: BackPressureMode,
    ) -> Buffered<S> {
        let receiver = self.register_service::<S>(backpressure);
        Buffered::new(receiver)
    }

    /// Finalize this builder and return the message router that can be attached to
    /// [`crate::ConnectionManager`]
    #[must_use]
    pub fn build(self) -> MessageRouter {
        MessageRouter {
            senders: self.handlers,
            shards: self.shards,
        }
    }
}

/// Ingress stream for network services
pub struct ServiceStream<S> {
    inner: mpsc::UnboundedReceiver<ServiceOp>,
    _marker: std::marker::PhantomData<S>,
}

impl<S: Service> ServiceStream<S> {
    fn new(receiver: mpsc::UnboundedReceiver<ServiceOp>) -> Self {
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

impl<S: Service> Stream for ServiceStream<S> {
    type Item = ServiceMessage<S>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(self.inner.poll_recv(cx)) {
            Some(op) => match op {
                ServiceOp::CallRpc(message) => {
                    let message = Incoming::from_raw_rpc(message);
                    Poll::Ready(Some(ServiceMessage::Rpc(message)))
                }
                ServiceOp::Watch(message) => {
                    let message = Incoming::from_raw_watch(message);
                    Poll::Ready(Some(ServiceMessage::Watch(message)))
                }
                ServiceOp::Unary(message) => {
                    let message = Incoming::from_raw_unary(message);
                    Poll::Ready(Some(ServiceMessage::Unary(message)))
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

/// A message sent to a network service
#[derive(derive_more::Debug)]
pub enum ServiceMessage<S> {
    Rpc(Incoming<RawSvcRpc<S>>),
    Watch(Incoming<RawSvcWatch<S>>),
    Unary(Incoming<RawSvcUnary<S>>),
}

impl<S: Service> ServiceMessage<S> {
    // For testing
    #[cfg(feature = "test-util")]
    pub fn fake_rpc<M>(
        msg: M,
        sort_code: Option<u64>,
        from_peer: restate_types::GenerationalNodeId,
        peer_metadata: Option<super::PeerMetadataVersion>,
    ) -> (Self, super::ReplyRx<M::Response>)
    where
        M: restate_types::net::RpcRequest<Service = S>,
    {
        use restate_memory::MemoryLease;
        use restate_types::net::CURRENT_PROTOCOL_VERSION;
        let protocol_version = CURRENT_PROTOCOL_VERSION;

        use crate::network::protobuf::network::rpc_reply;
        use crate::network::{RawRpcReply, ReplyRx, RpcReplyError, RpcReplyPort};

        let (reply_sender, reply_token) = ReplyRx::new();
        let payload = msg
            .encode_to_bytes(protocol_version)
            .expect("message encode-able");

        let (reply_port, reply_rx) = RpcReplyPort::new();

        let raw_rpc = RawRpc {
            reply_port,
            payload,
            sort_code,
            msg_type: M::TYPE.to_owned(),
            reservation: MemoryLease::unlinked(),
        };

        let raw_incoming = Incoming::new(
            protocol_version,
            raw_rpc,
            from_peer,
            peer_metadata.unwrap_or_default(),
        );

        tokio::spawn(async move {
            match reply_rx.await {
                Ok(envelope) => match envelope.body {
                    rpc_reply::Body::Payload(payload) => {
                        reply_sender.send(RawRpcReply::Success((protocol_version, payload)))
                    }
                    rpc_reply::Body::Status(status) => {
                        reply_sender.send(RawRpcReply::Error(RpcReplyError::from(status)))
                    }
                },
                // reply_port was closed, we'll not respond.
                Err(_) => {
                    reply_sender.send(RawRpcReply::Error(crate::network::RpcReplyError::Dropped))
                }
            }
        });

        (
            ServiceMessage::Rpc(Incoming::from_raw_rpc(raw_incoming)),
            reply_token,
        )
    }

    pub fn msg_type(&self) -> &str {
        match self {
            Self::Rpc(i) => i.msg_type(),
            Self::Watch(i) => i.msg_type(),
            Self::Unary(i) => i.msg_type(),
        }
    }

    pub fn sort_code(&self) -> Option<u64> {
        match self {
            Self::Rpc(i) => i.sort_code(),
            Self::Watch(i) => i.sort_code(),
            Self::Unary(i) => i.sort_code(),
        }
    }

    pub fn fail(self, v: Verdict) {
        match self {
            Self::Rpc(i) => i.fail(v),
            Self::Watch(i) => i.fail(v),
            Self::Unary(_) => {}
        }
    }
}

pub struct ServiceReceiver<S> {
    receiver: mpsc::UnboundedReceiver<ServiceOp>,
    started: Arc<AtomicBool>,
    _marker: std::marker::PhantomData<S>,
}

impl<S: Service> ServiceReceiver<S> {
    pub fn start(self) -> ServiceStream<S> {
        self.started.store(true, Ordering::Relaxed);
        ServiceStream::new(self.receiver)
    }

    /// Replaces self with default value and returns the previous receiver
    ///
    /// Use this method to take ownership of the receiver and close it.
    pub fn take(&mut self) -> ServiceReceiver<S> {
        std::mem::take(self)
    }
}

/// Creates a default closed receiver
impl<S: Service> Default for ServiceReceiver<S> {
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

/// Internal type-erased holder of network messages
///
/// The public version of this type is [`ServiceMessage`].
enum ServiceOp {
    CallRpc(Incoming<RawRpc>),
    Watch(Incoming<RawWatch>),
    Unary(Incoming<RawUnary>),
}

impl EstimatedMemorySize for ServiceOp {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        match self {
            ServiceOp::CallRpc(incoming) => incoming.estimated_memory_size(),
            ServiceOp::Watch(incoming) => incoming.estimated_memory_size(),
            ServiceOp::Unary(incoming) => incoming.estimated_memory_size(),
        }
    }
}

enum ServiceSink {
    Sharded(ShardedSender),
    Mono(ServiceSender),
}

impl ServiceSink {
    /// Reserve memory and return a lease when successful.
    ///
    /// Behavior depends on [`BackPressureMode`]:
    /// - `PushBack`: Waits asynchronously for memory to become available
    /// - `Lossy`: Returns [`RouterError::CapacityExceeded`] if pool is exhausted
    async fn reserve(
        &self,
        payload_size: usize,
        sort_code: Option<u64>,
    ) -> Result<MemoryLease, RouterError> {
        match self {
            ServiceSink::Sharded(parent_sender) => {
                if sort_code.is_none() {
                    warn!("RPC call to a sharded service without sort-code, dropping.");
                    return Err(RouterError::MessageUnrecognized);
                }
                parent_sender.reserve(payload_size).await
            }
            ServiceSink::Mono(sender) => sender.reserve(payload_size).await,
        }
    }
}

struct ServiceSender {
    raw_sender: RawSender,
    started: Arc<AtomicBool>,
    pool: MemoryPool,
    backpressure: BackPressureMode,
    bytes_accepted: Counter,
}

impl ServiceSender {
    fn new<S: Service>(
        pool: MemoryPool,
        backpressure: BackPressureMode,
    ) -> (Self, ServiceReceiver<S>) {
        let started = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::unbounded_channel();
        (
            Self {
                raw_sender: RawSender(sender),
                started: started.clone(),
                pool,
                backpressure,
                bytes_accepted: counter!(NETWORK_SERVICE_ACCEPTED_REQUEST_BYTES, "target" => S::TAG.as_str_name()),
            },
            ServiceReceiver {
                receiver,
                started,
                _marker: std::marker::PhantomData,
            },
        )
    }

    /// Reserve memory and return a lease when successful.
    ///
    /// Behavior depends on [`BackPressureMode`]:
    /// - `PushBack`: Waits asynchronously for memory to become available
    /// - `Lossy`: Returns [`RouterError::CapacityExceeded`] if pool is exhausted
    async fn reserve(&self, payload_size: usize) -> Result<MemoryLease, RouterError> {
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

    /// send a message through the channel
    fn send(&self, op: ServiceOp) -> Result<(), RouterError> {
        let size = op.estimated_memory_size();
        self.raw_sender
            .send(op)
            .map_err(|_| RouterError::ServiceStopped)?;
        self.bytes_accepted.increment(size as u64);
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct RawSender(mpsc::UnboundedSender<ServiceOp>);

impl RawSender {
    /// Creates a dummy sender for testing. Returns the sender and a receiver
    /// that keeps the channel alive.
    #[cfg(test)]
    fn test_channel() -> (Self, mpsc::UnboundedReceiver<ServiceOp>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self(tx), rx)
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    fn send(&self, op: ServiceOp) -> Result<(), mpsc::error::SendError<ServiceOp>> {
        self.0.send(op)
    }

    #[inline]
    fn same_channel(&self, other: &Self) -> bool {
        self.0.same_channel(&other.0)
    }
}

pub struct Buffered<S> {
    rx: ServiceReceiver<S>,
}

impl<S: Service> Buffered<S> {
    const fn new(rx: ServiceReceiver<S>) -> Self {
        Self { rx }
    }

    /// Creates a future that runs the handler
    ///
    /// the future is designed to run as a task-center task. it'll react to cancellation
    /// requests or continue to run until the message router is dropped.
    pub fn run<H>(self, handler: H) -> impl Future<Output = ()> + Send + 'static
    where
        H: Handler<Service = S> + Send + Sync + 'static,
    {
        let service_handler = ServiceHandler::new(handler, self.rx);
        service_handler.run()
    }

    /// Spawns a service handler as a *managed* task and immediately executes the provided handler
    pub fn start<H>(
        self,
        kind: TaskKind,
        task_name: impl Into<SharedString>,
        handler: H,
    ) -> Result<TaskId, ShutdownError>
    where
        H: Handler<Service = S> + Send + Sync + 'static,
    {
        TaskCenter::spawn(kind, task_name, async {
            self.run(handler).await;
            Ok(())
        })
    }
}

struct ServiceHandler<H: Handler> {
    inner: H,
    rx: ServiceReceiver<H::Service>,
}

impl<H> ServiceHandler<H>
where
    H: Handler + Send + Sync + 'static,
{
    const fn new(inner: H, rx: ServiceReceiver<H::Service>) -> Self {
        Self { inner, rx }
    }

    #[instrument(level = "error", skip_all, fields(service = %H::Service::TAG))]
    async fn run(mut self) {
        let cancel_token = cancellation_token();
        self.inner.on_start().await;
        let mut rx = self.rx.start();
        let mut draining = false;
        loop {
            tokio::select! {
                // Draining flag ensures that this branch is disabled after draining
                // is started. If we don't do this, the loop will be stuck in this branch
                // since cancelled() will always be `Poll::Ready`.
                _ = cancel_token.cancelled(), if !draining => {
                    let drain = self.inner.on_drain().await;
                    match drain {
                        // stop accepting messages and drain the queue
                        Drain::Graceful => {
                            draining = true;
                            rx.close();
                        }
                        Drain::Immediate => break,
                    }
                }
                Some(msg) = rx.next() => {
                    match msg {
                        ServiceMessage::Rpc(message) => {
                            self.inner.on_rpc(message).await;
                        }
                        ServiceMessage::Watch(message) => {
                            self.inner.on_watch(message).await;
                        }
                        ServiceMessage::Unary(message) => {
                            self.inner.on_unary(message).await;
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }
        self.inner.on_stop().await;
    }
}

static_assertions::assert_impl_all!(MessageRouter: Send);

#[cfg(test)]
mod tests {
    use tokio_stream::StreamExt;

    use restate_memory::MemoryPool;
    use restate_types::net::ServiceTag;
    use restate_types::net::log_server::LogServerDataService;

    use super::*;

    const SVC: ServiceTag = ServiceTag::LogServerDataService;
    const SORT_CODE: u64 = 42;

    /// Creates a `ShardedSender` + control stream + `Shards` triple ready for
    /// testing `get_sharded_sender` in isolation.
    fn sharded_setup() -> (
        ShardedSender,
        ShardControlStream<LogServerDataService>,
        Shards,
    ) {
        let (sender, receiver) = ShardedSender::new::<LogServerDataService>(
            MemoryPool::unlimited(),
            BackPressureMode::PushBack,
        );
        let stream = receiver.start();
        (sender, stream, Shards::default())
    }

    /// Minimal `MessageRouter` that only has a `Shards` map (no service sinks).
    fn router_with_shards(shards: Shards) -> MessageRouter {
        MessageRouter {
            senders: HashMap::default(),
            shards,
        }
    }

    /// Spawns a background task that drains the control stream, calling
    /// `on_register` for each registration request.
    fn spawn_control_handler<F>(
        mut stream: ShardControlStream<LogServerDataService>,
        mut on_register: F,
    ) -> tokio::task::JoinHandle<()>
    where
        F: FnMut(u64, ShardRegistrationDecision<LogServerDataService>) + Send + 'static,
    {
        tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                match msg {
                    ShardControlMessage::RegisterSortCode {
                        sort_code,
                        decision,
                    } => on_register(sort_code, decision),
                }
            }
        })
    }

    // -----------------------------------------------------------------------
    // get_sharded_sender
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn pre_registered_shard_returns_immediately() {
        let shards = Shards::default();
        let (parent, _stream) = ShardedSender::new::<LogServerDataService>(
            MemoryPool::unlimited(),
            BackPressureMode::PushBack,
        );
        let (shard_tx, _shard_rx) = RawSender::test_channel();
        shards.force_register(SVC, SORT_CODE, shard_tx.clone());

        let router = router_with_shards(shards);
        let result = router.get_sharded_sender(&parent, SVC, SORT_CODE).await;
        assert!(result.is_ok());
        assert!(result.unwrap().same_channel(&shard_tx));
    }

    #[tokio::test(start_paused = true)]
    async fn on_demand_registration_accepted() {
        let (parent, stream, shards) = sharded_setup();
        let router = router_with_shards(shards);

        // Handler accepts every registration request.
        let shard_rxs = Arc::new(std::sync::Mutex::new(Vec::new()));
        let rxs_clone = shard_rxs.clone();
        let _handler = spawn_control_handler(stream, move |_sort_code, decision| {
            let (sender, rx) = ShardSender::new();
            rxs_clone.lock().unwrap().push(rx);
            decision.accept(sender);
        });

        let result = router.get_sharded_sender(&parent, SVC, SORT_CODE).await;
        assert!(result.is_ok(), "expected Ok, got Err");

        let rxs = shard_rxs.lock().unwrap();
        assert_eq!(rxs.len(), 1, "exactly one shard should have been created");
    }

    #[tokio::test(start_paused = true)]
    async fn on_demand_registration_rejected_returns_verdict() {
        let (parent, stream, shards) = sharded_setup();
        let router = router_with_shards(shards);

        let _handler = spawn_control_handler(stream, |_sort_code, decision| {
            decision.fail(Verdict::SortCodeNotFound);
        });

        let result = router.get_sharded_sender(&parent, SVC, SORT_CODE).await;
        assert!(
            matches!(
                result,
                Err(ShardRegistrationError::Verdict(Verdict::SortCodeNotFound))
            ),
            "expected Verdict(SortCodeNotFound)"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn control_stream_dropped_returns_service_stopped() {
        let (parent, stream, shards) = sharded_setup();
        drop(stream);
        let router = router_with_shards(shards);

        let result = router.get_sharded_sender(&parent, SVC, SORT_CODE).await;
        assert!(
            matches!(
                result,
                Err(ShardRegistrationError::Router(RouterError::ServiceStopped))
            ),
            "expected ServiceStopped"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn second_call_reuses_cached_shard() {
        let (parent, stream, shards) = sharded_setup();
        let router = router_with_shards(shards);

        let registration_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let shard_rxs = Arc::new(std::sync::Mutex::new(Vec::new()));

        let count_clone = registration_count.clone();
        let rxs_clone = shard_rxs.clone();
        let _handler = spawn_control_handler(stream, move |_sort_code, decision| {
            count_clone.fetch_add(1, Ordering::Relaxed);
            let (sender, rx) = ShardSender::new();
            rxs_clone.lock().unwrap().push(rx);
            decision.accept(sender);
        });

        // First call triggers on-demand registration.
        let r1 = router.get_sharded_sender(&parent, SVC, SORT_CODE).await;
        assert!(r1.is_ok());

        // Second call should return the cached sender without registering again.
        let r2 = router.get_sharded_sender(&parent, SVC, SORT_CODE).await;
        assert!(r2.is_ok());
        assert!(r1.unwrap().same_channel(&r2.unwrap()));

        assert_eq!(
            registration_count.load(Ordering::Relaxed),
            1,
            "only one registration should have occurred"
        );
    }
}
