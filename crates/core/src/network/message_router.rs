// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Poll, ready};

use ahash::HashMap;
use futures::Stream;
use metrics::{Histogram, histogram};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, instrument};

use restate_types::SharedString;
use restate_types::net::{Service, ServiceTag};

use super::incoming::{Incoming, RawRpc, RawUnary, RawWatch};
use super::metric_definitions::NETWORK_MESSAGE_PROCESSING_DURATION;
use super::{RawSvcRpc, RawSvcUnary, RawSvcWatch, RouterError, Verdict};
use crate::network::{RawStream, RawSvcStream};
use crate::{ShutdownError, TaskCenter, TaskId, TaskKind, cancellation_token};

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

    fn on_stream(
        &mut self,
        message: Incoming<RawSvcStream<Self::Service>>,
    ) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

#[derive(Default)]
pub struct MessageRouter {
    senders: HashMap<ServiceTag, ServiceSender>,
}

impl MessageRouter {
    fn get_sender(&self, target: ServiceTag) -> Result<&ServiceSender, RouterError> {
        let Some(sender) = self.senders.get(&target) else {
            return Err(RouterError::ServiceNotFound);
        };
        Ok(sender)
    }

    pub async fn call_rpc(
        &self,
        target: ServiceTag,
        message: Incoming<RawRpc>,
    ) -> Result<(), RouterError> {
        let permit = self.get_sender(target)?.reserve().await?;
        permit.send(ServiceOp::CallRpc(message));

        Ok(())
    }

    pub async fn call_watch(
        &self,
        target: ServiceTag,
        message: Incoming<RawWatch>,
    ) -> Result<(), RouterError> {
        self.get_sender(target)?
            .reserve()
            .await?
            .send(ServiceOp::Watch(message));
        Ok(())
    }

    pub async fn call_unary(
        &self,
        target: ServiceTag,
        message: Incoming<RawUnary>,
    ) -> Result<(), RouterError> {
        self.get_sender(target)?
            .reserve()
            .await?
            .send(ServiceOp::Unary(message));
        Ok(())
    }

    pub async fn call_stream(
        &self,
        target: ServiceTag,
        message: Incoming<RawStream>,
    ) -> Result<(), RouterError> {
        self.get_sender(target)?
            .reserve()
            .await?
            .send(ServiceOp::Stream(message));
        Ok(())
    }
}

#[derive(Default)]
pub struct MessageRouterBuilder {
    handlers: HashMap<ServiceTag, ServiceSender>,
}

impl MessageRouterBuilder {
    /// Attach a handler that receives all messages targeting a certain [`ServiceTag`].
    #[track_caller]
    #[must_use]
    pub fn register_service<S: Service>(
        &mut self,
        buffer_size: usize,
        backpressure: BackPressureMode,
    ) -> ServiceReceiver<S> {
        let (sender, receiver) = ServiceSender::new(backpressure, buffer_size);
        if self.handlers.insert(S::TAG, sender).is_some() {
            panic!(
                "Handler for service {} has been registered already!",
                S::TAG
            );
        }
        receiver
    }

    /// Subscribes to messages for a specific service.
    ///
    /// [`Buffered`] provides a batteries-included implementation of a processor
    /// task that manages the service lifecycle and exposes a convenient API for
    /// handling requests and unary messages. The buffered processor task is started
    /// when calling [`Buffered::start()`].
    ///
    #[track_caller]
    #[must_use]
    pub fn register_buffered_service<S: Service>(
        &mut self,
        buffer_size: usize,
        backpressure: BackPressureMode,
    ) -> Buffered<S> {
        let (sender, receiver) = ServiceSender::new(backpressure, buffer_size);
        assert!(
            self.handlers.insert(S::TAG, sender).is_none(),
            "Handler for service {} has been registered already!",
            S::TAG
        );

        Buffered::new(receiver)
    }

    /// Finalize this builder and return the message router that can be attached to
    /// [`crate::ConnectionManager`]
    #[must_use]
    pub fn build(self) -> MessageRouter {
        MessageRouter {
            senders: self.handlers,
        }
    }
}

enum ServiceOp {
    CallRpc(Incoming<RawRpc>),
    Watch(Incoming<RawWatch>),
    Unary(Incoming<RawUnary>),
    Stream(Incoming<RawStream>),
}

/// A permit to route a message to a service
pub struct ServiceSendPermit<'a> {
    permit: mpsc::Permit<'a, ServiceOp>,
}

impl ServiceSendPermit<'_> {
    fn send(self, message: ServiceOp) {
        self.permit.send(message);
    }
}

/// Ingress stream for network services
pub struct ServiceStream<S> {
    inner: ReceiverStream<ServiceOp>,
    _marker: std::marker::PhantomData<S>,
}

impl<S: Service> ServiceStream<S> {
    fn new(receiver: mpsc::Receiver<ServiceOp>) -> Self {
        Self {
            inner: ReceiverStream::new(receiver),
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
}

impl<S: Service> Stream for ServiceStream<S> {
    type Item = ServiceMessage<S>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(self.as_mut().inner.as_mut().poll_recv(cx)) {
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
                ServiceOp::Stream(message) => {
                    let message = Incoming::from_raw_stream(message);
                    Poll::Ready(Some(ServiceMessage::Stream(message)))
                }
            },
            None => Poll::Ready(None),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// A message sent to a network service
pub enum ServiceMessage<S> {
    Rpc(Incoming<RawSvcRpc<S>>),
    Watch(Incoming<RawSvcWatch<S>>),
    Unary(Incoming<RawSvcUnary<S>>),
    Stream(Incoming<RawSvcStream<S>>),
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
        };

        let raw_incoming = Incoming::new(
            protocol_version,
            raw_rpc,
            from_peer,
            peer_metadata.unwrap_or_default(),
            None,
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
            Self::Stream(i) => i.msg_type(),
        }
    }

    pub fn sort_code(&self) -> Option<u64> {
        match self {
            Self::Rpc(i) => i.sort_code(),
            Self::Watch(i) => i.sort_code(),
            Self::Unary(i) => i.sort_code(),
            Self::Stream(i) => i.sort_code(),
        }
    }

    pub fn follow_from_sender(&mut self) {
        match self {
            Self::Rpc(i) => i.follow_from_sender(),
            Self::Watch(i) => i.follow_from_sender(),
            Self::Unary(i) => i.follow_from_sender(),
            Self::Stream(i) => i.follow_from_sender(),
        }
    }

    pub fn fail(self, v: Verdict) {
        match self {
            Self::Rpc(i) => i.fail(v),
            Self::Watch(i) => i.fail(v),
            Self::Unary(_) => {}
            Self::Stream(i) => i.fail(v),
        }
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub enum BackPressureMode {
    /// Pushes back to the sender if the service has no capacity
    #[default]
    PushBack,
    /// Drops messages if the service has no capacity. The sender will be
    /// notified that the message has been dropped due to load shedding.
    Lossy,
}

pub struct ServiceReceiver<S> {
    receiver: mpsc::Receiver<ServiceOp>,
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

// creates a default closed receiver
impl<S: Service> Default for ServiceReceiver<S> {
    fn default() -> Self {
        let (_tx, mut receiver) = mpsc::channel(1);
        receiver.close();
        Self {
            receiver,
            started: Arc::new(AtomicBool::new(false)),
            _marker: std::marker::PhantomData,
        }
    }
}

struct ServiceSender {
    sender: mpsc::Sender<ServiceOp>,
    started: Arc<AtomicBool>,
    backpressure: BackPressureMode,
    send_histogram: Histogram,
}

impl ServiceSender {
    fn new<S: Service>(
        backpressure: BackPressureMode,
        buffer_size: usize,
    ) -> (Self, ServiceReceiver<S>) {
        let started = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::channel(buffer_size);
        (
            Self {
                sender,
                backpressure,
                started: started.clone(),
                send_histogram: histogram!(NETWORK_MESSAGE_PROCESSING_DURATION, "target" => S::TAG.as_str_name()),
            },
            ServiceReceiver {
                receiver,
                started,
                _marker: std::marker::PhantomData,
            },
        )
    }

    async fn reserve(&self) -> Result<ServiceSendPermit<'_>, RouterError> {
        if !self.started.load(Ordering::Relaxed) {
            return Err(RouterError::ServiceNotReady);
        }
        let processing_started = Instant::now();
        let permit = match self.backpressure {
            BackPressureMode::PushBack => {
                let permit = self
                    .sender
                    .reserve()
                    .await
                    .map_err(|_| RouterError::ServiceStopped)?;
                // only measure latency for pushback mode to avoid skewing metrics
                // with lossy mode as they don't wait.
                self.send_histogram.record(processing_started.elapsed());
                permit
            }
            BackPressureMode::Lossy => match self.sender.try_reserve() {
                Ok(permit) => permit,
                Err(mpsc::error::TrySendError::Full(())) => {
                    return Err(RouterError::CapacityExceeded);
                }
                Err(mpsc::error::TrySendError::Closed(())) => {
                    return Err(RouterError::ServiceStopped);
                }
            },
        };
        Ok(ServiceSendPermit { permit })
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
                        ServiceMessage::Rpc(mut message) => {
                            message.follow_from_sender();
                            self.inner.on_rpc(message).await;
                        }
                        ServiceMessage::Watch(mut message) => {
                            message.follow_from_sender();
                            self.inner.on_watch(message).await;
                        }
                        ServiceMessage::Unary(mut message) => {
                            message.follow_from_sender();
                            self.inner.on_unary(message).await;
                        }
                        ServiceMessage::Stream(mut message) => {
                            message.follow_from_sender();
                            self.inner.on_stream(message).await;
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
