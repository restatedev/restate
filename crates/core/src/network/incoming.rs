// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use bytes::Bytes;
use futures::{Stream, ready};
use opentelemetry::Context;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use restate_types::GenerationalNodeId;
use restate_types::net::codec::{self, WireDecode, WireEncode};
use restate_types::net::{
    ProtocolVersion, Service, StreamRequest, StreamResponse, UnaryMessage, WatchResponse,
};
use restate_types::net::{RpcRequest, RpcResponse, WatchRequest};

use crate::network::protobuf::network;
use crate::network::{StreamClosed, StreamMessageError};

use super::protobuf::network::{rpc_reply, watch_update};
use super::{ConnectionClosed, PeerMetadataVersion, Verdict};

/// A wrapper for incoming messages over a network connection.
#[derive(Debug, Clone)]
pub struct Incoming<M> {
    protocol_version: ProtocolVersion,
    inner: M,
    peer: GenerationalNodeId,
    metadata_version: PeerMetadataVersion,
    parent_context: Option<Context>,
}

impl<M> Incoming<M> {
    pub(crate) fn new(
        protocol_version: ProtocolVersion,
        inner: M,
        peer: GenerationalNodeId,
        metadata_version: PeerMetadataVersion,
        parent_context: Option<Context>,
    ) -> Self {
        Self {
            protocol_version,
            inner,
            peer,
            metadata_version,
            parent_context,
        }
    }

    /// Sender's metadata version if it was set in headers
    pub fn metadata_version(&self) -> &PeerMetadataVersion {
        &self.metadata_version
    }

    /// The sender's node-id if known
    pub fn peer(&self) -> GenerationalNodeId {
        self.peer
    }

    /// Returns an open telemetry Context which is
    /// traced over from the sender of the message
    pub fn parent_context(&self) -> Option<&Context> {
        self.parent_context.as_ref()
    }

    /// A shortcut to set current tracing [`Span`] parent
    /// to remote caller span context
    ///
    /// This only works on the first call. Subsequent calls
    /// has no effect on the current [`Span`].
    ///
    /// If you need to create `parallel` spans for the same
    /// incoming message, use [`Self::parent_context()`] instead
    pub fn follow_from_sender(&mut self) {
        if let Some(context) = self.parent_context.take() {
            Span::current().set_parent(context)
        }
    }

    /// A shortcut to set given [`Span`] parent
    /// to remote caller span context
    ///
    /// This only works on the first call. Subsequent calls
    /// has no effect on the current [`Span`].
    ///
    /// If you need to create `parallel` spans for the same
    /// incoming message, use [`Self::parent_context()`] instead
    pub fn follow_from_sender_for(&mut self, span: &Span) {
        if let Some(context) = self.parent_context.take() {
            span.set_parent(context)
        }
    }
}

/// A type that represents a potential response (reciprocal to rpc requests) that
/// is used to send response(s) to the caller.
pub struct Reciprocal<O> {
    protocol_version: ProtocolVersion,
    reply_port: O,
    _phantom: PhantomData<O>,
}

// --- BEGIN RPC ---
/// A typed Rpc request
pub struct Rpc<M> {
    reply_port: RpcReplyPort,
    payload: Bytes,
    sort_code: Option<u64>,
    _phantom: PhantomData<M>,
}

/// Wrapper for reciprocal to send rpc responses
pub struct Oneshot<O: RpcResponse> {
    inner: RpcReplyPort,
    _phantom: PhantomData<O>,
}

pub struct ReplyEnvelope {
    pub(crate) body: rpc_reply::Body,
    pub(crate) span: Span,
}
// one-shot rpc reply
pub(crate) struct RpcReplyPort(oneshot::Sender<ReplyEnvelope>);
impl RpcReplyPort {
    pub fn new() -> (RpcReplyPort, oneshot::Receiver<ReplyEnvelope>) {
        let (tx, rx) = oneshot::channel();
        (Self(tx), rx)
    }
}

/// Untyped RPC message
#[derive(derive_more::Debug)]
pub struct RawRpc {
    #[debug(skip)]
    pub(super) reply_port: RpcReplyPort,
    #[debug("Bytes({} bytes)", payload.len())]
    pub(super) payload: Bytes,
    pub(super) sort_code: Option<u64>,
    pub(super) msg_type: String,
}

/// Untyped RPC message bound to a certain service type
#[derive(derive_more::Debug)]
pub struct RawSvcRpc<S> {
    #[debug(skip)]
    pub(super) reply_port: RpcReplyPort,
    #[debug("Bytes({} bytes)", payload.len())]
    pub(super) payload: Bytes,
    pub(super) sort_code: Option<u64>,
    msg_type: String,
    _phantom: PhantomData<S>,
}
// --- END RPC ---

// --- BEGIN WATCH ---
/// A typed watch subscription request
#[derive(derive_more::Debug)]
pub struct Watch<M> {
    #[debug(skip)]
    updates_port: WatchUpdatePort,
    #[debug("Bytes({} bytes)", payload.len())]
    payload: Bytes,
    sort_code: Option<u64>,
    _phantom: PhantomData<M>,
}

/// Wrapper for reciprocal to send rpc responses
pub struct Updates<O: WatchResponse> {
    inner: WatchUpdatePort,
    _phantom: PhantomData<O>,
}

/// Still WIP
#[allow(dead_code)]
pub struct WatchUpdateEnvelope {
    body: watch_update::Body,
}

// streaming watch updates
pub(crate) struct WatchUpdatePort(watch::Sender<WatchUpdateEnvelope>);

pub struct RawSvcWatch<S> {
    updates_port: WatchUpdatePort,
    pub(super) payload: Bytes,
    pub(super) sort_code: Option<u64>,
    msg_type: String,
    _phantom: PhantomData<S>,
}

pub struct RawWatch {
    reply_port: WatchUpdatePort,
    payload: Bytes,
    sort_code: Option<u64>,
    msg_type: String,
}

// --- END WATCH ---

// --- BEGIN UNARY ---
/// A typed Unary message
#[derive(Clone, derive_more::Debug)]
pub struct Unary<M> {
    #[debug("Bytes({} bytes)", payload.len())]
    pub(super) payload: Bytes,
    pub(super) sort_code: Option<u64>,
    pub(super) _phantom: PhantomData<M>,
}

/// Untyped Unary message
#[derive(Clone, derive_more::Debug)]
pub struct RawUnary {
    #[debug("Bytes({} bytes)", payload.len())]
    pub(super) payload: Bytes,
    pub(super) sort_code: Option<u64>,
    pub(super) msg_type: String,
}

#[derive(Clone, derive_more::Debug)]
pub struct RawSvcUnary<S> {
    #[debug("Bytes({} bytes)", payload.len())]
    pub(super) payload: Bytes,
    pub(super) sort_code: Option<u64>,
    msg_type: String,
    _phantom: PhantomData<S>,
}

// --- END UNARY ---

// --- BEGIN STREAM ---

pub struct Sink<O: StreamResponse> {
    inner: mpsc::UnboundedSender<StreamEnvelope>,
    _phantom: PhantomData<O>,
}

pub enum StreamEnvelope {
    Status(i32),
    Payload(Bytes),
}

pub(crate) struct StreamPort {
    inbound_rx: mpsc::Receiver<StreamEnvelope>,
    outbound_tx: mpsc::UnboundedSender<StreamEnvelope>,
}

impl StreamPort {
    pub fn new(capacity: usize) -> (Self, StreamPeerPort) {
        let (inbound_tx, inbound_rx) = mpsc::channel(capacity);
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();

        (
            Self {
                inbound_rx,
                outbound_tx,
            },
            StreamPeerPort {
                outbound_rx,
                inbound_tx,
            },
        )
    }
}

pub(crate) struct StreamPeerPort {
    outbound_rx: mpsc::UnboundedReceiver<StreamEnvelope>,
    inbound_tx: mpsc::Sender<StreamEnvelope>,
}

impl StreamPeerPort {
    pub fn split(
        self,
    ) -> (
        mpsc::Sender<StreamEnvelope>,
        mpsc::UnboundedReceiver<StreamEnvelope>,
    ) {
        (self.inbound_tx, self.outbound_rx)
    }
}

pub struct RawStream {
    pub(super) stream_port: StreamPort,
    pub(super) sort_code: Option<u64>,
    pub(super) msg_type: String,
}

#[derive(derive_more::Debug)]
pub struct RawSvcStream<S> {
    #[debug(skip)]
    stream_port: StreamPort,
    pub(super) sort_code: Option<u64>,
    msg_type: String,
    _phantom: PhantomData<S>,
}

#[derive(derive_more::Debug)]
pub struct BidiStream<M> {
    #[debug(skip)]
    stream_port: StreamPort,
    pub(super) sort_code: Option<u64>,
    _phantom: PhantomData<M>,
}
// --- END STREAM ---

// A polymorphic incoming RPC request bound to a certain service
impl<S> Incoming<RawSvcRpc<S>> {
    /// The sort-code is applicable if the sender specifies a target mailbox for this message.
    ///
    /// The original sender specifies the sort-code to guide the receiver to route the message
    /// internally before processing/decoding it. For instance, this can be a partition-id, log-id,
    /// loglet-id, or any value that can be encoded as u64.
    ///
    /// The value is opaque to the message fabric infrastructure.
    pub fn sort_code(&self) -> Option<u64> {
        self.inner.sort_code
    }

    /// Fails the request and report status back to the caller
    ///
    /// Check documentation of [[Verdict]] for more details
    pub fn fail(self, status: Verdict) {
        let status = rpc_reply::Status::from(status);
        let _ = self.inner.reply_port.0.send(ReplyEnvelope {
            body: rpc_reply::Body::Status(status.into()),
            span: Span::current(),
        });
    }
}

impl Incoming<RawRpc> {
    pub fn msg_type(&self) -> &str {
        &self.inner.msg_type
    }
}

impl<S: Service> Incoming<RawSvcRpc<S>> {
    pub(crate) fn from_raw_rpc(raw: Incoming<RawRpc>) -> Self {
        Incoming {
            protocol_version: raw.protocol_version,
            inner: RawSvcRpc {
                reply_port: raw.inner.reply_port,
                payload: raw.inner.payload,
                sort_code: raw.inner.sort_code,
                msg_type: raw.inner.msg_type,
                _phantom: PhantomData,
            },
            peer: raw.peer,
            metadata_version: raw.metadata_version,
            parent_context: raw.parent_context,
        }
    }

    #[inline(always)]
    pub fn msg_type(&self) -> &str {
        &self.inner.msg_type
    }

    /// Moves into a typed message.
    ///
    /// Returns the original message if the type of the message doesn't match the inner body.
    #[allow(clippy::result_large_err)]
    pub fn try_into_typed<M>(self) -> Result<Incoming<Rpc<M>>, Self>
    where
        M: RpcRequest<Service = S>,
    {
        if M::TYPE != self.inner.msg_type {
            return Err(self);
        }
        Ok(self.into_typed())
    }

    /// Moves into a typed message. The caller is responsible for ensuring that the raw
    /// payload can be decoded into the correct type.
    ///
    /// In debug builds, this panics if the message type string of the inner message doesn't match
    /// that of the the type M.
    pub fn into_typed<M>(self) -> Incoming<Rpc<M>>
    where
        M: RpcRequest<Service = S>,
    {
        debug_assert_eq!(M::TYPE, self.inner.msg_type);
        Incoming {
            inner: Rpc {
                reply_port: self.inner.reply_port,
                payload: self.inner.payload,
                sort_code: self.inner.sort_code,
                _phantom: PhantomData,
            },
            protocol_version: self.protocol_version,
            peer: self.peer,
            metadata_version: self.metadata_version,
            parent_context: self.parent_context,
        }
    }
}

impl<S> Incoming<RawSvcStream<S>> {
    /// The sort-code is applicable if the sender specifies a target mailbox for this message.
    ///
    /// The original sender specifies the sort-code to guide the receiver to route the message
    /// internally before processing/decoding it. For instance, this can be a partition-id, log-id,
    /// loglet-id, or any value that can be encoded as u64.
    ///
    /// The value is opaque to the message fabric infrastructure.
    pub fn sort_code(&self) -> Option<u64> {
        self.inner.sort_code
    }

    /// Fails the request and report status back to the caller
    ///
    /// Check documentation of [[Verdict]] for more details
    pub fn fail(self, status: Verdict) {
        let status = network::StreamStatus::from(status);
        _ = self
            .inner
            .stream_port
            .outbound_tx
            .send(StreamEnvelope::Status(status.into()));
    }
}

impl Incoming<RawStream> {
    pub fn msg_type(&self) -> &str {
        &self.inner.msg_type
    }
}

impl<S: Service> Incoming<RawSvcStream<S>> {
    pub(crate) fn from_raw_stream(raw: Incoming<RawStream>) -> Self {
        Incoming {
            protocol_version: raw.protocol_version,
            inner: RawSvcStream {
                stream_port: raw.inner.stream_port,
                sort_code: raw.inner.sort_code,
                msg_type: raw.inner.msg_type,
                _phantom: PhantomData,
            },
            peer: raw.peer,
            metadata_version: raw.metadata_version,
            parent_context: raw.parent_context,
        }
    }

    #[inline(always)]
    pub fn msg_type(&self) -> &str {
        &self.inner.msg_type
    }

    /// Moves into a typed message.
    ///
    /// Returns the original message if the type of the message doesn't match the inner body.
    #[allow(clippy::result_large_err)]
    pub fn try_into_typed<M>(self) -> Result<Incoming<BidiStream<M>>, Self>
    where
        M: StreamRequest<Service = S>,
    {
        if M::TYPE != self.inner.msg_type {
            return Err(self);
        }
        Ok(self.into_typed())
    }

    /// Moves into a typed message. The caller is responsible for ensuring that the raw
    /// payload can be decoded into the correct type.
    ///
    /// In debug builds, this panics if the message type string of the inner message doesn't match
    /// that of the the type M.
    pub fn into_typed<M>(self) -> Incoming<BidiStream<M>>
    where
        M: StreamRequest<Service = S>,
    {
        debug_assert_eq!(M::TYPE, self.inner.msg_type);
        Incoming {
            inner: BidiStream {
                stream_port: self.inner.stream_port,
                sort_code: self.inner.sort_code,
                _phantom: PhantomData,
            },
            protocol_version: self.protocol_version,
            peer: self.peer,
            metadata_version: self.metadata_version,
            parent_context: self.parent_context,
        }
    }
}

pub struct TypedStream<M> {
    inner: mpsc::Receiver<StreamEnvelope>,
    protocol_version: ProtocolVersion,
    _phantom: PhantomData<M>,
}

impl<M> TypedStream<M> {
    pub(crate) fn new(
        inner: mpsc::Receiver<StreamEnvelope>,
        protocol_version: ProtocolVersion,
    ) -> Self {
        Self {
            inner,
            protocol_version,
            _phantom: PhantomData,
        }
    }
}

impl<M> Stream for TypedStream<M>
where
    M: codec::WireEncode + codec::WireDecode + Unpin + Send,
{
    type Item = Result<M, StreamMessageError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let msg = ready!(self.inner.poll_recv(cx)).map(|envelope| match envelope {
            StreamEnvelope::Payload(bytes) => Ok(M::decode(bytes, self.protocol_version)),
            StreamEnvelope::Status(status) => Err(StreamMessageError::from(status)),
        });
        std::task::Poll::Ready(msg)
    }
}

impl<M: StreamRequest> Incoming<BidiStream<M>> {
    pub fn sort_code(&self) -> Option<u64> {
        self.inner.sort_code
    }

    /// Consumes the message and returns a tuple of a reciprocal (reply port) and typed stream
    /// of messages
    pub fn split(self) -> (Reciprocal<Sink<M::Response>>, TypedStream<M>) {
        let stream = TypedStream::new(self.inner.stream_port.inbound_rx, self.protocol_version);

        (
            Reciprocal {
                protocol_version: self.protocol_version,
                reply_port: Sink {
                    inner: self.inner.stream_port.outbound_tx,
                    _phantom: PhantomData,
                },
                _phantom: PhantomData,
            },
            stream,
        )
    }
}

impl<M: StreamResponse> Reciprocal<Sink<M>> {
    /// Send a message to upstream peer
    pub fn send(&self, message: M) -> Result<(), StreamClosed> {
        let bytes = message
            .encode_to_bytes(self.protocol_version)
            .expect("serialize stream message to work");

        self.reply_port
            .inner
            .send(StreamEnvelope::Payload(bytes))
            .map_err(|_| StreamClosed)
    }

    /// Send a failure message to upstream peer
    // todo(azmy): Verdict is the not the right failure message to send back
    // since the stream is already open this should be stream specific
    // for example, load-shedding is a possible failure, but not unrecognized message
    pub fn fail(&self, verdict: Verdict) -> Result<(), StreamClosed> {
        self.reply_port
            .inner
            .send(StreamEnvelope::Status(
                network::StreamStatus::from(verdict) as i32
            ))
            .map_err(|_| StreamClosed)
    }
}

// A polymorphic incoming Unary request bound to a certain service
impl<S> Incoming<RawSvcUnary<S>> {
    /// The sort-code is applicable if the sender specifies a target mailbox for this message.
    ///
    /// The original sender specifies the sort-code to guide the receiver to route the message
    /// internally before processing/decoding it. For instance, this can be a partition-id, log-id,
    /// loglet-id, or any value that can be encoded as u64.
    ///
    /// The value is opaque to the message fabric infrastructure.
    pub fn sort_code(&self) -> Option<u64> {
        self.inner.sort_code
    }
}

impl Incoming<RawUnary> {
    pub fn msg_type(&self) -> &str {
        &self.inner.msg_type
    }
}

impl<S: Service> Incoming<RawSvcUnary<S>> {
    pub(crate) fn from_raw_unary(raw: Incoming<RawUnary>) -> Self {
        Incoming {
            protocol_version: raw.protocol_version,
            inner: RawSvcUnary {
                payload: raw.inner.payload,
                sort_code: raw.inner.sort_code,
                msg_type: raw.inner.msg_type,
                _phantom: PhantomData,
            },
            peer: raw.peer,
            metadata_version: raw.metadata_version,
            parent_context: raw.parent_context,
        }
    }

    #[inline(always)]
    pub fn msg_type(&self) -> &str {
        &self.inner.msg_type
    }

    /// Moves into a typed message.
    ///
    /// Returns the original message if the type of the message doesn't match the inner body.
    #[allow(clippy::result_large_err)]
    pub fn try_into_typed<M>(self) -> Result<Incoming<Unary<M>>, Self>
    where
        M: UnaryMessage<Service = S>,
    {
        if M::TYPE != self.inner.msg_type {
            return Err(self);
        }
        Ok(self.into_typed())
    }

    /// Moves into a typed message. The caller is responsible for ensuring that the raw
    /// payload can be decoded into the correct type.
    ///
    /// In debug builds, this panics if the message type string of the inner message doesn't match
    /// that of the the type M.
    pub fn into_typed<M>(self) -> Incoming<Unary<M>>
    where
        M: UnaryMessage<Service = S>,
    {
        debug_assert_eq!(M::TYPE, self.inner.msg_type);
        Incoming {
            inner: Unary {
                payload: self.inner.payload,
                sort_code: self.inner.sort_code,
                _phantom: PhantomData,
            },
            protocol_version: self.protocol_version,
            peer: self.peer,
            metadata_version: self.metadata_version,
            parent_context: self.parent_context,
        }
    }
}

impl Incoming<RawWatch> {
    pub fn msg_type(&self) -> &str {
        &self.inner.msg_type
    }
}

// A polymorphic incoming subscription request bound to a certain service
impl<S> Incoming<RawSvcWatch<S>> {
    /// The sort-code is applicable if the sender specifies a target mailbox for this message.
    ///
    /// The original sender specifies the sort-code to guide the receiver to route the message
    /// internally before processing/decoding it. For instance, this can be a partition-id, log-id,
    /// loglet-id, or any value that can be encoded as u64.
    ///
    /// The value is opaque to the message fabric infrastructure.
    pub fn sort_code(&self) -> Option<u64> {
        self.inner.sort_code
    }
}

impl<S: Service> Incoming<RawSvcWatch<S>> {
    pub(crate) fn from_raw_watch(raw: Incoming<RawWatch>) -> Self {
        Incoming {
            protocol_version: raw.protocol_version,
            inner: RawSvcWatch {
                updates_port: raw.inner.reply_port,
                payload: raw.inner.payload,
                sort_code: raw.inner.sort_code,
                msg_type: raw.inner.msg_type,
                _phantom: PhantomData,
            },
            peer: raw.peer,
            metadata_version: raw.metadata_version,
            parent_context: raw.parent_context,
        }
    }

    #[inline(always)]
    pub fn msg_type(&self) -> &str {
        &self.inner.msg_type
    }

    /// Fails the request and report status back to the caller
    ///
    /// Check documentation of [[Verdict]] for more details
    pub fn fail(self, v: Verdict) {
        let status = watch_update::Start::from(v);
        let _ = self.inner.updates_port.0.send(WatchUpdateEnvelope {
            body: watch_update::Body::Start(status.into()),
        });
    }

    /// Moves into a typed message.
    ///
    ///
    /// Returns the original message if the type of the message doesn't match the inner body.
    #[allow(clippy::result_large_err)]
    pub fn try_into_typed<M>(self) -> Result<Incoming<Watch<M>>, Self>
    where
        M: WatchRequest<Service = S>,
    {
        if M::TYPE != self.inner.msg_type {
            return Err(self);
        }
        Ok(self.into_typed())
    }

    /// Moves into a typed message. The caller is responsible for ensuring that the raw
    /// payload can be decoded into the correct type.
    ///
    /// In debug builds, this panics if the message type string of the inner message doesn't match
    /// that of the the type M.
    pub fn into_typed<M>(self) -> Incoming<Watch<M>>
    where
        M: WatchRequest<Service = S>,
    {
        debug_assert_eq!(M::TYPE, self.inner.msg_type);
        Incoming {
            inner: Watch {
                updates_port: self.inner.updates_port,
                payload: self.inner.payload,
                sort_code: self.inner.sort_code,
                _phantom: PhantomData,
            },
            protocol_version: self.protocol_version,
            peer: self.peer,
            metadata_version: self.metadata_version,
            parent_context: self.parent_context,
        }
    }
}

// Incoming RPC request
impl<M> Incoming<Rpc<M>> {
    /// The sort-code is applicable if the sender specifies a target mailbox for this message.
    ///
    /// The original sender specifies the sort-code to guide the receiver to route the message
    /// internally before processing/decoding it. For instance, this can be a partition-id, log-id,
    /// loglet-id, or any value that can be encoded as u64.
    ///
    /// The value is opaque to the message fabric infrastructure.
    pub fn sort_code(&self) -> Option<u64> {
        self.inner.sort_code
    }
}

impl<M: RpcRequest + WireDecode> Incoming<Rpc<M>> {
    /// Decodes the message based on the message type.
    ///
    /// Note that this will immediately close the reply port. You'll not be able to reply to this
    /// rpc request if you use this method.
    ///
    /// **Panics** if message decoding failed
    pub fn into_body(self) -> M {
        M::decode(self.inner.payload, self.protocol_version)
    }

    /// Consumes the message and returns a tuple of a reciprocal (reply port) and the decoded body
    /// of the message.
    ///
    /// **Panics** if message decoding failed
    pub fn split(self) -> (Reciprocal<Oneshot<M::Response>>, M) {
        let body = M::decode(self.inner.payload, self.protocol_version);
        (
            Reciprocal {
                protocol_version: self.protocol_version,
                reply_port: Oneshot {
                    inner: self.inner.reply_port,
                    _phantom: PhantomData,
                },
                _phantom: PhantomData,
            },
            body,
        )
    }

    /// Consumes the message and returns a tuple of a reciprocal (reply port) and drops the body
    /// without decoding.
    ///
    /// This is useful if you want to send a response back to the caller without decoding the
    /// message.
    pub fn into_reciprocal(self) -> Reciprocal<Oneshot<M::Response>> {
        Reciprocal {
            protocol_version: self.protocol_version,
            reply_port: Oneshot {
                inner: self.inner.reply_port,
                _phantom: PhantomData,
            },
            _phantom: PhantomData,
        }
    }
}

impl<M: WireDecode> Incoming<Unary<M>> {
    /// The sort-code is applicable if the sender specifies a target mailbox for this message.
    ///
    /// The original sender specifies the sort-code to guide the receiver to route the message
    /// internally before processing/decoding it. For instance, this can be a partition-id, log-id,
    /// loglet-id, or any value that can be encoded as u64.
    ///
    /// The value is opaque to the message fabric infrastructure.
    pub fn sort_code(&self) -> Option<u64> {
        self.inner.sort_code
    }
    /// Decodes the message based on the message type
    pub fn try_into_body(self) -> Result<M, <M as WireDecode>::Error> {
        M::try_decode(self.inner.payload, self.protocol_version)
    }

    /// Decodes the message based on the message type
    ///
    /// **Panics** if message decoding failed
    pub fn into_body(self) -> M {
        M::decode(self.inner.payload, self.protocol_version)
    }
}

// Incoming Subscription request
impl<M> Incoming<Watch<M>> {
    /// The sort-code is applicable if the sender specifies a target mailbox for this message.
    ///
    /// The original sender specifies the sort-code to guide the receiver to route the message
    /// internally before processing/decoding it. For instance, this can be a partition-id, log-id,
    /// loglet-id, or any value that can be encoded as u64.
    ///
    /// The value is opaque to the message fabric infrastructure.
    pub fn sort_code(&self) -> Option<u64> {
        self.inner.sort_code
    }
}

impl<M: WatchRequest + WireDecode> Incoming<Watch<M>> {
    /// Consumes the message and returns a tuple of a reciprocal (reply port) and the decoded body
    /// of the message.
    ///
    /// **Panics** if message decoding failed
    pub fn split(self) -> (Reciprocal<Updates<M::Response>>, M) {
        let body = M::decode(self.inner.payload, self.protocol_version);
        (
            Reciprocal {
                protocol_version: self.protocol_version,
                reply_port: Updates {
                    inner: self.inner.updates_port,
                    _phantom: PhantomData,
                },
                _phantom: PhantomData,
            },
            body,
        )
    }
}

impl<O: RpcResponse + WireEncode> Reciprocal<Oneshot<O>> {
    /// Ignores the error if connection is closed.
    ///
    /// For a checked send, use `try_send` instead.
    pub fn send(self, msg: O) {
        let _ = self.try_send(msg);
    }

    /// Fails if connection was already dropped
    pub fn try_send(self, msg: O) -> Result<(), ConnectionClosed> {
        // The assumption here is that because this is a reply of an RPC request
        // that we received on this connection, we hard-assume that the reply type
        // serializable on the negotiated protocol version.
        let reply = O::encode_to_bytes(&msg, self.protocol_version)
            .expect("serialization of rpc reply should not fail");
        self.reply_port
            .inner
            .0
            .send(ReplyEnvelope {
                body: rpc_reply::Body::Payload(reply),
                span: Span::current(),
            })
            .map_err(|_| ConnectionClosed)
    }

    /// Returns true if the reply port/connection was closed
    pub fn is_closed(&self) -> bool {
        self.reply_port.inner.0.is_closed()
    }

    /// Sends a processing error to the caller
    ///
    /// Check documentation of [[Verdict]] for more details
    pub fn fail(self, v: Verdict) {
        let status = rpc_reply::Status::from(v);
        let _ = self.reply_port.inner.0.send(ReplyEnvelope {
            body: rpc_reply::Body::Status(status.into()),
            span: Span::current(),
        });
    }
}

impl<O: WatchResponse + WireEncode> Reciprocal<Updates<O>> {
    /// Ignores the error if connection is closed.
    ///
    /// For a checked send, use `try_send` instead.
    pub fn send(&self, msg: O) {
        let _ = self.try_send(msg);
    }

    /// Fails if connection was already dropped
    pub fn try_send(&self, msg: O) -> Result<(), ConnectionClosed> {
        // The assumption here is that because this is a reply of a watch request
        // that we received on this connection, we hard-assume that the reply type
        // serializable on the negotiated protocol version.
        let reply = O::encode_to_bytes(&msg, self.protocol_version)
            .expect("serialization of rpc reply should not fail");
        self.reply_port
            .inner
            .0
            .send(WatchUpdateEnvelope {
                body: watch_update::Body::Update(reply),
            })
            .map_err(|_| ConnectionClosed)
    }

    /// Returns true if the reply port/connection was closed
    pub fn is_closed(&self) -> bool {
        self.reply_port.inner.0.is_closed()
    }

    /// Close the watch subscription and send notification to the peer
    pub fn close(self) {
        let status = watch_update::End::Ok;

        let _ = self.reply_port.inner.0.send(WatchUpdateEnvelope {
            body: watch_update::Body::End(status.into()),
        });
    }
}

#[cfg(feature = "test-util")]
pub mod test_util {
    use super::*;

    use assert2::let_assert;

    pub struct OneshotRxMock<O>(oneshot::Receiver<ReplyEnvelope>, PhantomData<O>);

    impl<O: WireDecode> OneshotRxMock<O> {
        pub async fn recv(self) -> O {
            let_assert!(
                Ok(ReplyEnvelope {
                    body: rpc_reply::Body::Payload(buf),
                    ..
                }) = self.0.await
            );
            O::decode(buf, ProtocolVersion::V2)
        }

        pub fn assert_not_received(mut self) {
            let value = self.0.try_recv();
            assert!(value.err().is_some(), "Nothing received");
        }
    }

    impl<O: RpcResponse + WireEncode + WireDecode> Reciprocal<Oneshot<O>> {
        pub fn mock() -> (Self, OneshotRxMock<O>) {
            let (tx, rx) = RpcReplyPort::new();
            (
                Reciprocal {
                    protocol_version: Default::default(),
                    reply_port: Oneshot {
                        inner: tx,
                        _phantom: PhantomData,
                    },
                    _phantom: PhantomData,
                },
                OneshotRxMock(rx, PhantomData),
            )
        }
    }
}
