// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use bytes::Bytes;
use futures::{FutureExt, Stream};
use tokio::sync::{mpsc, oneshot};
use tracing::trace;

use restate_types::Versioned;
use restate_types::live::Live;
use restate_types::logs::metadata::Logs;
use restate_types::net::codec::EncodeError;
use restate_types::net::{ProtocolVersion, RpcRequest, Service, ServiceTag, UnaryMessage};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::schema::Schema;

use super::egress_sender::{Sent, UnboundedEgressSender};
use super::rpc_tracker::ReplyTracker;
use super::{EgressSender, SendToken};
use crate::Metadata;
use crate::network::protobuf::network::{self, Datagram};
use crate::network::protobuf::network::{
    Header, Message, message, message::Body, message::ConnectionControl,
};
use crate::network::{ReplyRx, RpcReplyTx};

/// A handle to drop the egress stream remotely, or to be notified if the egress stream has been
/// terminated via other means.
// todo: make pub(crate) after its usage in tests is removed
pub struct DropEgressStream(oneshot::Receiver<Infallible>);

impl Future for DropEgressStream {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(self.0.poll_unpin(cx)) {
            Ok(_) => unreachable!(),
            Err(_) => Poll::Ready(()),
        }
    }
}

#[derive(Debug)]
pub enum DrainReason {
    /// Node will shutdown and never come back
    Shutdown,
    /// Only this connection is terminating
    ConnectionDrain,
    CodecError(String),
}

impl From<DrainReason> for Body {
    fn from(value: DrainReason) -> Self {
        match value {
            DrainReason::Shutdown => ConnectionControl::shutdown().into(),
            DrainReason::ConnectionDrain => ConnectionControl::connection_reset().into(),
            DrainReason::CodecError(e) => ConnectionControl::codec_error(e).into(),
        }
    }
}

pub enum EgressMessage {
    /// An egress message to send to peer. Used only in tests, header must be populated correctly
    /// by sender
    #[cfg(feature = "test-util")]
    RawMessage(Message),
    UnaryMessage(Body),
    Unary {
        service_tag: ServiceTag,
        msg_type: &'static str,
        payload: Bytes,
        sort_code: Option<u64>,
        version: ProtocolVersion,
        notifier: Sent,
        #[cfg(feature = "test-util")]
        header: Option<Header>,
    },
    RpcCall {
        service_tag: ServiceTag,
        msg_type: &'static str,
        payload: Bytes,
        reply_sender: RpcReplyTx,
        sort_code: Option<u64>,
        version: ProtocolVersion,
        #[cfg(feature = "test-util")]
        header: Option<Header>,
    },
    /// An egress body to send to peer, header is populated by egress stream
    Message(Body),
    /// A signal to close the bounded stream. The inner stream cannot receive further messages but
    /// we'll continue to drain all buffered messages before dropping.
    Close(DrainReason),
}

impl EgressMessage {
    pub fn make_rpc_message<M: RpcRequest>(
        message: M,
        sort_code: Option<u64>,
        protocol_version: ProtocolVersion,
    ) -> Result<(EgressMessage, ReplyRx<M::Response>), EncodeError> {
        let (reply_sender, reply_token) = ReplyRx::new();
        let payload = message.encode_to_bytes(protocol_version)?;
        Ok((
            EgressMessage::RpcCall {
                payload,
                reply_sender,
                sort_code,
                service_tag: M::Service::TAG,
                msg_type: M::TYPE,
                version: protocol_version,
                #[cfg(feature = "test-util")]
                header: None,
            },
            reply_token,
        ))
    }

    #[cfg(feature = "test-util")]
    pub fn make_rpc_message_with_header<M: RpcRequest>(
        message: M,
        sort_code: Option<u64>,
        protocol_version: ProtocolVersion,
        header: Header,
    ) -> Result<(EgressMessage, ReplyRx<M::Response>), EncodeError> {
        let (reply_sender, reply_token) = ReplyRx::new();
        let payload = message.encode_to_bytes(protocol_version)?;
        Ok((
            EgressMessage::RpcCall {
                payload,
                reply_sender,
                sort_code,
                service_tag: M::Service::TAG,
                msg_type: M::TYPE,
                version: protocol_version,
                header: Some(header),
            },
            reply_token,
        ))
    }

    pub fn make_unary_message<M: UnaryMessage>(
        message: M,
        sort_code: Option<u64>,
        protocol_version: ProtocolVersion,
    ) -> Result<(EgressMessage, SendToken), EncodeError> {
        let (notifier, token) = Sent::create();
        let payload = message.encode_to_bytes(protocol_version)?;

        Ok((
            EgressMessage::Unary {
                service_tag: M::Service::TAG,
                msg_type: M::TYPE,
                sort_code,
                payload,
                version: protocol_version,
                notifier,
                #[cfg(feature = "test-util")]
                header: None,
            },
            token,
        ))
    }
}

struct MetadataVersionCache {
    nodes_config: Live<NodesConfiguration>,
    schema: Live<Schema>,
    partition_table: Live<PartitionTable>,
    logs_metadata: Live<Logs>,
}

impl MetadataVersionCache {
    fn new() -> Self {
        let metadata = Metadata::current();
        Self {
            nodes_config: metadata.updateable_nodes_config(),
            schema: metadata.updateable_schema(),
            partition_table: metadata.updateable_partition_table(),
            logs_metadata: metadata.updateable_logs_metadata(),
        }
    }
}

#[derive(Debug, Default)]
enum State {
    #[default]
    Open,
    // close reason has been sent
    Draining,
    // All channels closed
    Closed,
}

enum Decision {
    /// Ready to send the message
    Ready(Message),
    /// Sending a message indicating starting the drain
    StartDrain(Message),
    /// Sending a drained message
    SendDrainedMessage,
    /// message skipped, move to next
    Continue,
    /// No more messages will arrive
    Drained,
    /// Waiting for more messages
    Pending,
}

/// Egress stream is the egress side of the message fabric. The stream is driven externally
/// (currently tonic/hyper) to stream messages to a peer. In the normal case, it populates the
/// appropriate headers by efficiently fetching latest metadata versions. It also provides a
/// mechanism to externally drain or force-terminate.
pub struct EgressStream {
    msg_id: u64,
    state: State,
    /// requests or unary messages are written to this channel
    bounded: Option<mpsc::Receiver<EgressMessage>>,
    /// responses to rpcs or handshake and other control signals are written to this channel
    unbounded: Option<mpsc::UnboundedReceiver<EgressMessage>>,
    reply_tracker: Arc<ReplyTracker>,
    /// The sole purpose of this channel, is to force drop the egress channel even if the inner stream's
    /// didn't wake us up. For instance, if there is no available space on the socket's sendbuf and
    /// we still want to drop this stream. We'll signal this by dropping the receiver.
    /// This results a wake-up to drop all buffered messages and releasing the inner
    /// stream.
    drop_notification: Option<oneshot::Sender<Infallible>>,
    metadata_cache: MetadataVersionCache,
    /// Did we send out a RequestStreamDrained signal to peer or not?
    sent_request_stream_drained: bool,
    /// Did we send out a ResponseStreamDrained signal to peer or not?
    sent_response_stream_drained: bool,
}

impl EgressStream {
    // for loopback connections, we set the capacity high to avoid CSP-related deadlocks
    pub fn create_loopback() -> (EgressSender, Self, super::Shared) {
        Self::new(1024)
    }

    pub fn create() -> (EgressSender, Self, super::Shared) {
        // We want the majority of the buffering to happen on the socket so we set the capacity
        // pretty low.
        Self::new(10)
    }

    pub fn with_capacity(capacity: usize) -> (EgressSender, Self, super::Shared) {
        Self::new(capacity)
    }

    fn new(capacity: usize) -> (EgressSender, Self, super::Shared) {
        let (unbounded_tx, unbounded) = mpsc::unbounded_channel();
        let (tx, rx) = mpsc::channel(capacity);
        let (drop_tx, drop_rx) = oneshot::channel();
        let reply_tracker: Arc<ReplyTracker> = Default::default();
        (
            EgressSender::new(tx),
            Self {
                msg_id: 0,
                state: State::default(),
                bounded: Some(rx),
                unbounded: Some(unbounded),
                reply_tracker: reply_tracker.clone(),
                drop_notification: Some(drop_tx),
                metadata_cache: MetadataVersionCache::new(),
                sent_request_stream_drained: false,
                sent_response_stream_drained: false,
            },
            super::Shared {
                tx: Some(UnboundedEgressSender::new(unbounded_tx)),
                drop_egress: Some(DropEgressStream(drop_rx)),
                reply_tracker,
            },
        )
    }

    fn terminate_stream(&mut self) {
        self.bounded.take();
        self.unbounded.take();
        self.drop_notification.take();
        self.state = State::Closed;
    }

    #[inline(always)]
    fn next_msg_id(&mut self) -> u64 {
        self.msg_id = self.msg_id.wrapping_add(1);
        self.msg_id
    }

    fn stop_external_senders(&mut self) {
        self.state = State::Draining;
    }

    fn fill_header(&mut self, header: &mut Header) {
        header.my_nodes_config_version = self
            .metadata_cache
            .nodes_config
            .live_load()
            .version()
            .into();
        header.my_schema_version = self.metadata_cache.schema.live_load().version().into();
        header.my_partition_table_version = self
            .metadata_cache
            .partition_table
            .live_load()
            .version()
            .into();
        header.my_logs_version = self
            .metadata_cache
            .logs_metadata
            .live_load()
            .version()
            .into();
    }

    fn make_drained_message(&mut self, signal: message::Signal) -> Message {
        let mut header = Header::default();
        let control = ConnectionControl {
            signal: signal.into(),
            message: String::new(),
        };
        self.fill_header(&mut header);
        Message {
            header: Some(header),
            body: Some(control.into()),
        }
    }

    fn handle_message(
        &mut self,
        entry: Poll<Option<EgressMessage>>,
        drain_message_sent: bool,
    ) -> Decision {
        match entry {
            #[cfg(feature = "test-util")]
            Poll::Ready(Some(EgressMessage::RawMessage(msg))) => Decision::Ready(msg),
            Poll::Ready(Some(EgressMessage::Message(body))) => {
                let mut header = Header::default();
                self.fill_header(&mut header);
                let msg = Message {
                    header: Some(header),
                    body: Some(body),
                };
                Decision::Ready(msg)
            }
            Poll::Ready(Some(EgressMessage::UnaryMessage(body))) => {
                let mut header = Header::default();
                self.fill_header(&mut header);
                let msg = Message {
                    header: Some(header),
                    body: Some(body),
                };
                Decision::Ready(msg)
            }
            Poll::Ready(Some(EgressMessage::Unary {
                payload,
                msg_type,
                sort_code,
                service_tag,
                version: _,
                notifier,
                #[cfg(feature = "test-util")]
                    header: custom_header,
            })) => {
                let mut header = Header::default();
                self.fill_header(&mut header);
                #[cfg(feature = "test-util")]
                let header = custom_header.unwrap_or(header);
                let body = Datagram {
                    datagram: Some(
                        network::Unary {
                            payload,
                            service: service_tag as i32,
                            msg_type: msg_type.to_owned(),
                            sort_code,
                        }
                        .into(),
                    ),
                };
                let msg = Message {
                    header: Some(header),
                    body: Some(body.into()),
                };
                notifier.notify();
                Decision::Ready(msg)
            }
            Poll::Ready(Some(EgressMessage::RpcCall {
                payload,
                msg_type,
                service_tag,
                sort_code,
                reply_sender,
                version: _,
                #[cfg(feature = "test-util")]
                    header: custom_header,
            })) => {
                // note: if we want to support remote cancellation in the future, we need to
                // generate a new oneshot channel here and spawn a task to connect the two +
                // monitor the original sender for closure.
                if reply_sender.is_closed() {
                    return Decision::Continue;
                }

                let mut header = Header::default();
                self.fill_header(&mut header);
                #[cfg(feature = "test-util")]
                let header = custom_header.unwrap_or(header);

                let msg_id = self.next_msg_id();
                trace!(
                    rpc_id = %msg_id,
                    "Sending RPC call: {service_tag}::{msg_type}",
                );
                // V2+
                self.reply_tracker.register_rpc(msg_id, reply_sender);
                let body = Datagram {
                    datagram: Some(
                        network::RpcCall {
                            payload,
                            id: msg_id,
                            service: service_tag as i32,
                            msg_type: msg_type.to_owned(),
                            sort_code,
                        }
                        .into(),
                    ),
                };
                let msg = Message {
                    header: Some(header),
                    body: Some(body.into()),
                };
                Decision::Ready(msg)
            }
            Poll::Ready(Some(EgressMessage::Close(reason)))
                if matches!(self.state, State::Open) =>
            {
                // No new messages can be enqueued, and we'll continue to drain
                // already enqueued messages.
                self.stop_external_senders();
                let mut header = Header::default();
                self.fill_header(&mut header);
                let msg = Message {
                    header: Some(header),
                    body: Some(reason.into()),
                };
                Decision::StartDrain(msg)
            }
            // we are already draining, don't send another reason
            Poll::Ready(Some(EgressMessage::Close(_))) => Decision::Continue,
            Poll::Ready(None) if !drain_message_sent => Decision::SendDrainedMessage,
            Poll::Ready(None) => Decision::Drained,
            Poll::Pending => Decision::Pending,
        }
    }
}

impl Stream for EgressStream {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let State::Closed = self.state {
            return Poll::Ready(None);
        }

        match self.drop_notification {
            // This is a terminated stream already
            None => {
                self.terminate_stream();
                return Poll::Ready(None);
            }
            Some(ref mut drop_notification) => {
                match drop_notification.poll_closed(cx) {
                    Poll::Ready(()) => {
                        // drop has been requested, we'll immediately drop the inner sender and
                        // terminate this stream
                        self.terminate_stream();
                        return Poll::Ready(None);
                    }
                    // fall-through to read the inner stream
                    Poll::Pending => {}
                }
            }
        }

        // Unbounded channel is the priority [this might change in the future]
        // this means that rpc responses and control messages will take precedence vs. sending
        // requests or unary messages:
        // - Reduce tail latency for sending responses
        // - Allows the unbounded channel to be drained faster
        // - Allows us to react to drain signals quickly
        // - downside is potential starvation of the bounded stream.
        let unbounded_finished = loop {
            let drain_message_sent = self.sent_response_stream_drained;
            match self.unbounded.take() {
                Some(mut inner) => {
                    match self.handle_message(inner.poll_recv(cx), drain_message_sent) {
                        Decision::Ready(msg) => {
                            self.unbounded = Some(inner);
                            return Poll::Ready(Some(msg));
                        }
                        Decision::Continue => {
                            self.unbounded = Some(inner);
                        }
                        Decision::StartDrain(msg) => {
                            self.unbounded = Some(inner);
                            // drain only closes the request stream (bounded receiver) and doesn't
                            // impact the unbounded stream.
                            if let Some(stream) = self.bounded.as_mut() {
                                stream.close();
                            }
                            return Poll::Ready(Some(msg));
                        }
                        Decision::SendDrainedMessage => {
                            let msg =
                                self.make_drained_message(message::Signal::ResponseStreamDrained);
                            self.sent_response_stream_drained = true;
                            return Poll::Ready(Some(msg));
                        }
                        Decision::Pending => {
                            self.unbounded = Some(inner);
                            break false;
                        }
                        Decision::Drained => break true,
                    }
                }
                // channel was closed and drained
                None => {
                    break true;
                }
            }
        };

        // bounded channel
        let bounded_finished = loop {
            let drain_message_sent = self.sent_request_stream_drained;
            match self.bounded.take() {
                Some(mut inner) => {
                    match self.handle_message(inner.poll_recv(cx), drain_message_sent) {
                        Decision::Ready(msg) => {
                            self.bounded = Some(inner);
                            return Poll::Ready(Some(msg));
                        }
                        Decision::Continue => {
                            self.bounded = Some(inner);
                        }
                        Decision::StartDrain(msg) => {
                            inner.close();
                            self.bounded = Some(inner);
                            return Poll::Ready(Some(msg));
                        }
                        Decision::SendDrainedMessage => {
                            let msg =
                                self.make_drained_message(message::Signal::RequestStreamDrained);
                            self.sent_request_stream_drained = true;
                            return Poll::Ready(Some(msg));
                        }
                        Decision::Pending => {
                            self.bounded = Some(inner);
                            break false;
                        }
                        Decision::Drained => break true,
                    }
                }
                // channel was closed and drained
                None => {
                    break true;
                }
            }
        };

        if bounded_finished && unbounded_finished {
            self.terminate_stream();
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let bounded_len = self.bounded.as_ref().map(|inner| inner.len()).unwrap_or(0);
        let unbounded_len = self
            .unbounded
            .as_ref()
            .map(|inner| inner.len())
            .unwrap_or(0);
        (bounded_len + unbounded_len, None)
    }
}
