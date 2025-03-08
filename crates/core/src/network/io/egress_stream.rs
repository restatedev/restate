// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use std::sync::atomic::AtomicU64;
use std::task::{Context, Poll, ready};

use futures::{FutureExt, Stream};
use tokio::sync::{mpsc, oneshot};

use restate_types::Versioned;
use restate_types::live::Live;
use restate_types::logs::metadata::Logs;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::node::message::ConnectionControl;
use restate_types::protobuf::node::{Header, Message, message::Body};
use restate_types::schema::Schema;

use super::EgressSender;
use super::egress_sender::SendNotifier;
use crate::Metadata;

/// A handle to drop the egress stream remotely, or to be notified if the egress stream has been
/// terminated via other means.
// todo: make pub(crate) after its usage in tests is removed
pub struct DropEgressStream(oneshot::Receiver<Infallible>);

impl DropEgressStream {
    /// same effect as dropping, closing the egress stream and dropping any messages in the buffer
    pub fn close(&mut self) {
        self.0.close();
    }
}

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
pub enum CloseReason {
    /// Node will shutdown and never come back
    Shutdown,
    /// Only this connection is terminating
    ConnectionDrain,
    CodecError(String),
}

impl From<CloseReason> for Body {
    fn from(value: CloseReason) -> Self {
        match value {
            CloseReason::Shutdown => ConnectionControl::shutdown().into(),
            CloseReason::ConnectionDrain => ConnectionControl::connection_reset().into(),
            CloseReason::CodecError(e) => ConnectionControl::codec_error(e).into(),
        }
    }
}

// todo: make this pub(crate) when OwnedConnection::new_fake() stops needing it
pub enum EgressMessage {
    #[cfg(any(test, feature = "test-util"))]
    /// An egress message to send to peer. Used only in tests, header must be populated correctly
    /// by sender
    RawMessage(Message),
    /// An egress body to send to peer, header is populated by egress stream
    Message(Header, Body),
    /// The message that requires an ack that it was sent
    WithNotifer(Body, SendNotifier),
    /// A signal to close the inner stream. The inner stream cannot receive further messages but
    /// we'll continue to drain all buffered messages before droppingx
    Close(CloseReason),
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

    fn fill_header(&mut self, header: &mut Header) {
        header.my_nodes_config_version = Some(self.nodes_config.live_load().version().into());
        header.my_schema_version = Some(self.schema.live_load().version().into());
        header.my_partition_table_version = Some(self.partition_table.live_load().version().into());
        header.my_logs_version = Some(self.logs_metadata.live_load().version().into());
    }
}

/// Egress stream is the egress side of the message fabric. The stream is driven externally
/// (currently tonic/hyper) to stream messages to a peer. In the normal case, it populates the
/// appropriate headers by efficiently fetching latest metadata versions. It also provides a
/// mechanism to externally drain or force-terminate.
///
/// This stream will also handle outgoing rpc call mapping in future changes.
// todo: make pub(crate) after its usage in tests is removed
pub struct EgressStream {
    // connection identifier, local to this node, used to tie egress with ingress streams and
    // identify the connection in logs.
    #[allow(dead_code)]
    cid: u64,
    inner: Option<mpsc::Receiver<EgressMessage>>,
    /// The sole purpose of this channel, is to force drop the egress channel even if the inner stream's
    /// didn't wake us up. For instance, if there is no available space on the socket's sendbuf and
    /// we still want to drop this stream. We'll signal this receiver instead of the inline
    /// PoisonPill. This results a wake-up to drop all buffered messages and releasing the inner
    /// stream.
    drop_notification: Option<oneshot::Sender<Infallible>>,
    metadata_cache: MetadataVersionCache,
}

impl EgressStream {
    pub fn create(capacity: usize) -> (EgressSender, Self, DropEgressStream) {
        static NEXT_CID: AtomicU64 = const { AtomicU64::new(1) };

        let cid = NEXT_CID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(capacity);
        let (drop_tx, drop_rx) = oneshot::channel();
        (
            EgressSender { cid, inner: tx },
            Self {
                cid,
                inner: Some(rx),
                drop_notification: Some(drop_tx),
                metadata_cache: MetadataVersionCache::new(),
            },
            DropEgressStream(drop_rx),
        )
    }
}

impl EgressStream {
    fn terminate_stream(&mut self) {
        self.inner.take();
        self.drop_notification.take();
    }
}

impl Stream for EgressStream {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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

        match self.inner {
            Some(ref mut inner) => {
                match ready!(inner.poll_recv(cx)) {
                    #[cfg(any(test, feature = "test-util"))]
                    Some(EgressMessage::RawMessage(msg)) => Poll::Ready(Some(msg)),
                    Some(EgressMessage::Message(mut header, body)) => {
                        self.metadata_cache.fill_header(&mut header);
                        let msg = Message {
                            header: Some(header),
                            body: Some(body),
                        };
                        Poll::Ready(Some(msg))
                    }
                    Some(EgressMessage::WithNotifer(body, notifier)) => {
                        let mut header = Header::default();
                        self.metadata_cache.fill_header(&mut header);
                        let msg = Message {
                            header: Some(header),
                            body: Some(body),
                        };
                        notifier.notify();
                        Poll::Ready(Some(msg))
                    }
                    Some(EgressMessage::Close(reason)) => {
                        // No new messages can be enqueued, and we'll continue to drain
                        // already enqueued messages.
                        //
                        // reactor will only know when the stream is terminated.
                        inner.close();
                        let mut header = Header::default();
                        self.metadata_cache.fill_header(&mut header);
                        let msg = Message {
                            header: Some(header),
                            body: Some(reason.into()),
                        };
                        Poll::Ready(Some(msg))
                    }
                    None => {
                        self.terminate_stream();
                        Poll::Ready(None)
                    }
                }
            }
            None => {
                self.terminate_stream();
                Poll::Ready(None)
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self.inner {
            Some(ref inner) => (inner.len(), None),
            None => (0, None),
        }
    }
}
