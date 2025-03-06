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
use std::task::{Context, Poll, ready};

use futures::{FutureExt, Stream};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::trace;

use restate_types::Versioned;
use restate_types::live::Live;
use restate_types::logs::metadata::Logs;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::node::{Header, Message, message::Body};
use restate_types::schema::Schema;

use crate::Metadata;

/// A handle to drop the egress stream remotely, or to be notified if the egress stream has been
/// terminated via other means.
// todo: make pub(crate) after its usage in tests is removed
pub struct DropEgressStream(oneshot::Receiver<Infallible>);

impl DropEgressStream {
    /// same effect as dropping, closing the egress stream and dropping any messages in the buffer
    #[allow(dead_code)]
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

// todo: make this pub(crate) when OwnedConnection::new_fake() stops needing it.
pub enum EgressMessage {
    #[cfg(any(test, feature = "test-util"))]
    /// An egress message to send to peer. Used only in tests, header must be populated correctly
    /// by sender.
    RawMessage(Message),
    /// An egress body to send to peer, header is populated by egress stream
    Message(Header, Body),
    /// The message that requires an ack that it was sent. The sender part of this channel can use
    /// `closed` to wait for the notification.
    #[allow(dead_code)]
    MessageWithAck(Header, Body, oneshot::Receiver<Infallible>),
    /// Last message of the stream before dropping, the inner stream will be dropped upon processing this message.
    #[allow(dead_code)]
    PoisonPill,
    /// A signal to close the inner stream. The inner stream cannot receive further messages but
    /// we'll continue to drain all buffered messages before dropping.
    #[allow(dead_code)]
    Drain,
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

// todo: make pub(crate) after its usage in tests is removed
pub struct EgressStream {
    inner: Option<mpsc::Receiver<EgressMessage>>,
    /// The sole purpose of this channel, is to force drop the egress channel even if the inner stream was
    /// not polled. For instance, if there is no available space on the sendbuf of the socket and
    /// we'd still want to drop this stream. We'll signal this receiver instead of the inline
    /// PoisonPill. This results in dropped all buffered messages and dropping releasing this
    /// stream at the next poll (which will be triggered by this receiver).
    drop_notification: Option<oneshot::Sender<Infallible>>,
    metadata_cache: MetadataVersionCache,
}

impl EgressStream {
    pub fn create(capacity: usize) -> (mpsc::Sender<EgressMessage>, Self, DropEgressStream) {
        let (tx, rx) = mpsc::channel(capacity);
        let (drop_egress, egress) = Self::wrap_receiver(rx);
        (tx, egress, drop_egress)
    }

    pub(crate) fn wrap_receiver(inner: mpsc::Receiver<EgressMessage>) -> (DropEgressStream, Self) {
        let (drop_tx, drop_rx) = oneshot::channel();
        (
            DropEgressStream(drop_rx),
            Self {
                // capacity,
                inner: Some(inner),
                drop_notification: Some(drop_tx),
                metadata_cache: MetadataVersionCache::new(),
            },
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
        loop {
            match self.drop_notification {
                // This is a terminated stream already
                None => {
                    self.terminate_stream();
                    trace!("Egress stream was already terminated (drop_rx)");
                    return Poll::Ready(None);
                }
                Some(ref mut drop_notification) => {
                    match drop_notification.poll_closed(cx) {
                        Poll::Ready(()) => {
                            // drop has been requested, we'll immediately drop the inner sender and
                            // terminate this stream
                            trace!("Egress stream was terminated via drop_rx");
                            self.terminate_stream();
                            return Poll::Ready(None);
                        }
                        Poll::Pending => {
                            // read the inner stream
                        }
                    }
                }
            }

            match self.inner {
                Some(ref mut inner) => {
                    match ready!(inner.poll_recv(cx)) {
                        #[cfg(any(test, feature = "test-util"))]
                        Some(EgressMessage::RawMessage(msg)) => {
                            return Poll::Ready(Some(msg));
                        }
                        Some(EgressMessage::Message(mut header, body)) => {
                            self.metadata_cache.fill_header(&mut header);
                            let msg = Message {
                                header: Some(header),
                                body: Some(body),
                            };
                            return Poll::Ready(Some(msg));
                        }
                        Some(EgressMessage::MessageWithAck(mut header, body, _notify)) => {
                            self.metadata_cache.fill_header(&mut header);
                            let msg = Message {
                                header: Some(header),
                                body: Some(body),
                            };
                            // _notify is dropped after we return the result, this sender side will
                            // be notified then.
                            return Poll::Ready(Some(msg));
                        }
                        Some(EgressMessage::PoisonPill) => {
                            // take the receiver, and return None to terminate the stream.
                            trace!(
                                "Egress stream received a poison pill, {} messages are dropped",
                                inner.len()
                            );
                            self.terminate_stream();
                            return Poll::Ready(None);
                        }
                        Some(EgressMessage::Drain) => {
                            trace!("Egress stream was requested to drain");
                            inner.close();
                            // loop until we drain.
                        }
                        None => {
                            trace!("Egress stream is drained");
                            self.terminate_stream();
                            return Poll::Ready(None);
                        }
                    }
                }
                None => {
                    trace!("Egress stream was already terminated (inner)");
                    self.terminate_stream();
                    return Poll::Ready(None);
                }
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

#[cfg(test)]
mod tests {
    // use tokio_stream::StreamExt;
    //
    // use super::*;
    //
    // // test EgressStream behaviour
    // #[tokio::test]
    // async fn test_egress_stream() {
    //     let (tx, rx) = mpsc::channel(1);
    //     let (mut drop_tx, mut egress) = EgressStream::wrap_receiver(rx);
    //     assert!(egress.next().now_or_never().is_none());
    //     // test normal message
    //     let msg = "hello";
    //     tx.send(EgressMessage::Message(msg.to_string()))
    //         .await
    //         .unwrap();
    //     assert_eq!(egress.next().await, Some(msg.to_string()));
    //     // test message with ack
    //     let msg = "hello";
    //     let (ack_tx, ack_rx) = oneshot::channel();
    //     tx.send(EgressMessage::MessageWithAck(msg.to_string(), ack_rx))
    //         .await
    //         .unwrap();
    //     assert_eq!(egress.next().await, Some(msg.to_string()));
    //     assert!(ack_tx.is_closed());
    //     // test poison pill
    //     tx.send(EgressMessage::PoisonPill).await.unwrap();
    //     assert_eq!(egress.next().await, None);
    //     // test drain
    //     let msg = "hello";
    //     tx.send(EgressMessage::Message(msg.to_string()))
    //         .await
    //         .unwrap();
    //     tx.send(EgressMessage::Drain).await.unwrap();
    //     assert_eq!(egress.next().await, Some(msg.to_string()));
    //     assert_eq!(egress.next().await, None);
    //     // test drop
    //     drop_tx.close();
    //     assert_eq!(egress.next().await, None);
    // }
}
