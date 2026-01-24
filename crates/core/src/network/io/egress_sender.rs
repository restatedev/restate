// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures::FutureExt;
use tokio::sync::{mpsc, oneshot};

use crate::network::ConnectionClosed;

use super::{DrainReason, EgressMessage};

/// A future to track if an enqueued messages was sent over the socket or not. Note that the
/// send operation will be attempted regardless this token was dropped or not.
pub struct SendToken(oneshot::Receiver<()>);

/// Dropped means the message was not sent and the stream was closed
pub struct Sent(oneshot::Sender<()>);

impl Sent {
    pub fn create() -> (Sent, SendToken) {
        let (tx, rx) = oneshot::channel();
        (Sent(tx), SendToken(rx))
    }

    /// Notifes the receiver if not dropped
    pub fn notify(self) {
        let _ = self.0.send(());
    }
}

impl Future for SendToken {
    type Output = Result<(), ConnectionClosed>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(self.0.poll_unpin(cx)) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(_) => Poll::Ready(Err(ConnectionClosed)),
        }
    }
}

#[derive(derive_more::Deref, Clone)]
pub struct EgressSender {
    #[deref]
    inner: mpsc::Sender<EgressMessage>,
}

impl EgressSender {
    pub fn new(sender: mpsc::Sender<EgressMessage>) -> Self {
        Self { inner: sender }
    }

    #[cfg(feature = "test-util")]
    pub fn new_closed() -> Self {
        let (inner, _rx) = mpsc::channel(1);
        Self { inner }
    }

    // temporary, will be replaced in future work
    pub async fn reserve_owned(
        self,
    ) -> Result<mpsc::OwnedPermit<EgressMessage>, mpsc::error::SendError<()>> {
        self.inner.reserve_owned().await
    }

    pub fn try_reserve_owned(self) -> Option<mpsc::OwnedPermit<EgressMessage>> {
        self.inner.try_reserve_owned().ok()
    }

    /// Starts a drain of this stream. Enqueued messages will be sent before
    /// terminating but no new messages will be accepted after the connection processes
    /// the drain reason signal. Returns `ConnectionClosed` if the connection is draining
    /// or if it has already been closed.
    pub async fn close(self, reason: DrainReason) -> Result<(), ConnectionClosed> {
        self.inner
            .send(EgressMessage::Close(reason))
            .await
            .map_err(|_| ConnectionClosed)
    }
}

/// A channel used to send control signal and rpc responses.
///
/// The channel is unbounded to avoid CSP-related deadlocks for rpc-responses and control signals.
/// For rpc requests and unary messages, EgressSender is used.
///
/// The rationale for unbounded-ness for rpc responses is that we will already allocate N
/// oneshot senders for all in-flight rpc requests. The effective bound for those oneshot
/// senders is how fast the rpc-service allows us to enqueue requests. RPC services should
/// use bounded channels for their ingress side, therefore, pushing back on the network
/// receiver end. Since we rely on this technique to _influence_ (not limit) the number
/// of concurrent rpc senders, we get little value from limiting the network sender.
///
/// In summary, in network communication, we push back on the receive end. On the send end, we
/// push back on sending rpc requests and unary messages but not on shipping rpc responses for
/// requests we have already enqueued for processing.
#[derive(Clone)]
pub struct UnboundedEgressSender {
    inner: mpsc::UnboundedSender<EgressMessage>,
}

impl PartialEq for UnboundedEgressSender {
    fn eq(&self, other: &Self) -> bool {
        self.inner.same_channel(&other.inner)
    }
}

impl UnboundedEgressSender {
    pub fn new(unbounded: mpsc::UnboundedSender<EgressMessage>) -> Self {
        Self { inner: unbounded }
    }

    pub fn unbounded_send(&self, message: EgressMessage) -> Result<(), ConnectionClosed> {
        self.inner.send(message).map_err(|_| ConnectionClosed)
    }

    pub async fn closed(&self) {
        self.inner.closed().await
    }

    /// Starts a drain of this stream. Existing enqueued messages will be sent before
    /// terminating but no new messages will be accepted.
    pub fn unbounded_drain(&self, reason: DrainReason) {
        let _ = self.inner.send(EgressMessage::Close(reason));
    }
}
