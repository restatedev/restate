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
use std::task::{Context, Poll, ready};

use futures::FutureExt;
use tokio::sync::{mpsc, oneshot};
use tracing::trace;

use crate::network::ConnectionClosed;

use super::{CloseReason, EgressMessage};

/// A future to track if an enqueued messages was sent over the socket or not. Note that the
/// send operation will be attempted regardless this token was dropped or not.
pub struct SendToken(oneshot::Receiver<()>);

/// Dropped means the message was not sent and the stream was closed
pub struct SendNotifier(oneshot::Sender<()>);

impl SendNotifier {
    pub fn create() -> (SendNotifier, SendToken) {
        let (tx, rx) = oneshot::channel();
        (SendNotifier(tx), SendToken(rx))
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
            Ok(_) => Poll::Ready(Ok(())),
            Err(_) => Poll::Ready(Err(ConnectionClosed)),
        }
    }
}

#[derive(derive_more::Deref, Clone)]
pub struct EgressSender {
    pub(crate) cid: u64,
    #[deref]
    pub(crate) inner: mpsc::Sender<EgressMessage>,
}

impl EgressSender {
    // temporary, will be replaced in future work
    pub async fn reserve_owned(
        self,
    ) -> Result<mpsc::OwnedPermit<EgressMessage>, mpsc::error::SendError<()>> {
        self.inner.reserve_owned().await
    }

    /// Starts a drain of this stream. Existing enqueued messages will be sent before
    /// terminating but no new messages will be accepted.
    pub fn close(&self, reason: CloseReason) {
        if self
            .inner
            // todo: replace with send after moving to unbounded channel
            .try_send(EgressMessage::Close(CloseReason::Shutdown))
            .is_ok()
        {
            trace!(cid = %self.cid, ?reason, "Drain message was enqueued to egress stream");
        }
    }
}
