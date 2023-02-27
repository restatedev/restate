use common::types::{PeerId, PeerTarget};
use consensus::ProposalSender;
use futures::Sink;
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Sender which attaches the identity id to a sent message.
#[derive(Debug)]
#[pin_project]
pub(super) struct IdentitySender<T> {
    id: PeerId,
    #[pin]
    sender: ProposalSender<PeerTarget<T>>,
}

impl<T> IdentitySender<T> {
    pub(super) fn new(id: PeerId, sender: ProposalSender<PeerTarget<T>>) -> Self {
        Self { id, sender }
    }
}

impl<T: Send + 'static> Sink<T> for IdentitySender<T> {
    type Error = <ProposalSender<PeerTarget<T>> as Sink<PeerTarget<T>>>::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().sender.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let this = self.project();
        this.sender.start_send((*this.id, item))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().sender.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().sender.poll_close(cx)
    }
}
