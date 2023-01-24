use common::types::PeerId;
use consensus::{ProposalSender, Targeted};
use futures::Sink;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

pin_project! {
    /// Sender which attaches the identity id to a sent message.
    #[derive(Debug)]
    pub(super) struct IdentitySender<T> {
        id: PeerId,
        #[pin]
        sender: ProposalSender<Targeted<T>>
    }
}

impl<T> IdentitySender<T> {
    pub(super) fn new(id: PeerId, sender: ProposalSender<Targeted<T>>) -> Self {
        Self { id, sender }
    }
}

impl<T: Send + 'static> Sink<T> for IdentitySender<T> {
    type Error = <ProposalSender<Targeted<T>> as Sink<Targeted<T>>>::Error;

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
