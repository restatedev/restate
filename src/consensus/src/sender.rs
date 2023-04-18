use futures::ready;
use pin_project::pin_project;
use restate_common::types::PeerId;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

#[derive(Debug)]
pub(super) struct StateMachineSender<T> {
    peer_id: PeerId,
    sender: mpsc::Sender<T>,
}

impl<T> StateMachineSender<T> {
    pub(super) fn new(peer_id: PeerId, sender: mpsc::Sender<T>) -> Self {
        Self { peer_id, sender }
    }

    pub(super) fn reserve_owned(
        self,
    ) -> ReserveSendCapacity<
        impl Future<Output = Result<mpsc::OwnedPermit<T>, mpsc::error::SendError<()>>>,
        T,
    > {
        ReserveSendCapacity::new(self.peer_id, self.sender.reserve_owned())
    }
}

#[derive(Debug)]
#[pin_project]
pub(super) struct ReserveSendCapacity<F, T> {
    peer_id: PeerId,
    #[pin]
    reserve_future: F,
    _message_type: PhantomData<T>,
}

impl<F, T> ReserveSendCapacity<F, T>
where
    F: Future<Output = Result<mpsc::OwnedPermit<T>, mpsc::error::SendError<()>>>,
{
    fn new(peer_id: PeerId, reserve_future: F) -> Self {
        Self {
            peer_id,
            reserve_future,
            _message_type: Default::default(),
        }
    }
}

impl<F, T> Future for ReserveSendCapacity<F, T>
where
    F: Future<Output = Result<mpsc::OwnedPermit<T>, mpsc::error::SendError<()>>>,
{
    type Output = Result<StateMachineOwnedPermit<T>, StateMachineClosedError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let reserve_result = ready!(this.reserve_future.poll(cx));

        Poll::Ready(
            reserve_result
                .map(|permit| StateMachineOwnedPermit::new(*this.peer_id, permit))
                .map_err(|_| StateMachineClosedError(*this.peer_id)),
        )
    }
}

#[derive(Debug)]
pub(super) struct StateMachineOwnedPermit<T> {
    peer_id: PeerId,
    owned_permit: mpsc::OwnedPermit<T>,
}

impl<T> StateMachineOwnedPermit<T> {
    fn new(peer_id: PeerId, owned_permit: mpsc::OwnedPermit<T>) -> Self {
        Self {
            peer_id,
            owned_permit,
        }
    }

    pub(super) fn send(self, msg: T) -> StateMachineSender<T> {
        let sender = self.owned_permit.send(msg);
        StateMachineSender::new(self.peer_id, sender)
    }

    pub(super) fn release(self) -> StateMachineSender<T> {
        let sender = self.owned_permit.release();
        StateMachineSender::new(self.peer_id, sender)
    }

    pub(super) fn peer_id(&self) -> PeerId {
        self.peer_id
    }
}

#[derive(Debug, thiserror::Error)]
#[error("state machine with peer id '{0}' has been closed")]
pub(super) struct StateMachineClosedError(PeerId);
