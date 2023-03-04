use common::types::{PeerId, PeerTarget};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;

/// Sender which attaches the identity id to a sent message.
#[derive(Debug)]
pub(super) struct IdentitySender<T> {
    id: PeerId,
    sender: mpsc::Sender<PeerTarget<T>>,
}

impl<T> IdentitySender<T> {
    pub(super) fn new(id: PeerId, sender: mpsc::Sender<PeerTarget<T>>) -> Self {
        Self { id, sender }
    }

    pub(super) async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        self.sender
            .send((self.id, msg))
            .await
            .map_err(|SendError((_, msg))| SendError(msg))
    }
}
