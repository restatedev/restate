use std::fmt::Debug;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tracing::trace;

pub(super) struct ConsensusForwarder<T> {
    receiver: mpsc::Receiver<T>,
    sender: mpsc::Sender<T>,
}

impl<T> ConsensusForwarder<T>
where
    T: Debug,
{
    pub(super) fn new(receiver: mpsc::Receiver<T>, sender: mpsc::Sender<T>) -> Self {
        Self { receiver, sender }
    }

    pub(super) async fn run(mut self) -> Result<(), SendError<T>> {
        while let Some(message) = self.receiver.recv().await {
            trace!(?message, "Forwarding consensus message to itself.");
            self.sender.send(message).await?
        }

        Ok(())
    }
}
