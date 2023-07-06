use crate::{NetworkCommand, NetworkHandle, NetworkNotRunning, ShuffleSender};
use restate_types::identifiers::PeerId;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct UnboundedNetworkHandle<ShuffleIn, ShuffleOut> {
    network_command_tx: mpsc::UnboundedSender<NetworkCommand<ShuffleIn>>,
    shuffle_tx: mpsc::Sender<ShuffleOut>,
}

impl<ShuffleIn, ShuffleOut> UnboundedNetworkHandle<ShuffleIn, ShuffleOut> {
    pub(crate) fn new(
        network_command_tx: mpsc::UnboundedSender<NetworkCommand<ShuffleIn>>,
        shuffle_tx: mpsc::Sender<ShuffleOut>,
    ) -> Self {
        Self {
            network_command_tx,
            shuffle_tx,
        }
    }
}

impl<ShuffleIn, ShuffleOut> NetworkHandle<ShuffleIn, ShuffleOut>
    for UnboundedNetworkHandle<ShuffleIn, ShuffleOut>
where
    ShuffleIn: Send + 'static,
    ShuffleOut: Send + 'static,
{
    type Future = futures::future::Ready<Result<(), NetworkNotRunning>>;

    fn register_shuffle(
        &self,
        peer_id: PeerId,
        shuffle_tx: mpsc::Sender<ShuffleIn>,
    ) -> Self::Future {
        futures::future::ready(
            self.network_command_tx
                .send(NetworkCommand::RegisterShuffle {
                    peer_id,
                    shuffle_tx,
                })
                .map_err(|_| NetworkNotRunning),
        )
    }

    fn unregister_shuffle(&self, peer_id: PeerId) -> Self::Future {
        futures::future::ready(
            self.network_command_tx
                .send(NetworkCommand::UnregisterShuffle { peer_id })
                .map_err(|_| NetworkNotRunning),
        )
    }

    fn create_shuffle_sender(&self) -> ShuffleSender<ShuffleOut> {
        self.shuffle_tx.clone()
    }
}
