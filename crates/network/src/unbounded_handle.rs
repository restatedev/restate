// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{NetworkCommand, NetworkHandle, ShuffleSender};
use restate_errors::NotRunningError;
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
    type Future = futures::future::Ready<Result<(), NotRunningError>>;

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
                .map_err(|_| NotRunningError),
        )
    }

    fn unregister_shuffle(&self, peer_id: PeerId) -> Self::Future {
        futures::future::ready(
            self.network_command_tx
                .send(NetworkCommand::UnregisterShuffle { peer_id })
                .map_err(|_| NotRunningError),
        )
    }

    fn create_shuffle_sender(&self) -> ShuffleSender<ShuffleOut> {
        self.shuffle_tx.clone()
    }
}
