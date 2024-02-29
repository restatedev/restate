// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::routing::consensus::ConsensusForwarder;
use crate::routing::ingress::IngressRouter;
use crate::routing::shuffle::ShuffleRouter;
use crate::{NetworkCommand, UnboundedNetworkHandle};
use restate_core::cancellation_watcher;
use restate_types::identifiers::PeerId;
use restate_types::message::PartitionTarget;
use restate_types::partition_table::FindPartition;
use restate_wal_protocol::Envelope;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::{debug, trace};

mod consensus;
mod ingress;
mod shuffle;

pub type ConsensusSender<T> = mpsc::Sender<PartitionTarget<T>>;

pub type IngressSender<T> = mpsc::Sender<T>;

pub type PartitionProcessorSender<T> = mpsc::Sender<T>;

#[derive(Debug, thiserror::Error)]
pub enum RoutingError {
    #[error("consensus forwarder terminated: {0}")]
    ConsensusForwarder(TerminationCause),
    #[error("shuffle router terminated: {0}")]
    ShuffleRouter(TerminationCause),
    #[error("ingress router terminated: {0}")]
    IngressRouter(TerminationCause),
    #[error("partition processor router terminated: {0}")]
    PartitionProcessorRouter(TerminationCause),
}

#[derive(Debug, thiserror::Error)]
pub enum TerminationCause {
    #[error("unexpected termination")]
    Unexpected,
    #[error("error: {0}")]
    Error(#[from] anyhow::Error),
}

/// Component which is responsible for routing messages from different components.
#[derive(Debug)]
pub struct Network<ShuffleIn, IngressIn, PartitionTable> {
    /// Receiver for messages from the consensus module
    consensus_in_rx: mpsc::Receiver<PartitionTarget<Envelope>>,

    /// Sender for messages to the consensus module
    consensus_tx: mpsc::Sender<PartitionTarget<Envelope>>,

    network_command_rx: mpsc::UnboundedReceiver<NetworkCommand<ShuffleIn>>,

    shuffle_rx: mpsc::Receiver<Envelope>,

    ingress_in_rx: mpsc::Receiver<Envelope>,

    ingress_tx: mpsc::Sender<IngressIn>,

    partition_table: PartitionTable,

    // used for creating the ConsensusSender
    consensus_in_tx: mpsc::Sender<PartitionTarget<Envelope>>,

    // used for creating the network handle
    network_command_tx: mpsc::UnboundedSender<NetworkCommand<ShuffleIn>>,
    shuffle_tx: mpsc::Sender<Envelope>,

    // used for creating the ingress sender
    ingress_in_tx: mpsc::Sender<Envelope>,
}

impl<ShuffleIn, IngressIn, PartitionTable> Network<ShuffleIn, IngressIn, PartitionTable>
where
    ShuffleIn: Debug + Send + Sync + 'static,
    IngressIn: Debug + Send + Sync + 'static,
    PartitionTable: FindPartition + Clone,
{
    pub fn new(
        consensus_tx: mpsc::Sender<PartitionTarget<Envelope>>,
        ingress_tx: mpsc::Sender<IngressIn>,
        partition_table: PartitionTable,
        channel_size: usize,
    ) -> Self {
        let (consensus_in_tx, consensus_in_rx) = mpsc::channel(channel_size);
        let (shuffle_tx, shuffle_rx) = mpsc::channel(channel_size);
        let (ingress_in_tx, ingress_in_rx) = mpsc::channel(channel_size);
        let (network_command_tx, network_command_rx) = mpsc::unbounded_channel();

        Self {
            consensus_tx,
            consensus_in_rx,
            consensus_in_tx,
            network_command_rx,
            network_command_tx,
            shuffle_rx,
            shuffle_tx,
            ingress_in_rx,
            ingress_tx,
            ingress_in_tx,
            partition_table,
        }
    }

    pub fn create_consensus_sender(&self) -> ConsensusSender<Envelope> {
        self.consensus_in_tx.clone()
    }

    pub fn create_network_handle(&self) -> UnboundedNetworkHandle<ShuffleIn, Envelope> {
        UnboundedNetworkHandle::new(self.network_command_tx.clone(), self.shuffle_tx.clone())
    }

    pub fn create_ingress_sender(&self) -> IngressSender<Envelope> {
        self.ingress_in_tx.clone()
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let Network {
            consensus_in_rx,
            consensus_tx,
            mut network_command_rx,
            shuffle_rx,
            ingress_in_rx,
            partition_table,
            ..
        } = self;

        debug!("Run network");

        let shutdown = cancellation_watcher();
        let shuffles: Arc<Mutex<HashMap<PeerId, mpsc::Sender<ShuffleIn>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let consensus_forwarder = ConsensusForwarder::new(consensus_in_rx, consensus_tx.clone());
        let shuffle_router =
            ShuffleRouter::new(shuffle_rx, consensus_tx.clone(), partition_table.clone());
        let ingress_router =
            IngressRouter::new(ingress_in_rx, consensus_tx.clone(), partition_table.clone());

        let consensus_forwarder = consensus_forwarder.run();
        let shuffle_router = shuffle_router.run();
        let ingress_router = ingress_router.run();

        tokio::pin!(
            shutdown,
            consensus_forwarder,
            shuffle_router,
            ingress_router
        );

        loop {
            tokio::select! {
                result = &mut consensus_forwarder => {
                    Err(RoutingError::ConsensusForwarder(Self::result_into_termination_cause(result)))?;
                },
                result = &mut shuffle_router => {
                    Err(RoutingError::ShuffleRouter(Self::result_into_termination_cause(result)))?;
                },
                result = &mut ingress_router => {
                    Err(RoutingError::IngressRouter(Self::result_into_termination_cause(result)))?;
                },
                command = network_command_rx.recv() => {
                    let command = command.expect("Network owns the command sender, that's why the receiver will never be closed.");
                    match command {
                        NetworkCommand::RegisterShuffle { peer_id, shuffle_tx } => {
                            trace!(shuffle_id = peer_id, "Register new shuffle.");
                            shuffles.lock().unwrap().insert(peer_id, shuffle_tx);
                        },
                        NetworkCommand::UnregisterShuffle { peer_id } => {
                            trace!(shuffle_id = peer_id, "Unregister shuffle.");
                            shuffles.lock().unwrap().remove(&peer_id);
                        }
                    };
                },
                _ = &mut shutdown => {
                    debug!("Shutting network down.");
                    break;
                }
            }
        }

        Ok(())
    }

    fn result_into_termination_cause<E: std::error::Error + Send + Sync + 'static>(
        result: Result<(), E>,
    ) -> TerminationCause {
        result
            .map_err(|err| TerminationCause::Error(err.into()))
            .err()
            .unwrap_or(TerminationCause::Unexpected)
    }
}

#[cfg(test)]
mod tests;
