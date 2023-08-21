// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{
    ConsensusOrIngressTarget, ConsensusOrShuffleTarget, Network, NetworkHandle, PartitionTable,
    PartitionTableError, ShuffleOrIngressTarget, TargetConsensusOrIngress,
    TargetConsensusOrShuffle, TargetShuffle, TargetShuffleOrIngress,
};
use restate_test_util::test;
use restate_types::identifiers::WithPartitionKey;
use restate_types::identifiers::{PartitionKey, PeerId};
use restate_types::message::PeerTarget;
use std::fmt::Debug;
use std::future;
use tokio::sync::mpsc;

#[derive(Debug, Default, Clone)]
struct MockPartitionTable;

impl PartitionTable for MockPartitionTable {
    type Future = future::Ready<Result<PeerId, PartitionTableError>>;

    fn partition_key_to_target_peer(&self, _partition_key: PartitionKey) -> Self::Future {
        let peer_id = 0;
        future::ready(Ok(peer_id))
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
struct ConsensusMsg(u64);

impl WithPartitionKey for ConsensusMsg {
    fn partition_key(&self) -> PartitionKey {
        0
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
struct IngressMsg(u64);

#[derive(Debug, Copy, Clone, PartialEq)]
struct ShuffleMsg(u64);

impl TargetShuffle for ShuffleMsg {
    fn shuffle_target(&self) -> PeerId {
        0
    }
}

#[derive(Debug, Copy, Clone)]
enum ShuffleOut {
    Consensus(ConsensusMsg),
    Ingress(IngressMsg),
}

impl TargetConsensusOrIngress<ConsensusMsg, IngressMsg> for ShuffleOut {
    fn target(self) -> ConsensusOrIngressTarget<ConsensusMsg, IngressMsg> {
        match self {
            ShuffleOut::Consensus(msg) => ConsensusOrIngressTarget::Consensus(msg),
            ShuffleOut::Ingress(msg) => ConsensusOrIngressTarget::Ingress(msg),
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum IngressOut {
    Consensus(ConsensusMsg),
    Shuffle(ShuffleMsg),
}

impl TargetConsensusOrShuffle<ConsensusMsg, ShuffleMsg> for IngressOut {
    fn target(self) -> ConsensusOrShuffleTarget<ConsensusMsg, ShuffleMsg> {
        match self {
            IngressOut::Consensus(msg) => ConsensusOrShuffleTarget::Consensus(msg),
            IngressOut::Shuffle(msg) => ConsensusOrShuffleTarget::Shuffle(msg),
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum PPOut {
    Shuffle(ShuffleMsg),
    Ingress(IngressMsg),
}

impl TargetShuffleOrIngress<ShuffleMsg, IngressMsg> for PPOut {
    fn target(self) -> ShuffleOrIngressTarget<ShuffleMsg, IngressMsg> {
        match self {
            PPOut::Shuffle(msg) => ShuffleOrIngressTarget::Shuffle(msg),
            PPOut::Ingress(msg) => ShuffleOrIngressTarget::Ingress(msg),
        }
    }
}

type MockNetwork = Network<
    ConsensusMsg,
    ShuffleMsg,
    ShuffleOut,
    ConsensusMsg,
    IngressMsg,
    IngressOut,
    ConsensusMsg,
    ShuffleMsg,
    IngressMsg,
    PPOut,
    ShuffleMsg,
    IngressMsg,
    MockPartitionTable,
>;

fn mock_network() -> (
    MockNetwork,
    mpsc::Receiver<PeerTarget<ConsensusMsg>>,
    mpsc::Receiver<IngressMsg>,
) {
    let (consensus_tx, consensus_rx) = mpsc::channel(1);
    let (ingress_tx, ingress_rx) = mpsc::channel(1);
    let partition_table = MockPartitionTable::default();

    let network = Network::<
        ConsensusMsg,
        ShuffleMsg,
        ShuffleOut,
        ConsensusMsg,
        IngressMsg,
        IngressOut,
        ConsensusMsg,
        ShuffleMsg,
        IngressMsg,
        PPOut,
        ShuffleMsg,
        IngressMsg,
        _,
    >::new(consensus_tx, ingress_tx, partition_table, 1);

    (network, consensus_rx, ingress_rx)
}

#[test(tokio::test)]
async fn no_consensus_message_is_dropped() {
    let (network, mut consensus_rx, _ingress_rx) = mock_network();

    let network_handle = network.create_network_handle();
    let consensus_tx = network.create_consensus_sender();

    let (shutdown_signal, shutdown_watch) = drain::channel();

    let network_join_handle = tokio::spawn(network.run(shutdown_watch));

    let msg_1 = (0, ConsensusMsg(0));
    let msg_2 = (0, ConsensusMsg(1));
    let msg_3 = (0, ConsensusMsg(2));

    consensus_tx.send(msg_1).await.unwrap();
    consensus_tx.send(msg_2).await.unwrap();
    tokio::task::yield_now().await;
    network_handle.unregister_shuffle(0).await.unwrap();
    tokio::task::yield_now().await;
    consensus_tx.send(msg_3).await.unwrap();

    assert_eq!(consensus_rx.recv().await.unwrap(), msg_1);
    assert_eq!(consensus_rx.recv().await.unwrap(), msg_2);
    assert_eq!(consensus_rx.recv().await.unwrap(), msg_3);

    shutdown_signal.drain().await;
    network_join_handle.await.unwrap().unwrap();
}

#[test(tokio::test)]
async fn no_shuffle_to_consensus_message_is_dropped() {
    let msg_1 = ConsensusMsg(0);
    let msg_2 = ConsensusMsg(1);
    let msg_3 = ConsensusMsg(2);

    let input = [
        ShuffleOut::Consensus(msg_1),
        ShuffleOut::Consensus(msg_2),
        ShuffleOut::Consensus(msg_3),
    ];
    let expected_output = [(0, msg_1), (0, msg_2), (0, msg_3)];

    let (network, consensus_rx, _ingress_rx) = mock_network();

    let shuffle_tx = network.create_network_handle().create_shuffle_sender();

    run_router_test(network, shuffle_tx, input, consensus_rx, expected_output).await;
}

#[test(tokio::test)]
async fn no_shuffle_to_ingress_message_is_dropped() {
    let msg_1 = IngressMsg(0);
    let msg_2 = IngressMsg(1);
    let msg_3 = IngressMsg(2);

    let input = [
        ShuffleOut::Ingress(msg_1),
        ShuffleOut::Ingress(msg_2),
        ShuffleOut::Ingress(msg_3),
    ];
    let expected_output = [msg_1, msg_2, msg_3];

    let (network, _consensus_rx, ingress_rx) = mock_network();

    let shuffle_tx = network.create_network_handle().create_shuffle_sender();

    run_router_test(network, shuffle_tx, input, ingress_rx, expected_output).await;
}

#[test(tokio::test)]
async fn no_ingress_to_shuffle_message_is_dropped() {
    let msg_1 = ShuffleMsg(0);
    let msg_2 = ShuffleMsg(1);
    let msg_3 = ShuffleMsg(2);

    let input = [
        IngressOut::Shuffle(msg_1),
        IngressOut::Shuffle(msg_2),
        IngressOut::Shuffle(msg_3),
    ];
    let expected_output = [msg_1, msg_2, msg_3];

    let (network, _consensus_rx, _ingress_rx) = mock_network();

    let network_handle = network.create_network_handle();

    let (shuffle_tx, shuffle_rx) = mpsc::channel(1);

    network_handle
        .register_shuffle(0, shuffle_tx)
        .await
        .unwrap();

    let ingress_tx = network.create_ingress_sender();

    run_router_test(network, ingress_tx, input, shuffle_rx, expected_output).await;
}

#[test(tokio::test)]
async fn no_ingress_to_consensus_message_is_dropped() {
    let msg_1 = ConsensusMsg(0);
    let msg_2 = ConsensusMsg(1);
    let msg_3 = ConsensusMsg(2);

    let input = [
        IngressOut::Consensus(msg_1),
        IngressOut::Consensus(msg_2),
        IngressOut::Consensus(msg_3),
    ];
    let expected_output = [(0, msg_1), (0, msg_2), (0, msg_3)];

    let (network, consensus_rx, _ingress_rx) = mock_network();

    let ingress_tx = network.create_ingress_sender();

    run_router_test(network, ingress_tx, input, consensus_rx, expected_output).await;
}

#[test(tokio::test)]
async fn no_pp_to_shuffle_message_is_dropped() {
    let msg_1 = ShuffleMsg(0);
    let msg_2 = ShuffleMsg(1);
    let msg_3 = ShuffleMsg(2);

    let input = [
        PPOut::Shuffle(msg_1),
        PPOut::Shuffle(msg_2),
        PPOut::Shuffle(msg_3),
    ];
    let expected_output = [msg_1, msg_2, msg_3];

    let (network, _consensus_rx, _ingress_rx) = mock_network();

    let network_handle = network.create_network_handle();

    let (shuffle_tx, shuffle_rx) = mpsc::channel(1);
    network_handle
        .register_shuffle(0, shuffle_tx)
        .await
        .unwrap();

    let pp_tx = network.create_partition_processor_sender();

    run_router_test(network, pp_tx, input, shuffle_rx, expected_output).await;
}

#[test(tokio::test)]
async fn no_pp_to_ingress_message_is_dropped() {
    let msg_1 = IngressMsg(0);
    let msg_2 = IngressMsg(1);
    let msg_3 = IngressMsg(2);

    let input = [
        PPOut::Ingress(msg_1),
        PPOut::Ingress(msg_2),
        PPOut::Ingress(msg_3),
    ];
    let expected_output = [msg_1, msg_2, msg_3];

    let (network, _consensus_rx, ingress_rx) = mock_network();

    let pp_tx = network.create_partition_processor_sender();

    run_router_test(network, pp_tx, input, ingress_rx, expected_output).await;
}

async fn run_router_test<Input, Output>(
    network: MockNetwork,
    tx: mpsc::Sender<Input>,
    input: [Input; 3],
    mut rx: mpsc::Receiver<Output>,
    expected_output: [Output; 3],
) where
    Input: Debug + Copy,
    Output: PartialEq + Debug,
{
    let network_handle = network.create_network_handle();

    let (shutdown_signal, shutdown_watch) = drain::channel();
    let network_join_handle = tokio::spawn(network.run(shutdown_watch));

    // we have to yield in order to process register shuffle message
    tokio::task::yield_now().await;

    tx.send(input[0]).await.unwrap();
    tx.send(input[1]).await.unwrap();
    tokio::task::yield_now().await;
    network_handle.unregister_shuffle(99).await.unwrap();
    tokio::task::yield_now().await;
    tx.send(input[2]).await.unwrap();

    for output in expected_output {
        assert_eq!(rx.recv().await.unwrap(), output);
    }

    shutdown_signal.drain().await;
    network_join_handle.await.unwrap().unwrap();
}
