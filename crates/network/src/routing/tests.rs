// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;

use restate_core::TaskKind;
use test_log::test;
use tokio::sync::mpsc;

use restate_core::TestCoreEnv;
use restate_types::identifiers::PartitionId;
use restate_types::identifiers::{PartitionKey, PeerId};
use restate_types::invocation::ServiceInvocation;
use restate_types::message::PartitionTarget;
use restate_types::partition_table::{FindPartition, PartitionTableError};
use restate_wal_protocol::{AckMode, Command, Destination, Envelope, Header, Source};

use crate::{Network, NetworkHandle, TargetShuffle};

#[derive(Debug, Default, Clone)]
struct MockPartitionTable;

impl FindPartition for MockPartitionTable {
    fn find_partition_id(
        &self,
        _partition_key: PartitionKey,
    ) -> Result<PartitionId, PartitionTableError> {
        let partition_id = 0;
        Ok(partition_id)
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

type MockNetwork = Network<ShuffleMsg, IngressMsg, MockPartitionTable>;

fn mock_network() -> (
    MockNetwork,
    mpsc::Receiver<PartitionTarget<Envelope>>,
    mpsc::Receiver<IngressMsg>,
) {
    let (consensus_tx, consensus_rx) = mpsc::channel(1);
    let (ingress_tx, ingress_rx) = mpsc::channel(1);
    let partition_table = MockPartitionTable;

    let network =
        Network::<ShuffleMsg, IngressMsg, _>::new(consensus_tx, ingress_tx, partition_table, 1);

    (network, consensus_rx, ingress_rx)
}

fn create_envelope(partition_key: PartitionKey) -> Envelope {
    let header = Header {
        source: Source::ControlPlane {},
        dest: Destination::Processor {
            partition_key,
            dedup: None,
        },
        ack_mode: AckMode::None,
    };

    Envelope::new(header, Command::Invoke(ServiceInvocation::mock()))
}

#[test(tokio::test)]
async fn no_consensus_message_is_dropped() {
    let test_env = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
    let (network, mut consensus_rx, _ingress_rx) = mock_network();

    let network_handle = network.create_network_handle();
    let consensus_tx = network.create_consensus_sender();

    let networking_task = test_env
        .tc
        .spawn(TaskKind::SystemService, "networking", None, network.run())
        .unwrap();

    let msg_1 = (0, create_envelope(0));
    let msg_2 = (0, create_envelope(1));
    let msg_3 = (0, create_envelope(2));

    consensus_tx.send(msg_1.clone()).await.unwrap();
    consensus_tx.send(msg_2.clone()).await.unwrap();
    tokio::task::yield_now().await;
    network_handle.unregister_shuffle(0).await.unwrap();
    tokio::task::yield_now().await;
    consensus_tx.send(msg_3.clone()).await.unwrap();

    assert_eq!(consensus_rx.recv().await.unwrap(), msg_1);
    assert_eq!(consensus_rx.recv().await.unwrap(), msg_2);
    assert_eq!(consensus_rx.recv().await.unwrap(), msg_3);

    test_env
        .tc
        .cancel_task(networking_task)
        .unwrap()
        .await
        .unwrap();
}

#[test(tokio::test)]
async fn no_shuffle_to_consensus_message_is_dropped() {
    let msg_1 = create_envelope(0);
    let msg_2 = create_envelope(1);
    let msg_3 = create_envelope(2);

    let input = [msg_1.clone(), msg_2.clone(), msg_3.clone()];
    let expected_output = [(0, msg_1), (0, msg_2), (0, msg_3)];

    let (network, consensus_rx, _ingress_rx) = mock_network();

    let shuffle_tx = network.create_network_handle().create_shuffle_sender();

    run_router_test(network, shuffle_tx, input, consensus_rx, expected_output).await;
}

#[test(tokio::test)]
async fn no_ingress_to_consensus_message_is_dropped() {
    let msg_1 = create_envelope(0);
    let msg_2 = create_envelope(1);
    let msg_3 = create_envelope(2);

    let input = [msg_1.clone(), msg_2.clone(), msg_3.clone()];
    let expected_output = [(0, msg_1), (0, msg_2), (0, msg_3)];

    let (network, consensus_rx, _ingress_rx) = mock_network();

    let ingress_tx = network.create_ingress_sender();

    run_router_test(network, ingress_tx, input, consensus_rx, expected_output).await;
}

async fn run_router_test<Input, Output>(
    network: MockNetwork,
    tx: mpsc::Sender<Input>,
    input: [Input; 3],
    mut rx: mpsc::Receiver<Output>,
    expected_output: [Output; 3],
) where
    Input: Debug + Clone,
    Output: PartialEq + Debug,
{
    let test_env = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
    let network_handle = network.create_network_handle();

    let networking_task = test_env
        .tc
        .spawn(TaskKind::SystemService, "networking", None, network.run())
        .unwrap();

    // we have to yield in order to process register shuffle message
    tokio::task::yield_now().await;

    tx.send(input[0].clone()).await.unwrap();
    tx.send(input[1].clone()).await.unwrap();
    tokio::task::yield_now().await;
    network_handle.unregister_shuffle(99).await.unwrap();
    tokio::task::yield_now().await;
    tx.send(input[2].clone()).await.unwrap();

    for output in expected_output {
        assert_eq!(rx.recv().await.unwrap(), output);
    }

    test_env
        .tc
        .cancel_task(networking_task)
        .unwrap()
        .await
        .unwrap();
}
