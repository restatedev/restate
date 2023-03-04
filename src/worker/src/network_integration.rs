//! This module contains the glue code for converting the various messages into their
//! required formats when routing them through the network.

use crate::partition;
use crate::partition::shuffle;
use bytes::Bytes;
use common::traits::KeyedMessage;
use common::types::{PartitionKey, PeerId, ResponseResult};
use futures::future::{ok, Ready};
use ingress_grpc::{IngressError, IngressResponseMessage};
use network::{
    ConsensusOrIngressTarget, PartitionTable, PartitionTableError, ShuffleOrIngressTarget,
    TargetConsensusOrIngress, TargetShuffle, TargetShuffleOrIngress,
};

pub(super) type Network = network::Network<
    partition::AckableCommand,
    shuffle::ShuffleInput,
    shuffle::ShuffleOutput,
    ShuffleToConsensus,
    ShuffleToIngress,
    ingress_grpc::IngressOutput,
    ingress_grpc::IngressInput,
    partition::AckResponse,
    partition::ShuffleAckResponse,
    partition::IngressAckResponse,
    FixedPartitionTable,
>;

impl From<ingress_grpc::IngressOutput> for partition::AckableCommand {
    fn from(value: ingress_grpc::IngressOutput) -> Self {
        let service_invocation = value.into_inner();

        partition::AckableCommand::no_ack(partition::Command::Invocation(service_invocation))
    }
}

impl From<partition::IngressAckResponse> for ingress_grpc::IngressInput {
    fn from(value: partition::IngressAckResponse) -> Self {
        ingress_grpc::IngressInput::MessageAck(value.kind)
    }
}

#[derive(Debug)]
pub(crate) struct ShuffleToConsensus {
    msg: shuffle::InvocationOrResponse,
    shuffle_id: PeerId,
    msg_index: u64,
}

impl KeyedMessage for ShuffleToConsensus {
    type RoutingKey<'a> = &'a Bytes;

    fn routing_key(&self) -> &Bytes {
        match &self.msg {
            shuffle::InvocationOrResponse::Invocation(invocation) => &invocation.id.service_id.key,
            shuffle::InvocationOrResponse::Response(response) => &response.id.service_id.key,
        }
    }
}

impl From<ShuffleToConsensus> for partition::AckableCommand {
    fn from(value: ShuffleToConsensus) -> Self {
        let ShuffleToConsensus {
            msg,
            shuffle_id,
            msg_index,
        } = value;

        let ack_target = partition::AckTarget::shuffle(shuffle_id, msg_index);

        match msg {
            shuffle::InvocationOrResponse::Invocation(invocation) => {
                partition::AckableCommand::require_ack(
                    partition::Command::Invocation(invocation),
                    ack_target,
                )
            }
            shuffle::InvocationOrResponse::Response(response) => {
                partition::AckableCommand::require_ack(
                    partition::Command::Response(response),
                    ack_target,
                )
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct ShuffleToIngress(pub(crate) shuffle::IngressResponse);

impl TargetConsensusOrIngress<ShuffleToConsensus, ShuffleToIngress> for shuffle::ShuffleOutput {
    fn target(self) -> ConsensusOrIngressTarget<ShuffleToConsensus, ShuffleToIngress> {
        let (shuffle_id, msg_index, target) = self.into_inner();

        match target {
            shuffle::ShuffleTarget::PartitionProcessor(outbox_message) => {
                ConsensusOrIngressTarget::Consensus(ShuffleToConsensus {
                    msg: outbox_message,
                    shuffle_id,
                    msg_index,
                })
            }
            shuffle::ShuffleTarget::Ingress(invocation_response) => {
                ConsensusOrIngressTarget::Ingress(ShuffleToIngress(invocation_response))
            }
        }
    }
}

impl From<ShuffleToIngress> for ingress_grpc::IngressInput {
    fn from(value: ShuffleToIngress) -> Self {
        let shuffle::IngressResponse {
            service_invocation_id,
            response,
            ..
        } = value.0;

        let result = match response {
            ResponseResult::Success(result) => Ok(result),
            ResponseResult::Failure(i32, error_msg) => Err(IngressError::new(i32, error_msg)),
        };

        ingress_grpc::IngressInput::Response(IngressResponseMessage {
            service_invocation_id,
            result,
        })
    }
}

impl TargetShuffleOrIngress<partition::ShuffleAckResponse, partition::IngressAckResponse>
    for partition::AckResponse
{
    fn target(
        self,
    ) -> ShuffleOrIngressTarget<partition::ShuffleAckResponse, partition::IngressAckResponse> {
        match self {
            partition::AckResponse::Shuffle(ack) => ShuffleOrIngressTarget::Shuffle(ack),
            partition::AckResponse::Ingress(ack) => ShuffleOrIngressTarget::Ingress(ack),
        }
    }
}

impl From<partition::ShuffleAckResponse> for shuffle::ShuffleInput {
    fn from(value: partition::ShuffleAckResponse) -> Self {
        shuffle::ShuffleInput(value.kind)
    }
}

impl TargetShuffle for partition::ShuffleAckResponse {
    fn shuffle_target(&self) -> PeerId {
        self.shuffle_target
    }
}

#[derive(Debug, Clone)]
pub(super) struct FixedPartitionTable {
    number_partitions: u64,
}

impl FixedPartitionTable {
    pub(super) fn new(number_partitions: u64) -> Self {
        Self { number_partitions }
    }
}

impl PartitionTable for FixedPartitionTable {
    type Future = Ready<Result<u64, PartitionTableError>>;

    fn partition_key_to_target_peer(&self, partition_key: PartitionKey) -> Self::Future {
        let target_partition = partition_key % self.number_partitions;
        ok(target_partition)
    }
}
