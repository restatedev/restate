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
    partition::Command,
    shuffle::ShuffleInput,
    shuffle::ShuffleOutput,
    ConsensusMessage,
    IngressMessage,
    ingress_grpc::IngressOutput,
    ingress_grpc::IngressInput,
    partition::MessageAck,
    partition::ShuffleMessageAck,
    partition::IngressMessageAck,
    FixedPartitionTable,
>;

impl From<ingress_grpc::IngressOutput> for partition::Command {
    fn from(value: ingress_grpc::IngressOutput) -> Self {
        let service_invocation = value.into_inner();

        partition::Command::Invocation(service_invocation)
    }
}

impl From<partition::IngressMessageAck> for ingress_grpc::IngressInput {
    fn from(value: partition::IngressMessageAck) -> Self {
        ingress_grpc::IngressInput::MessageAck(value.0)
    }
}

#[derive(Debug)]
pub(crate) struct ConsensusMessage(shuffle::InvocationOrResponse);

impl KeyedMessage for ConsensusMessage {
    type RoutingKey<'a> = &'a Bytes;

    fn routing_key(&self) -> &Bytes {
        match &self.0 {
            shuffle::InvocationOrResponse::Invocation(invocation) => &invocation.id.service_id.key,
            shuffle::InvocationOrResponse::Response(response) => &response.id.service_id.key,
        }
    }
}

impl From<ConsensusMessage> for partition::Command {
    fn from(value: ConsensusMessage) -> Self {
        match value.0 {
            shuffle::InvocationOrResponse::Invocation(invocation) => {
                partition::Command::Invocation(invocation)
            }
            shuffle::InvocationOrResponse::Response(response) => {
                partition::Command::Response(response)
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct IngressMessage(pub(crate) shuffle::IngressResponse);

impl TargetConsensusOrIngress<ConsensusMessage, IngressMessage> for shuffle::ShuffleOutput {
    fn target(self) -> ConsensusOrIngressTarget<ConsensusMessage, IngressMessage> {
        match self {
            shuffle::ShuffleOutput::PartitionProcessor(outbox_message) => {
                ConsensusOrIngressTarget::Consensus(ConsensusMessage(outbox_message))
            }
            shuffle::ShuffleOutput::Ingress(invocation_response) => {
                ConsensusOrIngressTarget::Ingress(IngressMessage(invocation_response))
            }
        }
    }
}

impl From<IngressMessage> for ingress_grpc::IngressInput {
    fn from(value: IngressMessage) -> Self {
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

impl TargetShuffleOrIngress<partition::ShuffleMessageAck, partition::IngressMessageAck>
    for partition::MessageAck
{
    fn target(
        self,
    ) -> ShuffleOrIngressTarget<partition::ShuffleMessageAck, partition::IngressMessageAck> {
        match self {
            partition::MessageAck::Shuffle(ack) => ShuffleOrIngressTarget::Shuffle(ack),
            partition::MessageAck::Ingress(ack) => ShuffleOrIngressTarget::Ingress(ack),
        }
    }
}

impl From<partition::ShuffleMessageAck> for shuffle::ShuffleInput {
    fn from(value: partition::ShuffleMessageAck) -> Self {
        shuffle::ShuffleInput(value.kind)
    }
}

impl TargetShuffle for partition::ShuffleMessageAck {
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
