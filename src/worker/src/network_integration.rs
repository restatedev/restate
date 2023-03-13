//! This module contains the glue code for converting the various messages into their
//! required formats when routing them through the network.

use crate::partition;
use crate::partition::shuffle;
use common::types::PartitionKey;
use futures::future::{ok, Ready};
use network::{PartitionTable, PartitionTableError};

pub(super) type Network = network::Network<
    partition::AckableCommand,
    shuffle::ShuffleInput,
    shuffle::ShuffleOutput,
    shuffle_integration::ShuffleToConsensus,
    shuffle_integration::ShuffleToIngress,
    ingress_grpc::IngressOutput,
    ingress_integration::IngressToConsensus,
    ingress_integration::IngressToShuffle,
    ingress_grpc::IngressInput,
    partition::AckResponse,
    partition::ShuffleAckResponse,
    partition::IngressAckResponse,
    FixedPartitionTable,
>;

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

mod ingress_integration {
    use crate::partition;
    use crate::partition::shuffle;
    use bytes::Bytes;
    use common::traits::KeyedMessage;
    use common::types::{AckKind, IngressId, PeerId, ServiceInvocation};
    use network::{ConsensusOrShuffleTarget, TargetConsensusOrShuffle, TargetShuffle};

    impl TargetConsensusOrShuffle<IngressToConsensus, IngressToShuffle>
        for ingress_grpc::IngressOutput
    {
        fn target(self) -> ConsensusOrShuffleTarget<IngressToConsensus, IngressToShuffle> {
            match self {
                ingress_grpc::IngressOutput::Invocation {
                    service_invocation,
                    ingress_id,
                    msg_index,
                } => ConsensusOrShuffleTarget::Consensus(IngressToConsensus {
                    service_invocation,
                    ingress_id,
                    msg_index,
                }),
                ingress_grpc::IngressOutput::Ack(ingress_grpc::AckResponse {
                    kind,
                    shuffle_target,
                }) => ConsensusOrShuffleTarget::Shuffle(IngressToShuffle {
                    shuffle_target,
                    kind,
                }),
            }
        }
    }

    #[derive(Debug)]
    pub(crate) struct IngressToConsensus {
        service_invocation: ServiceInvocation,
        ingress_id: IngressId,
        msg_index: u64,
    }

    impl KeyedMessage for IngressToConsensus {
        type RoutingKey<'a> = &'a Bytes;

        fn routing_key(&self) -> Self::RoutingKey<'_> {
            &self.service_invocation.id.service_id.key
        }
    }

    impl From<IngressToConsensus> for partition::AckableCommand {
        fn from(ingress_to_consensus: IngressToConsensus) -> Self {
            let IngressToConsensus {
                service_invocation,
                ingress_id,
                msg_index,
            } = ingress_to_consensus;

            partition::AckableCommand::require_ack(
                partition::Command::Invocation(service_invocation),
                partition::AckTarget::ingress(ingress_id, msg_index),
            )
        }
    }

    #[derive(Debug)]
    pub(crate) struct IngressToShuffle {
        shuffle_target: PeerId,
        kind: AckKind,
    }

    impl From<IngressToShuffle> for shuffle::ShuffleInput {
        fn from(value: IngressToShuffle) -> Self {
            shuffle::ShuffleInput(value.kind)
        }
    }

    impl TargetShuffle for IngressToShuffle {
        fn shuffle_target(&self) -> PeerId {
            self.shuffle_target
        }
    }
}

mod shuffle_integration {
    use crate::partition;
    use crate::partition::shuffle;
    use bytes::Bytes;
    use common::traits::KeyedMessage;
    use common::types::{PeerId, ResponseResult};
    use ingress_grpc::{IngressError, IngressResponseMessage};
    use network::{ConsensusOrIngressTarget, TargetConsensusOrIngress};

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
                shuffle::InvocationOrResponse::Invocation(invocation) => {
                    &invocation.id.service_id.key
                }
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
    pub(crate) struct ShuffleToIngress {
        msg: shuffle::IngressResponse,
        shuffle_id: PeerId,
        msg_index: u64,
    }

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
                    ConsensusOrIngressTarget::Ingress(ShuffleToIngress {
                        msg: invocation_response,
                        shuffle_id,
                        msg_index,
                    })
                }
            }
        }
    }

    impl From<ShuffleToIngress> for ingress_grpc::IngressInput {
        fn from(value: ShuffleToIngress) -> Self {
            let ShuffleToIngress {
                msg:
                    shuffle::IngressResponse {
                        service_invocation_id,
                        response,
                        ..
                    },
                msg_index,
                shuffle_id,
            } = value;

            let result = match response {
                ResponseResult::Success(result) => Ok(result),
                ResponseResult::Failure(i32, error_msg) => Err(IngressError::new(i32, error_msg)),
            };

            ingress_grpc::IngressInput::response(IngressResponseMessage {
                service_invocation_id,
                result,
                ack_target: ingress_grpc::AckTarget::new(shuffle_id, msg_index),
            })
        }
    }
}

mod partition_integration {
    use crate::partition;
    use crate::partition::shuffle;
    use common::types::PeerId;
    use network::{ShuffleOrIngressTarget, TargetShuffle, TargetShuffleOrIngress};

    impl TargetShuffleOrIngress<partition::ShuffleAckResponse, partition::IngressAckResponse>
        for partition::AckResponse
    {
        fn target(
            self,
        ) -> ShuffleOrIngressTarget<partition::ShuffleAckResponse, partition::IngressAckResponse>
        {
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

    impl From<partition::IngressAckResponse> for ingress_grpc::IngressInput {
        fn from(value: partition::IngressAckResponse) -> Self {
            ingress_grpc::IngressInput::message_ack(value.kind)
        }
    }
}
