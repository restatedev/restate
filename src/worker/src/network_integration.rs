//! This module contains the glue code for converting the various messages into their
//! required formats when routing them through the network.

use crate::partition;
use crate::partition::shuffle;
use futures::future::{ok, Ready};
use restate_common::types::PartitionKey;
use restate_network::{PartitionTable, PartitionTableError};

pub(super) type Network = restate_network::Network<
    partition::AckCommand,
    shuffle::ShuffleInput,
    shuffle::ShuffleOutput,
    shuffle_integration::ShuffleToConsensus,
    shuffle_integration::ShuffleToIngress,
    restate_ingress_grpc::IngressOutput,
    ingress_integration::IngressToConsensus,
    ingress_integration::IngressToShuffle,
    restate_ingress_grpc::IngressInput,
    partition::AckResponse,
    partition::ShuffleDeduplicationResponse,
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
    use restate_common::traits::KeyedMessage;
    use restate_common::types::{AckKind, IngressId, MessageIndex, PeerId, ServiceInvocation};
    use restate_network::{ConsensusOrShuffleTarget, TargetConsensusOrShuffle, TargetShuffle};

    impl TargetConsensusOrShuffle<IngressToConsensus, IngressToShuffle>
        for restate_ingress_grpc::IngressOutput
    {
        fn target(self) -> ConsensusOrShuffleTarget<IngressToConsensus, IngressToShuffle> {
            match self {
                restate_ingress_grpc::IngressOutput::Invocation {
                    service_invocation,
                    ingress_id,
                    msg_index,
                } => ConsensusOrShuffleTarget::Consensus(IngressToConsensus {
                    service_invocation,
                    ingress_id,
                    msg_index,
                }),
                restate_ingress_grpc::IngressOutput::Ack(restate_ingress_grpc::AckResponse {
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
        msg_index: MessageIndex,
    }

    impl KeyedMessage for IngressToConsensus {
        type RoutingKey<'a> = &'a Bytes;

        fn routing_key(&self) -> Self::RoutingKey<'_> {
            &self.service_invocation.id.service_id.key
        }
    }

    impl From<IngressToConsensus> for partition::AckCommand {
        fn from(ingress_to_consensus: IngressToConsensus) -> Self {
            let IngressToConsensus {
                service_invocation,
                ingress_id,
                msg_index,
            } = ingress_to_consensus;

            partition::AckCommand::ack(
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
    use restate_common::traits::KeyedMessage;
    use restate_common::types::{MessageIndex, PartitionId, PeerId, ResponseResult};
    use restate_ingress_grpc::{IngressError, IngressResponseMessage};
    use restate_network::{ConsensusOrIngressTarget, TargetConsensusOrIngress};

    #[derive(Debug)]
    pub(crate) struct ShuffleToConsensus {
        msg: shuffle::InvocationOrResponse,
        shuffle_id: PeerId,
        partition_id: PartitionId,
        msg_index: MessageIndex,
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

    impl From<ShuffleToConsensus> for partition::AckCommand {
        fn from(value: ShuffleToConsensus) -> Self {
            let ShuffleToConsensus {
                msg,
                shuffle_id,
                partition_id,
                msg_index,
            } = value;

            let deduplication_source =
                partition::DeduplicationSource::shuffle(shuffle_id, partition_id, msg_index);

            match msg {
                shuffle::InvocationOrResponse::Invocation(invocation) => {
                    partition::AckCommand::dedup(
                        partition::Command::Invocation(invocation),
                        deduplication_source,
                    )
                }
                shuffle::InvocationOrResponse::Response(response) => partition::AckCommand::dedup(
                    partition::Command::Response(response),
                    deduplication_source,
                ),
            }
        }
    }

    #[derive(Debug)]
    pub(crate) struct ShuffleToIngress {
        msg: shuffle::IngressResponse,
        shuffle_id: PeerId,
        msg_index: MessageIndex,
    }

    impl TargetConsensusOrIngress<ShuffleToConsensus, ShuffleToIngress> for shuffle::ShuffleOutput {
        fn target(self) -> ConsensusOrIngressTarget<ShuffleToConsensus, ShuffleToIngress> {
            let (shuffle_id, partition_id, msg_index, target) = self.into_inner();

            match target {
                shuffle::ShuffleMessageDestination::PartitionProcessor(outbox_message) => {
                    ConsensusOrIngressTarget::Consensus(ShuffleToConsensus {
                        msg: outbox_message,
                        partition_id,
                        shuffle_id,
                        msg_index,
                    })
                }
                shuffle::ShuffleMessageDestination::Ingress(invocation_response) => {
                    ConsensusOrIngressTarget::Ingress(ShuffleToIngress {
                        msg: invocation_response,
                        shuffle_id,
                        msg_index,
                    })
                }
            }
        }
    }

    impl From<ShuffleToIngress> for restate_ingress_grpc::IngressInput {
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

            restate_ingress_grpc::IngressInput::response(IngressResponseMessage {
                service_invocation_id,
                result,
                ack_target: restate_ingress_grpc::AckTarget::new(shuffle_id, msg_index),
            })
        }
    }
}

mod partition_integration {
    use crate::partition;
    use crate::partition::shuffle;
    use restate_common::types::PeerId;
    use restate_network::{ShuffleOrIngressTarget, TargetShuffle, TargetShuffleOrIngress};

    impl
        TargetShuffleOrIngress<
            partition::ShuffleDeduplicationResponse,
            partition::IngressAckResponse,
        > for partition::AckResponse
    {
        fn target(
            self,
        ) -> ShuffleOrIngressTarget<
            partition::ShuffleDeduplicationResponse,
            partition::IngressAckResponse,
        > {
            match self {
                partition::AckResponse::Shuffle(ack) => ShuffleOrIngressTarget::Shuffle(ack),
                partition::AckResponse::Ingress(ack) => ShuffleOrIngressTarget::Ingress(ack),
            }
        }
    }

    impl From<partition::ShuffleDeduplicationResponse> for shuffle::ShuffleInput {
        fn from(value: partition::ShuffleDeduplicationResponse) -> Self {
            shuffle::ShuffleInput(value.kind)
        }
    }

    impl TargetShuffle for partition::ShuffleDeduplicationResponse {
        fn shuffle_target(&self) -> PeerId {
            self.shuffle_target
        }
    }

    impl From<partition::IngressAckResponse> for restate_ingress_grpc::IngressInput {
        fn from(value: partition::IngressAckResponse) -> Self {
            restate_ingress_grpc::IngressInput::message_ack(value.seq_number)
        }
    }
}
