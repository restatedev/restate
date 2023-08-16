// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains the glue code for converting the various messages into their
//! required formats when routing them through the network.

use crate::partition;
use crate::partition::shuffle;
use crate::partitioning_scheme::FixedConsecutivePartitions;

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
    FixedConsecutivePartitions,
>;

mod ingress_integration {
    use crate::partition;
    use crate::partition::shuffle;
    use restate_network::{ConsensusOrShuffleTarget, TargetConsensusOrShuffle, TargetShuffle};
    use restate_types::identifiers::{IngressId, PartitionKey, PeerId};
    use restate_types::invocation::ServiceInvocation;
    use restate_types::message::PartitionedMessage;
    use restate_types::message::{AckKind, MessageIndex};

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

    impl PartitionedMessage for IngressToConsensus {
        fn partition_key(&self) -> PartitionKey {
            self.service_invocation.id.service_id.partition_key()
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
    use restate_ingress_grpc::{IngressError, IngressResponseMessage};
    use restate_network::{ConsensusOrIngressTarget, TargetConsensusOrIngress};
    use restate_types::identifiers::{PartitionId, PartitionKey, PeerId};
    use restate_types::invocation::ResponseResult;
    use restate_types::message::MessageIndex;
    use restate_types::message::PartitionedMessage;

    #[derive(Debug)]
    pub(crate) struct ShuffleToConsensus {
        msg: shuffle::InvocationOrResponse,
        shuffle_id: PeerId,
        partition_id: PartitionId,
        msg_index: MessageIndex,
    }

    impl PartitionedMessage for ShuffleToConsensus {
        fn partition_key(&self) -> PartitionKey {
            match &self.msg {
                shuffle::InvocationOrResponse::Invocation(invocation) => {
                    invocation.id.service_id.partition_key()
                }
                shuffle::InvocationOrResponse::Response(response) => {
                    response.id.service_id.partition_key()
                }
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
                ResponseResult::Failure(err_code, error_msg) => {
                    Err(IngressError::new(err_code, error_msg.to_string()))
                }
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
    use restate_network::{ShuffleOrIngressTarget, TargetShuffle, TargetShuffleOrIngress};
    use restate_types::identifiers::PeerId;

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
