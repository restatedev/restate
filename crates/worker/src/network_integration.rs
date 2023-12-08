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
    partition::StateMachineAckCommand,
    shuffle::ShuffleInput,
    shuffle::ShuffleOutput,
    shuffle_integration::ShuffleToConsensus,
    shuffle_integration::ShuffleToIngress,
    restate_ingress_dispatcher::IngressDispatcherOutput,
    ingress_integration::IngressToConsensus,
    ingress_integration::IngressToShuffle,
    restate_ingress_dispatcher::IngressDispatcherInput,
    partition::StateMachineAckResponse,
    partition::StateMachineShuffleDeduplicationResponse,
    partition::StateMachineIngressAckResponse,
    FixedConsecutivePartitions,
>;

mod ingress_integration {

    use crate::partition;
    use crate::partition::shuffle;
    use restate_network::{ConsensusOrShuffleTarget, TargetConsensusOrShuffle, TargetShuffle};
    use restate_types::identifiers::WithPartitionKey;
    use restate_types::identifiers::{IngressDispatcherId, PartitionKey, PeerId};
    use restate_types::invocation::ServiceInvocation;
    use restate_types::message::{AckKind, MessageIndex};

    impl TargetConsensusOrShuffle<IngressToConsensus, IngressToShuffle>
        for restate_ingress_dispatcher::IngressDispatcherOutput
    {
        fn target(self) -> ConsensusOrShuffleTarget<IngressToConsensus, IngressToShuffle> {
            match self {
                restate_ingress_dispatcher::IngressDispatcherOutput::Invocation {
                    service_invocation,
                    ingress_dispatcher_id,
                    deduplication_source,
                    msg_index,
                } => ConsensusOrShuffleTarget::Consensus(IngressToConsensus {
                    service_invocation,
                    ingress_dispatcher_id,
                    deduplication_source,
                    msg_index,
                }),
                restate_ingress_dispatcher::IngressDispatcherOutput::Ack(
                    restate_ingress_dispatcher::AckResponse {
                        kind,
                        shuffle_target,
                    },
                ) => ConsensusOrShuffleTarget::Shuffle(IngressToShuffle {
                    shuffle_target,
                    kind,
                }),
            }
        }
    }

    #[derive(Debug)]
    pub(crate) struct IngressToConsensus {
        service_invocation: ServiceInvocation,
        ingress_dispatcher_id: IngressDispatcherId,
        deduplication_source: Option<String>,
        msg_index: MessageIndex,
    }

    impl WithPartitionKey for IngressToConsensus {
        fn partition_key(&self) -> PartitionKey {
            self.service_invocation.fid.service_id.partition_key()
        }
    }

    impl From<IngressToConsensus> for partition::StateMachineAckCommand {
        fn from(ingress_to_consensus: IngressToConsensus) -> Self {
            let IngressToConsensus {
                service_invocation,
                ingress_dispatcher_id,
                deduplication_source,
                msg_index,
            } = ingress_to_consensus;

            let cmd = partition::StateMachineCommand::Invocation(service_invocation);

            match deduplication_source {
                None => partition::StateMachineAckCommand::ack(
                    cmd,
                    partition::StateMachineAckTarget::ingress(ingress_dispatcher_id, msg_index),
                ),
                Some(source_id) => partition::StateMachineAckCommand::dedup(
                    cmd,
                    partition::StateMachineDeduplicationSource::ingress(
                        ingress_dispatcher_id,
                        source_id,
                        msg_index,
                    ),
                ),
            }
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
    use crate::partition::shuffle::PartitionProcessorMessage;
    use restate_network::{ConsensusOrIngressTarget, TargetConsensusOrIngress};
    use restate_types::errors::InvocationError;
    use restate_types::identifiers::WithPartitionKey;
    use restate_types::identifiers::{PartitionId, PartitionKey, PeerId};
    use restate_types::invocation::{MaybeFullInvocationId, ResponseResult};
    use restate_types::message::MessageIndex;

    #[derive(Debug)]
    pub(crate) struct ShuffleToConsensus {
        msg: shuffle::PartitionProcessorMessage,
        shuffle_id: PeerId,
        partition_id: PartitionId,
        msg_index: MessageIndex,
    }

    impl WithPartitionKey for ShuffleToConsensus {
        fn partition_key(&self) -> PartitionKey {
            match &self.msg {
                shuffle::PartitionProcessorMessage::Invocation(invocation) => {
                    invocation.fid.service_id.partition_key()
                }
                shuffle::PartitionProcessorMessage::Response(response) => {
                    response.id.partition_key()
                }
                PartitionProcessorMessage::Kill(fid) => fid.partition_key(),
            }
        }
    }

    impl From<ShuffleToConsensus> for partition::StateMachineAckCommand {
        fn from(value: ShuffleToConsensus) -> Self {
            let ShuffleToConsensus {
                msg,
                shuffle_id,
                partition_id,
                msg_index,
            } = value;

            let deduplication_source = partition::StateMachineDeduplicationSource::shuffle(
                shuffle_id,
                partition_id,
                msg_index,
            );

            match msg {
                shuffle::PartitionProcessorMessage::Invocation(invocation) => {
                    partition::StateMachineAckCommand::dedup(
                        partition::StateMachineCommand::Invocation(invocation),
                        deduplication_source,
                    )
                }
                shuffle::PartitionProcessorMessage::Response(response) => {
                    partition::StateMachineAckCommand::dedup(
                        partition::StateMachineCommand::Response(response),
                        deduplication_source,
                    )
                }
                shuffle::PartitionProcessorMessage::Kill(fid) => {
                    partition::StateMachineAckCommand::dedup(
                        partition::StateMachineCommand::Kill(MaybeFullInvocationId::from(fid)),
                        deduplication_source,
                    )
                }
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

    impl From<ShuffleToIngress> for restate_ingress_dispatcher::IngressDispatcherInput {
        fn from(value: ShuffleToIngress) -> Self {
            let ShuffleToIngress {
                msg:
                    shuffle::IngressResponse {
                        full_invocation_id,
                        response,
                        ..
                    },
                msg_index,
                shuffle_id,
            } = value;

            let result = match response {
                ResponseResult::Success(result) => Ok(result),
                ResponseResult::Failure(err_code, error_msg) => {
                    Err(InvocationError::new(err_code, error_msg.to_string()))
                }
            };

            restate_ingress_dispatcher::IngressDispatcherInput::response(
                restate_ingress_dispatcher::IngressResponseMessage {
                    full_invocation_id,
                    result,
                    ack_target: restate_ingress_dispatcher::AckTarget::new(shuffle_id, msg_index),
                },
            )
        }
    }
}

mod partition_integration {
    use crate::partition;
    use crate::partition::shuffle;
    use restate_network::{ShuffleOrIngressTarget, TargetShuffle, TargetShuffleOrIngress};
    use restate_types::identifiers::PeerId;
    use restate_types::message::AckKind;

    impl
        TargetShuffleOrIngress<
            partition::StateMachineShuffleDeduplicationResponse,
            partition::StateMachineIngressAckResponse,
        > for partition::StateMachineAckResponse
    {
        fn target(
            self,
        ) -> ShuffleOrIngressTarget<
            partition::StateMachineShuffleDeduplicationResponse,
            partition::StateMachineIngressAckResponse,
        > {
            match self {
                partition::StateMachineAckResponse::Shuffle(ack) => {
                    ShuffleOrIngressTarget::Shuffle(ack)
                }
                partition::StateMachineAckResponse::Ingress(ack) => {
                    ShuffleOrIngressTarget::Ingress(ack)
                }
            }
        }
    }

    impl From<partition::StateMachineShuffleDeduplicationResponse> for shuffle::ShuffleInput {
        fn from(value: partition::StateMachineShuffleDeduplicationResponse) -> Self {
            shuffle::ShuffleInput(value.kind)
        }
    }

    impl TargetShuffle for partition::StateMachineShuffleDeduplicationResponse {
        fn shuffle_target(&self) -> PeerId {
            self.shuffle_target
        }
    }

    impl From<partition::StateMachineIngressAckResponse>
        for restate_ingress_dispatcher::IngressDispatcherInput
    {
        fn from(value: partition::StateMachineIngressAckResponse) -> Self {
            let seq_number = match value.kind {
                AckKind::Acknowledge(seq_number) => seq_number,
                AckKind::Duplicate { seq_number, .. } => {
                    // Ingress dispatcher doesn't currently support handling duplicates
                    seq_number
                }
            };

            if let Some(dedup_source) = value.dedup_source {
                restate_ingress_dispatcher::IngressDispatcherInput::DedupMessageAck(
                    dedup_source,
                    seq_number,
                )
            } else {
                restate_ingress_dispatcher::IngressDispatcherInput::MessageAck(seq_number)
            }
        }
    }
}
