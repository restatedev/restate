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
    restate_wal_protocol::Envelope,
    shuffle::ShuffleInput,
    shuffle::ShuffleOutput,
    restate_wal_protocol::Envelope,
    shuffle_integration::ShuffleToIngress,
    restate_ingress_dispatcher::IngressDispatcherOutput,
    restate_wal_protocol::Envelope,
    ingress_integration::IngressToShuffle,
    restate_ingress_dispatcher::IngressDispatcherInput,
    partition::types::AckResponse,
    partition::types::ShuffleAckResponse,
    partition::types::IngressAckResponse,
    FixedConsecutivePartitions,
>;

mod ingress_integration {

    use crate::partition::shuffle;
    use restate_network::{ConsensusOrShuffleTarget, TargetConsensusOrShuffle, TargetShuffle};
    use restate_types::identifiers::PeerId;
    use restate_types::message::AckKind;
    use restate_wal_protocol::Envelope;

    impl TargetConsensusOrShuffle<Envelope, IngressToShuffle>
        for restate_ingress_dispatcher::IngressDispatcherOutput
    {
        fn into_target(self) -> ConsensusOrShuffleTarget<Envelope, IngressToShuffle> {
            match self {
                restate_ingress_dispatcher::IngressDispatcherOutput::Envelope(envelope) => {
                    ConsensusOrShuffleTarget::Consensus(envelope)
                }
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
    use crate::partition::shuffle;
    use restate_network::{ConsensusOrIngressTarget, TargetConsensusOrIngress};
    use restate_types::errors::InvocationError;
    use restate_types::identifiers::PeerId;
    use restate_types::invocation::ResponseResult;
    use restate_types::message::MessageIndex;
    use restate_wal_protocol::Envelope;

    #[derive(Debug)]
    pub(crate) struct ShuffleToIngress {
        msg: shuffle::IngressResponse,
        shuffle_id: PeerId,
        msg_index: MessageIndex,
    }

    impl TargetConsensusOrIngress<Envelope, ShuffleToIngress> for shuffle::ShuffleOutput {
        fn into_target(self) -> ConsensusOrIngressTarget<Envelope, ShuffleToIngress> {
            match self {
                shuffle::ShuffleOutput::Envelope(envelope) => {
                    ConsensusOrIngressTarget::Consensus(envelope)
                }
                shuffle::ShuffleOutput::Ingress {
                    response,
                    shuffle_id,
                    seq_number,
                } => ConsensusOrIngressTarget::Ingress(ShuffleToIngress {
                    msg: response,
                    shuffle_id,
                    msg_index: seq_number,
                }),
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
            partition::types::ShuffleAckResponse,
            partition::types::IngressAckResponse,
        > for partition::types::AckResponse
    {
        fn into_target(
            self,
        ) -> ShuffleOrIngressTarget<
            partition::types::ShuffleAckResponse,
            partition::types::IngressAckResponse,
        > {
            match self {
                partition::types::AckResponse::Shuffle(ack) => ShuffleOrIngressTarget::Shuffle(ack),
                partition::types::AckResponse::Ingress(ack) => ShuffleOrIngressTarget::Ingress(ack),
            }
        }
    }

    impl From<partition::types::ShuffleAckResponse> for shuffle::ShuffleInput {
        fn from(value: partition::types::ShuffleAckResponse) -> Self {
            shuffle::ShuffleInput(value.kind)
        }
    }

    impl TargetShuffle for partition::types::ShuffleAckResponse {
        fn shuffle_target(&self) -> PeerId {
            self.shuffle_target
        }
    }

    impl From<partition::types::IngressAckResponse>
        for restate_ingress_dispatcher::IngressDispatcherInput
    {
        fn from(value: partition::types::IngressAckResponse) -> Self {
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
