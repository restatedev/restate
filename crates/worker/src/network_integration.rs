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
use restate_types::partition_table::FixedPartitionTable;
use std::sync::Arc;

pub(super) type Network = restate_network::Network<
    shuffle::ShuffleInput,
    restate_ingress_dispatcher::IngressDispatcherInput,
    partition::types::AckResponse,
    partition::types::ShuffleAckResponse,
    partition::types::IngressAckResponse,
    Arc<FixedPartitionTable>,
>;

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
