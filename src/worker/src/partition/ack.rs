// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition;
use restate_types::identifiers::{IngressDispatcherId, PartitionId, PeerId};
use restate_types::message::{AckKind, MessageIndex};

/// Envelope for [`partition::Command`] that might require an explicit acknowledge.
#[derive(Debug)]
pub(crate) struct AckCommand {
    cmd: partition::Command,
    ack_mode: AckMode,
}

#[derive(Debug)]
pub(crate) enum AckMode {
    Ack(AckTarget),
    Dedup(DeduplicationSource),
    None,
}

impl AckCommand {
    /// Create a command that requires an acknowledgement upon reception.
    pub(crate) fn ack(cmd: partition::Command, ack_target: AckTarget) -> Self {
        Self {
            cmd,
            ack_mode: AckMode::Ack(ack_target),
        }
    }

    /// Create a command that should be de-duplicated with respect to the `producer_id` and the
    /// `seq_number` by the receiver.
    pub(crate) fn dedup(
        cmd: partition::Command,
        deduplication_source: DeduplicationSource,
    ) -> Self {
        Self {
            cmd,
            ack_mode: AckMode::Dedup(deduplication_source),
        }
    }

    /// Create a command that should not be acknowledged.
    pub(crate) fn no_ack(cmd: partition::Command) -> Self {
        Self {
            cmd,
            ack_mode: AckMode::None,
        }
    }

    pub(super) fn into_inner(self) -> (partition::Command, AckMode) {
        (self.cmd, self.ack_mode)
    }
}

#[derive(Debug)]
pub(crate) enum DeduplicationSource {
    Shuffle {
        producing_partition_id: PartitionId,
        shuffle_id: PeerId,
        seq_number: MessageIndex,
    },
    Ingress {
        ingress_dispatcher_id: IngressDispatcherId,
        // String used to distinguish between different seq_numbers indexes produced by the ingress
        source_id: String,
        seq_number: MessageIndex,
    },
}

impl DeduplicationSource {
    pub(crate) fn shuffle(
        shuffle_id: PeerId,
        producing_partition_id: PartitionId,
        seq_number: MessageIndex,
    ) -> Self {
        DeduplicationSource::Shuffle {
            shuffle_id,
            producing_partition_id,
            seq_number,
        }
    }

    pub(crate) fn ingress(
        ingress_dispatcher_id: IngressDispatcherId,
        source_id: String,
        seq_number: MessageIndex,
    ) -> Self {
        DeduplicationSource::Ingress {
            ingress_dispatcher_id,
            source_id,
            seq_number,
        }
    }

    pub(crate) fn acknowledge(self) -> AckResponse {
        match self {
            DeduplicationSource::Shuffle {
                shuffle_id,
                seq_number,
                ..
            } => AckResponse::Shuffle(ShuffleDeduplicationResponse {
                shuffle_target: shuffle_id,
                kind: AckKind::Acknowledge(seq_number),
            }),
            DeduplicationSource::Ingress {
                ingress_dispatcher_id,
                seq_number,
                source_id,
            } => AckResponse::Ingress(IngressAckResponse {
                _ingress_dispatcher_id: ingress_dispatcher_id,
                dedup_source: Some(source_id),
                kind: AckKind::Acknowledge(seq_number),
            }),
        }
    }

    pub(crate) fn duplicate(self, last_known_seq_number: MessageIndex) -> AckResponse {
        match self {
            DeduplicationSource::Shuffle {
                shuffle_id,
                seq_number,
                ..
            } => AckResponse::Shuffle(ShuffleDeduplicationResponse {
                shuffle_target: shuffle_id,
                kind: AckKind::Duplicate {
                    seq_number,
                    last_known_seq_number,
                },
            }),
            DeduplicationSource::Ingress {
                ingress_dispatcher_id,
                seq_number,
                source_id,
            } => AckResponse::Ingress(IngressAckResponse {
                _ingress_dispatcher_id: ingress_dispatcher_id,
                dedup_source: Some(source_id),
                kind: AckKind::Duplicate {
                    seq_number,
                    last_known_seq_number,
                },
            }),
        }
    }
}

#[derive(Debug)]
pub(crate) enum AckTarget {
    Ingress {
        ingress_dispatcher_id: IngressDispatcherId,
        seq_number: MessageIndex,
    },
}

impl AckTarget {
    pub(crate) fn ingress(
        ingress_dispatcher_id: IngressDispatcherId,
        seq_number: MessageIndex,
    ) -> Self {
        AckTarget::Ingress {
            ingress_dispatcher_id,
            seq_number,
        }
    }

    pub(super) fn acknowledge(self) -> AckResponse {
        match self {
            AckTarget::Ingress {
                ingress_dispatcher_id,
                seq_number,
            } => AckResponse::Ingress(IngressAckResponse {
                _ingress_dispatcher_id: ingress_dispatcher_id,
                dedup_source: None,
                kind: AckKind::Acknowledge(seq_number),
            }),
        }
    }
}

#[derive(Debug)]
pub(crate) enum AckResponse {
    Shuffle(ShuffleDeduplicationResponse),
    Ingress(IngressAckResponse),
}

#[derive(Debug)]
pub(crate) struct ShuffleDeduplicationResponse {
    pub(crate) shuffle_target: PeerId,
    pub(crate) kind: AckKind,
}

#[derive(Debug)]
pub(crate) struct IngressAckResponse {
    pub(crate) _ingress_dispatcher_id: IngressDispatcherId,
    pub(crate) dedup_source: Option<String>,
    pub(crate) kind: AckKind,
}
