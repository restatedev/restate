use crate::partition;
use restate_common::types::{AckKind, IngressId, MessageIndex, PartitionId, PeerId};

/// Envelope for [`partition::Command`] that might require an explicit acknowledge.
#[derive(Debug)]
pub(crate) struct AckCommand {
    cmd: partition::Command,
    ack_mode: AckMode,
}

#[derive(Debug)]
pub(crate) enum AckMode {
    Ack(AckTarget),
    Dedup {
        producer_id: PartitionId,
        ack_target: AckTarget,
    },
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
    /// de-duplication sequence number of the [`AckTarget`].
    pub(crate) fn dedup(
        cmd: partition::Command,
        producer_id: PartitionId,
        ack_target: AckTarget,
    ) -> Self {
        Self {
            cmd,
            ack_mode: AckMode::Dedup {
                producer_id,
                ack_target,
            },
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
pub(crate) enum AckTarget {
    Shuffle {
        shuffle_target: PeerId,
        msg_index: MessageIndex,
    },
    Ingress {
        ingress_id: IngressId,
        msg_index: MessageIndex,
    },
}

impl AckTarget {
    pub(crate) fn shuffle(shuffle_target: PeerId, msg_index: MessageIndex) -> Self {
        AckTarget::Shuffle {
            shuffle_target,
            msg_index,
        }
    }

    pub(crate) fn ingress(ingress_id: IngressId, msg_index: MessageIndex) -> Self {
        AckTarget::Ingress {
            ingress_id,
            msg_index,
        }
    }

    pub(super) fn acknowledge(self) -> AckResponse {
        match self {
            AckTarget::Shuffle {
                shuffle_target,
                msg_index,
            } => AckResponse::Shuffle(ShuffleAckResponse {
                shuffle_target,
                kind: AckKind::Acknowledge(msg_index),
            }),
            AckTarget::Ingress {
                ingress_id,
                msg_index,
            } => AckResponse::Ingress(IngressAckResponse {
                _ingress_id: ingress_id,
                kind: AckKind::Acknowledge(msg_index),
            }),
        }
    }

    pub(super) fn duplicate(self, last_known_seq_number: MessageIndex) -> AckResponse {
        match self {
            AckTarget::Shuffle {
                shuffle_target,
                msg_index,
            } => AckResponse::Shuffle(ShuffleAckResponse {
                shuffle_target,
                kind: AckKind::Duplicate {
                    seq_number: msg_index,
                    last_known_seq_number,
                },
            }),
            AckTarget::Ingress {
                ingress_id,
                msg_index,
            } => AckResponse::Ingress(IngressAckResponse {
                _ingress_id: ingress_id,
                kind: AckKind::Duplicate {
                    seq_number: msg_index,
                    last_known_seq_number,
                },
            }),
        }
    }

    pub(super) fn dedup_seq_number(&self) -> MessageIndex {
        match self {
            AckTarget::Shuffle { msg_index, .. } => *msg_index,
            AckTarget::Ingress { msg_index, .. } => *msg_index,
        }
    }
}

#[derive(Debug)]
pub(crate) enum AckResponse {
    Shuffle(ShuffleAckResponse),
    Ingress(IngressAckResponse),
}

#[derive(Debug)]
pub(crate) struct ShuffleAckResponse {
    pub(crate) shuffle_target: PeerId,
    pub(crate) kind: AckKind,
}

#[derive(Debug)]
pub(crate) struct IngressAckResponse {
    pub(crate) _ingress_id: IngressId,
    pub(crate) kind: AckKind,
}
