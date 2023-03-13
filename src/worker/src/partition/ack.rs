use crate::partition;
use common::types::{AckKind, IngressId, PeerId};

/// Envelope for [`partition::Command`] that might require an explicit acknowledge.
#[derive(Debug)]
pub(crate) struct AckableCommand {
    cmd: partition::Command,
    ack_target: Option<AckTarget>,
}

impl AckableCommand {
    pub(crate) fn require_ack(cmd: partition::Command, ack_target: AckTarget) -> Self {
        Self {
            cmd,
            ack_target: Some(ack_target),
        }
    }

    pub(crate) fn no_ack(cmd: partition::Command) -> Self {
        Self {
            cmd,
            ack_target: None,
        }
    }

    pub(super) fn into_inner(self) -> (partition::Command, Option<AckTarget>) {
        (self.cmd, self.ack_target)
    }
}

#[derive(Debug)]
pub(crate) enum AckTarget {
    Shuffle {
        shuffle_target: PeerId,
        msg_index: u64,
    },
    Ingress {
        ingress_id: IngressId,
        msg_index: u64,
    },
}

impl AckTarget {
    pub(crate) fn shuffle(shuffle_target: PeerId, msg_index: u64) -> Self {
        AckTarget::Shuffle {
            shuffle_target,
            msg_index,
        }
    }

    pub(crate) fn ingress(ingress_id: IngressId, msg_index: u64) -> Self {
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
