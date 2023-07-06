//! This module defines types used for the internal messaging between Restate components.

use crate::identifiers::{PartitionKey, PeerId};

/// Wrapper that extends a message with its target peer to which the message should be sent.
pub type PeerTarget<Msg> = (PeerId, Msg);

/// Index type used messages in the runtime
pub type MessageIndex = u64;

#[derive(Debug, Clone, Copy)]
pub enum AckKind {
    Acknowledge(MessageIndex),
    Duplicate {
        // Sequence number of the duplicate message.
        seq_number: MessageIndex,
        // Currently last known sequence number by the receiver for a producer.
        // See `DeduplicatingStateMachine` for more details.
        last_known_seq_number: MessageIndex,
    },
}

/// Trait for messages that have a partition key
pub trait PartitionedMessage {
    /// Returns the partition key
    fn partition_key(&self) -> PartitionKey;
}
