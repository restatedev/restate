use crate::types::PartitionKey;

/// Trait for messages that have a partition key
pub trait PartitionedMessage {
    /// Returns the partition key
    fn partition_key(&self) -> PartitionKey;
}
