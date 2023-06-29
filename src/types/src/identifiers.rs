//! Restate uses many identifiers to uniquely identify its components and entities.

use bytes::Bytes;
use bytestring::ByteString;
use uuid::Uuid;

/// Identifying a member of a raft group
pub type PeerId = u64;

/// Identifying the leader epoch of a raft group leader
pub type LeaderEpoch = u64;

/// Identifying the partition
pub type PartitionId = u64;

/// The leader epoch of a given partition
pub type PartitionLeaderEpoch = (PartitionId, LeaderEpoch);

// Just an alias
pub use crate::journal::EntryIndex;

/// Identifying to which partition a key belongs. This is unlike the [`PartitionId`]
/// which identifies a consecutive range of partition keys.
pub type PartitionKey = u32;

/// Discriminator for invocation instances
pub type InvocationId = Uuid;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct IngressId(pub std::net::SocketAddr);

// Just an alias
pub use crate::invocation::ServiceInvocationId;

/// Id of a keyed service instance.
///
/// Services are isolated by key. This means that there cannot be two concurrent
/// invocations for the same service instance (service name, key).
#[derive(Eq, Hash, PartialEq, PartialOrd, Ord, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ServiceId {
    /// Identifies the grpc service
    pub service_name: ByteString,
    /// Identifies the service instance for the given service name
    pub key: Bytes,

    partition_key: PartitionKey,
}

impl ServiceId {
    pub fn new(service_name: impl Into<ByteString>, key: impl Into<Bytes>) -> Self {
        let key = key.into();
        let partition_key = partitioner::HashPartitioner::compute_partition_key(&key);
        Self::with_partition_key(partition_key, service_name, key)
    }

    /// # Important
    /// The `partition_key` must be hash of the `key` computed via [`HashPartitioner`].
    pub fn with_partition_key(
        partition_key: PartitionKey,
        service_name: impl Into<ByteString>,
        key: impl Into<Bytes>,
    ) -> Self {
        Self {
            service_name: service_name.into(),
            key: key.into(),
            partition_key,
        }
    }

    pub fn partition_key(&self) -> PartitionKey {
        self.partition_key
    }
}

mod partitioner {
    use super::PartitionKey;

    use std::hash::{Hash, Hasher};

    /// Computes the [`PartitionKey`] based on xxh3 hashing.
    pub(super) struct HashPartitioner;

    impl HashPartitioner {
        pub(super) fn compute_partition_key(value: &impl Hash) -> PartitionKey {
            let mut hasher = xxhash_rust::xxh3::Xxh3::default();
            value.hash(&mut hasher);
            // Safe to only take the lower 32 bits: See https://github.com/Cyan4973/xxHash/issues/453#issuecomment-696838445
            hasher.finish() as u32
        }
    }
}
