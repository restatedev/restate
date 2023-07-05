//! Restate uses many identifiers to uniquely identify its components and entities.

use base64::display::Base64Display;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use bytestring::ByteString;
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;
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
pub type EntryIndex = u32;

/// Unique Id of an endpoint.
///
/// Currently this will contain the endpoint url authority and path base64 encoded, but this might change in future.
pub type EndpointId = String;

/// Identifying to which partition a key belongs. This is unlike the [`PartitionId`]
/// which identifies a consecutive range of partition keys.
pub type PartitionKey = u32;

/// Discriminator for invocation instances
pub type InvocationId = Uuid;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct IngressId(pub std::net::SocketAddr);

/// Id of a single service invocation.
///
/// A service invocation id is composed of a [`ServiceId`] and an [`InvocationId`]
/// that makes the id unique.
#[derive(Eq, Hash, PartialEq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ServiceInvocationId {
    /// Identifies the invoked service
    pub service_id: ServiceId,
    /// Uniquely identifies this invocation instance
    pub invocation_id: InvocationId,
}

impl ServiceInvocationId {
    pub fn new(
        service_name: impl Into<ByteString>,
        key: impl Into<Bytes>,
        invocation_id: impl Into<InvocationId>,
    ) -> Self {
        Self::with_service_id(ServiceId::new(service_name, key), invocation_id)
    }

    pub fn with_service_id(service_id: ServiceId, invocation_id: impl Into<InvocationId>) -> Self {
        Self {
            service_id,
            invocation_id: invocation_id.into(),
        }
    }
}

impl Display for ServiceInvocationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}-{}",
            self.service_id.service_name,
            Base64Display::new(&self.service_id.key, &BASE64_STANDARD),
            self.invocation_id.as_simple()
        )
    }
}

#[derive(Debug, Default, thiserror::Error)]
#[error("cannot parse the opaque id, bad format")]
pub struct ServiceInvocationIdParseError {
    #[source]
    cause: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl ServiceInvocationIdParseError {
    pub fn from_cause(cause: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            cause: Some(Box::new(cause)),
        }
    }
}

impl FromStr for ServiceInvocationId {
    type Err = ServiceInvocationIdParseError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        // This encoding is based on the fact that neither invocation_id
        // nor service_name can contain the '-' character
        // Invocation id is serialized as simple
        // Service name follows the fullIdent ABNF here:
        // https://protobuf.dev/reference/protobuf/proto3-spec/#identifiers
        let mut splits: Vec<&str> = str.splitn(3, '-').collect();
        if splits.len() != 3 {
            return Err(ServiceInvocationIdParseError::default());
        }
        let invocation_id: Uuid = splits
            .pop()
            .unwrap()
            .parse()
            .map_err(ServiceInvocationIdParseError::from_cause)?;
        let key = BASE64_STANDARD
            .decode(splits.pop().unwrap())
            .map_err(ServiceInvocationIdParseError::from_cause)?;
        let service_name = splits.pop().unwrap().to_string();

        Ok(ServiceInvocationId::new(service_name, key, invocation_id))
    }
}

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

/// Incremental id defining the service revision.
pub type ServiceRevision = usize;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_test(expected_sid: ServiceInvocationId) {
        let opaque_sid: String = expected_sid.to_string();
        let actual_sid: ServiceInvocationId = opaque_sid.parse().unwrap();
        assert_eq!(expected_sid, actual_sid);
    }

    #[test]
    fn roundtrip_keyed_service_with_hyphens() {
        roundtrip_test(ServiceInvocationId::new(
            "my.example.Service",
            "-------stuff------".as_bytes().to_vec(),
            Uuid::now_v7(),
        ));
    }

    #[test]
    fn roundtrip_unkeyed_service() {
        roundtrip_test(ServiceInvocationId::new(
            "my.example.Service",
            Uuid::now_v7().as_bytes().to_vec(),
            Uuid::now_v7(),
        ));
    }

    #[test]
    fn roundtrip_empty_key() {
        roundtrip_test(ServiceInvocationId::new(
            "my.example.Service",
            "".as_bytes().to_vec(),
            Uuid::now_v7(),
        ));
    }
}
