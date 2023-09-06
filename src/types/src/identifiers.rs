// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Restate uses many identifiers to uniquely identify its components and entities.

use base64::display::Base64Display;
use base64::Engine;
use bytes::Bytes;
use bytestring::ByteString;
use std::fmt;
use std::mem::size_of;
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

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct IngressDispatcherId(pub std::net::SocketAddr);

impl fmt::Display for IngressDispatcherId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for IngressDispatcherId {
    type Err = std::net::AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(IngressDispatcherId(s.parse()?))
    }
}

/// Identifying to which partition a key belongs. This is unlike the [`PartitionId`]
/// which identifies a consecutive range of partition keys.
pub type PartitionKey = u64;

/// Trait for data structures that have a partition key
pub trait WithPartitionKey {
    /// Returns the partition key
    fn partition_key(&self) -> PartitionKey;
}

/// Discriminator for invocation instances
#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug, Ord, PartialOrd, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InvocationUuid(Uuid);

impl InvocationUuid {
    pub fn from_slice(b: &[u8]) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::from_slice(b)?))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn now_v7() -> Self {
        Self(Uuid::now_v7())
    }
}

impl fmt::Display for InvocationUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.as_simple().fmt(f)
    }
}

impl AsRef<[u8]> for InvocationUuid {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<Uuid> for InvocationUuid {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl From<InvocationUuid> for Uuid {
    fn from(value: InvocationUuid) -> Self {
        value.0
    }
}

impl From<InvocationUuid> for opentelemetry_api::trace::TraceId {
    fn from(value: InvocationUuid) -> Self {
        let uuid: Uuid = value.into();
        Self::from_bytes(uuid.into_bytes())
    }
}

impl From<InvocationUuid> for opentelemetry_api::trace::SpanId {
    fn from(value: InvocationUuid) -> Self {
        let uuid: Uuid = value.into();
        let last8: [u8; 8] = std::convert::TryInto::try_into(&uuid.as_bytes()[8..16]).unwrap();
        Self::from_bytes(last8)
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
}

impl WithPartitionKey for ServiceId {
    fn partition_key(&self) -> PartitionKey {
        self.partition_key
    }
}

/// InvocationId is a unique identifier of the invocation,
/// including enough routing information for the network component
/// to route requests to the correct partition processors.
#[derive(Eq, Hash, PartialEq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(try_from = "EncodedInvocationId", into = "EncodedInvocationId")
)]
pub struct InvocationId {
    /// Partition key of the called service
    partition_key: PartitionKey,
    /// Uniquely identifies this invocation instance
    invocation_uuid: InvocationUuid,
}

pub type EncodedInvocationId = [u8; size_of::<PartitionKey>() + size_of::<uuid::Bytes>()];

impl InvocationId {
    pub fn new(partition_key: PartitionKey, invocation_uuid: impl Into<InvocationUuid>) -> Self {
        Self {
            partition_key,
            invocation_uuid: invocation_uuid.into(),
        }
    }

    pub fn from_slice(b: &[u8]) -> Result<Self, InvocationIdParseError> {
        let mut encoded_id = EncodedInvocationId::default();
        if b.len() != size_of::<EncodedInvocationId>() {
            return Err(InvocationIdParseError::BadSliceLength);
        }

        encoded_id.copy_from_slice(b);
        encoded_id.try_into()
    }

    pub fn invocation_uuid(&self) -> InvocationUuid {
        self.invocation_uuid
    }

    pub fn as_bytes(&self) -> EncodedInvocationId {
        encode_invocation_id(&self.partition_key, &self.invocation_uuid)
    }
}

impl TryFrom<EncodedInvocationId> for InvocationId {
    type Error = InvocationIdParseError;

    fn try_from(encoded_id: EncodedInvocationId) -> Result<Self, InvocationIdParseError> {
        let mut partition_key_buf = [0; size_of::<PartitionKey>()];
        partition_key_buf.copy_from_slice(&encoded_id[..size_of::<PartitionKey>()]);
        let partition_key = PartitionKey::from_be_bytes(partition_key_buf);

        let uuid = Uuid::from_slice(&encoded_id[size_of::<PartitionKey>()..])?;

        Ok(Self {
            partition_key,
            invocation_uuid: InvocationUuid(uuid),
        })
    }
}

impl From<InvocationId> for EncodedInvocationId {
    fn from(value: InvocationId) -> Self {
        value.as_bytes()
    }
}

impl WithPartitionKey for InvocationId {
    fn partition_key(&self) -> PartitionKey {
        self.partition_key
    }
}

impl fmt::Display for InvocationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        display_invocation_id(&self.partition_key, &self.invocation_uuid, f)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InvocationIdParseError {
    #[error("cannot parse the invocation id, bad slice length")]
    BadSliceLength,
    #[error("cannot parse the invocation id uuid: {0}")]
    Uuid(#[from] uuid::Error),
    #[error("cannot parse the invocation id encoded as base64: {0}")]
    Base64(#[from] base64::DecodeSliceError),
}

impl FromStr for InvocationId {
    type Err = InvocationIdParseError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let mut encoded_id = EncodedInvocationId::default();

        // Length check will be performed by the base64 lib directly
        restate_base64_util::URL_SAFE.decode_slice(str, &mut encoded_id)?;

        encoded_id.try_into()
    }
}

/// Id of a single service invocation.
///
/// A service invocation id is composed of a [`ServiceId`] and an [`InvocationUuid`]
/// that makes the id unique.
#[derive(Eq, Hash, PartialEq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FullInvocationId {
    /// Identifies the invoked service
    pub service_id: ServiceId,
    /// Uniquely identifies this invocation instance
    pub invocation_uuid: InvocationUuid,
}

impl FullInvocationId {
    pub fn new(
        service_name: impl Into<ByteString>,
        key: impl Into<Bytes>,
        invocation_id: impl Into<InvocationUuid>,
    ) -> Self {
        Self::with_service_id(ServiceId::new(service_name, key), invocation_id)
    }

    pub fn generate(service_name: impl Into<ByteString>, key: impl Into<Bytes>) -> Self {
        Self::with_service_id(ServiceId::new(service_name, key), InvocationUuid::now_v7())
    }

    pub fn with_service_id(
        service_id: ServiceId,
        invocation_id: impl Into<InvocationUuid>,
    ) -> Self {
        Self {
            service_id,
            invocation_uuid: invocation_id.into(),
        }
    }

    pub fn to_invocation_id_bytes(&self) -> EncodedInvocationId {
        encode_invocation_id(&self.service_id.partition_key, &self.invocation_uuid)
    }
}

impl WithPartitionKey for FullInvocationId {
    fn partition_key(&self) -> PartitionKey {
        self.service_id.partition_key()
    }
}

impl fmt::Display for FullInvocationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        display_invocation_id(&self.service_id.partition_key, &self.invocation_uuid, f)
    }
}

impl From<FullInvocationId> for InvocationId {
    fn from(value: FullInvocationId) -> Self {
        Self {
            partition_key: value.partition_key(),
            invocation_uuid: value.invocation_uuid,
        }
    }
}

impl From<FullInvocationId> for EncodedInvocationId {
    fn from(value: FullInvocationId) -> Self {
        value.to_invocation_id_bytes()
    }
}

/// Incremental id defining the service revision.
pub type ServiceRevision = u32;

mod partitioner {
    use super::PartitionKey;

    use std::hash::{Hash, Hasher};

    /// Computes the [`PartitionKey`] based on xxh3 hashing.
    pub(super) struct HashPartitioner;

    impl HashPartitioner {
        pub(super) fn compute_partition_key(value: &impl Hash) -> PartitionKey {
            let mut hasher = xxhash_rust::xxh3::Xxh3::default();
            value.hash(&mut hasher);
            hasher.finish()
        }
    }
}

fn encode_invocation_id(
    partition_key: &PartitionKey,
    invocation_uuid: &InvocationUuid,
) -> EncodedInvocationId {
    let mut buf = [0_u8; size_of::<PartitionKey>() + size_of::<uuid::Bytes>()];
    buf[..size_of::<PartitionKey>()].copy_from_slice(&partition_key.to_be_bytes());
    buf[size_of::<PartitionKey>()..].copy_from_slice(invocation_uuid.0.as_bytes());
    buf
}

#[inline]
fn display_invocation_id(
    partition_key: &PartitionKey,
    invocation_uuid: &InvocationUuid,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    write!(
        f,
        "{}",
        Base64Display::new(
            &encode_invocation_id(partition_key, invocation_uuid),
            &restate_base64_util::URL_SAFE
        ),
    )
}

#[cfg(any(test, feature = "mocks"))]
mod mocks {
    use super::*;

    use rand::distributions::{Alphanumeric, DistString};
    use rand::Rng;

    impl IngressDispatcherId {
        pub fn mock() -> Self {
            Self("127.0.0.1:8080".parse().unwrap())
        }
    }

    impl InvocationId {
        pub fn mock_random() -> Self {
            Self::new(
                rand::thread_rng().sample::<PartitionKey, _>(rand::distributions::Standard),
                Uuid::now_v7(),
            )
        }
    }

    impl FullInvocationId {
        pub fn mock_random() -> Self {
            Self::new(
                Alphanumeric.sample_string(&mut rand::thread_rng(), 8),
                Bytes::copy_from_slice(
                    &rand::thread_rng().sample::<[u8; 32], _>(rand::distributions::Standard),
                ),
                Uuid::now_v7(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_invocation_id() {
        let expected = InvocationId::new(92, InvocationUuid::now_v7());
        assert_eq!(
            expected,
            InvocationId::from_slice(&expected.as_bytes()).unwrap()
        )
    }
}
