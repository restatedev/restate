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

use bytes::Bytes;
use bytestring::ByteString;
use ulid::Ulid;

use rand::RngCore;
use std::fmt;
use std::hash::Hash;
use std::mem::size_of;
use std::str::FromStr;
use uuid::Uuid;

use crate::base62_util::base62_encode_fixed_width;
use crate::base62_util::base62_max_length_for_type;
use crate::errors::IdDecodeError;
use crate::id_util::IdDecoder;
use crate::id_util::IdEncoder;
use crate::id_util::IdResourceType;
use crate::invocation::InvocationTarget;
use crate::time::MillisSinceEpoch;

/// Identifying the leader epoch of a partition processor
#[derive(
    Debug,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Clone,
    Copy,
    Hash,
    derive_more::From,
    derive_more::Into,
    derive_more::Display,
    serde::Serialize,
    serde::Deserialize,
)]
#[display(fmt = "e{}", _0)]
pub struct LeaderEpoch(u64);
impl LeaderEpoch {
    pub const INITIAL: Self = Self(1);

    pub fn next(self) -> Self {
        LeaderEpoch(self.0 + 1)
    }
}

impl Default for LeaderEpoch {
    fn default() -> Self {
        Self::INITIAL
    }
}

/// Identifying the partition
pub type PartitionId = u64;

/// The leader epoch of a given partition
pub type PartitionLeaderEpoch = (PartitionId, LeaderEpoch);

// Just an alias
pub type EntryIndex = u32;

/// Unique Id of a deployment.
#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    Hash,
    PartialOrd,
    Ord,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
pub struct DeploymentId(pub(crate) Ulid);

impl DeploymentId {
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    pub const fn from_parts(timestamp_ms: u64, random: u128) -> Self {
        Self(Ulid::from_parts(timestamp_ms, random))
    }
}

impl Default for DeploymentId {
    fn default() -> Self {
        Self::new()
    }
}

/// Unique Id of a subscription.
#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    Hash,
    PartialOrd,
    Ord,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
pub struct SubscriptionId(pub(crate) Ulid);

impl SubscriptionId {
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    pub const fn from_parts(timestamp_ms: u64, random: u128) -> Self {
        Self(Ulid::from_parts(timestamp_ms, random))
    }
}

impl Default for SubscriptionId {
    fn default() -> Self {
        Self::new()
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

/// A family of resource identifiers that tracks the timestamp of its creation.
pub trait TimestampAwareId {
    /// The timestamp when this ID was created.
    fn timestamp(&self) -> MillisSinceEpoch;
}

// A marker trait for serializable IDs that represent restate resources or entities.
// Those could be user-facing or not.
pub trait ResourceId {
    const SIZE_IN_BYTES: usize;
    const RESOURCE_TYPE: IdResourceType;
    /// The number of characters/bytes needed to string-serialize this resource (without the
    /// prefix or separator)
    const STRING_CAPACITY_HINT: usize;

    /// The resource type of this ID
    fn resource_type(&self) -> IdResourceType {
        Self::RESOURCE_TYPE
    }

    /// The max number of bytes needed to store the binary representation of this ID
    fn size_in_bytes(&self) -> usize {
        Self::SIZE_IN_BYTES
    }

    /// Adds the various fields of this resource ID into the pre-initialized encoder
    fn push_contents_to_encoder(&self, encoder: &mut IdEncoder<Self>);
}

/// Discriminator for invocation instances
#[derive(
    Eq,
    Hash,
    PartialEq,
    Clone,
    Copy,
    Debug,
    Ord,
    PartialOrd,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
pub struct InvocationUuid(Ulid);

impl InvocationUuid {
    pub const SIZE_IN_BYTES: usize = size_of::<u128>();

    pub fn new() -> Self {
        Self(Ulid::new())
    }

    pub fn from_slice(b: &[u8]) -> Result<Self, IdDecodeError> {
        let ulid = Ulid::from_bytes(b.try_into().map_err(|_| IdDecodeError::Length)?);
        debug_assert!(!ulid.is_nil());
        Ok(Self(ulid))
    }

    pub fn from_bytes(b: [u8; Self::SIZE_IN_BYTES]) -> Self {
        Self(Ulid::from_bytes(b))
    }

    pub fn to_bytes(&self) -> [u8; Self::SIZE_IN_BYTES] {
        self.0.to_bytes()
    }
}

impl Default for InvocationUuid {
    fn default() -> Self {
        Self::new()
    }
}

impl TimestampAwareId for InvocationUuid {
    fn timestamp(&self) -> MillisSinceEpoch {
        self.0.timestamp_ms().into()
    }
}

impl fmt::Display for InvocationUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let raw: u128 = self.0.into();
        let mut buf = String::with_capacity(base62_max_length_for_type::<u128>());
        base62_encode_fixed_width(raw, &mut buf);
        fmt::Display::fmt(&buf, f)
    }
}

impl FromStr for InvocationUuid {
    type Err = IdDecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut decoder = IdDecoder::new_ignore_prefix(
            crate::id_util::IdSchemeVersion::default(),
            IdResourceType::Invocation,
            input,
        )?;

        // ulid (u128)
        let raw_ulid: u128 = decoder.cursor.decode_next()?;
        Ok(Self::from(raw_ulid))
    }
}

impl From<InvocationUuid> for Bytes {
    fn from(value: InvocationUuid) -> Self {
        Bytes::copy_from_slice(&value.to_bytes())
    }
}

impl From<u128> for InvocationUuid {
    fn from(value: u128) -> Self {
        Self(Ulid::from(value))
    }
}

impl From<InvocationUuid> for opentelemetry::trace::TraceId {
    fn from(value: InvocationUuid) -> Self {
        Self::from_bytes(value.to_bytes())
    }
}

impl From<InvocationUuid> for opentelemetry::trace::SpanId {
    fn from(value: InvocationUuid) -> Self {
        let raw_be_bytes = value.to_bytes();
        let last8: [u8; 8] = std::convert::TryInto::try_into(&raw_be_bytes[8..16]).unwrap();
        Self::from_bytes(last8)
    }
}

/// Id of a keyed service instance.
///
/// Services are isolated by key. This means that there cannot be two concurrent
/// invocations for the same service instance (service name, key).
#[derive(
    Eq, Hash, PartialEq, PartialOrd, Ord, Clone, Debug, serde::Serialize, serde::Deserialize,
)]
pub struct ServiceId {
    // TODO rename this to KeyedServiceId. This type can be used only by keyed service types (virtual objects and workflows)
    /// Identifies the grpc service
    pub service_name: ByteString,
    /// Identifies the service instance for the given service name
    pub key: Bytes, // TODO change this to ByteString

    partition_key: PartitionKey,
}

impl ServiceId {
    pub fn new(service_name: impl Into<ByteString>, key: impl Into<Bytes>) -> Self {
        let key = key.into();
        let partition_key = partitioner::HashPartitioner::compute_partition_key(&key);
        Self::with_partition_key(partition_key, service_name, key)
    }

    // TODO remove this
    pub fn unkeyed(service_name: impl Into<ByteString>) -> Self {
        Self::new(
            service_name,
            Bytes::copy_from_slice(Uuid::now_v7().to_string().as_ref()),
        )
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
#[derive(
    Eq,
    Hash,
    PartialEq,
    Clone,
    Copy,
    Debug,
    PartialOrd,
    Ord,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
pub struct InvocationId {
    /// Partition key of the called service
    partition_key: PartitionKey,
    /// Uniquely identifies this invocation instance
    inner: InvocationUuid,
}

pub type EncodedInvocationId = [u8; InvocationId::SIZE_IN_BYTES];

impl InvocationId {
    // TODO deprecated, to remove
    pub fn new(partition_key: PartitionKey, invocation_uuid: impl Into<InvocationUuid>) -> Self {
        Self {
            partition_key,
            inner: invocation_uuid.into(),
        }
    }

    pub fn generate(invocation_target: &InvocationTarget) -> Self {
        InvocationId::generate_with_idempotency_key(invocation_target, None::<String>)
    }

    pub fn generate_with_idempotency_key<IKey: Hash>(
        invocation_target: &InvocationTarget,
        idempotency_key: Option<IKey>,
    ) -> Self {
        let partition_key = invocation_target
            .key()
            // If the invocation target is keyed, the PK is inferred from the key
            .map(|k| partitioner::HashPartitioner::compute_partition_key(&k))
            // If there is an idempotency key, the PK is inferred from the idempotency key
            .or_else(|| {
                idempotency_key.map(|k| partitioner::HashPartitioner::compute_partition_key(&k))
            })
            // In all the other cases, the PK is random
            .unwrap_or_else(|| rand::thread_rng().next_u64());

        InvocationId::from_parts(partition_key, InvocationUuid::new())
    }

    pub const fn from_parts(partition_key: PartitionKey, invocation_uuid: InvocationUuid) -> Self {
        Self {
            partition_key,
            inner: invocation_uuid,
        }
    }

    pub fn from_slice(b: &[u8]) -> Result<Self, IdDecodeError> {
        Self::try_from(b)
    }

    pub fn invocation_uuid(&self) -> InvocationUuid {
        self.inner
    }

    pub fn to_bytes(&self) -> EncodedInvocationId {
        encode_invocation_id(&self.partition_key, &self.inner)
    }
}

impl From<InvocationId> for Bytes {
    fn from(value: InvocationId) -> Self {
        Bytes::copy_from_slice(&value.to_bytes())
    }
}

impl TimestampAwareId for InvocationId {
    fn timestamp(&self) -> MillisSinceEpoch {
        self.inner.timestamp()
    }
}

impl ResourceId for InvocationId {
    const SIZE_IN_BYTES: usize = size_of::<PartitionKey>() + InvocationUuid::SIZE_IN_BYTES;
    const RESOURCE_TYPE: IdResourceType = IdResourceType::Invocation;
    const STRING_CAPACITY_HINT: usize =
        base62_max_length_for_type::<PartitionKey>() + base62_max_length_for_type::<u128>();

    fn push_contents_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
        encoder.encode_fixed_width(self.partition_key);
        let ulid_raw: u128 = self.inner.0.into();
        encoder.encode_fixed_width(ulid_raw);
    }
}

impl TryFrom<&[u8]> for InvocationId {
    type Error = IdDecodeError;

    fn try_from(encoded_id: &[u8]) -> Result<Self, Self::Error> {
        if encoded_id.len() < size_of::<EncodedInvocationId>() {
            return Err(IdDecodeError::Length);
        }
        let buf: [u8; InvocationId::SIZE_IN_BYTES] =
            encoded_id.try_into().map_err(|_| IdDecodeError::Length)?;
        Ok(buf.into())
    }
}

impl From<EncodedInvocationId> for InvocationId {
    fn from(encoded_id: EncodedInvocationId) -> Self {
        // This optimizes nicely by the compiler. We unwrap because array length is guaranteed to
        // fit both components according to EncodedInvocatioId type definition.
        let partition_key_bytes = encoded_id[..size_of::<PartitionKey>()].try_into().unwrap();
        let partition_key = PartitionKey::from_be_bytes(partition_key_bytes);

        let offset = size_of::<PartitionKey>();
        let inner_id_bytes = encoded_id[offset..offset + InvocationUuid::SIZE_IN_BYTES]
            .try_into()
            .unwrap();
        let inner = InvocationUuid::from_bytes(inner_id_bytes);

        Self {
            partition_key,
            inner,
        }
    }
}

impl WithPartitionKey for InvocationId {
    fn partition_key(&self) -> PartitionKey {
        self.partition_key
    }
}

impl fmt::Display for InvocationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // encode the id such that it is possible to do a string prefix search for a
        // partition key using the first 17 characters.
        let mut encoder = IdEncoder::<Self>::new();
        self.push_contents_to_encoder(&mut encoder);
        fmt::Display::fmt(&encoder.finalize(), f)
    }
}

impl FromStr for InvocationId {
    type Err = IdDecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut decoder = IdDecoder::new(input)?;
        // Ensure we are decoding the right type
        if decoder.resource_type != Self::RESOURCE_TYPE {
            return Err(IdDecodeError::TypeMismatch);
        }

        // partition key (u64)
        let partition_key: PartitionKey = decoder.cursor.decode_next()?;

        // ulid (u128)
        let raw_ulid: u128 = decoder.cursor.decode_next()?;
        let inner = InvocationUuid::from(raw_ulid);
        Ok(Self {
            partition_key,
            inner,
        })
    }
}

/// Id of a single service invocation.
///
/// A service invocation id is composed of a [`ServiceId`] and an [`InvocationUuid`]
/// that makes the id unique.
#[derive(Eq, Hash, PartialEq, Clone, Debug, serde::Serialize, serde::Deserialize)]
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
        invocation_uuid: impl Into<InvocationUuid>,
    ) -> Self {
        Self {
            service_id: ServiceId::new(service_name, key),
            invocation_uuid: invocation_uuid.into(),
        }
    }

    pub fn generate(service_id: ServiceId) -> Self {
        Self {
            service_id,
            invocation_uuid: InvocationUuid::new(),
        }
    }

    pub fn combine(service_id: ServiceId, invocation_id: InvocationId) -> Self {
        debug_assert_eq!(
            service_id.partition_key, invocation_id.partition_key,
            "Cannot combine ServiceId and InvocationId with different partition keys."
        );
        Self {
            service_id,
            invocation_uuid: invocation_id.invocation_uuid(),
        }
    }

    pub fn to_invocation_id_bytes(&self) -> EncodedInvocationId {
        InvocationId {
            partition_key: self.service_id.partition_key,
            inner: self.invocation_uuid,
        }
        .to_bytes()
    }
}

impl WithPartitionKey for FullInvocationId {
    fn partition_key(&self) -> PartitionKey {
        self.service_id.partition_key()
    }
}

impl fmt::Display for FullInvocationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Passthrough to InvocationId's Display
        fmt::Display::fmt(
            &InvocationId::new(self.service_id.partition_key, self.invocation_uuid),
            f,
        )
    }
}

impl From<FullInvocationId> for InvocationId {
    fn from(value: FullInvocationId) -> Self {
        InvocationId::from(&value)
    }
}

impl From<&FullInvocationId> for InvocationId {
    fn from(value: &FullInvocationId) -> Self {
        Self {
            partition_key: value.partition_key(),
            inner: value.invocation_uuid,
        }
    }
}

impl From<FullInvocationId> for EncodedInvocationId {
    fn from(value: FullInvocationId) -> Self {
        value.to_invocation_id_bytes()
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct IdempotencyId {
    /// Identifies the invoked component
    pub component_name: ByteString,
    /// Component key, if any
    pub component_key: Option<Bytes>,
    /// Identifies the invoked component handler
    pub component_handler: ByteString,
    /// The user supplied idempotency_key
    pub idempotency_key: ByteString,

    partition_key: PartitionKey,
}

impl IdempotencyId {
    pub fn new(
        component_name: ByteString,
        component_key: Option<Bytes>,
        component_handler: ByteString,
        idempotency_key: ByteString,
    ) -> Self {
        // The ownership model for idempotent invocations is the following:
        //
        // * For components without key, the partition key is the hash(idempotency key).
        //   This makes sure that for a given idempotency key and its scope, we always land in the same partition.
        // * For components with key, the partition key is the hash(component key), this due to the virtual object locking requirement.
        let partition_key = component_key
            .as_ref()
            .map(|k| partitioner::HashPartitioner::compute_partition_key(&k))
            .unwrap_or_else(|| {
                partitioner::HashPartitioner::compute_partition_key(&idempotency_key)
            });

        Self {
            component_name,
            component_key,
            component_handler,
            idempotency_key,
            partition_key,
        }
    }

    // TODO remove this
    pub fn combine(
        service_id: ServiceId,
        handler_name: ByteString,
        idempotency_key: ByteString,
    ) -> Self {
        IdempotencyId::new(
            service_id.service_name,
            // The service_id.key will always be the idempotency key now for regular services,
            // until we get rid of that field with https://github.com/restatedev/restate/issues/1329
            Some(service_id.key),
            handler_name,
            idempotency_key,
        )
    }

    pub fn new_combine(
        invocation_id: InvocationId,
        invocation_target: &InvocationTarget,
        idempotency_key: ByteString,
    ) -> Self {
        IdempotencyId {
            component_name: invocation_target.handler_name().clone(),
            component_key: invocation_target.key().map(|bs| bs.as_bytes().clone()),
            component_handler: invocation_target.handler_name().clone(),
            idempotency_key,
            partition_key: invocation_id.partition_key(),
        }
    }
}

impl WithPartitionKey for IdempotencyId {
    fn partition_key(&self) -> PartitionKey {
        self.partition_key
    }
}

/// Incremental id defining the service revision.
pub type ComponentRevision = u32;

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
    let mut buf = EncodedInvocationId::default();
    buf[..size_of::<PartitionKey>()].copy_from_slice(&partition_key.to_be_bytes());
    buf[size_of::<PartitionKey>()..].copy_from_slice(&invocation_uuid.to_bytes());
    buf
}

#[derive(Debug, Clone, serde_with::SerializeDisplay, serde_with::DeserializeFromStr)]
pub struct LambdaARN {
    partition: ByteString,
    region: ByteString,
    account_id: ByteString,
    name: ByteString,
    version: ByteString,
}

impl LambdaARN {
    pub fn region(&self) -> &str {
        &self.region
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for LambdaARN {
    fn schema_name() -> String {
        "LambdaARN".into()
    }

    fn json_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            format: Some("arn".to_string()),
            ..Default::default()
        }
        .into()
    }
}

impl fmt::Display for LambdaARN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let LambdaARN {
            partition,
            region,
            account_id,
            name,
            version,
        } = self;
        write!(
            f,
            "arn:{partition}:lambda:{region}:{account_id}:function:{name}:{version}"
        )
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum InvalidLambdaARN {
    #[error("A qualified ARN must have 8 components delimited by `:`")]
    InvalidFormat,
    #[error("A qualified ARN needs a version or alias suffix. If you want to use the unpublished version, provide $LATEST and make sure your shell doesn't treat it as a variable")]
    MissingVersionSuffix,
    #[error("First component of the ARN must be `arn`")]
    InvalidPrefix,
    #[error("ARN must refer to a `function` resource")]
    InvalidResourceType,
    #[error(
        "Partition, service, region, account ID, function name and version must all be non-empty"
    )]
    InvalidComponent,
    #[error("ARN must be for the lambda service")]
    InvalidService,
    #[error("Could not create valid URI for this ARN; likely malformed")]
    InvalidURI,
}

impl FromStr for LambdaARN {
    type Err = InvalidLambdaARN;

    fn from_str(arn: &str) -> Result<Self, Self::Err> {
        // allocate once
        let arn = ByteString::from(arn);
        let mut split = arn.splitn(8, ':');
        let invalid_format = || InvalidLambdaARN::InvalidFormat;
        let prefix = split.next().ok_or_else(invalid_format)?;
        let partition = split.next().ok_or_else(invalid_format)?;
        let service = split.next().ok_or_else(invalid_format)?;
        let region = split.next().ok_or_else(invalid_format)?;
        let account_id = split.next().ok_or_else(invalid_format)?;
        let resource_type = split.next().ok_or_else(invalid_format)?;
        let name = split.next().ok_or_else(invalid_format)?;
        let version = split.next().ok_or(InvalidLambdaARN::MissingVersionSuffix)?;

        if prefix != "arn" {
            return Err(InvalidLambdaARN::InvalidPrefix);
        }
        if resource_type != "function" {
            return Err(InvalidLambdaARN::InvalidResourceType);
        }
        if service != "lambda" {
            return Err(InvalidLambdaARN::InvalidService);
        }
        if partition.is_empty() || region.is_empty() || account_id.is_empty() || name.is_empty() {
            return Err(InvalidLambdaARN::InvalidComponent);
        }

        if version.is_empty() {
            // special case this common mistake
            return Err(InvalidLambdaARN::MissingVersionSuffix);
        }
        let lambda = Self {
            partition: arn.slice_ref(partition),
            region: arn.slice_ref(region),
            account_id: arn.slice_ref(account_id),
            name: arn.slice_ref(name),
            version: arn.slice_ref(version),
        };

        Ok(lambda)
    }
}

#[cfg(any(test, feature = "test-util"))]
mod mocks {
    use super::*;

    use rand::distributions::{Alphanumeric, DistString};
    use rand::Rng;

    impl InvocationUuid {
        /// Craft an invocation id from raw parts. Should be used only in tests.
        pub fn from_timestamp(timestamp_ms: u64) -> Self {
            use std::time::{Duration, SystemTime};

            Self(Ulid::from_datetime(
                SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp_ms),
            ))
        }

        /// Craft an invocation id from raw parts. Should be used only in tests.
        pub fn as_raw_parts(&self) -> (u64, u128) {
            (self.0.timestamp_ms(), self.0.random())
        }

        /// Increment the random part of the id, useful for testing purposes
        pub fn increment_random(mut self) -> Self {
            // this is called from tests, it's the caller responsibility to check if
            // we are not overflowing the random part;
            self.0 = self.0.increment().expect("ulid overflow");
            self
        }

        /// Increment the random part of the id, useful for testing purposes
        pub fn increment_timestamp(self) -> Self {
            let (ts, random) = self.as_raw_parts();
            Self::from_parts(ts + 1, random)
        }

        /// Craft an invocation id from raw parts. Should be used only in tests.
        pub const fn from_parts(timestamp_ms: u64, random: u128) -> Self {
            Self(Ulid::from_parts(timestamp_ms, random))
        }
    }

    impl InvocationId {
        pub fn mock_random() -> Self {
            Self::new(
                rand::thread_rng().sample::<PartitionKey, _>(rand::distributions::Standard),
                InvocationUuid::new(),
            )
        }
    }

    impl ServiceId {
        pub fn mock_random() -> Self {
            Self::new(
                Alphanumeric.sample_string(&mut rand::thread_rng(), 8),
                Bytes::from(
                    Alphanumeric
                        .sample_string(&mut rand::thread_rng(), 16)
                        .into_bytes(),
                ),
            )
        }
    }

    impl FullInvocationId {
        pub fn mock_random() -> Self {
            Self::generate(ServiceId::mock_random())
        }
    }

    impl IdempotencyId {
        pub const fn unkeyed(
            partition_key: PartitionKey,
            component_name: &'static str,
            component_handler: &'static str,
            idempotency_key: &'static str,
        ) -> Self {
            Self {
                component_name: ByteString::from_static(component_name),
                component_key: None,
                component_handler: ByteString::from_static(component_handler),
                idempotency_key: ByteString::from_static(idempotency_key),
                partition_key,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_invocation_id() {
        let expected = InvocationId::new(92, InvocationUuid::new());
        assert_eq!(
            expected,
            InvocationId::from_slice(&expected.to_bytes()).unwrap()
        )
    }

    #[test]
    fn invocation_codec_capacity() {
        assert_eq!(38, IdEncoder::<InvocationId>::estimate_buf_capacity())
    }

    #[test]
    fn roundtrip_invocation_id_str() {
        // torture test (poor's man property check test)
        for _ in 0..100000 {
            let expected = InvocationId::mock_random();
            let serialized = expected.to_string();
            assert_eq!(38, serialized.len(), "{} => {:?}", serialized, expected);
            let parsed = InvocationId::from_str(&serialized).unwrap();
            assert_eq!(expected, parsed, "serialized: {}", serialized);
        }
    }

    #[test]
    fn bad_invocation_id_str() {
        let bad_strs = [
            ("", IdDecodeError::Length),
            (
                "mxvgUOrwIb8cYrGPHkAAKSKY3O!6IEy_g",
                IdDecodeError::UnrecognizedType("mxvgUOrwIb8cYrGPHkAAKSKY3O!6IEy".to_string()),
            ),
            ("mxvgUOrwIb8", IdDecodeError::Format),
            (
                "inv_ub23411ba", // wrong version
                IdDecodeError::Version,
            ),
            ("inv_1b234d1ba", IdDecodeError::Length),
        ];

        for (bad, error) in bad_strs {
            assert_eq!(
                error,
                InvocationId::from_str(bad).unwrap_err(),
                "invocation id: '{}' fails with {}",
                bad,
                error
            )
        }
    }

    #[test]
    fn roundtrip_lambda_arn() {
        let good = "arn:aws:lambda:eu-central-1:1234567890:function:e2e-node-services:version";

        let expected = LambdaARN::from_str(good).unwrap();
        let parsed = expected.to_string();

        assert_eq!(good, parsed)
    }

    #[test]
    fn missing_version_lambda_arn() {
        for bad in [
            "arn:aws:lambda:eu-central-1:1234567890:function:e2e-node-services",
            "arn:aws:lambda:eu-central-1:1234567890:function:e2e-node-services:",
        ] {
            assert_eq!(
                LambdaARN::from_str(bad).unwrap_err(),
                InvalidLambdaARN::MissingVersionSuffix
            );
        }
    }
}
