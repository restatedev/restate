// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Restate uses many identifiers to uniquely identify its services and entities.

use bytes::Bytes;
use bytestring::ByteString;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::fmt;
use std::hash::Hash;
use std::mem::size_of;
use std::str::FromStr;
use ulid::Ulid;

use crate::base62_util::base62_encode_fixed_width;
use crate::base62_util::base62_max_length_for_type;
use crate::errors::IdDecodeError;
use crate::id_util::IdDecoder;
use crate::id_util::IdEncoder;
use crate::id_util::IdResourceType;
use crate::invocation::{InvocationTarget, InvocationTargetType, WorkflowHandlerType};
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
#[display("e{}", _0)]
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

impl From<crate::protobuf::common::LeaderEpoch> for LeaderEpoch {
    fn from(epoch: crate::protobuf::common::LeaderEpoch) -> Self {
        Self::from(epoch.value)
    }
}

impl From<LeaderEpoch> for crate::protobuf::common::LeaderEpoch {
    fn from(epoch: LeaderEpoch) -> Self {
        let value: u64 = epoch.into();
        Self { value }
    }
}

/// Identifying the partition
#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
    derive_more::Add,
    derive_more::Display,
    derive_more::FromStr,
    serde::Serialize,
    serde::Deserialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct PartitionId(u16);

impl From<PartitionId> for u32 {
    fn from(value: PartitionId) -> Self {
        u32::from(value.0)
    }
}

impl PartitionId {
    /// It's your responsibility to ensure the value is within the valid range.
    pub const fn new_unchecked(v: u16) -> Self {
        Self(v)
    }

    pub const MIN: Self = Self(u16::MIN);
    // 65535 partitions.
    pub const MAX: Self = Self(u16::MAX);

    #[inline]
    pub fn next(self) -> Self {
        Self(std::cmp::min(*Self::MAX, self.0.saturating_add(1)))
    }
}

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

/// Returns the partition key computed from either the service_key, or idempotency_key, if possible
fn deterministic_partition_key(
    service_key: Option<&str>,
    idempotency_key: Option<&str>,
) -> Option<PartitionKey> {
    service_key
        .map(partitioner::HashPartitioner::compute_partition_key)
        .or_else(|| idempotency_key.map(partitioner::HashPartitioner::compute_partition_key))
}

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
pub struct InvocationUuid(u128);

impl InvocationUuid {
    pub const SIZE_IN_BYTES: usize = size_of::<u128>();

    pub fn from_slice(b: &[u8]) -> Result<Self, IdDecodeError> {
        Ok(Self::from_u128(u128::from_be_bytes(
            b.try_into().map_err(|_| IdDecodeError::Length)?,
        )))
    }

    pub const fn from_u128(id: u128) -> Self {
        debug_assert!(id != 0);
        Self(id)
    }

    pub const fn from_bytes(b: [u8; Self::SIZE_IN_BYTES]) -> Self {
        Self::from_u128(u128::from_be_bytes(b))
    }

    pub fn to_bytes(&self) -> [u8; Self::SIZE_IN_BYTES] {
        self.0.to_be_bytes()
    }

    pub fn generate(invocation_target: &InvocationTarget, idempotency_key: Option<&str>) -> Self {
        const HASH_SEPARATOR: u8 = 0x2c;

        // --- Rules for deterministic ID
        // * If the target IS a workflow run, use workflow name + key
        // * If the target IS an idempotent request, use the idempotency scope + key
        // * If the target IS NEITHER an idempotent request or a workflow run, then just generate a random ulid

        let id = match (idempotency_key, invocation_target.invocation_target_ty()) {
            (_, InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)) => {
                // Workflow run
                let mut hasher = Sha256::new();
                hasher.update(b"wf");
                hasher.update([HASH_SEPARATOR]);
                hasher.update(invocation_target.service_name());
                hasher.update([HASH_SEPARATOR]);
                hasher.update(
                    invocation_target
                        .key()
                        .expect("Workflow targets MUST contain a key"),
                );
                let result = hasher.finalize();
                let (int_bytes, _) = result.split_at(size_of::<u128>());
                u128::from_be_bytes(
                    int_bytes
                        .try_into()
                        .expect("Conversion after split can't fail"),
                )
            }
            (Some(idempotency_key), _) => {
                // Invocations with Idempotency key
                let mut hasher = Sha256::new();
                hasher.update(b"ik");
                hasher.update([HASH_SEPARATOR]);
                hasher.update(invocation_target.service_name());
                if let Some(key) = invocation_target.key() {
                    hasher.update([HASH_SEPARATOR]);
                    hasher.update(key);
                }
                hasher.update([HASH_SEPARATOR]);
                hasher.update(invocation_target.handler_name());
                hasher.update([HASH_SEPARATOR]);
                hasher.update(idempotency_key);
                let result = hasher.finalize();
                let (int_bytes, _) = result.split_at(size_of::<u128>());
                u128::from_be_bytes(
                    int_bytes
                        .try_into()
                        .expect("Conversion after split can't fail"),
                )
            }
            (_, _) => {
                // Regular invocation
                Ulid::new().into()
            }
        };

        debug_assert!(id != 0);
        InvocationUuid(id)
    }
}

impl fmt::Display for InvocationUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let raw: u128 = self.0;
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
        Self(value)
    }
}

impl From<InvocationUuid> for u128 {
    fn from(value: InvocationUuid) -> Self {
        value.0
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
    pub key: ByteString,

    partition_key: PartitionKey,
}

impl ServiceId {
    pub fn new(service_name: impl Into<ByteString>, key: impl Into<ByteString>) -> Self {
        let key = key.into();
        let partition_key = partitioner::HashPartitioner::compute_partition_key(&key);
        Self::with_partition_key(partition_key, service_name, key)
    }

    /// # Important
    /// The `partition_key` must be hash of the `key` computed via [`HashPartitioner`].
    pub fn with_partition_key(
        partition_key: PartitionKey,
        service_name: impl Into<ByteString>,
        key: impl Into<ByteString>,
    ) -> Self {
        Self::from_parts(partition_key, service_name.into(), key.into())
    }

    /// # Important
    /// The `partition_key` must be hash of the `key` computed via [`HashPartitioner`].
    pub const fn from_parts(
        partition_key: PartitionKey,
        service_name: ByteString,
        key: ByteString,
    ) -> Self {
        Self {
            service_name,
            key,
            partition_key,
        }
    }
}

impl WithPartitionKey for ServiceId {
    fn partition_key(&self) -> PartitionKey {
        self.partition_key
    }
}

impl fmt::Display for ServiceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.service_name, self.key)
    }
}

/// InvocationId is a unique identifier of the invocation,
/// including enough routing information for the network service
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

pub trait WithInvocationId {
    /// Returns the invocation id
    fn invocation_id(&self) -> InvocationId;
}

pub type EncodedInvocationId = [u8; InvocationId::SIZE_IN_BYTES];

impl InvocationId {
    pub fn generate(invocation_target: &InvocationTarget, idempotency_key: Option<&str>) -> Self {
        // --- Partition key generation
        let partition_key =
                // Either try to generate the deterministic partition key, if possible
                deterministic_partition_key(
                    invocation_target.key().map(|bs| bs.as_ref()),
                    idempotency_key,
                )
                // If no deterministic partition key can be generated, just pick a random number
                .unwrap_or_else(|| rand::thread_rng().next_u64());

        // --- Invocation UUID generation
        InvocationId::from_parts(
            partition_key,
            InvocationUuid::generate(invocation_target, idempotency_key),
        )
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

impl ResourceId for InvocationId {
    const SIZE_IN_BYTES: usize = size_of::<PartitionKey>() + InvocationUuid::SIZE_IN_BYTES;
    const RESOURCE_TYPE: IdResourceType = IdResourceType::Invocation;
    const STRING_CAPACITY_HINT: usize =
        base62_max_length_for_type::<PartitionKey>() + base62_max_length_for_type::<u128>();

    fn push_contents_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
        encoder.encode_fixed_width(self.partition_key);
        let uuid_raw: u128 = self.inner.0;
        encoder.encode_fixed_width(uuid_raw);
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
        // fit both services according to EncodedInvocatioId type definition.
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

impl<T: WithInvocationId> WithPartitionKey for T {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_id().partition_key
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

#[derive(Eq, Hash, PartialEq, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct IdempotencyId {
    /// Identifies the invoked service
    pub service_name: ByteString,
    /// Service key, if any
    pub service_key: Option<ByteString>,
    /// Identifies the invoked service handler
    pub service_handler: ByteString,
    /// The user supplied idempotency_key
    pub idempotency_key: ByteString,

    partition_key: PartitionKey,
}

impl IdempotencyId {
    pub fn new(
        service_name: ByteString,
        service_key: Option<ByteString>,
        service_handler: ByteString,
        idempotency_key: ByteString,
    ) -> Self {
        // The ownership model for idempotent invocations is the following:
        //
        // * For services without key, the partition key is the hash(idempotency key).
        //   This makes sure that for a given idempotency key and its scope, we always land in the same partition.
        // * For services with key, the partition key is the hash(service key), this due to the virtual object locking requirement.
        let partition_key = deterministic_partition_key(
            service_key.as_ref().map(|bs| bs.as_ref()),
            Some(&idempotency_key),
        )
        .expect("A deterministic partition key can always be generated for idempotency id");

        Self {
            service_name,
            service_key,
            service_handler,
            idempotency_key,
            partition_key,
        }
    }

    pub fn combine(
        invocation_id: InvocationId,
        invocation_target: &InvocationTarget,
        idempotency_key: ByteString,
    ) -> Self {
        IdempotencyId {
            service_name: invocation_target.service_name().clone(),
            service_key: invocation_target.key().cloned(),
            service_handler: invocation_target.handler_name().clone(),
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
pub type ServiceRevision = u32;

pub mod partitioner {
    use super::PartitionKey;

    use std::hash::{Hash, Hasher};

    /// Computes the [`PartitionKey`] based on xxh3 hashing.
    pub struct HashPartitioner;

    impl HashPartitioner {
        pub fn compute_partition_key(value: impl Hash) -> PartitionKey {
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

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct JournalEntryId {
    invocation_id: InvocationId,
    journal_index: EntryIndex,
}

impl JournalEntryId {
    pub const fn from_parts(invocation_id: InvocationId, journal_index: EntryIndex) -> Self {
        Self {
            invocation_id,
            journal_index,
        }
    }

    pub fn journal_index(&self) -> EntryIndex {
        self.journal_index
    }
}

impl From<(InvocationId, EntryIndex)> for JournalEntryId {
    fn from(value: (InvocationId, EntryIndex)) -> Self {
        Self::from_parts(value.0, value.1)
    }
}

impl WithInvocationId for JournalEntryId {
    fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }
}

#[derive(Debug, Clone, PartialEq, serde_with::SerializeDisplay, serde_with::DeserializeFromStr)]
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

#[derive(
    Debug,
    PartialOrd,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Copy,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
pub struct PartitionProcessorRpcRequestId(Ulid);

impl PartitionProcessorRpcRequestId {
    pub fn from_slice(b: &[u8]) -> Result<Self, IdDecodeError> {
        let ulid = Ulid::from_bytes(b.try_into().map_err(|_| IdDecodeError::Length)?);
        debug_assert!(!ulid.is_nil());
        Ok(Self(ulid))
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let ulid = Ulid::from_bytes(bytes);
        debug_assert!(!ulid.is_nil());
        Self(ulid)
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        self.0.to_bytes()
    }
}

impl Default for PartitionProcessorRpcRequestId {
    fn default() -> Self {
        Self(Ulid::new())
    }
}

impl fmt::Display for PartitionProcessorRpcRequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for PartitionProcessorRpcRequestId {
    type Err = ulid::DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Ulid::from_string(s)?))
    }
}

/// Unique Id of a partition snapshot.
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
pub struct SnapshotId(pub(crate) Ulid);

impl SnapshotId {
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    pub const fn from_parts(timestamp_ms: u64, random: u128) -> Self {
        Self(Ulid::from_parts(timestamp_ms, random))
    }
}

impl Default for SnapshotId {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceId for SnapshotId {
    const SIZE_IN_BYTES: usize = size_of::<Ulid>();
    const RESOURCE_TYPE: IdResourceType = IdResourceType::Snapshot;
    const STRING_CAPACITY_HINT: usize = base62_max_length_for_type::<u128>();

    fn push_contents_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
        let raw: u128 = self.0.into();
        encoder.encode_fixed_width(raw);
    }
}

impl TimestampAwareId for SnapshotId {
    fn timestamp(&self) -> MillisSinceEpoch {
        self.0.timestamp_ms().into()
    }
}

impl FromStr for SnapshotId {
    type Err = IdDecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut decoder = IdDecoder::new(input)?;
        // Ensure we are decoding the correct resource type
        if decoder.resource_type != Self::RESOURCE_TYPE {
            return Err(IdDecodeError::TypeMismatch);
        }

        // ulid (u128)
        let raw_ulid: u128 = decoder.cursor.decode_next()?;
        Ok(Self::from(raw_ulid))
    }
}

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut encoder = IdEncoder::<Self>::new();
        self.push_contents_to_encoder(&mut encoder);
        fmt::Display::fmt(&encoder.finalize(), f)
    }
}

impl From<u128> for SnapshotId {
    fn from(value: u128) -> Self {
        Self(Ulid::from(value))
    }
}

#[cfg(any(test, feature = "test-util"))]
mod mocks {
    use super::*;

    use rand::distributions::{Alphanumeric, DistString};
    use rand::Rng;

    impl InvocationUuid {
        pub fn mock_generate(invocation_target: &InvocationTarget) -> Self {
            InvocationUuid::generate(invocation_target, None)
        }

        pub fn mock_random() -> Self {
            InvocationUuid::mock_generate(&InvocationTarget::mock_service())
        }
    }

    impl InvocationId {
        pub fn mock_generate(invocation_target: &InvocationTarget) -> Self {
            InvocationId::generate(invocation_target, None)
        }

        pub fn mock_random() -> Self {
            Self::from_parts(
                rand::thread_rng().sample::<PartitionKey, _>(rand::distributions::Standard),
                InvocationUuid::mock_random(),
            )
        }
    }

    impl ServiceId {
        pub fn mock_random() -> Self {
            Self::new(
                Alphanumeric.sample_string(&mut rand::thread_rng(), 8),
                Alphanumeric.sample_string(&mut rand::thread_rng(), 16),
            )
        }

        pub const fn from_static(
            partition_key: PartitionKey,
            service_name: &'static str,
            service_key: &'static str,
        ) -> Self {
            Self {
                service_name: ByteString::from_static(service_name),
                key: ByteString::from_static(service_key),
                partition_key,
            }
        }
    }

    impl IdempotencyId {
        pub const fn unkeyed(
            partition_key: PartitionKey,
            service_name: &'static str,
            service_handler: &'static str,
            idempotency_key: &'static str,
        ) -> Self {
            Self {
                service_name: ByteString::from_static(service_name),
                service_key: None,
                service_handler: ByteString::from_static(service_handler),
                idempotency_key: ByteString::from_static(idempotency_key),
                partition_key,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::invocation::VirtualObjectHandlerType;
    use rand::distributions::{Alphanumeric, DistString};

    #[test]
    fn service_id_and_invocation_id_partition_key_should_match() {
        let invocation_target = InvocationTarget::virtual_object(
            "MyService",
            "MyKey",
            "MyMethod",
            VirtualObjectHandlerType::Exclusive,
        );
        let invocation_id = InvocationId::mock_generate(&invocation_target);

        assert_eq!(
            invocation_id.partition_key(),
            invocation_target
                .as_keyed_service_id()
                .unwrap()
                .partition_key()
        );
    }

    #[test]
    fn roundtrip_invocation_id() {
        let target = InvocationTarget::mock_service();
        let expected = InvocationId::from_parts(92, InvocationUuid::mock_generate(&target));
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

    #[test]
    fn deterministic_invocation_id_for_idempotent_request() {
        let invocation_target = InvocationTarget::mock_service();
        let idempotent_key = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);

        assert_eq!(
            InvocationId::generate(&invocation_target, Some(&idempotent_key)),
            InvocationId::generate(&invocation_target, Some(&idempotent_key))
        );
    }

    #[test]
    fn deterministic_invocation_id_for_workflow_request() {
        let invocation_target = InvocationTarget::mock_workflow();

        assert_eq!(
            InvocationId::mock_generate(&invocation_target),
            InvocationId::mock_generate(&invocation_target)
        );
    }
}
