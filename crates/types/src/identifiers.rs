// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Restate uses many identifiers to uniquely identify its services and entities.

use std::fmt::{self, Display, Formatter};
use std::hash::Hash;
use std::mem::size_of;
use std::str::FromStr;
use std::sync::Arc;

use base64::Engine;
use bytes::Bytes;
use bytestring::ByteString;
use generic_array::ArrayLength;
use rand::RngCore;
use sha2::{Digest, Sha256};
use ulid::Ulid;

use restate_encoding::{BilrostNewType, NetSerde};

use crate::base62_util::{base62_encode_fixed_width_u128, base62_max_length_for_type};
use crate::errors::IdDecodeError;
use crate::id_util::IdResourceType;
use crate::id_util::{IdDecoder, IdEncoder};
use crate::invocation::{InvocationTarget, InvocationTargetType, WorkflowHandlerType};
use crate::journal_v2::SignalId;
use crate::time::MillisSinceEpoch;

/// Identifying the leader epoch of a partition processor
#[derive(
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
    derive_more::Debug,
    serde::Serialize,
    serde::Deserialize,
    BilrostNewType,
    NetSerde,
)]
#[display("e{}", _0)]
#[debug("e{}", _0)]
pub struct LeaderEpoch(u64);
impl LeaderEpoch {
    pub const INVALID: Self = Self(0);
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
    derive_more::Debug,
    derive_more::FromStr,
    serde::Serialize,
    serde::Deserialize,
    BilrostNewType,
    NetSerde,
)]
#[repr(transparent)]
#[serde(transparent)]
#[debug("{}", _0)]
pub struct PartitionId(u16);

impl From<PartitionId> for u32 {
    fn from(value: PartitionId) -> Self {
        u32::from(value.0)
    }
}

impl From<PartitionId> for u64 {
    fn from(value: PartitionId) -> Self {
        u64::from(value.0)
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
    const RAW_BYTES_LEN: usize;
    const RESOURCE_TYPE: IdResourceType;

    type StrEncodedLen: ArrayLength;

    /// The number of characters/bytes needed to string-serialize this resource identifier
    fn str_encoded_len() -> usize {
        <Self::StrEncodedLen as generic_array::typenum::Unsigned>::USIZE
    }

    /// Adds the various fields of this resource ID into the pre-initialized encoder
    fn push_to_encoder(&self, encoder: &mut IdEncoder<Self>);
}

/// Discriminator for invocation instances
#[derive(
    Eq,
    Hash,
    PartialEq,
    Clone,
    Copy,
    Debug,
    Default,
    Ord,
    PartialOrd,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
pub struct InvocationUuid(u128);

impl InvocationUuid {
    pub const RAW_BYTES_LEN: usize = size_of::<u128>();

    pub fn from_slice(b: &[u8]) -> Result<Self, IdDecodeError> {
        Ok(Self::from_u128(u128::from_be_bytes(
            b.try_into().map_err(|_| IdDecodeError::Length)?,
        )))
    }

    pub const fn from_u128(id: u128) -> Self {
        debug_assert!(id != 0);
        Self(id)
    }

    pub const fn from_bytes(b: [u8; Self::RAW_BYTES_LEN]) -> Self {
        Self::from_u128(u128::from_be_bytes(b))
    }

    pub fn to_bytes(&self) -> [u8; Self::RAW_BYTES_LEN] {
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

impl Display for InvocationUuid {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut buf = [b'0'; base62_max_length_for_type::<u128>()];
        let raw: u128 = self.0;
        let written = base62_encode_fixed_width_u128(raw, &mut buf);
        // SAFETY; the array was initialised with valid utf8 and encode_alternative_bytes only writes utf8
        f.write_str(unsafe { std::str::from_utf8_unchecked(&buf[0..written]) })
    }
}

impl FromStr for InvocationUuid {
    type Err = IdDecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut decoder = IdDecoder::new_ignore_prefix(
            crate::id_util::IdSchemeVersion::latest(),
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

impl Display for ServiceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
    Default,
)]
pub struct InvocationId {
    /// Partition key of the called service
    partition_key: PartitionKey,
    /// Uniquely identifies this invocation instance
    inner: InvocationUuid,
}

restate_encoding::bilrost_as_display_from_str!(InvocationId);

pub trait WithInvocationId {
    /// Returns the invocation id
    fn invocation_id(&self) -> InvocationId;
}

pub type EncodedInvocationId = [u8; InvocationId::RAW_BYTES_LEN];

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
                .unwrap_or_else(|| rand::rng().next_u64());

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
        let mut buf = EncodedInvocationId::default();
        self.encode_raw_bytes(&mut buf);
        buf
    }

    /// Returns the number of bytes written to the buffer
    ///
    /// The buffer must be at least `InvocationId::RAW_BYTES_LEN` bytes long.
    pub fn encode_raw_bytes(&self, buf: &mut [u8]) -> usize {
        let pk = self.partition_key.to_be_bytes();
        let uuid = self.inner.to_bytes();

        buf[..size_of::<PartitionKey>()].copy_from_slice(&pk);
        buf[size_of::<PartitionKey>()..].copy_from_slice(&uuid);
        pk.len() + uuid.len()
    }

    /// Generate random seed to feed RNG in SDKs.
    pub fn to_random_seed(&self) -> u64 {
        use std::hash::{DefaultHasher, Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.to_bytes().hash(&mut hasher);
        hasher.finish()
    }
}

impl From<InvocationId> for Bytes {
    fn from(value: InvocationId) -> Self {
        Bytes::copy_from_slice(&value.to_bytes())
    }
}

impl ResourceId for InvocationId {
    const RAW_BYTES_LEN: usize = size_of::<PartitionKey>() + InvocationUuid::RAW_BYTES_LEN;
    const RESOURCE_TYPE: IdResourceType = IdResourceType::Invocation;

    type StrEncodedLen = generic_array::ConstArrayLength<
        // prefix + separator + version + suffix
        {
            Self::RESOURCE_TYPE.as_str().len()
                // separator + version
                + 2
                + base62_max_length_for_type::<PartitionKey>()
                + base62_max_length_for_type::<u128>()
        },
    >;

    fn push_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
        encoder.push_u64(self.partition_key);
        encoder.push_u128(self.inner.0);
    }
}

impl TryFrom<&[u8]> for InvocationId {
    type Error = IdDecodeError;

    fn try_from(encoded_id: &[u8]) -> Result<Self, Self::Error> {
        if encoded_id.len() < size_of::<EncodedInvocationId>() {
            return Err(IdDecodeError::Length);
        }
        let buf: [u8; InvocationId::RAW_BYTES_LEN] =
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
        let inner_id_bytes = encoded_id[offset..offset + InvocationUuid::RAW_BYTES_LEN]
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

impl Display for InvocationId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // encode the id such that it is possible to do a string prefix search for a
        // partition key using the first 17 characters.
        let mut encoder = IdEncoder::new();
        self.push_to_encoder(&mut encoder);
        f.write_str(encoder.as_str())
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

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for InvocationId {
    fn schema_name() -> String {
        <String as schemars::JsonSchema>::schema_name()
    }

    fn json_schema(g: &mut schemars::SchemaGenerator) -> schemars::schema::Schema {
        <String as schemars::JsonSchema>::json_schema(g)
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

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug)]
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
    arn: Arc<str>,
    region: std::ops::Range<u32>,
}

impl LambdaARN {
    pub fn region(&self) -> &str {
        &self.arn[(self.region.start as usize)..(self.region.end as usize)]
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for LambdaARN {
    fn schema_name() -> String {
        "LambdaARN".into()
    }

    fn json_schema(_: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            format: Some("arn".to_string()),
            ..Default::default()
        }
        .into()
    }
}

impl Display for LambdaARN {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.arn.fmt(f)
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum InvalidLambdaARN {
    #[error("A qualified ARN must have 8 components delimited by `:`")]
    InvalidFormat,
    #[error(
        "A qualified ARN needs a version or alias suffix. If you want to use the unpublished version, provide $LATEST and make sure your shell doesn't treat it as a variable"
    )]
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

        // arn:<partition>:lambda:<region>:
        //                        ^       ^
        let region_start = 3 + 1 + (partition.len() as u32) + 1 + 6 + 1;
        let region_end = region_start + (region.len() as u32);
        let lambda = Self {
            arn: Arc::<str>::from(arn),
            region: region_start..region_end,
        };

        Ok(lambda)
    }
}

/// Generate an identifier backed by ULID.
///
/// This generates the Id struct and some associated methods: `new`, `from_parts`, `from_slice`, `from_bytes`, `to_bytes`,
/// plus implements `Default`, `Display`, `Debug`, `FromStr`, `JsonSchema` and `TimestampAwareId`.
///
/// To use:
///
/// ```ignore
/// ulid_backed_id!(MyResource);
/// ```
///
/// If the resource has an associated [`ResourceId`]:
///
/// ```ignore
/// ulid_backed_id!(MyResource @with_resource_id);
/// ```
///
/// The difference between the two will be the usage of ResourceId for serde and string representations.
macro_rules! ulid_backed_id {
    ($res_name:ident) => {
        ulid_backed_id!(@common $res_name);

        paste::paste! {
            impl fmt::Display for [< $res_name Id >] {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    fmt::Display::fmt(&self.0, f)
                }
            }

            impl FromStr for [< $res_name Id >] {
                type Err = ulid::DecodeError;

                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    Ok(Self(Ulid::from_string(s)?))
                }
            }
        }
    };
    ($res_name:ident @with_resource_id) => {
        ulid_backed_id!(@common $res_name);

        paste::paste! {
            impl ResourceId for [< $res_name Id >] {
                const RAW_BYTES_LEN: usize = size_of::<Ulid>();
                const RESOURCE_TYPE: IdResourceType = IdResourceType::$res_name;

                type StrEncodedLen = ::generic_array::ConstArrayLength<
                    // prefix + separator + version + suffix
                    { Self::RESOURCE_TYPE.as_str().len() + 2 + base62_max_length_for_type::<u128>() },
                >;

                fn push_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
                    let raw: u128 = self.0.into();
                    encoder.push_u128(raw);
                }
            }

            impl FromStr for [< $res_name Id >] {
                type Err = IdDecodeError;

                fn from_str(input: &str) -> Result<Self, Self::Err> {
                    let mut decoder = IdDecoder::new(input)?;
                    // Ensure we are decoding the correct resource type
                    if decoder.resource_type != Self::RESOURCE_TYPE {
                        return Err(IdDecodeError::TypeMismatch);
                    }

                    // ulid (u128)
                    let raw_ulid: u128 = decoder.cursor.decode_next()?;
                    if decoder.cursor.remaining() > 0 {
                        return Err(IdDecodeError::Length);
                    }

                    Ok(Self::from(raw_ulid))
                }
            }

            impl fmt::Display for [< $res_name Id >] {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    let mut encoder = IdEncoder::new();
                    self.push_to_encoder(&mut encoder);
                    f.write_str(encoder.as_str())
                }
            }
        }
    };
    (@common $res_name:ident) => {
        paste::paste! {
            #[derive(
                PartialEq,
                Eq,
                Clone,
                Copy,
                Hash,
                PartialOrd,
                Ord,
                serde_with::SerializeDisplay,
                serde_with::DeserializeFromStr,
                ::restate_encoding::BilrostAs
            )]
            #[bilrost_as([< $res_name IdMessage >])]
            pub struct [< $res_name Id >](pub(crate) Ulid);

            impl [< $res_name Id >] {
                pub fn new() -> Self {
                    Self(Ulid::new())
                }

                pub const fn from_parts(timestamp_ms: u64, random: u128) -> Self {
                    Self(Ulid::from_parts(timestamp_ms, random))
                }

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

            impl Default for [< $res_name Id >] {
                fn default() -> Self {
                    Self::new()
                }
            }

            impl TimestampAwareId for [< $res_name Id >] {
                fn timestamp(&self) -> MillisSinceEpoch {
                    self.0.timestamp_ms().into()
                }
            }

            impl fmt::Debug for [< $res_name Id >] {
                fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                    // use the same formatting for debug and display to show a consistent representation
                    fmt::Display::fmt(self, f)
                }
            }

            impl From<u128> for [< $res_name Id >] {
                fn from(value: u128) -> Self {
                    Self(Ulid::from(value))
                }
            }

            #[cfg(feature = "schemars")]
            impl schemars::JsonSchema for [< $res_name Id >] {
                fn schema_name() -> String {
                    <String as schemars::JsonSchema>::schema_name()
                }

                fn json_schema(g: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
                    <String as schemars::JsonSchema>::json_schema(g)
                }
            }

            #[derive(::bilrost::Message)]
            struct [< $res_name IdMessage >](::restate_encoding::U128);

            impl From<&[< $res_name Id >]> for [< $res_name IdMessage >] {
                fn from(value: &[< $res_name Id >]) -> Self {
                    Self(u128::from(value.0).into())
                }
            }

            impl From<[< $res_name IdMessage >]> for [< $res_name Id >] {
                fn from(value: [< $res_name IdMessage >]) -> Self {
                    Self(u128::from(value.0).into())
                }
            }
        }
    };
}

ulid_backed_id!(Deployment @with_resource_id);
ulid_backed_id!(Subscription @with_resource_id);
ulid_backed_id!(PartitionProcessorRpcRequest);
ulid_backed_id!(Snapshot @with_resource_id);

#[derive(
    Debug, Clone, PartialEq, Eq, serde_with::SerializeDisplay, serde_with::DeserializeFromStr,
)]
pub struct AwakeableIdentifier {
    invocation_id: InvocationId,
    entry_index: EntryIndex,
}

impl ResourceId for AwakeableIdentifier {
    const RAW_BYTES_LEN: usize = InvocationId::RAW_BYTES_LEN + size_of::<EntryIndex>();
    const RESOURCE_TYPE: IdResourceType = IdResourceType::Awakeable;

    type StrEncodedLen = ::generic_array::ConstArrayLength<
        // prefix + separator + version + suffix (38 chars)
        {
            Self::RESOURCE_TYPE.as_str().len()
                + 2
                + base64::encoded_len(
                    size_of::<EncodedInvocationId>() + size_of::<EntryIndex>(),
                    false,
                )
                .expect("awakeable id is far from usize limit")
        },
    >;

    /// We use a custom strategy for awakeable identifiers since they need to be encoded as base64
    /// for wider language support.
    fn push_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
        let mut input_buf = [0u8; Self::RAW_BYTES_LEN];
        let pos = self
            .invocation_id
            .encode_raw_bytes(&mut input_buf[..InvocationId::RAW_BYTES_LEN]);
        input_buf[pos..].copy_from_slice(&self.entry_index.to_be_bytes());

        let written = restate_base64_util::URL_SAFE
            .encode_slice(input_buf, encoder.remaining_mut())
            .expect("base64 encoding succeeds for system-generated ids");
        encoder.advance(written);
    }
}

impl AwakeableIdentifier {
    pub fn new(invocation_id: InvocationId, entry_index: EntryIndex) -> Self {
        Self {
            invocation_id,
            entry_index,
        }
    }

    pub fn into_inner(self) -> (InvocationId, EntryIndex) {
        (self.invocation_id, self.entry_index)
    }
}

impl FromStr for AwakeableIdentifier {
    type Err = IdDecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let decoder = IdDecoder::new(input)?;
        // Ensure we are decoding the right type
        if decoder.resource_type != Self::RESOURCE_TYPE {
            return Err(IdDecodeError::TypeMismatch);
        }
        let remaining = decoder.cursor.take_remaining()?;

        let buffer = restate_base64_util::URL_SAFE
            .decode(remaining)
            .map_err(|_| IdDecodeError::Codec)?;

        if buffer.len() != size_of::<EncodedInvocationId>() + size_of::<EntryIndex>() {
            return Err(IdDecodeError::Length);
        }

        let invocation_id: InvocationId =
            InvocationId::from_slice(&buffer[..size_of::<EncodedInvocationId>()])?;
        let entry_index = EntryIndex::from_be_bytes(
            buffer[size_of::<EncodedInvocationId>()..]
                .try_into()
                // Unwrap is safe because we check the size above.
                .unwrap(),
        );

        Ok(Self {
            invocation_id,
            entry_index,
        })
    }
}

impl Display for AwakeableIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // encode the id such that it is possible to do a string prefix search for a
        // partition key using the first 17 characters.
        let mut encoder = IdEncoder::new();
        self.push_to_encoder(&mut encoder);
        f.write_str(encoder.as_str())
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, serde_with::SerializeDisplay, serde_with::DeserializeFromStr,
)]
pub struct ExternalSignalIdentifier {
    invocation_id: InvocationId,
    signal_index: u32,
}

impl ResourceId for ExternalSignalIdentifier {
    const RAW_BYTES_LEN: usize = size_of::<EncodedInvocationId>() + size_of::<EntryIndex>();
    const RESOURCE_TYPE: IdResourceType = IdResourceType::Signal;

    type StrEncodedLen = ::generic_array::ConstArrayLength<
        // prefix + separator + version + suffix (38 chars)
        {
            Self::RESOURCE_TYPE.as_str().len()
                + 2
                + base64::encoded_len(
                    size_of::<EncodedInvocationId>() + size_of::<EntryIndex>(),
                    false,
                )
                .expect("awakeable id is far from usize limit")
        },
    >;

    /// We use a custom strategy for awakeable identifiers since they need to be encoded as base64
    /// for wider language support.
    fn push_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
        let mut input_buf = [0u8; Self::RAW_BYTES_LEN];
        let pos = self
            .invocation_id
            .encode_raw_bytes(&mut input_buf[..InvocationId::RAW_BYTES_LEN]);
        input_buf[pos..].copy_from_slice(&self.signal_index.to_be_bytes());

        let written = restate_base64_util::URL_SAFE
            .encode_slice(input_buf, encoder.remaining_mut())
            .expect("base64 encoding succeeds for system-generated ids");
        encoder.advance(written);
    }
}

impl ExternalSignalIdentifier {
    pub fn new(invocation_id: InvocationId, signal_index: u32) -> Self {
        Self {
            invocation_id,
            signal_index,
        }
    }

    pub fn into_inner(self) -> (InvocationId, SignalId) {
        (self.invocation_id, SignalId::for_index(self.signal_index))
    }
}

impl FromStr for ExternalSignalIdentifier {
    type Err = IdDecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let decoder = IdDecoder::new(input)?;
        // Ensure we are decoding the right type
        if decoder.resource_type != Self::RESOURCE_TYPE {
            return Err(IdDecodeError::TypeMismatch);
        }
        let remaining = decoder.cursor.take_remaining()?;

        let buffer = restate_base64_util::URL_SAFE
            .decode(remaining)
            .map_err(|_| IdDecodeError::Codec)?;

        if buffer.len() != size_of::<EncodedInvocationId>() + size_of::<EntryIndex>() {
            return Err(IdDecodeError::Length);
        }

        let invocation_id: InvocationId =
            InvocationId::from_slice(&buffer[..size_of::<EncodedInvocationId>()])?;
        let signal_index = u32::from_be_bytes(
            buffer[size_of::<EncodedInvocationId>()..]
                .try_into()
                // Unwrap is safe because we check the size above.
                .unwrap(),
        );

        Ok(Self {
            invocation_id,
            signal_index,
        })
    }
}

impl Display for ExternalSignalIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut encoder = IdEncoder::new();
        self.push_to_encoder(&mut encoder);
        f.write_str(encoder.as_str())
    }
}

impl WithInvocationId for ExternalSignalIdentifier {
    fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }
}

#[cfg(any(test, feature = "test-util"))]
mod mocks {
    use super::*;

    use rand::Rng;
    use rand::distr::{Alphanumeric, SampleString};

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
                rand::rng().sample::<PartitionKey, _>(rand::distr::StandardUniform),
                InvocationUuid::mock_random(),
            )
        }
    }

    impl ServiceId {
        pub fn mock_random() -> Self {
            Self::new(
                Alphanumeric.sample_string(&mut rand::rng(), 8),
                Alphanumeric.sample_string(&mut rand::rng(), 16),
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

        pub fn mock_random() -> Self {
            Self::new(
                Alphanumeric.sample_string(&mut rand::rng(), 8).into(),
                Some(Alphanumeric.sample_string(&mut rand::rng(), 16).into()),
                Alphanumeric.sample_string(&mut rand::rng(), 8).into(),
                Alphanumeric.sample_string(&mut rand::rng(), 8).into(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::invocation::VirtualObjectHandlerType;
    use rand::distr::{Alphanumeric, SampleString};

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
        assert_eq!(38, InvocationId::str_encoded_len())
    }

    #[test]
    fn roundtrip_invocation_id_str() {
        // torture test (poor's man property check test)
        for _ in 0..100000 {
            let expected = InvocationId::mock_random();
            let serialized = expected.to_string();
            assert_eq!(38, serialized.len(), "{serialized} => {expected:?}");
            let parsed = InvocationId::from_str(&serialized).unwrap();
            assert_eq!(expected, parsed, "serialized: {serialized}");
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
                "invocation id: '{bad}' fails with {error}"
            )
        }
    }

    #[test]
    fn roundtrip_lambda_arn() {
        let good = "arn:aws:lambda:eu-central-1:1234567890:function:e2e-node-services:version";

        let expected = LambdaARN::from_str(good).unwrap();
        let parsed = expected.to_string();

        assert_eq!(good, parsed);
        assert_eq!("eu-central-1", expected.region());
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
        let idempotent_key = Alphanumeric.sample_string(&mut rand::rng(), 16);

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

    #[test]
    fn test_subscription_id_format() {
        let a = SubscriptionId::new();
        assert!(a.timestamp().as_u64() > 0);
        let a_str = a.to_string();
        assert!(a_str.starts_with("sub_"));
    }

    #[test]
    fn test_subscription_roundtrip() {
        let a = SubscriptionId::new();
        let b: SubscriptionId = a.to_string().parse().unwrap();
        assert_eq!(a, b);
        assert_eq!(a.to_string(), b.to_string());
    }

    #[test]
    fn test_deployment_id_from_str() {
        let deployment_id = "dp_11nGQpCRmau6ypL82KH2TnP";
        let from_str_result = DeploymentId::from_str(deployment_id);
        assert!(from_str_result.is_ok());
        assert_eq!(
            from_str_result.unwrap().to_string(),
            deployment_id.to_string()
        );

        let deployment_id = "dp_11nGQpCRmau6ypL82KH2TnP123456";
        let from_str_result = DeploymentId::from_str(deployment_id);
        assert!(from_str_result.is_err());
        assert_eq!(from_str_result.unwrap_err(), IdDecodeError::Length);
    }

    #[test]
    fn roundtrip_awakeable_id() {
        let expected_invocation_id = InvocationId::mock_random();
        let expected_entry_index = 2_u32;

        let input_str = AwakeableIdentifier {
            invocation_id: expected_invocation_id,
            entry_index: expected_entry_index,
        }
        .to_string();
        dbg!(&input_str);

        let actual = AwakeableIdentifier::from_str(&input_str).unwrap();
        let (actual_invocation_id, actual_entry_index) = actual.into_inner();

        assert_eq!(expected_invocation_id, actual_invocation_id);
        assert_eq!(expected_entry_index, actual_entry_index);
    }

    #[test]
    fn roundtrip_signal_id() {
        let expected_invocation_id = InvocationId::mock_random();
        let expected_signal_index = 2_u32;

        let input_str = ExternalSignalIdentifier {
            invocation_id: expected_invocation_id,
            signal_index: expected_signal_index,
        }
        .to_string();
        dbg!(&input_str);

        let actual = ExternalSignalIdentifier::from_str(&input_str).unwrap();
        let (actual_invocation_id, actual_signal_id) = actual.into_inner();

        assert_eq!(expected_invocation_id, actual_invocation_id);
        assert_eq!(SignalId::for_index(expected_signal_index), actual_signal_id);
    }
}
