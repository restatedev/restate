// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{self, Display, Formatter};
use std::hash::Hash;
use std::mem::size_of;
use std::str::FromStr;

use bytes::Bytes;
use rand::RngCore;
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::base62_util::{base62_encode_fixed_width_u128, base62_max_length_for_type};
use crate::invocation::{InvocationTarget, InvocationTargetType, WorkflowHandlerType};
use crate::partitions::{PartitionKey, WithPartitionKey, deterministic_partition_key};

use super::{IdDecodeError, IdDecoder, IdEncoder, IdResourceType, IdSchemeVersion, ResourceId};

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

    #[inline]
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

    pub const fn to_bytes(&self) -> [u8; Self::RAW_BYTES_LEN] {
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
            IdSchemeVersion::latest(),
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
}
