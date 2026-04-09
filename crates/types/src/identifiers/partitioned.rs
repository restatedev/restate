// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Resource IDs with a partition key
pub trait PartitionedResourceId: std::fmt::Display + std::fmt::Debug {
    fn from_partition_key_and_slice(
        partition_key: crate::PartitionKey,
        remainder: &[u8],
    ) -> Result<Self, IdDecodeError>
    where
        Self: Sized;

    fn partition_key(&self) -> crate::PartitionKey;
    /// The remainder byte-slice portion of the ID that doesn't include the partition key
    fn remainder_slice(&self) -> &[u8];
}

/// Generate an identifier internally backed by ULID + partition key (with resource id)
///
/// This generates the Id struct and some associated methods: `generate`, `from_parts`, `from_slice`, `from_bytes`, `to_bytes`,
/// plus implements `Display`, `Debug`, `FromStr`, `JsonSchema` and `TimestampAwareId`.
///
/// To use:
///
/// ```ignore
/// partitioned_resource_id!(MyResource);
/// ```
///
/// The difference between the two will be the usage of ResourceId for serde and string representations.
macro_rules! partitioned_resource_id {
    (
        $(#[$m:meta])*
        $res_name:ident
    ) => {
        partitioned_resource_id!(@common $(#[$m])* $res_name);

        paste::paste! {
            impl $crate::identifiers::ResourceId for [< $res_name Id >] {
                const RAW_BYTES_LEN: usize = 24;
                const RESOURCE_TYPE: $crate::id_util::IdResourceType = $crate::id_util::IdResourceType::$res_name;

                type StrEncodedLen = ::generic_array::ConstArrayLength<
                    // prefix + separator + version + suffix
                    {
                        Self::RESOURCE_TYPE.as_str().len()
                            + 2
                            // partition key
                            + $crate::base62_util::base62_max_length_for_type::<u64>()
                            // ts + random
                            + $crate::base62_util::base62_max_length_for_type::<u128>()
                    },
                >;

                fn push_to_encoder(&self, encoder: &mut $crate::id_util::IdEncoder<Self>) {
                    encoder.push_u64(self.partition_key);
                    encoder.push_u128(u128::from_be_bytes(self.remainder));
                }
            }

            impl ::std::str::FromStr for [< $res_name Id >] {
                type Err = $crate::errors::IdDecodeError;

                fn from_str(input: &str) -> ::std::result::Result<Self, Self::Err> {
                    if input.len() < <Self as $crate::identifiers::ResourceId>::str_encoded_len() {
                        return Err($crate::errors::IdDecodeError::Length);
                    }

                    let mut decoder = $crate::id_util::IdDecoder::new(input)?;
                    // Ensure we are decoding the correct resource type
                    if decoder.resource_type != <Self as $crate::identifiers::ResourceId>::RESOURCE_TYPE {
                        return Err($crate::errors::IdDecodeError::TypeMismatch);
                    }

                    // partition key (u64)
                    let partition_key = decoder.cursor.decode_next()?;

                    // ulid (u128)
                    let raw_ulid: u128 = decoder.cursor.decode_next()?;
                    assert_eq!(0, decoder.cursor.remaining());

                    Ok(Self { partition_key, remainder: raw_ulid.to_be_bytes() })
                }
            }

            impl ::std::fmt::Display for [< $res_name Id >] {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    use $crate::identifiers::ResourceId;
                    let mut encoder = $crate::id_util::IdEncoder::new();
                    self.push_to_encoder(&mut encoder);
                    f.write_str(encoder.as_str())
                }
            }
        }
    };
    (@common $(#[$meta:meta])* $res_name:ident) => {
        paste::paste! {
            $(#[$meta])*
            #[derive(
                PartialEq,
                Eq,
                Clone,
                Hash,
                ::serde_with::SerializeDisplay,
                ::serde_with::DeserializeFromStr,
                ::bilrost::Message,
            )]
            pub struct [< $res_name Id >] {
                #[bilrost(tag(1))]
                partition_key: $crate::PartitionKey,
                #[bilrost(tag(2), encoding(plainbytes))]
                remainder: [u8; 16],
            }

            impl [< $res_name Id >] {
                /// Length of the raw bytes needed when serializing this resource identifiers using
                /// `to_bytes()`. This does not apply to `to_string()` or `serde` implementations.
                pub const fn serialized_length_fixed() -> usize {
                    std::mem::size_of::<$crate::PartitionKey>() + 16
                }

                pub fn generate(partition_key: $crate::PartitionKey) -> Self {
                    Self { partition_key, remainder: ::ulid::Ulid::new().to_bytes() }
                }

                pub const fn from_partition_key_and_bytes(partition_key: $crate::PartitionKey, remainder: [u8;16]) -> Self {
                    Self { partition_key, remainder }
                }

                pub const fn from_parts(partition_key: $crate::PartitionKey, timestamp_ms: u64, random: u128) -> Self {
                    Self {
                        partition_key,
                        remainder: ::ulid::Ulid::from_parts(timestamp_ms, random).to_bytes(),
                    }
                }

                pub fn from_slice(b: &[u8]) -> Result<Self, $crate::errors::IdDecodeError> {
                    if b.len() < Self::serialized_length_fixed() {
                        return Err($crate::errors::IdDecodeError::Length);
                    }
                    // read pkey as u64 (big-endian)
                    let partition_key = u64::from_be_bytes(b[..size_of::<$crate::PartitionKey>()].try_into().unwrap());
                    let remainder = b[size_of::<$crate::PartitionKey>()..].try_into().map_err(|_| $crate::errors::IdDecodeError::Length)?;
                    Ok(Self{ partition_key, remainder })
                }

                pub fn from_bytes(b: [u8; 24]) -> Self {
                    let partition_key = u64::from_be_bytes(b[..size_of::<$crate::PartitionKey>()].try_into().unwrap());
                    let remainder = b[size_of::<$crate::PartitionKey>()..].try_into().unwrap();
                    Self{ partition_key, remainder }
                }

                pub fn to_bytes(&self) -> [u8; 24] {
                    let mut buf = [0u8; 24];
                    buf[..size_of::<$crate::PartitionKey>()].copy_from_slice(&self.partition_key.to_be_bytes());
                    buf[size_of::<$crate::PartitionKey>()..].copy_from_slice(&self.remainder);
                    buf
                }

                #[inline]
                pub fn to_remainder_bytes(&self) -> [u8; 16] {
                    self.remainder
                }

                #[inline]
                pub fn as_remainder_bytes(&self) -> &[u8; 16] {
                    &self.remainder
                }
            }

            impl $crate::identifiers::TimestampAwareId for [< $res_name Id >] {
                fn timestamp(&self) -> ::restate_clock::time::MillisSinceEpoch {
                    let ulid = ::ulid::Ulid::from_bytes(self.remainder);
                    ulid.timestamp_ms().into()
                }
            }

            impl $crate::PartitionedResourceId for [< $res_name Id >] {
                #[inline]
                fn from_partition_key_and_slice(
                    partition_key: crate::PartitionKey,
                    remainder: &[u8],
                ) -> Result<Self, crate::errors::IdDecodeError> {
                    let remainder = remainder.try_into().map_err(|_| crate::errors::IdDecodeError::Length)?;
                    Ok(Self { partition_key, remainder })
                }

                #[inline]
                fn partition_key(&self) -> $crate::PartitionKey {
                    self.partition_key
                }

                #[inline]
                fn remainder_slice(&self) -> &[u8] {
                    &self.remainder
                }
            }

            impl $crate::WithPartitionKey for [< $res_name Id >] {
                #[inline]
                fn partition_key(&self) -> $crate::PartitionKey {
                    self.partition_key
                }
            }

            impl ::std::fmt::Debug for [< $res_name Id >] {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    // use the same formatting for debug and display to show a consistent representation
                    ::std::fmt::Display::fmt(self, f)
                }
            }

            #[cfg(feature = "schemars")]
            impl ::schemars::JsonSchema for [< $res_name Id >] {
                fn schema_name() -> ::std::borrow::Cow<'static, str> {
                    <String as ::schemars::JsonSchema>::schema_name()
                }

                fn json_schema(g: &mut ::schemars::SchemaGenerator) -> ::schemars::Schema {
                    <String as ::schemars::JsonSchema>::json_schema(g)
                }
            }
        }
    };
}

pub(crate) use partitioned_resource_id;

use crate::errors::IdDecodeError;

#[cfg(test)]
mod tests {
    use crate::identifiers::WithPartitionKey;

    partitioned_resource_id!(Test);

    #[test]
    fn test_roundtrip() {
        let id = TestId::generate(13);
        assert_eq!(id.partition_key(), 13);
        let encoded = id.to_string();
        assert!(encoded.starts_with("tst_1"));
        assert_eq!(encoded.len(), 38);
        println!("{encoded}");
        let decoded: TestId = encoded.parse().unwrap();
        assert_eq!(id, decoded);
        assert_eq!(decoded.partition_key(), 13);

        // bytes roundtrip
        let bytes = id.to_bytes();
        assert_eq!(bytes.len(), 24);
        let decoded = TestId::from_bytes(bytes);
        assert_eq!(id, decoded);
        assert_eq!(decoded.partition_key(), 13);
    }
}
