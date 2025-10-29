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
mod awakeable;
mod deployment;
mod idempotency;
mod invocation;
#[cfg(any(test, feature = "test-util"))]
pub mod mocks;
mod partition_processor_rpc_request;
mod signal;
mod snapshot;
mod subscription;

use std::str::FromStr;

use generic_array::ArrayLength;
use generic_array::GenericArray;
use generic_array::sequence::GenericSequence;
use num_traits::PrimInt;

use crate::base62_util::{
    base62_encode_fixed_width_u64, base62_encode_fixed_width_u128, base62_max_length_for_type,
};

pub const ID_RESOURCE_SEPARATOR: char = '_';

/// Error parsing/decoding a resource ID.
#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq)]
pub enum IdDecodeError {
    #[error("bad length")]
    Length,
    #[error("base62 decode error")]
    Codec,
    #[error("bad format")]
    Format,
    #[error("unrecognized codec version")]
    Version,
    #[error("id doesn't match the expected type")]
    TypeMismatch,
    #[error("unrecognized resource type: {0}")]
    UnrecognizedType(String),
}

prefixed_ids! {
    /// The set of resources that we can generate IDs for. Those resource IDs will
    /// follow the same encoding scheme according to the [default] version.
    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    pub enum IdResourceType {
        Invocation("inv"),
        Deployment("dp"),
        Subscription("sub"),
        Awakeable("prom"),
        Signal("sign"),
        Snapshot("snap"),
    }
}

pub use awakeable::AwakeableIdentifier;
pub use deployment::DeploymentId;
pub use idempotency::IdempotencyId;
pub use invocation::*;
pub use partition_processor_rpc_request::PartitionProcessorRpcRequestId;
pub use signal::ExternalSignalIdentifier;
pub use snapshot::SnapshotId;
pub use subscription::SubscriptionId;

///  versions of the ID encoding scheme that we use to generate user-facing ID tokens.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum IdSchemeVersion {
    /// V1 is the first version of the ID encoding scheme.
    ///
    /// V1 IDs are encoded as follows:
    /// - up to 4c for the resource type (defined in [`IdResourceType`])
    /// - a separator character `_` as defined in [`ID_RESOURCE_SEPARATOR`]
    /// - 1c for the codec version, currently `1`
    /// - A type-specific base62 encoded string for the ID type.
    V1,
}

impl IdSchemeVersion {
    pub const fn latest() -> Self {
        Self::V1
    }

    pub const fn as_char(&self) -> char {
        match self {
            Self::V1 => '1',
        }
    }
}

impl FromStr for IdSchemeVersion {
    type Err = IdDecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "1" => Ok(Self::V1),
            _ => Err(IdDecodeError::Version),
        }
    }
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

/// A family of resource identifiers that tracks the timestamp of its creation.
pub trait TimestampAwareId {
    /// The timestamp when this ID was created.
    fn timestamp_ms(&self) -> u64;
}

/// strings and extracts the next encoded token and tracks the buffer offset.
pub struct IdStrCursor<'a> {
    inner: &'a str,
    offset: usize,
}

impl<'a> IdStrCursor<'a> {
    pub fn new(inner: &'a str) -> Self {
        Self { inner, offset: 0 }
    }

    /// Reads the next encoded token from the wrapped string and advances the offset.
    /// The number of characters to read depends on the type T. Type T is any integer
    /// primitive that is of size u128 or smaller.
    pub fn decode_next<T>(&mut self) -> Result<T, IdDecodeError>
    where
        T: PrimInt + TryFrom<u128>,
        <T as TryFrom<u128>>::Error: std::fmt::Debug,
    {
        let size_to_read = base62_max_length_for_type::<T>();
        let sliced_view = self
            .inner
            .get(self.offset..self.offset + size_to_read)
            .ok_or(IdDecodeError::Length)?;
        // de-pad.
        let decoded = base62::decode_alternative(sliced_view.trim_start_matches('0')).or_else(
            |e| match e {
                // If we trim all zeros and nothing left, we assume there was a
                // single zero value in the original input.
                base62::DecodeError::EmptyInput => Ok(0),
                _ => Err(IdDecodeError::Codec),
            },
        )?;
        let out = T::from_be(decoded.try_into().map_err(|_| IdDecodeError::Codec)?);
        self.offset += size_to_read;
        Ok(out)
    }

    /// Reads an exact slice of the input string based on the number of "bytes" specified
    /// in `length`
    pub fn next_str_exact(&mut self, length: usize) -> Result<&'a str, IdDecodeError> {
        let out = self
            .inner
            .get(self.offset..self.offset + length)
            .ok_or(IdDecodeError::Length)?;
        self.offset += length;
        Ok(out)
    }

    /// Reads remaining bytes as string slice without decoding
    pub fn take_remaining(self) -> Result<&'a str, IdDecodeError> {
        let out = self.inner.get(self.offset..).ok_or(IdDecodeError::Length)?;
        Ok(out)
    }

    /// The number of characters remaining in the wrapped string that haven't been decoded yet.
    #[allow(dead_code)]
    pub fn remaining(&self) -> usize {
        self.inner.len().saturating_sub(self.offset)
    }
}

pub struct IdDecoder<'a> {
    #[allow(unused)]
    pub version: IdSchemeVersion,
    pub resource_type: IdResourceType,
    pub cursor: IdStrCursor<'a>,
}

impl<'a> IdDecoder<'a> {
    /// Decode an string that doesn't have the prefix, type, nor version fields.
    pub fn new_ignore_prefix(
        version: IdSchemeVersion,
        resource_type: IdResourceType,
        input: &'a str,
    ) -> Result<Self, IdDecodeError> {
        Ok(Self {
            version,
            resource_type,
            cursor: IdStrCursor::new(input),
        })
    }

    /// Start decoding a well-formed ID string.
    pub fn new(input: &'a str) -> Result<Self, IdDecodeError> {
        if input.is_empty() {
            return Err(IdDecodeError::Length);
        }
        // prefix token
        let (prefix, id_part) = input
            .split_once(ID_RESOURCE_SEPARATOR)
            .ok_or(IdDecodeError::Format)?;

        // Which resource type is this?
        let resource_type: IdResourceType = prefix.parse()?;
        let mut cursor = IdStrCursor::new(id_part);
        // Version
        let version: IdSchemeVersion = cursor.next_str_exact(1)?.parse()?;

        Ok(Self {
            version,
            resource_type,
            cursor,
        })
    }
}

pub struct IdEncoder<T: ResourceId + ?Sized> {
    buf: GenericArray<u8, <T as ResourceId>::StrEncodedLen>,
    pos: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<T: ResourceId + ?Sized> IdEncoder<T> {
    pub(crate) fn new() -> IdEncoder<T> {
        use std::io::{IoSlice, Write};

        static SEP_AND_VER: [u8; 2] = [
            ID_RESOURCE_SEPARATOR as u8,
            IdSchemeVersion::latest().as_char() as u8,
        ];

        let buf = generic_array::GenericArray::generate(|_| b'0');

        let mut encoder = Self {
            buf,
            pos: 0,
            _marker: std::marker::PhantomData,
        };

        let pos = (&mut encoder.buf[..])
            .write_vectored(&[
                // prefix token
                IoSlice::new(T::RESOURCE_TYPE.as_str().as_bytes()),
                // Separator + ID Scheme Version
                IoSlice::new(&SEP_AND_VER),
            ])
            .expect("buf must fit");

        encoder.pos = pos;
        encoder
    }
    /// Appends a u64 value as a padded base62 encoded string to the underlying buffer
    pub(crate) fn push_u64(&mut self, i: u64) {
        let width = base62_encode_fixed_width_u64(i, &mut self.buf[self.pos..]);
        self.pos += width;
        debug_assert!(self.pos <= self.buf.len());
    }

    /// Appends a u128 value as a padded base62 encoded string to the underlying buffer
    pub(crate) fn push_u128(&mut self, i: u128) {
        let width = base62_encode_fixed_width_u128(i, &mut self.buf[self.pos..]);
        self.pos += width;
        debug_assert!(self.pos <= self.buf.len());
    }

    pub(crate) fn remaining_mut(&mut self) -> &mut [u8] {
        &mut self.buf[self.pos..]
    }

    pub(crate) fn advance(&mut self, cnt: usize) {
        self.pos += cnt;
    }

    pub fn as_str(&self) -> &str {
        debug_assert!(self.pos <= self.buf.len());
        // SAFETY; the array was initialised with valid utf8 and we only write valid utf8 to the
        // buffer.
        unsafe { std::str::from_utf8_unchecked(&self.buf[..self.pos]) }
    }
}

// Helper macro to generate serialization primitives back and forth for id types with well-defined prefixes.
macro_rules! prefixed_ids {
    (
    $(#[$m:meta])*
    $type_vis:vis enum $typename:ident {
        $(
            $(#[$variant_meta:meta])*
            $variant:ident($variant_prefix:literal),
        )+
    }
    ) => {
        #[allow(clippy::all)]
        $(#[$m])*
        $type_vis enum $typename {
            $(
                $(#[$variant_meta])*
                $variant,
            )+
        }

        impl $typename {
            #[doc = "The prefix string for this identifier"]
            pub const fn as_str(&self) -> &'static str {
                match self {
                    $(
                        $typename::$variant => $variant_prefix,
                    )+
                }
            }
            pub fn iter() -> ::core::slice::Iter<'static, $typename> {
                static VARIANTS: &'static [$typename] = &[
                    $(
                        $typename::$variant,
                    )+
                ];
                VARIANTS.iter()
            }
        }


        #[automatically_derived]
        impl ::core::str::FromStr for $typename {
            type Err = IdDecodeError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $(
                        $variant_prefix => Ok($typename::$variant),
                    )+
                    _ => Err(IdDecodeError::UnrecognizedType(value.to_string())),
                }
            }
        }

    };
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
            impl ::std::fmt::Display for [< $res_name Id >] {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    ::std::fmt::Display::fmt(&self.0, f)
                }
            }

            impl ::std::str::FromStr for [< $res_name Id >] {
                type Err = ulid::DecodeError;

                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    Ok(Self(ulid::Ulid::from_string(s)?))
                }
            }
        }
    };
    ($res_name:ident @with_resource_id) => {
        ulid_backed_id!(@common $res_name);

        paste::paste! {
            impl crate::identifiers::ResourceId for [< $res_name Id >] {
                const RAW_BYTES_LEN: usize = ::std::mem::size_of::<::ulid::Ulid>();
                const RESOURCE_TYPE: crate::identifiers::IdResourceType = crate::identifiers::IdResourceType::$res_name;

                type StrEncodedLen = ::generic_array::ConstArrayLength<
                    // prefix + separator + version + suffix
                    { Self::RESOURCE_TYPE.as_str().len() + 2 + crate::base62_util::base62_max_length_for_type::<u128>() },
                >;

                fn push_to_encoder(&self, encoder: &mut crate::identifiers::IdEncoder<Self>) {
                    let raw: u128 = self.0.into();
                    encoder.push_u128(raw);
                }
            }

            impl ::std::str::FromStr for [< $res_name Id >] {
                type Err = crate::identifiers::IdDecodeError;

                fn from_str(input: &str) -> Result<Self, Self::Err> {
                    use crate::identifiers::ResourceId;
                    let mut decoder = crate::identifiers::IdDecoder::new(input)?;
                    // Ensure we are decoding the correct resource type
                    if decoder.resource_type != Self::RESOURCE_TYPE {
                        return Err(crate::identifiers::IdDecodeError::TypeMismatch);
                    }

                    // ulid (u128)
                    let raw_ulid: u128 = decoder.cursor.decode_next()?;
                    if decoder.cursor.remaining() > 0 {
                        return Err(crate::identifiers::IdDecodeError::Length);
                    }

                    Ok(Self::from(raw_ulid))
                }
            }

            impl ::std::fmt::Display for [< $res_name Id >] {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    use crate::identifiers::ResourceId;
                    let mut encoder = crate::identifiers::IdEncoder::new();
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
            pub struct [< $res_name Id >](pub(crate) ::ulid::Ulid);

            impl [< $res_name Id >] {
                pub fn new() -> Self {
                    Self(::ulid::Ulid::new())
                }

                pub const fn from_parts(timestamp_ms: u64, random: u128) -> Self {
                    Self(::ulid::Ulid::from_parts(timestamp_ms, random))
                }

                pub fn from_slice(b: &[u8]) -> Result<Self, crate::identifiers::IdDecodeError> {
                    let ulid = ::ulid::Ulid::from_bytes(b.try_into().map_err(|_| crate::identifiers::IdDecodeError::Length)?);
                    debug_assert!(!ulid.is_nil());
                    Ok(Self(ulid))
                }

                pub fn from_bytes(bytes: [u8; 16]) -> Self {
                    let ulid = ::ulid::Ulid::from_bytes(bytes);
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

            impl crate::identifiers::TimestampAwareId for [< $res_name Id >] {
                fn timestamp_ms(&self) -> u64 {
                    self.0.timestamp_ms()
                }
            }

            impl ::std::fmt::Debug for [< $res_name Id >] {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    // use the same formatting for debug and display to show a consistent representation
                    ::std::fmt::Display::fmt(self, f)
                }
            }

            impl From<u128> for [< $res_name Id >] {
                fn from(value: u128) -> Self {
                    Self(::ulid::Ulid::from(value))
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

// to allow the macro to be used even if the macro is defined at the end of the file
use {prefixed_ids, ulid_backed_id};
