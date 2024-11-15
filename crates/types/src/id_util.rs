// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Resource identifier helpers and core structures

use std::fmt::Write;
use std::str::FromStr;

use num_traits::PrimInt;

use crate::base62_util::{base62_encode_fixed_width, base62_max_length_for_type};
use crate::errors::IdDecodeError;
use crate::identifiers::ResourceId;
use crate::macros::prefixed_ids;

pub const ID_RESOURCE_SEPARATOR: char = '_';

///  versions of the ID encoding scheme that we use to generate user-facing ID tokens.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum IdSchemeVersion {
    #[default]
    /// V1 is the first version of the ID encoding scheme.
    ///
    /// V1 IDs are encoded as follows:
    /// - up to 4c for the resource type (defined in [`IdResourceType`])
    /// - a separator character `_` as defined in [`ID_RESOURCE_SEPARATOR`]
    /// - 1c for the codec version, currently `1`
    /// - A type-specific base62 encoded string for the ID type.
    V1,
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
        Snapshot("snap"),
        PartitionProcessorRpcRequest("rpcid"),
    }
}

impl IdSchemeVersion {
    const fn as_char(&self) -> char {
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

pub struct IdEncoder<T: ?Sized> {
    buf: String,
    _marker: std::marker::PhantomData<T>,
}

impl<T: ResourceId + ?Sized> Default for IdEncoder<T> {
    fn default() -> Self {
        let mut buf = String::with_capacity(Self::estimate_buf_capacity());
        // prefix token
        buf.write_str(T::RESOURCE_TYPE.as_str()).unwrap();
        // Separator
        buf.write_char(ID_RESOURCE_SEPARATOR).unwrap();

        // ID Scheme Version
        buf.write_char(IdSchemeVersion::default().as_char())
            .unwrap();

        Self {
            buf,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: ResourceId + ?Sized> IdEncoder<T> {
    /// Create a new encoder already filled with the provided buffer and resource type.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a value as a padded base62 encoded string to the underlying buffer
    pub fn encode_fixed_width<U>(&mut self, i: U)
    where
        U: Into<u128> + PrimInt,
    {
        base62_encode_fixed_width(i, &mut self.buf);
    }

    /// Estimates the capacity of string buffer needed to encode this ResourceId
    pub const fn estimate_buf_capacity() -> usize {
        T::RESOURCE_TYPE.as_str().len() + /* separator =*/1 + /* version =*/ 1 + T::STRING_CAPACITY_HINT
    }

    /// Adds the given string to the end of the buffer
    pub fn push_str<S>(&mut self, i: S)
    where
        S: AsRef<str>,
    {
        self.buf.push_str(i.as_ref());
    }

    pub fn finalize(self) -> String {
        self.buf
    }
}

#[macro_export]
macro_rules! uuid_backed_id {
    ($name:ident) => {
        paste::paste! {
            uuid_backed_id!(@gen [< $name Id >], $name);
        }
    };
    (@gen $id_name:ident, $resource_type:ident) => {
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
        )]
        pub struct $id_name(pub(crate) Ulid);

        impl $id_name {
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

        impl Default for $id_name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl ResourceId for $id_name {
            const SIZE_IN_BYTES: usize = size_of::<Ulid>();
            const RESOURCE_TYPE: IdResourceType = IdResourceType::$resource_type;
            const STRING_CAPACITY_HINT: usize = base62_max_length_for_type::<u128>();

            fn push_contents_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
                let raw: u128 = self.0.into();
                encoder.encode_fixed_width(raw);
            }
        }

        impl TimestampAwareId for $id_name {
            fn timestamp(&self) -> MillisSinceEpoch {
                self.0.timestamp_ms().into()
            }
        }

        impl FromStr for $id_name {
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

        impl fmt::Display for $id_name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut encoder = IdEncoder::<Self>::new();
                self.push_contents_to_encoder(&mut encoder);
                fmt::Display::fmt(&encoder.finalize(), f)
            }
        }

        impl fmt::Debug for $id_name {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                // use the same formatting for debug and display to show a consistent representation
                fmt::Display::fmt(self, f)
            }
        }

        impl From<u128> for $id_name {
            fn from(value: u128) -> Self {
                Self(Ulid::from(value))
            }
        }

        #[cfg(feature = "schemars")]
        impl schemars::JsonSchema for $id_name {
            fn schema_name() -> String {
                <String as schemars::JsonSchema>::schema_name()
            }

            fn json_schema(g: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
                <String as schemars::JsonSchema>::json_schema(g)
            }
        }
    };
}
