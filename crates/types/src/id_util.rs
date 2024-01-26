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
