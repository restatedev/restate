// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::type_name;
use std::sync::Arc;

use anyhow::Context;
use bilrost::OwnedMessage;
use bytes::Buf;
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::protobuf::common::ProtocolVersion;

#[derive(thiserror::Error, Debug)]
pub enum EncodeError {
    #[error(
        "message of type {type_tag} requires peer to have minimum version {min_required:?} but actual negotiated version is {actual:?}."
    )]
    IncompatibleVersion {
        type_tag: &'static str,
        min_required: ProtocolVersion,
        actual: ProtocolVersion,
    },
}

pub trait WireEncode {
    fn encode_to_bytes(&self, protocol_version: ProtocolVersion) -> Result<Bytes, EncodeError>;
}

pub trait WireDecode {
    type Error: std::fmt::Debug + Into<anyhow::Error>;
    /// Panics if decode failed
    fn decode(buf: impl Buf, protocol_version: ProtocolVersion) -> Self
    where
        Self: Sized,
    {
        Self::try_decode(buf, protocol_version).expect("decode WireDecode message failed")
    }

    fn try_decode(buf: impl Buf, protocol_version: ProtocolVersion) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

impl<T> WireEncode for Box<T>
where
    T: WireEncode,
{
    fn encode_to_bytes(&self, protocol_version: ProtocolVersion) -> Result<Bytes, EncodeError> {
        self.as_ref().encode_to_bytes(protocol_version)
    }
}

impl<T> WireDecode for Box<T>
where
    T: WireDecode,
{
    type Error = T::Error;
    fn try_decode(buf: impl Buf, protocol_version: ProtocolVersion) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Box::new(T::try_decode(buf, protocol_version)?))
    }
}

impl<T> WireDecode for Arc<T>
where
    T: WireDecode,
{
    type Error = T::Error;

    fn try_decode(buf: impl Buf, protocol_version: ProtocolVersion) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Arc::new(T::try_decode(buf, protocol_version)?))
    }
}

/// Utility method to raw-encode a [`Serialize`] type as flexbuffers using serde without adding
/// version tag. This must be decoded with `decode_from_untagged_flexbuffers`. This is used as the default
/// encoding for network messages since networking has its own protocol versioning.
pub fn encode_as_flexbuffers<T: Serialize>(value: T) -> Vec<u8> {
    flexbuffers::to_vec(value).expect("network message serde can't fail")
}

/// Utility method to decode a [`DeserializeOwned`] type from flexbuffers using serde. the buffer
/// must have the complete message and not internally chunked.
pub fn decode_as_flexbuffers<T: DeserializeOwned>(
    buf: impl Buf,
    protocol_version: ProtocolVersion,
) -> Result<T, anyhow::Error> {
    assert!(
        protocol_version >= ProtocolVersion::V2,
        "unknown protocol version should never be set"
    );

    flexbuffers::from_slice(buf.chunk()).with_context(|| {
        format!(
            "failed decoding (flexbuffers) network message {}",
            type_name::<T>()
        )
    })
}

pub fn encode_as_bilrost<T: bilrost::Message>(value: &T) -> Bytes {
    let buf = value.encode_fast();
    Bytes::from(buf.into_vec())
}

pub fn decode_as_bilrost<T: OwnedMessage>(
    buf: impl Buf,
    protocol_version: ProtocolVersion,
) -> Result<T, anyhow::Error> {
    assert!(
        protocol_version >= ProtocolVersion::V2,
        "bilrost encoding is supported from protocol version v2"
    );

    T::decode(buf).context("failed decoding (bilrost) network message")
}
