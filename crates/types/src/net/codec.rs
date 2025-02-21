// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::net::CodecError;
use crate::protobuf::common::ProtocolVersion;
use crate::protobuf::common::TargetName;
use crate::protobuf::node::message;
use crate::protobuf::node::message::BinaryMessage;

pub trait Targeted {
    const TARGET: TargetName;
    fn target(&self) -> TargetName {
        Self::TARGET
    }
    fn kind(&self) -> &'static str;
}

impl<T> Targeted for &T
where
    T: Targeted,
{
    const TARGET: TargetName = T::TARGET;
    fn kind(&self) -> &'static str {
        (*self).kind()
    }
}

impl<T> Targeted for &mut T
where
    T: Targeted,
{
    const TARGET: TargetName = T::TARGET;
    fn kind(&self) -> &'static str {
        (**self).kind()
    }
}

impl<T> Targeted for Box<T>
where
    T: Targeted,
{
    const TARGET: TargetName = T::TARGET;
    fn kind(&self) -> &'static str {
        (**self).kind()
    }
}

impl<T> Targeted for Arc<T>
where
    T: Targeted,
{
    const TARGET: TargetName = T::TARGET;
    fn kind(&self) -> &'static str {
        (**self).kind()
    }
}

pub trait WireEncode {
    fn encode(self, protocol_version: ProtocolVersion) -> Result<message::Body, CodecError>;
}

pub trait WireDecode {
    fn decode(buf: impl Buf, protocol_version: ProtocolVersion) -> Result<Self, CodecError>
    where
        Self: Sized;
}

impl<T> WireEncode for Box<T>
where
    T: WireEncode,
{
    fn encode(self, protocol_version: ProtocolVersion) -> Result<message::Body, CodecError> {
        (*self).encode(protocol_version)
    }
}

impl<T> WireDecode for Box<T>
where
    T: WireDecode,
{
    fn decode(buf: impl Buf, protocol_version: ProtocolVersion) -> Result<Self, CodecError>
    where
        Self: Sized,
    {
        Ok(Box::new(T::decode(buf, protocol_version)?))
    }
}

impl<T> WireDecode for Arc<T>
where
    T: WireDecode,
{
    fn decode(buf: impl Buf, protocol_version: ProtocolVersion) -> Result<Self, CodecError>
    where
        Self: Sized,
    {
        Ok(Arc::new(T::decode(buf, protocol_version)?))
    }
}

/// Utility method to raw-encode a [`Serialize`] type as flexbuffers using serde without adding
/// version tag. This must be decoded with `decode_from_untagged_flexbuffers`. This is used as the default
/// encoding for network messages since networking has its own protocol versioning.
pub fn encode_default<T: Serialize>(
    value: T,
    protocol_version: ProtocolVersion,
) -> Result<Bytes, CodecError> {
    match protocol_version {
        ProtocolVersion::V1 => Ok(Bytes::from(
            flexbuffers::to_vec(value).map_err(|err| CodecError::Encode(err.into()))?,
        )),
        ProtocolVersion::Unknown => {
            unreachable!("unknown protocol version should never be set")
        }
    }
}

/// Utility method to decode a [`DeserializeOwned`] type from flexbuffers using serde. the buffer
/// must have the complete message and not internally chunked.
pub fn decode_default<T: DeserializeOwned>(
    buf: impl Buf,
    protocol_version: ProtocolVersion,
) -> Result<T, CodecError> {
    match protocol_version {
        ProtocolVersion::V1 => {
            flexbuffers::from_slice(buf.chunk()).map_err(|err| CodecError::Decode(err.into()))
        }
        ProtocolVersion::Unknown => {
            unreachable!("unknown protocol version should never be set")
        }
    }
}

pub trait MessageBodyExt {
    fn try_as_binary_body(
        self,
        protocol_version: ProtocolVersion,
    ) -> Result<BinaryMessage, CodecError>;

    fn try_decode<T: WireDecode>(self, protocol_version: ProtocolVersion) -> Result<T, CodecError>;
}

impl MessageBodyExt for crate::protobuf::node::message::Body {
    fn try_as_binary_body(
        self,
        _protocol_version: ProtocolVersion,
    ) -> Result<BinaryMessage, CodecError> {
        let message::Body::Encoded(binary) = self else {
            return Err(CodecError::Decode(
                "Cannot deserialize message, message is not of type BinaryMessage".into(),
            ));
        };
        Ok(binary)
    }

    fn try_decode<T: WireDecode>(self, protocol_version: ProtocolVersion) -> Result<T, CodecError> {
        let mut binary_message = self.try_as_binary_body(protocol_version)?;
        <T as WireDecode>::decode(&mut binary_message.payload, protocol_version)
    }
}
