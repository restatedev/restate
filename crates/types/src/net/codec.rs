// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::net::CodecError;
use crate::protobuf::common::ProtocolVersion;
use crate::protobuf::common::TargetName;
use crate::protobuf::node::message;
use crate::protobuf::node::message::BinaryMessage;
use crate::storage::{decode_from_flexbuffers, encode_as_flexbuffers};

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
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        protocol_version: ProtocolVersion,
    ) -> Result<(), CodecError>;
}

pub trait WireDecode {
    fn decode<B: Buf>(buf: &mut B, protocol_version: ProtocolVersion) -> Result<Self, CodecError>
    where
        Self: Sized;
}

impl<T> WireEncode for &T
where
    T: WireEncode,
{
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        protocol_version: ProtocolVersion,
    ) -> Result<(), CodecError> {
        (*self).encode(buf, protocol_version)
    }
}

impl<T> WireEncode for &mut T
where
    T: WireEncode,
{
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        protocol_version: ProtocolVersion,
    ) -> Result<(), CodecError> {
        (**self).encode(buf, protocol_version)
    }
}

impl<T> WireEncode for Box<T>
where
    T: WireEncode,
{
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        protocol_version: ProtocolVersion,
    ) -> Result<(), CodecError> {
        (**self).encode(buf, protocol_version)
    }
}

impl<T> WireDecode for Box<T>
where
    T: WireDecode,
{
    fn decode<B: Buf>(buf: &mut B, protocol_version: ProtocolVersion) -> Result<Self, CodecError>
    where
        Self: Sized,
    {
        Ok(Box::new(T::decode(buf, protocol_version)?))
    }
}

impl<T> WireEncode for Arc<T>
where
    T: WireEncode,
{
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        protocol_version: ProtocolVersion,
    ) -> Result<(), CodecError> {
        (**self).encode(buf, protocol_version)
    }
}

impl<T> WireDecode for Arc<T>
where
    T: WireDecode,
{
    fn decode<B: Buf>(buf: &mut B, protocol_version: ProtocolVersion) -> Result<Self, CodecError>
    where
        Self: Sized,
    {
        Ok(Arc::new(T::decode(buf, protocol_version)?))
    }
}

pub fn serialize_message<M: WireEncode + Targeted>(
    msg: &M,
    protocol_version: ProtocolVersion,
) -> Result<message::Body, CodecError> {
    let mut payload = BytesMut::new();
    msg.encode(&mut payload, protocol_version)?;
    let target = M::TARGET.into();
    Ok(message::Body::Encoded(BinaryMessage {
        target,
        payload: payload.freeze(),
    }))
}

/// Helper function for default encoding of values.
pub fn encode_default<T: Serialize, B: BufMut>(
    value: T,
    buf: &mut B,
    protocol_version: ProtocolVersion,
) -> Result<(), CodecError> {
    match protocol_version {
        ProtocolVersion::Flexbuffers => {
            encode_as_flexbuffers(value, buf).map_err(|err| CodecError::Encode(err.into()))
        }
        ProtocolVersion::Unknown => {
            unreachable!("unknown protocol version should never be set")
        }
    }
}

pub fn decode_default<T: DeserializeOwned, B: Buf>(
    buf: &mut B,
    protocol_version: ProtocolVersion,
) -> Result<T, CodecError> {
    match protocol_version {
        ProtocolVersion::Flexbuffers => {
            decode_from_flexbuffers(buf).map_err(|err| CodecError::Decode(err.into()))
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
