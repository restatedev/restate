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

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::common::ProtocolVersion;
use crate::common::TargetName;
use crate::node::message;
use crate::node::message::BinaryMessage;
use crate::CodecError;

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

pub trait WireSerde {
    fn encode(&self, protocol_version: ProtocolVersion) -> Result<Bytes, CodecError>;
    fn decode(bytes: Bytes, protocol_version: ProtocolVersion) -> Result<Self, CodecError>
    where
        Self: Sized;
}

impl<T> WireSerde for &T
where
    T: WireSerde,
{
    fn encode(&self, protocol_version: ProtocolVersion) -> Result<Bytes, CodecError> {
        (**self).encode(protocol_version)
    }

    fn decode(_bytes: Bytes, _protocol_version: ProtocolVersion) -> Result<Self, CodecError>
    where
        Self: Sized,
    {
        unreachable!()
    }
}

impl<T> WireSerde for &mut T
where
    T: WireSerde,
{
    fn encode(&self, protocol_version: ProtocolVersion) -> Result<Bytes, CodecError> {
        (**self).encode(protocol_version)
    }

    fn decode(_bytes: Bytes, _protocol_version: ProtocolVersion) -> Result<Self, CodecError>
    where
        Self: Sized,
    {
        unreachable!()
    }
}

impl<T> WireSerde for Box<T>
where
    T: WireSerde,
{
    fn encode(&self, protocol_version: ProtocolVersion) -> Result<Bytes, CodecError> {
        (**self).encode(protocol_version)
    }

    fn decode(bytes: Bytes, protocol_version: ProtocolVersion) -> Result<Self, CodecError>
    where
        Self: Sized,
    {
        Ok(Box::new(T::decode(bytes, protocol_version)?))
    }
}

impl<T> WireSerde for Arc<T>
where
    T: WireSerde,
{
    fn encode(&self, protocol_version: ProtocolVersion) -> Result<Bytes, CodecError> {
        (**self).encode(protocol_version)
    }

    fn decode(bytes: Bytes, protocol_version: ProtocolVersion) -> Result<Self, CodecError>
    where
        Self: Sized,
    {
        Ok(Arc::new(T::decode(bytes, protocol_version)?))
    }
}

pub fn serialize_message<M: WireSerde + Targeted>(
    msg: M,
    protocol_version: ProtocolVersion,
) -> Result<message::Body, CodecError> {
    let payload = msg.encode(protocol_version)?;
    let target = M::TARGET.into();
    Ok(message::Body::Encoded(message::BinaryMessage {
        target,
        payload,
    }))
}

pub fn deserialize_message(
    msg: message::Body,
    _protocol_version: ProtocolVersion,
) -> Result<BinaryMessage, CodecError> {
    let message::Body::Encoded(binary) = msg else {
        // at the moment, we only support bincoded messages
        return Err(CodecError::ProtobufDecode(
            "Cannot deserialize message, message is not of type BinaryMessage",
        ));
    };
    Ok(binary)
}

/// Helper function for default encoding of values.
pub fn encode_default<T: Serialize>(
    value: T,
    protocol_version: ProtocolVersion,
) -> Result<Bytes, CodecError> {
    match protocol_version {
        ProtocolVersion::Bincoded => {
            bincode::serde::encode_to_vec(value, bincode::config::standard())
                .map(Into::into)
                .map_err(Into::into)
        }
        ProtocolVersion::Unknown => {
            unreachable!("unknown protocol version should never be set")
        }
    }
}

pub fn decode_default<T: DeserializeOwned>(
    bytes: Bytes,
    protocol_version: ProtocolVersion,
) -> Result<T, CodecError> {
    match protocol_version {
        ProtocolVersion::Bincoded => {
            bincode::serde::decode_from_slice(bytes.as_ref(), bincode::config::standard())
                .map(|(value, _)| value)
                .map_err(Into::into)
        }
        ProtocolVersion::Unknown => {
            unreachable!("unknown protocol version should never be set")
        }
    }
}
