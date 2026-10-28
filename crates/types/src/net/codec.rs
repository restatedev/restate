// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

/// Utility method to decode a [`DeserializeOwned`] type from flexbuffers using serde. `buf` must
/// hold exactly one message, which is consumed. Flexbuffers reads from one contiguous slice:
/// contiguous input is decoded in place, segmented input is first copied into one buffer.
pub fn decode_as_flexbuffers<T: DeserializeOwned>(
    mut buf: impl Buf,
    protocol_version: ProtocolVersion,
) -> Result<T, anyhow::Error> {
    assert!(
        protocol_version >= ProtocolVersion::V2,
        "unknown protocol version should never be set"
    );

    let len = buf.remaining();
    let decoded = if buf.chunk().len() == len {
        let decoded = flexbuffers::from_slice(buf.chunk());
        buf.advance(len);
        decoded
    } else {
        flexbuffers::from_slice(&buf.copy_to_bytes(len))
    };
    decoded.with_context(|| {
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

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Message {
        id: u64,
        name: String,
        values: Vec<u32>,
    }

    fn decode(mut buf: impl Buf) -> Message {
        let message = decode_as_flexbuffers(&mut buf, ProtocolVersion::V2).unwrap();
        assert!(!buf.has_remaining(), "decoding consumes the message");
        message
    }

    #[test]
    fn flexbuffers_decode_from_segmented_input() {
        let message = Message {
            id: 7,
            name: "restate".to_owned(),
            values: vec![1, 2, 3],
        };
        let encoded = Bytes::from(encode_as_flexbuffers(&message));
        assert_eq!(decode(encoded.clone()), message);
        for at in 0..=encoded.len() {
            let chained = encoded.slice(..at).chain(encoded.slice(at..));
            assert_eq!(decode(chained), message, "split at {at}");
        }
        let third = encoded.len() / 3;
        let three = encoded
            .slice(..third)
            .chain(encoded.slice(third..2 * third))
            .chain(encoded.slice(2 * third..));
        assert_eq!(decode(three), message);
    }
}
