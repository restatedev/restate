// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use std::mem;

use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;

use super::{StorageCodecKind, StorageEncodeError};

/// Encode a [`Serialize`] type to a buffer using serde if it is supported by the codec.
pub fn encode_serde<T: Serialize>(
    value: &T,
    buf: &mut BytesMut,
    codec: StorageCodecKind,
) -> Result<(), StorageEncodeError> {
    match codec {
        StorageCodecKind::FlexbuffersSerde => encode_serde_as_flexbuffers(value, buf)
            .map_err(|err| StorageEncodeError::EncodeValue(err.into())),
        StorageCodecKind::Json => encode_serde_as_json(value, buf)
            .map_err(|err| StorageEncodeError::EncodeValue(err.into())),
        codec => Err(StorageEncodeError::EncodeValue(
            anyhow::anyhow!("Cannot encode serde type with codec {}", codec).into(),
        )),
    }
}

/// Utility method to encode a [`Serialize`] type as flexbuffers using serde.
fn encode_serde_as_flexbuffers<T: Serialize>(
    value: T,
    buf: &mut BytesMut,
) -> Result<(), flexbuffers::SerializationError> {
    let vec = flexbuffers::to_vec(value)?;
    let size_tag = u32::try_from(vec.len())
        .map_err(|_| serde::ser::Error::custom("only support serializing types of size <= 4GB"))?;

    buf.reserve(vec.len() + mem::size_of::<u32>());
    // write the length
    buf.put_u32_le(size_tag);
    // write the data
    buf.put_slice(&vec);
    Ok(())
}

/// Utility method to encode a [`Serialize`] type as json using serde.
fn encode_serde_as_json<T: Serialize>(
    value: &T,
    buf: &mut BytesMut,
) -> Result<(), serde_json::error::Error> {
    serde_json::to_writer(buf.writer(), value)?;

    Ok(())
}

/// Utility method to encode a [`bilrost::Message`] type
// todo(azmy): Contiguous encoding is supposedly faster for complex
// and nested types. Confirm this claim by running benchmarks.
// (is it still beneficial to encode_contiguous if we still have to
// copy the bytes over to the buffer)
pub fn encode_bilrost_contiguous<T: bilrost::Message>(value: &T) -> Bytes {
    value.encode_contiguous().into_vec().into()
}

/// Utility method to encode a [`bilrost::Message`] type
pub fn encode_bilrost<T: bilrost::Message>(
    value: &T,
    buf: &mut BytesMut,
) -> Result<(), StorageEncodeError> {
    value
        .encode(buf)
        .map_err(|err| StorageEncodeError::EncodeValue(err.into()))
}
