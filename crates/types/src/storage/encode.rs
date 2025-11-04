// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use restate_ty::storage::{StorageCodecKind, StorageEncodeError};

/// Encode a [`Serialize`] type to a buffer using serde if it is supported by the codec.
pub fn encode_serde<T: Serialize>(
    value: &T,
    buf: &mut BytesMut,
    codec: StorageCodecKind,
) -> Result<(), StorageEncodeError> {
    match codec {
        StorageCodecKind::FlexbuffersSerde => encode_serde_as_flexbuffers(value, buf)
            .map_err(|err| StorageEncodeError::EncodeValue(err.into())),
        StorageCodecKind::BincodeSerde => encode_serde_as_bincode(value, buf)
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

/// Utility method to encode a [`Serialize`] type as bincode using serde.
fn encode_serde_as_bincode<T: Serialize>(
    value: &T,
    buf: &mut BytesMut,
) -> Result<(), bincode::error::EncodeError> {
    struct BytesWriter<'a>(&'a mut BytesMut);

    impl bincode::enc::write::Writer for BytesWriter<'_> {
        fn write(&mut self, bytes: &[u8]) -> Result<(), bincode::error::EncodeError> {
            self.0.put_slice(bytes);
            Ok(())
        }
    }
    // write the data
    bincode::serde::encode_into_writer(value, BytesWriter(buf), bincode::config::standard())?;

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
pub fn encode_bilrost<T: bilrost::Message>(value: &T) -> Bytes {
    value.encode_contiguous().into_vec().into()
}
