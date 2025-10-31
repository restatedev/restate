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

use bytes::Buf;
use serde::de::{DeserializeOwned, Error as DeserializationError};
use tracing::error;

use restate_ty::storage::{StorageCodecKind, StorageDecodeError};

/// Decode a [`DeserializeOwned`] type from a buffer using serde if it is supported by the codec.
pub fn decode_serde<T: DeserializeOwned, B: Buf>(
    buf: &mut B,
    codec: StorageCodecKind,
) -> Result<T, StorageDecodeError> {
    match codec {
        StorageCodecKind::FlexbuffersSerde => decode_serde_from_flexbuffers(buf)
            .map_err(|err| StorageDecodeError::DecodeValue(err.into())),
        StorageCodecKind::BincodeSerde => decode_serde_from_bincode(buf)
            .map_err(|err| StorageDecodeError::DecodeValue(err.into())),
        StorageCodecKind::Json => {
            decode_serde_from_json(buf).map_err(|err| StorageDecodeError::DecodeValue(err.into()))
        }
        codec => Err(StorageDecodeError::UnsupportedCodecKind(codec)),
    }
}

/// Utility method to decode a [`DeserializeOwned`] type from flexbuffers using serde.
fn decode_serde_from_flexbuffers<T: DeserializeOwned, B: Buf>(
    buf: &mut B,
) -> Result<T, flexbuffers::DeserializationError> {
    if buf.remaining() < mem::size_of::<u32>() {
        return Err(flexbuffers::DeserializationError::custom(format!(
            "insufficient data: expecting {} bytes for length",
            mem::size_of::<u32>()
        )));
    }
    let length = usize::try_from(buf.get_u32_le()).expect("u32 to fit into usize");

    if buf.remaining() < length {
        return Err(flexbuffers::DeserializationError::custom(format!(
            "insufficient data: expecting {length} bytes for flexbuffers"
        )));
    }

    if buf.chunk().len() >= length {
        let deserializer = flexbuffers::Reader::get_root(&buf.chunk()[..length])?;
        let result = serde_path_to_error::deserialize(deserializer).map_err(|err| {
            error!(%err, "Flexbuffers error at field {}", err.path());
            err.into_inner()
        })?;
        buf.advance(length);
        Ok(result)
    } else {
        // need to allocate contiguous buffer of length for flexbuffers
        let bytes = buf.copy_to_bytes(length);
        let deserializer = flexbuffers::Reader::get_root(bytes.chunk())?;
        let result = serde_path_to_error::deserialize(deserializer).map_err(|err| {
            error!(%err, "Flexbuffers error at field {}", err.path());
            err.into_inner()
        })?;
        Ok(result)
    }
}

/// Utility method to decode a [`DeserializeOwned`] type from bincode using serde.
fn decode_serde_from_bincode<T: DeserializeOwned, B: Buf>(
    buf: &mut B,
) -> Result<T, bincode::error::DecodeError> {
    let (result, length) =
        bincode::serde::decode_from_slice(buf.chunk(), bincode::config::standard())?;
    buf.advance(length);
    Ok(result)
}

/// Utility method to decode a [`DeserializeOwned`] type from Json using serde.
fn decode_serde_from_json<T: DeserializeOwned, B: Buf>(
    buf: &mut B,
) -> Result<T, serde_json::Error> {
    let deserializer = &mut serde_json::Deserializer::from_reader(buf.reader());
    let result = serde_path_to_error::deserialize(deserializer).map_err(|err| {
        error!(%err, "Json error at field {}", err.path());
        err.into_inner()
    })?;
    Ok(result)
}

pub fn decode_bilrost<T: bilrost::OwnedMessage, B: Buf>(buf: B) -> Result<T, StorageDecodeError> {
    T::decode(buf).map_err(|err| StorageDecodeError::DecodeValue(err.into()))
}
