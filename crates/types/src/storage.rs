// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::errors::GenericError;
use bytes::{Buf, BufMut};
use std::mem;

#[derive(Debug, thiserror::Error)]
pub enum StorageEncodeError {
    #[error("encoding failed: {0}")]
    EncodeValue(GenericError),
}

#[derive(Debug, thiserror::Error)]
pub enum StorageDecodeError {
    #[error("failed reading codec: {0}")]
    ReadingCodec(String),
    #[error("decoding failed: {0}")]
    DecodeValue(GenericError),
    #[error("unsupported codec kind: {0}")]
    UnsupportedCodecKind(StorageCodecKind),
}

#[derive(Debug, strum_macros::FromRepr, derive_more::Display)]
#[repr(u8)]
pub enum StorageCodecKind {
    // plain old protobuf
    Protobuf = 1,
    // flexbuffers + serde
    FlexbuffersSerde = 2,
}

impl From<StorageCodecKind> for u8 {
    fn from(value: StorageCodecKind) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for StorageCodecKind {
    type Error = StorageDecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        StorageCodecKind::from_repr(value).ok_or(StorageDecodeError::ReadingCodec(format!(
            "unknown discriminant '{}'",
            value
        )))
    }
}

/// Codec which encodes [`StorageEncode`] implementations by first writing the
/// [`StorageEncode::DEFAULT_CODEC`] byte and then encoding the value part via
/// [`StorageEncode::encode`].
///
/// To decode a value, the codec first reads the codec bytes and then calls
/// [`StorageDecode::decode`] providing the read codec.
pub struct StorageCodec;

impl StorageCodec {
    pub fn encode<T: StorageEncode, B: BufMut>(
        value: &T,
        buf: &mut B,
    ) -> Result<(), StorageEncodeError> {
        // write codec
        buf.put_u8(T::DEFAULT_CODEC.into());
        // encode value
        value.encode(buf)
    }

    pub fn decode<T: StorageDecode, B: Buf>(buf: &mut B) -> Result<T, StorageDecodeError> {
        if buf.remaining() < mem::size_of::<u8>() {
            return Err(StorageDecodeError::ReadingCodec(format!(
                "remaining bytes in buf '{}' < version bytes '{}'",
                buf.remaining(),
                mem::size_of::<u8>()
            )));
        }

        // read version
        let codec = StorageCodecKind::try_from(buf.get_u8())?;

        // decode value
        T::decode(buf, codec)
    }
}

/// Trait to encode a value using the specified [`Self::DEFAULT_CODEC`]. The trait is used by the
/// [`StorageCodec`] to first write the codec byte and then the serialized value via
/// [`Self::encode`].
///
/// # Important
/// The [`Self::encode`] implementation should use the codec specified by [`Self::DEFAULT_CODEC`].
pub trait StorageEncode {
    /// Codec which is used when encode new values.
    const DEFAULT_CODEC: StorageCodecKind;

    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), StorageEncodeError>;
}

/// Trait to decode a value given the [`StorageCodecKind`]. This trait is used by the
/// [`StorageCodec`] to decode a value after reading the used storage codec.
///
/// # Important
/// To support codec evolution, this trait implementation needs to be able to decode values encoded
/// with any previously used codec.
pub trait StorageDecode {
    fn decode<B: Buf>(buf: &mut B, kind: StorageCodecKind) -> Result<Self, StorageDecodeError>
    where
        Self: Sized;
}

impl<T: StorageEncode> StorageEncode for &T {
    const DEFAULT_CODEC: StorageCodecKind = T::DEFAULT_CODEC;

    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), StorageEncodeError> {
        (*self).encode(buf)
    }
}

impl<T: StorageEncode> StorageEncode for &mut T {
    const DEFAULT_CODEC: StorageCodecKind = T::DEFAULT_CODEC;

    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), StorageEncodeError> {
        T::encode(*self, buf)
    }
}

impl<T: StorageEncode> StorageEncode for Box<T> {
    const DEFAULT_CODEC: StorageCodecKind = T::DEFAULT_CODEC;

    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), StorageEncodeError> {
        T::encode(&**self, buf)
    }
}

impl<T: StorageDecode> StorageDecode for Box<T> {
    fn decode<B: Buf>(buf: &mut B, kind: StorageCodecKind) -> Result<Self, StorageDecodeError>
    where
        Self: Sized,
    {
        T::decode(buf, kind).map(Box::new)
    }
}

/// Implements the [`StorageEncode`] and [`StorageDecode`] by encoding/decoding the implementing
/// type using [`flexbuffers`] and [`serde`].
#[macro_export]
macro_rules! flexbuffers_storage_encode_decode {
    ($name:tt) => {
        impl $crate::storage::StorageEncode for $name {
            const DEFAULT_CODEC: $crate::storage::StorageCodecKind =
                $crate::storage::StorageCodecKind::FlexbuffersSerde;

            fn encode<B: ::bytes::BufMut>(
                &self,
                buf: &mut B,
            ) -> Result<(), $crate::storage::StorageEncodeError> {
                let vec = flexbuffers::to_vec(self)
                    .map_err(|err| $crate::storage::StorageEncodeError::EncodeValue(err.into()))?;
                // write the length
                buf.put_u32_le(u32::try_from(vec.len()).map_err(|_| {
                    $crate::storage::StorageEncodeError::EncodeValue(
                        "only support serializing types of size <= 4GB".into(),
                    )
                })?);
                buf.put(&vec[..]);
                Ok(())
            }
        }

        impl $crate::storage::StorageDecode for $name {
            fn decode<B: ::bytes::Buf>(
                buf: &mut B,
                kind: $crate::storage::StorageCodecKind,
            ) -> Result<Self, $crate::storage::StorageDecodeError>
            where
                Self: Sized,
            {
                match kind {
                    $crate::storage::StorageCodecKind::FlexbuffersSerde => {
                        if buf.remaining() < 4 {
                            return Err($crate::storage::StorageDecodeError::DecodeValue(
                                "insufficient data: expecting 4 bytes for length".into(),
                            ));
                        }
                        let length =
                            usize::try_from(buf.get_u32_le()).expect("u32 to fit into usize");

                        if buf.remaining() < length {
                            return Err($crate::storage::StorageDecodeError::DecodeValue(
                                format!(
                                    "insufficient data: expecting {} bytes for flexbuffers",
                                    length
                                )
                                .into(),
                            ));
                        }

                        if buf.chunk().len() >= length {
                            let result = flexbuffers::from_slice(buf.chunk()).map_err(|err| {
                                $crate::storage::StorageDecodeError::DecodeValue(err.into())
                            })?;
                            buf.advance(length);

                            Ok(result)
                        } else {
                            // need to allocate contiguous buffer of length for flexbuffers
                            let mut bytes = buf.copy_to_bytes(length);

                            flexbuffers::from_slice(&mut bytes).map_err(|err| {
                                $crate::storage::StorageDecodeError::DecodeValue(err.into())
                            })
                        }
                    }
                    codec => Err($crate::storage::StorageDecodeError::UnsupportedCodecKind(
                        codec,
                    )),
                }
            }
        }
    };
}
