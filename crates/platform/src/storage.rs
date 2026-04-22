// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BufMut, BytesMut};
use downcast_rs::{DowncastSync, impl_downcast};

use restate_util_string::{ReString, format_restring};

use crate::errors::GenericError;

#[derive(Debug, thiserror::Error)]
pub enum StorageEncodeError {
    #[error("encoding failed: {0}")]
    EncodeValue(GenericError),
    #[error("only support serializing types of size <= 4GB, got: {0} bytes")]
    SizeOverflow(usize),
}

#[derive(Debug, thiserror::Error)]
pub enum StorageDecodeError {
    #[error("failed reading codec: {0}")]
    ReadingCodec(ReString),
    #[error("decoding failed: {0}")]
    DecodeValue(GenericError),
    #[error("unsupported codec kind: {0}")]
    UnsupportedCodecKind(StorageCodecKind),
}

/// Trait to encode a value using the specified [`Self::default_codec`]. The trait is used by the
/// [`StorageCodec`] to first write the codec byte and then the serialized value via
/// [`Self::encode`].
///
/// # Important
/// The [`Self::encode`] implementation should use the codec specified by [`Self::default_codec`].
pub trait StorageEncode: DowncastSync {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError>;

    /// Codec which is used when encode new values.
    fn default_codec(&self) -> StorageCodecKind;
}

impl_downcast!(sync StorageEncode);

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

#[derive(Debug, Copy, Clone, derive_more::Display, PartialEq, Eq)]
#[cfg_attr(feature = "bilrost", derive(bilrost::Enumeration))]
#[repr(u8)]
pub enum StorageCodecKind {
    /// plain old protobuf
    Protobuf = 1,
    /// flexbuffers + serde (length-prefixed)
    FlexbuffersSerde = 2,
    /// length-prefixed raw-bytes. length is u32
    LengthPrefixedRawBytes = 3,
    // Note: Discriminant 4 was previously `BincodeSerde`, which was added to prepare for future
    // use but was never actually used for persistent storage. It has been removed because bincode
    // is no longer maintained. The discriminant is intentionally skipped to prevent accidental
    // reuse.
    /// Json (no length prefix)
    Json = 5,
    /// Bilrost (no length-prefixed)
    Bilrost = 6,
    /// A custom encoding that does not rely on any of the standard encoding formats
    /// supported by the [`encode`] and [`decode`] modules.
    ///
    /// When using this variant, the encoding and decoding logic is entirely defined
    /// by the implementation of the [`StorageEncode`] and [`StorageDecode`] traits.
    ///
    /// While you may still use utility functions from the [`encode`] and [`decode`] modules,
    /// it is up to your implementation to decide how (or if) to use them, and how the final
    /// byte representation is constructed.
    Custom = 7,
}

impl From<StorageCodecKind> for u8 {
    #[inline]
    fn from(value: StorageCodecKind) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for StorageCodecKind {
    type Error = StorageDecodeError;

    #[inline]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Protobuf),
            2 => Ok(Self::FlexbuffersSerde),
            3 => Ok(Self::LengthPrefixedRawBytes),
            5 => Ok(Self::Json),
            6 => Ok(Self::Bilrost),
            7 => Ok(Self::Custom),
            value => Err(StorageDecodeError::ReadingCodec(format_restring!(
                "unknown discriminant '{value}'"
            ))),
        }
    }
}

/// Enable simple serialization of String types as length-prefixed byte slice
impl StorageEncode for String {
    fn default_codec(&self) -> StorageCodecKind {
        StorageCodecKind::LengthPrefixedRawBytes
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
        let my_bytes = self.as_bytes();
        buf.put_u32_le(
            u32::try_from(my_bytes.len())
                .map_err(|_| StorageEncodeError::SizeOverflow(my_bytes.len()))?,
        );
        if buf.remaining_mut() < my_bytes.len() {
            return Err(StorageEncodeError::EncodeValue(
                format!(
                    "not enough buffer space to serialize value;\
                        required {} bytes but free capacity was {}",
                    my_bytes.len(),
                    buf.remaining_mut()
                )
                .into(),
            ));
        }
        buf.put_slice(my_bytes);
        Ok(())
    }
}
impl StorageDecode for String {
    fn decode<B: ::bytes::Buf>(
        buf: &mut B,
        kind: StorageCodecKind,
    ) -> Result<Self, StorageDecodeError>
    where
        Self: Sized,
    {
        match kind {
            StorageCodecKind::LengthPrefixedRawBytes => {
                if buf.remaining() < std::mem::size_of::<u32>() {
                    return Err(StorageDecodeError::DecodeValue(
                        format!(
                            "insufficient data: expecting {} bytes for length",
                            std::mem::size_of::<u32>()
                        )
                        .into(),
                    ));
                }
                let length = usize::try_from(buf.get_u32_le()).expect("u32 to fit into usize");

                if buf.remaining() < length {
                    return Err(StorageDecodeError::DecodeValue(
                        format!(
                            "insufficient data: expecting {} bytes for flexbuffers",
                            length
                        )
                        .into(),
                    ));
                }

                let bytes = buf.take(length);
                Ok(String::from_utf8_lossy(bytes.chunk()).to_string())
            }
            codec => Err(StorageDecodeError::UnsupportedCodecKind(codec)),
        }
    }
}

// Enable simple serialization of Bytes types as length-prefixed byte slice
impl StorageEncode for bytes::Bytes {
    fn default_codec(&self) -> StorageCodecKind {
        StorageCodecKind::LengthPrefixedRawBytes
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
        buf.put_u32_le(
            u32::try_from(self.len()).map_err(|_| StorageEncodeError::SizeOverflow(self.len()))?,
        );
        if buf.remaining_mut() < self.len() {
            return Err(StorageEncodeError::EncodeValue(
                format!(
                    "not enough buffer space to serialize value;\
                        required {} bytes but free capacity was {}",
                    self.len(),
                    buf.remaining_mut()
                )
                .into(),
            ));
        }
        buf.put_slice(&self[..]);
        Ok(())
    }
}
