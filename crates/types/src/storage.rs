// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod decode;
pub mod encode;

use std::mem;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::Utc;
use downcast_rs::{DowncastSync, impl_downcast};
use serde::Deserialize;
use tracing::error;

use restate_encoding::{BilrostAs, NetSerde};

use crate::errors::GenericError;
use crate::journal_v2::raw::{RawEntry, RawEntryError, TryFromEntry};
use crate::journal_v2::{Decoder, EntryMetadata, EntryType};
use crate::time::MillisSinceEpoch;

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

#[derive(
    Debug, Copy, Clone, strum::FromRepr, derive_more::Display, PartialEq, Eq, bilrost::Enumeration,
)]
#[repr(u8)]
pub enum StorageCodecKind {
    /// plain old protobuf
    Protobuf = 1,
    /// flexbuffers + serde (length-prefixed)
    FlexbuffersSerde = 2,
    /// length-prefixed raw-bytes. length is u32
    LengthPrefixedRawBytes = 3,
    /// bincode (with serde compatibility mode, no length prefix)
    BincodeSerde = 4,
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
    fn from(value: StorageCodecKind) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for StorageCodecKind {
    type Error = StorageDecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        StorageCodecKind::from_repr(value).ok_or(StorageDecodeError::ReadingCodec(format!(
            "unknown discriminant '{value}'"
        )))
    }
}

/// Codec which encodes [`StorageEncode`] implementations by first writing the
/// [`StorageEncode::default_codec`] byte and then encoding the value part via
/// [`StorageEncode::encode`].
///
/// To decode a value, the codec first reads the codec bytes and then calls
/// [`StorageDecode::decode`] providing the read codec.
pub struct StorageCodec;

impl StorageCodec {
    pub fn encode<T: StorageEncode + ?Sized>(
        value: &T,
        buf: &mut BytesMut,
    ) -> Result<(), StorageEncodeError> {
        // write codec
        buf.put_u8(value.default_codec().into());
        // encode value
        value.encode(buf)
    }

    pub fn encode_and_split<T: StorageEncode + ?Sized>(
        value: &T,
        buf: &mut BytesMut,
    ) -> Result<BytesMut, StorageEncodeError> {
        Self::encode(value, buf)?;
        Ok(buf.split())
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

static_assertions::assert_obj_safe!(StorageEncode);

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

/// Implements the [`StorageEncode`] and [`StorageDecode`] by encoding/decoding the implementing
/// type using [`flexbuffers`] and [`serde`].
#[macro_export]
macro_rules! flexbuffers_storage_encode_decode {
    ($name:tt) => {
        impl $crate::storage::StorageEncode for $name {
            fn default_codec(&self) -> $crate::storage::StorageCodecKind {
                $crate::storage::StorageCodecKind::FlexbuffersSerde
            }

            fn encode(
                &self,
                buf: &mut ::bytes::BytesMut,
            ) -> Result<(), $crate::storage::StorageEncodeError> {
                $crate::storage::encode::encode_serde(self, buf, self.default_codec())
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
                $crate::storage::decode::decode_serde(buf, kind).map_err(|err| {
                    ::tracing::error!(%err, "{} decode failure (decoding {})", kind, stringify!($name));
                    err
                })

            }
        }
    };
}

/// A polymorphic container of a buffer or a cached storage-encodeable object
#[derive(Clone, derive_more::Debug, BilrostAs)]
#[bilrost_as(dto::PolyBytes)]
pub enum PolyBytes {
    /// Raw bytes backed by (Bytes), so it's cheap to clone
    #[debug("Bytes({} bytes)", _0.len())]
    Bytes(bytes::Bytes),
    /// A cached deserialized value that can be downcasted to the original type
    #[debug("Typed")]
    Typed(Arc<dyn StorageEncode>),
}

/// Required by the BilrostAs type for
/// the `empty state`
impl Default for PolyBytes {
    fn default() -> Self {
        Self::Bytes(bytes::Bytes::default())
    }
}
// implement NetSerde for PolyBytes manually
impl NetSerde for PolyBytes {}

impl PolyBytes {
    /// Returns true if we are holding raw encoded bytes
    pub fn is_encoded(&self) -> bool {
        matches!(self, PolyBytes::Bytes(_))
    }

    pub fn estimated_encode_size(&self) -> usize {
        match self {
            PolyBytes::Bytes(bytes) => bytes.len(),
            PolyBytes::Typed(_) => {
                // constant, assumption based on base envelope size of ~600 bytes.
                // todo: use StorageEncode trait to get an actual estimate based
                // on the underlying type
                2_048 // 2KiB
            }
        }
    }
}

impl StorageEncode for PolyBytes {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
        match self {
            PolyBytes::Bytes(bytes) => buf.put_slice(bytes.as_ref()),
            PolyBytes::Typed(typed) => {
                StorageCodec::encode(&**typed, buf)?;
            }
        };
        Ok(())
    }

    fn default_codec(&self) -> StorageCodecKind {
        StorageCodecKind::FlexbuffersSerde
    }
}

/// SerializeAs/DeserializeAs to implement ser/de trait for [`PolyBytes`]
/// Use it with `#[serde(with = "serde_with::As::<EncodedPolyBytes>")]`.
pub struct EncodedPolyBytes {}

impl serde_with::SerializeAs<PolyBytes> for EncodedPolyBytes {
    fn serialize_as<S>(source: &PolyBytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match source {
            PolyBytes::Bytes(bytes) => serializer.serialize_bytes(bytes.as_ref()),
            PolyBytes::Typed(typed) => {
                // todo: estimate size to avoid re allocations
                let mut buf = BytesMut::new();
                StorageCodec::encode(&**typed, &mut buf).expect("record serde is infallible");
                serializer.serialize_bytes(buf.as_ref())
            }
        }
    }
}

impl<'de> serde_with::DeserializeAs<'de, PolyBytes> for EncodedPolyBytes {
    fn deserialize_as<D>(deserializer: D) -> Result<PolyBytes, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf = Bytes::deserialize(deserializer)?;
        Ok(PolyBytes::Bytes(buf))
    }
}

static_assertions::assert_impl_all!(PolyBytes: Send, Sync);

/// Enable simple serialization of String types as length-prefixed byte slice
impl StorageEncode for String {
    fn default_codec(&self) -> StorageCodecKind {
        StorageCodecKind::LengthPrefixedRawBytes
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
        let my_bytes = self.as_bytes();
        buf.put_u32_le(u32::try_from(my_bytes.len()).map_err(|_| {
            StorageEncodeError::EncodeValue(
                anyhow::anyhow!("only support serializing types of size <= 4GB").into(),
            )
        })?);
        if buf.remaining_mut() < my_bytes.len() {
            return Err(StorageEncodeError::EncodeValue(
                anyhow::anyhow!(format!(
                    "not enough buffer space to serialize value;\
                        required {} bytes but free capacity was {}",
                    my_bytes.len(),
                    buf.remaining_mut()
                ))
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
                if buf.remaining() < mem::size_of::<u32>() {
                    return Err(StorageDecodeError::DecodeValue(
                        anyhow::anyhow!(
                            "insufficient data: expecting {} bytes for length",
                            mem::size_of::<u32>()
                        )
                        .into(),
                    ));
                }
                let length = usize::try_from(buf.get_u32_le()).expect("u32 to fit into usize");

                if buf.remaining() < length {
                    return Err(StorageDecodeError::DecodeValue(
                        anyhow::anyhow!(
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
        buf.put_u32_le(u32::try_from(self.len()).map_err(|_| {
            StorageEncodeError::EncodeValue(
                anyhow::anyhow!("only support serializing types of size <= 4GB").into(),
            )
        })?);
        if buf.remaining_mut() < self.len() {
            return Err(StorageEncodeError::EncodeValue(
                anyhow::anyhow!(format!(
                    "not enough buffer space to serialize value;\
                        required {} bytes but free capacity was {}",
                    self.len(),
                    buf.remaining_mut()
                ))
                .into(),
            ));
        }
        buf.put_slice(&self[..]);
        Ok(())
    }
}

/// A marker stored in the storage
///
/// The marker is used to sanity-check if the storage is correctly initialized and whether
/// we lost the database or not.
///
/// The marker is stored as Json to help with debugging.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StorageMarker<T> {
    id: T,
    created_at: chrono::DateTime<Utc>,
}

impl<T> StorageMarker<T>
where
    T: serde::Serialize + for<'a> serde::Deserialize<'a>,
{
    pub fn new(id: T) -> Self {
        Self {
            id,
            created_at: Utc::now(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("infallible serde")
    }

    pub fn from_slice(data: impl AsRef<[u8]>) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data.as_ref())
    }

    pub fn id(&self) -> &T {
        &self.id
    }

    pub fn created_at(&self) -> chrono::DateTime<Utc> {
        self.created_at
    }
}

mod dto {
    use bytes::{Bytes, BytesMut};

    use super::StorageCodec;

    #[derive(bilrost::Message)]
    pub struct PolyBytes {
        #[bilrost(1)]
        inner: Bytes,
    }

    impl From<&super::PolyBytes> for PolyBytes {
        fn from(value: &super::PolyBytes) -> Self {
            let inner = match value {
                super::PolyBytes::Bytes(bytes) => bytes.clone(),
                super::PolyBytes::Typed(typed) => {
                    let mut buf = BytesMut::new();
                    StorageCodec::encode(&**typed, &mut buf).expect("PolyBytes to serialize");
                    buf.freeze()
                }
            };

            Self { inner }
        }
    }

    impl From<PolyBytes> for super::PolyBytes {
        fn from(value: PolyBytes) -> Self {
            Self::Bytes(value.inner)
        }
    }
}

/// The stored raw entry headers are created when applying/storing the journal entry to capture
/// metadata that can be derived deterministically when applying the raw journal entry.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredRawEntryHeader {
    pub append_time: MillisSinceEpoch,
}

impl StoredRawEntryHeader {
    pub fn new(append_time: MillisSinceEpoch) -> Self {
        Self { append_time }
    }
}

/// Container of the raw entry that is enriched with additional metadata derived from Bifrost before
/// storing it in the partition processor storage.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredRawEntry {
    header: StoredRawEntryHeader,
    pub inner: RawEntry,
}

impl StoredRawEntry {
    pub fn new(header: StoredRawEntryHeader, inner: impl Into<RawEntry>) -> Self {
        Self {
            header,
            inner: inner.into(),
        }
    }

    pub fn header(&self) -> &StoredRawEntryHeader {
        &self.header
    }

    pub fn header_mut(&mut self) -> &mut StoredRawEntryHeader {
        &mut self.header
    }
}

impl StoredRawEntry {
    pub fn decode<D: Decoder, T: TryFromEntry>(&self) -> Result<T, RawEntryError> {
        Ok(<T as TryFromEntry>::try_from(D::decode_entry(
            &self.inner,
        )?)?)
    }
}

impl EntryMetadata for StoredRawEntry {
    fn ty(&self) -> EntryType {
        self.inner.ty()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_polybytes() {
        let bytes = PolyBytes::Bytes(Bytes::from_static(b"hello"));
        assert_eq!(format!("{bytes:?}"), "Bytes(5 bytes)");
        let typed = PolyBytes::Typed(Arc::new("hello".to_string()));
        assert_eq!(format!("{typed:?}"), "Typed");
        // can be downcasted.
        let a: Arc<dyn StorageEncode> = Arc::new("hello".to_string());
        assert!(a.is::<String>());
    }
}
