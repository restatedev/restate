// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use restate_platform::network::NetSerde;
// Re-export the traits from restate-platform
pub use restate_platform::storage::{
    StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError,
};

use restate_encoding::BilrostAs;
use restate_platform::memory::EstimatedMemorySize;
use restate_serde_util::ByteCount;
use restate_util_string::format_restring;

use crate::journal_v2::raw::{RawEntry, RawEntryError, TryFromEntry};
use crate::journal_v2::{Decoder, EntryMetadata, EntryType};
use crate::time::MillisSinceEpoch;

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
            return Err(StorageDecodeError::ReadingCodec(format_restring!(
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

/// Implements the [`StorageEncode`] and [`StorageDecode`] by encoding/decoding the implementing
/// type using [`bilrost`].
#[macro_export]
macro_rules! bilrost_storage_encode_decode {
    ($name:tt) => {
        impl $crate::storage::StorageEncode for $name {
            fn default_codec(&self) -> $crate::storage::StorageCodecKind {
                $crate::storage::StorageCodecKind::Bilrost
            }

            fn encode(
                &self,
                buf: &mut ::bytes::BytesMut,
            ) -> Result<(), $crate::storage::StorageEncodeError> {
                $crate::storage::encode::encode_bilrost(self, buf)
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
                debug_assert_eq!(kind, $crate::storage::StorageCodecKind::Bilrost);
                $crate::storage::decode::decode_bilrost(buf)
            }
        }
    };
}

/// A polymorphic container of a buffer or a cached storage-encodeable object
#[derive(Clone, derive_more::Debug, BilrostAs)]
#[bilrost_as(dto::PolyBytes)]
pub enum PolyBytes {
    /// Raw bytes backed by (Bytes), so it's cheap to clone
    #[debug("Bytes({})", ByteCount::from(_0.len()))]
    Bytes(bytes::Bytes),
    /// A cached deserialized value that can be downcasted to the original type
    #[debug("Typed")]
    Typed(Arc<dyn StorageEncode>),
    /// A cached deserialized value along with the raw bytes of the serialized value
    #[debug("Both({})", ByteCount::from(_1.len()))]
    Both(Arc<dyn StorageEncode>, bytes::Bytes),
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

impl EstimatedMemorySize for PolyBytes {
    fn estimated_memory_size(&self) -> usize {
        match self {
            PolyBytes::Bytes(bytes) => bytes.len(),
            PolyBytes::Both(_, bytes) => bytes.len(),
            PolyBytes::Typed(_) => {
                // constant, assumption based on base envelope size of ~600 bytes.
                // todo: use StorageEncode trait to get an actual estimate based
                // on the underlying type
                2_048 // 2KiB
            }
        }
    }
}

impl PolyBytes {
    pub fn estimated_encode_size(&self) -> usize {
        self.estimated_memory_size()
    }

    #[tracing::instrument(skip_all)]
    pub fn encode_to_bytes(&self, scratch: &mut BytesMut) -> Result<Bytes, StorageEncodeError> {
        match self {
            PolyBytes::Bytes(bytes) => Ok(bytes.clone()),
            PolyBytes::Both(_, bytes) => Ok(bytes.clone()),
            PolyBytes::Typed(typed) => {
                // note: this currently doesn't do a good job of estimating the size but it's better than
                // nothing.
                scratch.reserve(self.estimated_encode_size());
                StorageCodec::encode(&**typed, scratch)?;
                Ok(scratch.split().freeze())
            }
        }
    }
}

impl StorageEncode for PolyBytes {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
        match self {
            PolyBytes::Bytes(bytes) => buf.put_slice(bytes.as_ref()),
            PolyBytes::Both(_, bytes) => buf.put_slice(bytes.as_ref()),
            PolyBytes::Typed(typed) => {
                StorageCodec::encode(&**typed, buf)?;
            }
        };
        Ok(())
    }

    fn default_codec(&self) -> StorageCodecKind {
        // Note that this means nothing since encoding PolyBytes is opaque, as in,
        // the serialized value is always the raw bytes of the inner types.
        StorageCodecKind::FlexbuffersSerde
    }
}

static_assertions::assert_impl_all!(PolyBytes: Send, Sync);

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
                super::PolyBytes::Both(_, bytes) => bytes.clone(),
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
    pub header: StoredRawEntryHeader,
    pub inner: RawEntry,
}

impl StoredRawEntry {
    pub fn new(header: StoredRawEntryHeader, inner: impl Into<RawEntry>) -> Self {
        Self {
            header,
            inner: inner.into(),
        }
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
        assert_eq!(format!("{bytes:?}"), "Bytes(5 B)");
        let typed = PolyBytes::Typed(Arc::new("hello".to_string()));
        assert_eq!(format!("{typed:?}"), "Typed");
        // can be downcasted.
        let a: Arc<dyn StorageEncode> = Arc::new("hello".to_string());
        assert!(a.is::<String>());
    }
}
