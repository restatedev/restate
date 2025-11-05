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
pub use restate_ty::storage::*;

use chrono::Utc;

use crate::journal_v2::raw::{RawEntry, RawEntryError, TryFromEntry};
use crate::journal_v2::{Decoder, EntryMetadata, EntryType};
use crate::time::MillisSinceEpoch;

/// Implements the [`StorageEncode`] and [`StorageDecode`] by encoding/decoding the implementing
/// type using [`flexbuffers`] and [`serde`].
#[macro_export]
macro_rules! flexbuffers_storage_encode_decode {
    ($name:tt) => {
        impl restate_ty::storage::StorageEncode for $name {
            fn default_codec(&self) -> restate_ty::storage::StorageCodecKind {
                restate_ty::storage::StorageCodecKind::FlexbuffersSerde
            }

            fn encode(
                &self,
                buf: &mut ::bytes::BytesMut,
            ) -> Result<(), restate_ty::storage::StorageEncodeError> {
                $crate::storage::encode::encode_serde(self, buf, self.default_codec())
            }
        }

        impl restate_ty::storage::StorageDecode for $name {
            fn decode<B: ::bytes::Buf>(
                buf: &mut B,
                kind: restate_ty::storage::StorageCodecKind,
            ) -> Result<Self, restate_ty::storage::StorageDecodeError>
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
