// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::num::NonZeroU16;
use std::ops::RangeInclusive;

use bytes::{BufMut, BytesMut};
use restate_types::blob::{BlobId, BlobKind};
use restate_types::storage::{StorageCodec, StorageDecode, StorageEncode};
use restate_types::time::MillisSinceEpoch;

use crate::{Result, StorageError};

pub trait ReadBlobTable {
    fn get_blob<D: BlobDecoder>(
        &mut self,
        blob_id: BlobId,
    ) -> impl Future<Output = Result<Option<D::Type>>> + Send;
}

pub trait WriteBlobTable {
    fn put_blob<B: BlobEncode>(&mut self, blob_id: BlobId, blob: Blob<B>) -> Result<()>;

    fn increment_ref_count(&mut self, blob_id: BlobId, count: u16);
    fn decrement_ref_count(&mut self, blob_id: BlobId, count: u16);
}

#[derive(Debug)]
pub struct BlobBuilder {
    kind: BlobKind,
    ref_count: NonZeroU16,
}

impl Default for BlobBuilder {
    fn default() -> Self {
        Self {
            kind: BlobKind::Unknown,
            ref_count: NonZeroU16::MIN,
        }
    }
}

impl BlobBuilder {
    pub fn with_ref_count(mut self, count: NonZeroU16) -> Self {
        self.ref_count = count;
        self
    }

    /// Sets the blob kind. Defaults to [`BlobKind::Unknown`].
    pub fn with_kind(mut self, kind: BlobKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn build<B: BlobEncode>(self, inner: B) -> Blob<B> {
        Blob {
            kind: self.kind,
            ref_count: self.ref_count,
            inner,
        }
    }
}

pub trait BlobEncode {
    fn encode_blob(&self, buf: &mut BytesMut) -> Result<(), StorageError>;
}

impl<C> BlobEncode for &C
where
    C: StorageEncode,
{
    fn encode_blob(&self, buf: &mut BytesMut) -> Result<(), StorageError> {
        StorageCodec::encode(*self, buf).map_err(|err| StorageError::Generic(err.into()))
    }
}

impl BlobEncode for &[u8] {
    fn encode_blob(&self, buf: &mut BytesMut) -> Result<(), StorageError> {
        buf.put_slice(self);
        Ok(())
    }
}

pub trait BlobDecoder {
    type Type;

    fn decode_blob(header: impl Header, raw: &[u8]) -> Result<Self::Type>;
}

impl<T: StorageDecode> BlobDecoder for T {
    type Type = Self;

    fn decode_blob(_header: impl Header, mut raw: &[u8]) -> Result<Self::Type> {
        StorageCodec::decode(&mut raw).map_err(|err| StorageError::Generic(err.into()))
    }
}

// BlobBuilder is the only possible way to create a Blob
// and can verify the blob params when creating a blob

pub struct Blob<B> {
    kind: BlobKind,
    ref_count: NonZeroU16,
    inner: B,
}

impl<B> Blob<B> {
    pub fn inner(&self) -> &B {
        &self.inner
    }

    pub fn into_inner(self) -> B {
        self.inner
    }
}

impl<B> Header for Blob<B> {
    fn kind(&self) -> BlobKind {
        self.kind
    }

    fn ref_count(&self) -> u16 {
        self.ref_count.get()
    }
}

pub trait Header {
    fn kind(&self) -> BlobKind;
    fn ref_count(&self) -> u16;
}

// A decoder that return raw parts of the blob including kind, ref_count, and raw payload
pub struct RawParts;

impl BlobDecoder for RawParts {
    type Type = (BlobKind, u16, Vec<u8>);

    fn decode_blob(header: impl Header, raw: &[u8]) -> Result<Self::Type> {
        Ok((header.kind(), header.ref_count(), raw.to_vec()))
    }
}

/// A raw blob as stored, returned by [`ScanBlobTable`].
#[derive(Debug, Clone, Copy)]
pub struct BlobEntry<'a> {
    pub id: BlobId,
    pub kind: BlobKind,
    pub ref_count: u16,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone)]
pub enum ScanBlobTableRange {
    /// Every blob of the partition created within this inclusive range.
    CreatedAt(RangeInclusive<MillisSinceEpoch>),
    BlobIdSet(BTreeSet<BlobId>),
}

pub trait ScanBlobTable {
    fn for_each_blob<
        F: for<'a> FnMut(BlobEntry<'a>) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: ScanBlobTableRange,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}
