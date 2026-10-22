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

use bytes::{BufMut, BytesMut};
use restate_types::blob::{BaseBlobId, BlobKind};
use restate_types::invocation::ResponseResult;
use restate_types::sharding::KeyRange;
use restate_types::storage::{StorageCodec, StorageDecode, StorageEncode};

use crate::protobuf_types::PartitionStoreProtobufValue;
use crate::{Result, StorageError};

#[derive(Debug, Default)]
pub struct BlobBuilder {
    header: InputHeader,
}

impl BlobBuilder {
    pub fn with_ref_count(mut self, count: NonZeroU16) -> Self {
        self.header.ref_count = count;
        self
    }

    /// Sets the blob kind. Mainly used for debugging, defaults to [`BlobKind::Unknown`].
    pub fn with_kind(mut self, kind: BlobKind) -> Self {
        self.header.kind = kind;
        self
    }

    pub fn typed<T: StorageEncode>(self, inner: &T) -> impl Blob {
        TypedBlob {
            header: self.header,
            inner,
        }
    }

    pub fn raw(self, inner: &[u8]) -> impl Blob {
        RawBlob {
            header: self.header,
            inner,
        }
    }
}

// By sealing the train we make sure the BlobBuilder is the
// only possible way to create a Blob and can verify the blob
// params when creating a blob

pub trait Blob: sealed::Sealed {
    fn header(&self) -> impl Header;
    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageError>;
}

pub trait Header {
    fn kind(&self) -> BlobKind;
    fn ref_count(&self) -> u16;
}

#[derive(Debug, Clone, Copy)]
struct InputHeader {
    kind: BlobKind,
    ref_count: NonZeroU16,
}

impl Default for InputHeader {
    fn default() -> Self {
        Self {
            kind: BlobKind::Unknown,
            ref_count: NonZeroU16::MIN,
        }
    }
}

impl Header for InputHeader {
    fn kind(&self) -> BlobKind {
        self.kind
    }

    fn ref_count(&self) -> u16 {
        self.ref_count.get()
    }
}

impl Header for &InputHeader {
    fn kind(&self) -> BlobKind {
        self.kind
    }

    fn ref_count(&self) -> u16 {
        self.ref_count.get()
    }
}

struct TypedBlob<'a, T> {
    header: InputHeader,
    inner: &'a T,
}

impl<'a, T> sealed::Sealed for TypedBlob<'a, T> {}

impl<'a, T> Blob for TypedBlob<'a, T>
where
    T: StorageEncode,
{
    fn header(&self) -> impl Header {
        &self.header
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageError> {
        StorageCodec::encode(self.inner, buf).map_err(|err| StorageError::Generic(err.into()))
    }
}

struct RawBlob<'a> {
    header: InputHeader,
    inner: &'a [u8],
}

impl<'a> sealed::Sealed for RawBlob<'a> {}

impl<'a> Blob for RawBlob<'a> {
    fn header(&self) -> impl Header {
        &self.header
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageError> {
        buf.put(self.inner.as_ref());
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

pub struct RawDecoder;

impl BlobDecoder for RawDecoder {
    type Type = (BlobKind, u16, Vec<u8>);

    fn decode_blob(header: impl Header, raw: &[u8]) -> Result<Self::Type> {
        Ok((header.kind(), header.ref_count(), raw.to_vec()))
    }
}

pub trait ReadBlobTable {
    fn get_blob<D: BlobDecoder>(
        &mut self,
        blob_id: BaseBlobId,
    ) -> impl Future<Output = Result<Option<D::Type>>> + Send;
}

impl PartitionStoreProtobufValue for ResponseResult {
    type ProtobufType = crate::protobuf_types::v1::ResponseResult;
}

/// A raw blob as stored, returned by [`ScanBlobTable`].
#[derive(Debug, Clone, Copy)]
pub struct BlobEntry<'a> {
    pub id: BaseBlobId,
    pub kind: BlobKind,
    pub ref_count: u16,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone)]
pub enum ScanBlobTableRange {
    PartitionKey(KeyRange),
    BlobIdSet(BTreeSet<BaseBlobId>),
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

pub trait WriteBlobTable {
    fn put_blob(&mut self, blob_id: BaseBlobId, blob: impl Blob) -> Result<()>;

    fn increment_ref_count(&mut self, blob_id: BaseBlobId, count: u16);
    fn decrement_ref_count(&mut self, blob_id: BaseBlobId, count: u16);
}

mod sealed {
    pub trait Sealed {}
}
