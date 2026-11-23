// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU64;

use bytes::{BufMut, BytesMut};
use restate_types::blob::{BlobId, BlobKind};
use restate_types::storage::{StorageCodec, StorageDecode, StorageEncode};

use crate::{Result, StorageError};

pub trait ReadBlobTable {
    fn get_blob<D: BlobDecoder>(
        &mut self,
        blob_id: BlobId,
    ) -> impl Future<Output = Result<Option<D::Type>>> + Send;
}

pub trait WriteBlobTable {
    /// Returns the size in bytes of the stored blob payload, excluding the blob header.
    fn put_blob<B: BlobEncode>(&mut self, blob_id: BlobId, blob: Blob<B>) -> Result<u64>;

    /// Once a blob's ref count reaches 0 it is marked for deletion and later
    /// ref count updates on it are no-ops.
    fn increment_ref_count(&mut self, blob_id: BlobId, count: u16);
    /// Reaching a ref count of 0 marks the blob for deletion; it is never revived.
    fn decrement_ref_count(&mut self, blob_id: BlobId, count: u16);
}

#[derive(Debug)]
pub struct BlobBuilder {
    kind: BlobKind,
    ref_count: NonZeroU64,
}

impl BlobBuilder {
    pub fn new(kind: BlobKind) -> Self {
        Self {
            kind,
            ref_count: NonZeroU64::MIN,
        }
    }

    pub fn with_ref_count(mut self, count: NonZeroU64) -> Self {
        self.ref_count = count;
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
    ref_count: NonZeroU64,
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

    fn ref_count(&self) -> u64 {
        self.ref_count.get()
    }
}

pub trait Header {
    fn kind(&self) -> BlobKind;
    fn ref_count(&self) -> u64;
}

// A decoder that return raw parts of the blob including kind, ref_count, and raw payload
pub struct RawParts;

impl BlobDecoder for RawParts {
    type Type = (BlobKind, u64, Vec<u8>);

    fn decode_blob(header: impl Header, raw: &[u8]) -> Result<Self::Type> {
        Ok((header.kind(), header.ref_count(), raw.to_vec()))
    }
}
