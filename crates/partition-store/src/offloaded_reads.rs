// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Point reads that don't wait for the disk on the calling task.
//!
//! A read is answered on the calling task when what it reads is in memory.
//! Otherwise a storage worker reads it, and the calling task gets the value
//! from there. Either way the value isn't copied: it stays pinned in RocksDB's
//! memory until it's dropped.

use restate_core::io::IoExecutor;
use restate_rocksdb::{CachedOpFn, Gated, OpContext, RocksError, StorageOp};
use restate_storage_api::protobuf_types::{PartitionStoreProtobufValue, ProtobufStorageWrapper};
use restate_storage_api::{Result, StorageError};
use restate_types::storage::StorageCodec;

use crate::keys::{EncodeTableKey, KeyKind};
use crate::partition_store::StorageRead;

/// Storage whose point reads leave their disk reads to a storage worker.
pub(crate) trait OffloadedRead: StorageRead {
    /// The value of a key. It points into RocksDB's memory and keeps that
    /// memory in place until it's dropped.
    type Value: AsRef<[u8]> + Send + 'static;

    /// Like [`StorageRead::get`], but if the value isn't in memory, a storage
    /// worker reads it, so that the calling task doesn't wait for the disk.
    ///
    /// The read from memory happens before this returns. The returned future
    /// resolves to the value, at once if it was in memory. A worker of
    /// `executor` reads it otherwise, with its own copy of `key`. `name` labels
    /// the read's perf metrics.
    fn get_cached_or_offload<'a>(
        &'a self,
        name: &'static str,
        key_kind: KeyKind,
        key: &'a [u8],
        executor: &(impl IoExecutor + Sync),
    ) -> Result<impl Future<Output = Result<Option<Self::Value>>> + Send + 'static>;
}

/// Runs the point read `read` on the calling task if what it reads is in
/// memory, and on a worker of `executor` otherwise. `name` labels its read
/// perf metrics.
pub(crate) fn call_cached_or_offload<C, F, V, E>(
    read: StorageOp<C, F, Gated<'_>>,
    name: &'static str,
    executor: &E,
) -> Result<impl Future<Output = Result<Option<V>>> + Send + 'static>
where
    C: OpContext,
    F: CachedOpFn<C, Output = std::result::Result<Option<V>, RocksError>>,
    V: Send + 'static,
    E: IoExecutor,
{
    let read = read
        .with_read_perf(name)
        .call_cached_or_spawn(executor)
        .map_err(|_| StorageError::OperationalError)?;
    Ok(async move {
        read.await
            .map_err(|_| StorageError::OperationalError)?
            .map_err(|err| StorageError::Generic(err.into()))
    })
}

/// Like [`StorageRead::get_value_proto`], but reads with
/// [`OffloadedRead::get_cached_or_offload`]. `name` labels the read's perf
/// metrics.
pub(crate) async fn get_value_proto_cached_or_offload<S, K, V>(
    storage: &mut S,
    name: &'static str,
    key: K,
    executor: &(impl IoExecutor + Sync),
) -> Result<Option<V>>
where
    S: OffloadedRead,
    K: EncodeTableKey,
    V: PartitionStoreProtobufValue,
    <<V as PartitionStoreProtobufValue>::ProtobufType as TryInto<V>>::Error: Into<anyhow::Error>,
{
    let key_buf = storage.cleared_key_buffer_mut(key.serialized_length());
    key.serialize_to(key_buf);
    let key = key_buf.split();

    let Some(value) = storage
        .get_cached_or_offload(name, K::KEY_KIND, &key, executor)?
        .await?
    else {
        return Ok(None);
    };
    let mut value = value.as_ref();
    let decoded: ProtobufStorageWrapper<V::ProtobufType> =
        StorageCodec::decode(&mut value).map_err(|err| StorageError::Generic(err.into()))?;
    let value = decoded
        .0
        .try_into()
        .map_err(|err| StorageError::Conversion(err.into()))?;
    Ok(Some(value))
}
