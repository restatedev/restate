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
use std::ops::ControlFlow;

use bytes::BytesMut;
use futures::{FutureExt, Stream};
use rocksdb::ReadOptions;

use restate_rocksdb::{Priority, RocksDbReadPerfGuard, StorageTaskKind};
use restate_storage_api::invocation_status_table::{
    InvocationLite, InvocationStatus, InvocationStatusDiscriminants, InvokedInvocationStatusLite,
    ReadInvocationStatusTable, ScanInvocationStatusTable, ScanInvocationStatusTableRange,
    WriteInvocationStatusTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_util_string::format_restring;

use crate::TableScan::ScanPartitionKeyRange;
use crate::keys::{DecodeTableKey, EncodeTableKey, KeyKind, define_table_key};
use crate::scan::TableScan;
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess, TableKind, break_on_err};

define_table_key!(
    TableKind::InvocationStatus,
    KeyKind::InvocationStatus,
    InvocationStatusKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid
    )
);

impl InvocationStatusKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + std::mem::size_of::<PartitionKey>()
            + InvocationUuid::RAW_BYTES_LEN
    }
}

/// Maximum number of invocation-status keys passed to one RocksDB multi-get call.
/// This one is set to a low value due to the possibility of loading a large value from rocksdb
/// when the invocation holds a large output payload. This is an unfortunate side effect of storing
/// the output payload into invocation-status!
const INVOCATION_STATUS_MULTI_GET_BATCH_SIZE: usize = 25;

#[inline]
fn create_invocation_status_key(invocation_id: &InvocationId) -> InvocationStatusKey {
    InvocationStatusKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
    }
}

#[inline]
fn invocation_id_from_key_bytes<B: bytes::Buf>(bytes: &mut B) -> crate::Result<InvocationId> {
    let key = InvocationStatusKey::deserialize_from(bytes)?;
    Ok(InvocationId::from_parts(
        key.partition_key,
        key.invocation_uuid,
    ))
}

fn multi_get_invocation_status_lazy<E, F>(
    store: &PartitionStore,
    ids: BTreeSet<InvocationId>,
    mut f: F,
) -> impl Future<Output = Result<()>> + Send
where
    E: Into<anyhow::Error> + 'static,
    F: for<'a> FnMut(
            (InvocationId, &'a InvocationStatusV2Lazy<'a>),
        ) -> ControlFlow<std::result::Result<(), E>>
        + Send
        + Sync
        + 'static,
{
    const KEY_LEN: usize = InvocationStatusKey::serialized_length_fixed();

    let rocksdb = store.partition_db().rocksdb().clone();
    let cf_name: restate_rocksdb::CfName = store.partition_db().partition().cf_name().into();

    async move {
        rocksdb
            .run_background_read_op(
                "df-for-each-invocation-status",
                StorageTaskKind::MultiGet,
                Priority::Low,
                move |raw_db| -> Result<()> {
                    let Some(cf) = raw_db.cf_handle(cf_name.as_str()) else {
                        return Err(StorageError::Generic(anyhow::anyhow!(
                            "column family {cf_name} not found for invocation-status multi-get"
                        )));
                    };

                    let batch_capacity = ids.len().min(INVOCATION_STATUS_MULTI_GET_BATCH_SIZE);
                    let mut key_buf = BytesMut::with_capacity(batch_capacity * KEY_LEN);
                    let mut batch_ids = Vec::with_capacity(batch_capacity);

                    let mut readopts = ReadOptions::default();
                    // future proofing to make use of parallel L0 reads and async-io
                    // if/when we build rocksdb with COROUTINES=1 and IO-URING support.
                    // by default, this will not do anything.
                    readopts.set_async_io(true);
                    readopts.set_optimize_multiget_for_io(true);

                    let mut ids = ids.into_iter();
                    loop {
                        key_buf.clear();
                        batch_ids.clear();

                        for id in ids.by_ref().take(INVOCATION_STATUS_MULTI_GET_BATCH_SIZE) {
                            EncodeTableKey::serialize_to(
                                &create_invocation_status_key(&id),
                                &mut key_buf,
                            );
                            batch_ids.push(id);
                        }

                        if batch_ids.is_empty() {
                            break;
                        }

                        let results = raw_db.batched_multi_get_cf_opt(
                            &cf,
                            key_buf.chunks_exact(KEY_LEN),
                            true,
                            &readopts,
                        );

                        for (id, result) in batch_ids.iter().zip(results) {
                            let Some(value) = result.map_err(|e| StorageError::Generic(e.into()))?
                            else {
                                continue;
                            };
                            let mut value = value.as_ref();

                            if value.len() < std::mem::size_of::<u8>() {
                                return Err(StorageError::Conversion(
                                    restate_types::storage::StorageDecodeError::ReadingCodec(
                                        format_restring!(
                                            "remaining bytes in buf '{}' < version bytes '{}'",
                                            value.len(),
                                            std::mem::size_of::<u8>()
                                        ),
                                    )
                                    .into(),
                                ));
                            }

                            let codec = restate_types::storage::StorageCodecKind::try_from(
                                bytes::Buf::get_u8(&mut value),
                            )
                            .map_err(|e| StorageError::Conversion(e.into()))?;
                            let restate_types::storage::StorageCodecKind::Protobuf = codec else {
                                return Err(StorageError::Conversion(
                                    restate_types::storage::StorageDecodeError::UnsupportedCodecKind(
                                        codec,
                                    )
                                    .into(),
                                ));
                            };

                            let mut inv_status_v2_lazy = InvocationStatusV2Lazy::default();
                            inv_status_v2_lazy
                                .merge(value)
                                .map_err(|e| StorageError::Conversion(e.into()))?;

                            match f((*id, &inv_status_v2_lazy)) {
                                ControlFlow::Continue(()) => {}
                                ControlFlow::Break(Ok(())) => return Ok(()),
                                ControlFlow::Break(Err(e)) => {
                                    return Err(StorageError::Conversion(e.into()));
                                }
                            }
                        }

                        if batch_ids.len() < INVOCATION_STATUS_MULTI_GET_BATCH_SIZE {
                            break;
                        }
                    }

                    Ok(())
                },
            )
            .await
            .map_err(|_| StorageError::OperationalError)?
    }
}

fn put_invocation_status<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    status: &InvocationStatus,
) -> Result<()> {
    match status {
        InvocationStatus::Free => storage.delete_key(&create_invocation_status_key(invocation_id)),
        _ => storage.put_kv_proto(create_invocation_status_key(invocation_id), status),
    }
}

fn get_invocation_status<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<InvocationStatus> {
    let _x = RocksDbReadPerfGuard::new("get-invocation-status");

    storage
        .get_value_proto::<_, InvocationStatus>(create_invocation_status_key(invocation_id))
        .map(|value| {
            if let Some(invocation_status) = value {
                invocation_status
            } else {
                InvocationStatus::Free
            }
        })
}

fn delete_invocation_status<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<()> {
    storage.delete_key(&create_invocation_status_key(invocation_id))
}

fn any_non_completed_invocation_in_range<S: StorageAccess>(
    storage: &S,
    range: KeyRange,
) -> Result<bool> {
    let mut iterator = storage.iterator_from(TableScan::ScanPartitionKeyRange::<
        InvocationStatusKey,
    >(range))?;

    while let Some((_, mut value)) = iterator.item() {
        let lite = InvocationLite::decode(&mut value)?;
        if !matches!(lite.status, InvocationStatusDiscriminants::Completed)
            && !matches!(lite.status, InvocationStatusDiscriminants::Killed)
        {
            return Ok(true);
        }
        iterator.next();
    }

    if let Some(err) = iterator.status().err() {
        return Err(StorageError::Generic(err.into()));
    }

    Ok(false)
}

// NOTE: This will only consider invoked invocations that have not been migrated to vqueues
fn read_invoked_full_invocation_id(
    mut kv: (&[u8], &[u8]),
) -> Result<Option<InvokedInvocationStatusLite>> {
    let invocation_id = invocation_id_from_key_bytes(&mut kv.0)?;
    let invocation_status = InvocationLite::decode(&mut kv.1)?;
    if invocation_status.vqueue_id.is_none()
        && let InvocationStatusDiscriminants::Invoked = invocation_status.status
    {
        Ok(Some(InvokedInvocationStatusLite {
            invocation_id,
            invocation_target: invocation_status.invocation_target,
        }))
    } else {
        Ok(None)
    }
}

impl ReadInvocationStatusTable for PartitionStore {
    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<InvocationStatus> {
        self.assert_partition_key(invocation_id)?;
        get_invocation_status(self, invocation_id)
    }

    async fn any_non_completed_invocation_in_range(&mut self, range: KeyRange) -> Result<bool> {
        any_non_completed_invocation_in_range(self, range)
    }
}

impl ScanInvocationStatusTable for PartitionStore {
    fn scan_legacy_invoked_invocations(
        &self,
    ) -> Result<impl Stream<Item = Result<InvokedInvocationStatusLite>> + Send> {
        self.iterator_filter_map(
            "scan-all-invoked",
            Priority::High,
            ScanPartitionKeyRange::<InvocationStatusKey>(self.partition_key_range()),
            read_invoked_full_invocation_id,
        )
        .map_err(|_| StorageError::OperationalError)
    }

    fn for_each_invocation_status_lazy<
        E: Into<anyhow::Error> + 'static,
        F: for<'a> FnMut(
                (InvocationId, &'a InvocationStatusV2Lazy<'a>),
            ) -> ControlFlow<std::result::Result<(), E>>
            + Send
            + Sync
            + 'static,
    >(
        &self,
        range: ScanInvocationStatusTableRange,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        if let ScanInvocationStatusTableRange::InvocationIdSet(ids) = range {
            return Ok(multi_get_invocation_status_lazy(self, ids, f).boxed());
        }

        let scan = match range {
            ScanInvocationStatusTableRange::PartitionKey(partition_key) => {
                TableScan::ScanPartitionKeyRange::<InvocationStatusKeyBuilder>(partition_key)
            }
            ScanInvocationStatusTableRange::InvocationId(invocation_id) => {
                let start = InvocationStatusKey::builder()
                    .partition_key(invocation_id.start().partition_key())
                    .invocation_uuid(invocation_id.start().invocation_uuid());

                let end = InvocationStatusKey::builder()
                    .partition_key(invocation_id.end().partition_key())
                    .invocation_uuid(invocation_id.end().invocation_uuid());

                TableScan::RangeInclusive(start, end)
            }
            ScanInvocationStatusTableRange::InvocationIdSet(_) => unreachable!("handled above"),
        };

        let new_status_keys = self
            .iterator_for_each(
                "df-for-each-invocation-status",
                Priority::Low,
                scan,
                {
                    move |(mut key, mut value)| {
                        let status_key =
                            break_on_err(InvocationStatusKey::deserialize_from(&mut key))?;

                        if value.len() < std::mem::size_of::<u8>() {
                            return ControlFlow::Break(Err(StorageError::Conversion(restate_types::storage::StorageDecodeError::ReadingCodec(format_restring!(
                                "remaining bytes in buf '{}' < version bytes '{}'",
                                value.len(),
                                std::mem::size_of::<u8>()
                            )).into())));
                        }

                        // read version
                        let codec = break_on_err(restate_types::storage::StorageCodecKind::try_from(bytes::Buf::get_u8(&mut value)).map_err(|e|StorageError::Conversion(e.into())))?;

                        let restate_types::storage::StorageCodecKind::Protobuf = codec else {
                            return ControlFlow::Break(Err(StorageError::Conversion(restate_types::storage::StorageDecodeError::UnsupportedCodecKind(codec).into())));
                        };

                        let mut inv_status_v2_lazy = restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy::default();
                        break_on_err(inv_status_v2_lazy.merge(value).map_err(|e| StorageError::Conversion(e.into())))?;

                        let (partition_key, invocation_uuid) = status_key.split();

                        let result = f((
                            InvocationId::from_parts(partition_key, invocation_uuid),
                            &inv_status_v2_lazy,
                        ));

                        result.map_break(|result| {
                            result.map_err(|err| StorageError::Conversion(err.into()))
                        })
                    }
                },
            )
            .map_err(|_| StorageError::OperationalError)?;

        Ok(new_status_keys.boxed())
    }

    fn filter_map_invocation_status_lazy<
        O: Send + 'static,
        E: Into<anyhow::Error>,
        F: for<'a> FnMut(
                (InvocationId, &'a InvocationStatusV2Lazy<'a>),
            ) -> std::result::Result<Option<O>, E>
            + Send
            + Sync
            + 'static,
    >(
        &self,
        mut f: F,
    ) -> Result<impl Stream<Item = Result<O>> + Send> {
        let new_status_keys = self
            .iterator_filter_map(
                "df-filter-map-invocation-status",
                Priority::Low,
                TableScan::ScanPartitionKeyRange::<InvocationStatusKey>(
                    self.partition_key_range(),
                ),
                {
                    move |(mut key, mut value)| {
                        let status_key = InvocationStatusKey::deserialize_from(&mut key)?;

                        if value.len() < std::mem::size_of::<u8>() {
                            return Err(StorageError::Conversion(restate_types::storage::StorageDecodeError::ReadingCodec(format_restring!(
                                "remaining bytes in buf '{}' < version bytes '{}'",
                                value.len(),
                                std::mem::size_of::<u8>()
                            )).into()));
                        }

                        // read version
                        let codec = restate_types::storage::StorageCodecKind::try_from(bytes::Buf::get_u8(&mut value)).map_err(|e|StorageError::Conversion(e.into()))?;

                        let restate_types::storage::StorageCodecKind::Protobuf = codec else {
                            return Err(StorageError::Conversion(restate_types::storage::StorageDecodeError::UnsupportedCodecKind(codec).into()));
                        };

                        let mut inv_status_v2_lazy = restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy::default();
                        inv_status_v2_lazy.merge(value).map_err(|e| StorageError::Conversion(e.into()))?;

                        let (partition_key, invocation_uuid) = status_key.split();

                        f((
                            InvocationId::from_parts(partition_key, invocation_uuid),
                            &inv_status_v2_lazy,
                        ))
                        .map_err(|err| StorageError::Conversion(err.into()))
                    }
                },
            )
            .map_err(|_| StorageError::OperationalError)?;

        Ok(new_status_keys)
    }
}

impl ReadInvocationStatusTable for PartitionStoreTransaction<'_> {
    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<InvocationStatus> {
        self.assert_partition_key(invocation_id)?;
        get_invocation_status(self, invocation_id)
    }

    async fn any_non_completed_invocation_in_range(&mut self, range: KeyRange) -> Result<bool> {
        any_non_completed_invocation_in_range(self, range)
    }
}

impl WriteInvocationStatusTable for PartitionStoreTransaction<'_> {
    fn put_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
        status: &InvocationStatus,
    ) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        put_invocation_status(self, invocation_id, status)
    }

    fn delete_invocation_status(&mut self, invocation_id: &InvocationId) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        delete_invocation_status(self, invocation_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::keys::EncodeTableKeyPrefix;

    use super::*;

    #[test]
    fn round_trip() {
        let expected_invocation_id = InvocationId::mock_random();

        let key = create_invocation_status_key(&expected_invocation_id).serialize();

        let actual_invocation_id = invocation_id_from_key_bytes(&mut key.freeze()).unwrap();

        assert_eq!(actual_invocation_id, expected_invocation_id);
    }
}
