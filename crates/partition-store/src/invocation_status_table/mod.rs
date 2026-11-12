// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::ops::ControlFlow;

use futures::{FutureExt, Stream};

use restate_rocksdb::{Priority, RocksDbReadPerfGuard};
use restate_storage_api::invocation_status_table::{
    InvocationLite, InvocationStatus, InvocationStatusDiscriminants, ReadInvocationStatusTable,
    ScanInvocationStatusTable, ScanInvocationStatusTableRange, WriteInvocationStatusTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_types::storage::{StorageCodecKind, StorageDecodeError};
use restate_util_string::format_restring;

use crate::keys::{DecodeTableKey, FixedLengthTableKey, KeyKind, define_table_key};
use crate::scan::TableScan;
use crate::{
    PartitionStore, PartitionStoreTransaction, StorageAccess, StorageIterator, StorageRead,
    TableKind, break_on_err,
};

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

impl FixedLengthTableKey<{ InvocationStatusKey::serialized_length_fixed() }>
    for InvocationStatusKey
{
}

/// Maximum number of invocation-status keys passed to one RocksDB multi-get call.
/// This one is set to a low value due to the possibility of loading a large value from rocksdb
/// when the invocation holds a large output payload. This is an unfortunate side effect of storing
/// the output payload into invocation-status!
const INVOCATION_STATUS_MULTI_GET_BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(25).unwrap();

#[inline]
fn create_invocation_status_key(invocation_id: &InvocationId) -> InvocationStatusKey {
    InvocationStatusKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
    }
}

fn decode_invocation_status_lazy(mut value: &[u8]) -> Result<InvocationStatusV2Lazy<'_>> {
    if value.is_empty() {
        return Err(StorageError::Conversion(
            StorageDecodeError::ReadingCodec(format_restring!(
                "remaining bytes in buf '{}' < version bytes '{}'",
                value.len(),
                std::mem::size_of::<u8>()
            ))
            .into(),
        ));
    }

    let codec = StorageCodecKind::try_from(bytes::Buf::get_u8(&mut value))
        .map_err(|e| StorageError::Conversion(e.into()))?;
    let StorageCodecKind::Protobuf = codec else {
        return Err(StorageError::Conversion(
            StorageDecodeError::UnsupportedCodecKind(codec).into(),
        ));
    };

    let mut inv_status_v2_lazy = InvocationStatusV2Lazy::default();
    inv_status_v2_lazy
        .merge(value)
        .map_err(|e| StorageError::Conversion(e.into()))?;
    Ok(inv_status_v2_lazy)
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

fn get_invocation_status<S: StorageRead>(
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

fn any_non_completed_invocation_in_range<S: StorageRead>(
    storage: &S,
    range: KeyRange,
) -> Result<bool> {
    let mut iterator = storage.iterator_from(TableScan::ScanPartitionKeyRange::<
        InvocationStatusKey,
    >(range))?;

    while let Some((_, mut value)) = iterator.item()? {
        let lite = InvocationLite::decode(&mut value)?;
        if !matches!(lite.status, InvocationStatusDiscriminants::Completed)
            && !matches!(lite.status, InvocationStatusDiscriminants::Killed)
        {
            return Ok(true);
        }
        iterator.next();
    }

    Ok(false)
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
            let fut = self.multi_get_for_each(
                "df-for-each-invocation-status",
                INVOCATION_STATUS_MULTI_GET_BATCH_SIZE,
                ids,
                create_invocation_status_key,
                move |id, value| {
                    let status = break_on_err(decode_invocation_status_lazy(value))?;
                    f((*id, &status))
                        .map_break(|result| result.map_err(|e| StorageError::Conversion(e.into())))
                },
            );
            return Ok(fut.boxed());
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
            .iterator_for_each("df-for-each-invocation-status", Priority::Low, scan, {
                move |(mut key, value)| {
                    let status_key = break_on_err(InvocationStatusKey::deserialize_from(&mut key))?;
                    let inv_status_v2_lazy = break_on_err(decode_invocation_status_lazy(value))?;

                    let (partition_key, invocation_uuid) = status_key.split();

                    let result = f((
                        InvocationId::from_parts(partition_key, invocation_uuid),
                        &inv_status_v2_lazy,
                    ));

                    result.map_break(|result| {
                        result.map_err(|err| StorageError::Conversion(err.into()))
                    })
                }
            })
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
        from: Option<InvocationId>,
        mut f: F,
    ) -> Result<impl Stream<Item = Result<O>> + Send> {
        let scan = match from {
            None => TableScan::ScanPartitionKeyRange::<InvocationStatusKeyBuilder>(
                self.partition_key_range(),
            ),
            Some(from) => TableScan::RangeInclusive(
                InvocationStatusKey::builder()
                    .partition_key(from.partition_key())
                    .invocation_uuid(from.invocation_uuid()),
                InvocationStatusKey::builder().partition_key(self.partition_key_range().end()),
            ),
        };

        let new_status_keys = self
            .iterator_filter_map("df-filter-map-invocation-status", Priority::Low, scan, {
                move |(mut key, value)| {
                    let status_key = InvocationStatusKey::deserialize_from(&mut key)?;
                    let inv_status_v2_lazy = decode_invocation_status_lazy(value)?;

                    let (partition_key, invocation_uuid) = status_key.split();

                    f((
                        InvocationId::from_parts(partition_key, invocation_uuid),
                        &inv_status_v2_lazy,
                    ))
                    .map_err(|err| StorageError::Conversion(err.into()))
                }
            })
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

        let key = InvocationStatusKey::deserialize_from(&mut key.as_ref()).unwrap();
        let actual_invocation_id = InvocationId::from_parts(key.partition_key, key.invocation_uuid);

        assert_eq!(actual_invocation_id, expected_invocation_id);
    }
}
