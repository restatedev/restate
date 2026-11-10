// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;

use futures::Stream;

use restate_rocksdb::RocksDbReadPerfGuard;
use restate_storage_api::invocation_status_table::{
    InvocationLite, InvocationStatus, InvocationStatusDiscriminants, ReadInvocationStatusTable,
    ScanInvocationStatusTable, ScanInvocationStatusTableRange, WriteInvocationStatusTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
use restate_storage_api::scan::{RowConsumer, ScanClosePolicy};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_util_string::format_restring;

use crate::keys::{DecodeTableKey, KeyKind, define_table_key};
use crate::scan::{ScanOp, ScanPlan, TableScan};
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess, TableKind};

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

/// Decodes an invocation-status entry lazily: the status borrows `value`.
fn decode_invocation_status_lazy<'a>(
    mut key: &[u8],
    mut value: &'a [u8],
) -> Result<(InvocationId, InvocationStatusV2Lazy<'a>)> {
    let status_key = InvocationStatusKey::deserialize_from(&mut key)?;

    if value.len() < std::mem::size_of::<u8>() {
        return Err(StorageError::Conversion(
            restate_types::storage::StorageDecodeError::ReadingCodec(format_restring!(
                "remaining bytes in buf '{}' < version bytes '{}'",
                value.len(),
                std::mem::size_of::<u8>()
            ))
            .into(),
        ));
    }

    // read version
    let codec = restate_types::storage::StorageCodecKind::try_from(bytes::Buf::get_u8(&mut value))
        .map_err(|e| StorageError::Conversion(e.into()))?;
    let restate_types::storage::StorageCodecKind::Protobuf = codec else {
        return Err(StorageError::Conversion(
            restate_types::storage::StorageDecodeError::UnsupportedCodecKind(codec).into(),
        ));
    };

    let mut status = InvocationStatusV2Lazy::default();
    status
        .merge(value)
        .map_err(|e| StorageError::Conversion(e.into()))?;

    let (partition_key, invocation_uuid) = status_key.split();
    Ok((
        InvocationId::from_parts(partition_key, invocation_uuid),
        status,
    ))
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

/// Invocation statuses, decoded lazily, for data-fusion queries: those of a
/// partition-key range, of a range of invocations, or of a set of invocations.
pub struct InvocationStatusScan {
    pub range: ScanInvocationStatusTableRange,
}

impl ScanOp for InvocationStatusScan {
    type Item<'a> = (InvocationId, &'a InvocationStatusV2Lazy<'a>);
    const NAME: &'static str = "df-for-each-invocation-status";

    fn plan(&self) -> ScanPlan {
        match &self.range {
            ScanInvocationStatusTableRange::PartitionKey(partition_key) => ScanPlan::range(
                TableScan::ScanPartitionKeyRange::<InvocationStatusKeyBuilder>(*partition_key),
            ),
            ScanInvocationStatusTableRange::InvocationId(invocation_id) => {
                let start = InvocationStatusKey::builder()
                    .partition_key(invocation_id.start().partition_key())
                    .invocation_uuid(invocation_id.start().invocation_uuid());
                let end = InvocationStatusKey::builder()
                    .partition_key(invocation_id.end().partition_key())
                    .invocation_uuid(invocation_id.end().invocation_uuid());
                ScanPlan::range(TableScan::RangeInclusive(start, end))
            }
            // Each id maps to exactly one fixed-length key, so a set of ids is
            // looked up instead of scanned. The set is in key order.
            ScanInvocationStatusTableRange::InvocationIdSet(ids) => ScanPlan::keys(
                InvocationStatusKey::serialized_length_fixed(),
                INVOCATION_STATUS_MULTI_GET_BATCH_SIZE,
                ids.iter().map(create_invocation_status_key),
            ),
        }
    }

    async fn visit<C>(&self, key: &[u8], value: &[u8], consumer: &mut C) -> Result<ControlFlow<()>>
    where
        C: for<'a> RowConsumer<Item<'a> = (InvocationId, &'a InvocationStatusV2Lazy<'a>)>,
    {
        let (invocation_id, status) = decode_invocation_status_lazy(key, value)?;
        consumer.consume((invocation_id, &status)).await
    }
}

impl ScanInvocationStatusTable for PartitionStore {
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
            .iterator_filter_map(
                "df-filter-map-invocation-status",
                self.partition_db().rocksdb().executor().low_pri(),
                scan,
                // The cleaner consumes this stream in paginated batches with
                // its own cancellation, so it isn't cut short by a close.
                ScanClosePolicy::FinishBeforeClose,
                move |(key, value)| {
                    let (invocation_id, status) = decode_invocation_status_lazy(key, value)?;
                    f((invocation_id, &status)).map_err(|err| StorageError::Conversion(err.into()))
                },
            )
            .map_err(|_| StorageError::AdmissionClosed)?;

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
