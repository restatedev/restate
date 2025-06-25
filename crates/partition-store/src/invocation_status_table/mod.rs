// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use futures::Stream;
use tokio_stream::StreamExt;
use tracing::trace;

use restate_rocksdb::{Priority, RocksDbPerfGuard};
use restate_storage_api::invocation_status_table::{
    InvocationLite, InvocationStatus, InvocationStatusDiscriminants, InvocationStatusTable,
    InvocationStatusV1, InvokedInvocationStatusLite, ReadOnlyInvocationStatusTable,
    ScanInvocationStatusTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};

use crate::TableScan::FullScanPartitionKeyRange;
use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::scan::TableScan;
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess, TableKind};

// TODO remove this once we remove the old InvocationStatus
define_table_key!(
    TableKind::InvocationStatus,
    KeyKind::InvocationStatusV1,
    InvocationStatusKeyV1(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid
    )
);

// TODO remove this once we remove the old InvocationStatus
fn create_invocation_status_key_v1(invocation_id: &InvocationId) -> InvocationStatusKeyV1 {
    InvocationStatusKeyV1::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
}

define_table_key!(
    TableKind::InvocationStatus,
    KeyKind::InvocationStatus,
    InvocationStatusKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid
    )
);

fn create_invocation_status_key(invocation_id: &InvocationId) -> InvocationStatusKey {
    InvocationStatusKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
}

// TODO remove this once we remove the old InvocationStatus
fn invocation_id_from_v1_key_bytes<B: bytes::Buf>(bytes: &mut B) -> crate::Result<InvocationId> {
    let mut key = InvocationStatusKeyV1::deserialize_from(bytes)?;
    let partition_key = key
        .partition_key
        .take()
        .ok_or(StorageError::DataIntegrityError)?;
    Ok(InvocationId::from_parts(
        partition_key,
        key.invocation_uuid
            .take()
            .ok_or(StorageError::DataIntegrityError)?,
    ))
}

fn invocation_id_from_key_bytes<B: bytes::Buf>(bytes: &mut B) -> crate::Result<InvocationId> {
    let mut key = InvocationStatusKey::deserialize_from(bytes)?;
    let partition_key = key
        .partition_key
        .take()
        .ok_or(StorageError::DataIntegrityError)?;
    Ok(InvocationId::from_parts(
        partition_key,
        key.invocation_uuid
            .take()
            .ok_or(StorageError::DataIntegrityError)?,
    ))
}

fn put_invocation_status<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    status: &InvocationStatus,
) -> Result<()> {
    match status {
        InvocationStatus::Free => storage.delete_key(&create_invocation_status_key(invocation_id)),
        _ => storage.put_kv(create_invocation_status_key(invocation_id), status),
    }
}

fn get_invocation_status<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<InvocationStatus> {
    let _x = RocksDbPerfGuard::new("get-invocation-status");

    if let Some(s) =
        storage.get_value::<_, InvocationStatus>(create_invocation_status_key(invocation_id))?
    {
        return Ok(s);
    }

    // todo: Remove this once we remove the old InvocationStatus
    // The underlying assumption is that an invocation status will never exist in both old and new
    // invocation status table.
    storage
        .get_value::<_, InvocationStatusV1>(create_invocation_status_key_v1(invocation_id))
        .map(|value| {
            if let Some(invocation_status) = value {
                invocation_status.0
            } else {
                InvocationStatus::Free
            }
        })
}

fn try_migrate_and_get_invocation_status<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<InvocationStatus> {
    let _x = RocksDbPerfGuard::new("try-migrate-and-get-invocation-status");

    let v1_key = create_invocation_status_key_v1(invocation_id);
    if let Some(status_v1) = storage.get_value::<_, InvocationStatusV1>(v1_key.clone())? {
        trace!("Migrating invocation {invocation_id} from InvocationStatus V1");
        put_invocation_status(
            storage,
            &InvocationId::from_parts(
                *v1_key.partition_key_ok_or()?,
                *v1_key.invocation_uuid_ok_or()?,
            ),
            &status_v1.0,
        )?;
        storage.delete_key(&v1_key)?;
        return Ok(status_v1.0);
    }

    storage
        .get_value::<_, InvocationStatus>(create_invocation_status_key(invocation_id))
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
    // TODO remove this once we remove the old InvocationStatus
    storage.delete_key(&create_invocation_status_key_v1(invocation_id))?;
    storage.delete_key(&create_invocation_status_key(invocation_id))
}

// TODO remove this once we remove the old InvocationStatus
fn read_invoked_v1_full_invocation_id(
    mut kv: (&[u8], &[u8]),
) -> Result<Option<InvokedInvocationStatusLite>> {
    let invocation_id = invocation_id_from_v1_key_bytes(&mut kv.0)?;
    let invocation_status = InvocationStatusV1::decode(&mut kv.1)?;
    if let InvocationStatus::Invoked(invocation_meta) = invocation_status.0 {
        Ok(Some(InvokedInvocationStatusLite {
            invocation_id,
            invocation_target: invocation_meta.invocation_target,
            current_invocation_epoch: 0,
        }))
    } else {
        Ok(None)
    }
}

fn read_invoked_full_invocation_id(
    mut kv: (&[u8], &[u8]),
) -> Result<Option<InvokedInvocationStatusLite>> {
    let invocation_id = invocation_id_from_key_bytes(&mut kv.0)?;
    let invocation_status = InvocationLite::decode(&mut kv.1)?;
    if let InvocationStatusDiscriminants::Invoked = invocation_status.status {
        Ok(Some(InvokedInvocationStatusLite {
            invocation_id,
            invocation_target: invocation_status.invocation_target,
            current_invocation_epoch: invocation_status.current_invocation_epoch,
        }))
    } else {
        Ok(None)
    }
}

impl ReadOnlyInvocationStatusTable for PartitionStore {
    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<InvocationStatus> {
        self.assert_partition_key(invocation_id)?;
        get_invocation_status(self, invocation_id)
    }
}

impl ScanInvocationStatusTable for PartitionStore {
    fn scan_invoked_invocations(
        &self,
    ) -> Result<impl Stream<Item = Result<InvokedInvocationStatusLite>> + Send> {
        // Note: this is a high priority iterator because it's used by the leadership
        // state machine when switching from candidate to leader.
        let invocations_1 = self
            .run_iterator(
                "scan-all-invoked-v1",
                Priority::High,
                FullScanPartitionKeyRange::<InvocationStatusKeyV1>(
                    self.partition_key_range().clone(),
                ),
                read_invoked_v1_full_invocation_id,
            )
            .map_err(|_| StorageError::OperationalError)?;
        // fitler and map the stream from Result<Option<T>> to Result<T>

        let invocations_2 = self
            .run_iterator(
                "scan-all-invoked",
                Priority::High,
                FullScanPartitionKeyRange::<InvocationStatusKey>(
                    self.partition_key_range().clone(),
                ),
                read_invoked_full_invocation_id,
            )
            .map_err(|_| StorageError::OperationalError)?;

        Ok(invocations_1
            .chain(invocations_2)
            .filter_map(|result| match result {
                Ok(Some(res)) => Some(Ok(res)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }))
    }

    fn scan_invocation_statuses(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = Result<(InvocationId, InvocationStatus)>> + Send> {
        let old_status_keys = self
            .run_iterator(
                "df-invocation-status-v1",
                Priority::Low,
                TableScan::FullScanPartitionKeyRange::<InvocationStatusKeyV1>(range.clone()),
                |(mut key, mut value)| {
                    let state_key = InvocationStatusKeyV1::deserialize_from(&mut key)?;
                    let state_value = InvocationStatusV1::decode(&mut value)?;

                    let (partition_key, invocation_uuid) = state_key.into_inner_ok_or()?;
                    Ok((
                        InvocationId::from_parts(partition_key, invocation_uuid),
                        state_value.0,
                    ))
                },
            )
            .map_err(|_| StorageError::OperationalError)?;

        let new_status_keys = self
            .run_iterator(
                "df-invocation-status",
                Priority::Low,
                TableScan::FullScanPartitionKeyRange::<InvocationStatusKey>(range.clone()),
                |(mut key, mut value)| {
                    let state_key = InvocationStatusKey::deserialize_from(&mut key)?;
                    let state_value = InvocationStatus::decode(&mut value)?;

                    let (partition_key, invocation_uuid) = state_key.into_inner_ok_or()?;
                    Ok((
                        InvocationId::from_parts(partition_key, invocation_uuid),
                        state_value,
                    ))
                },
            )
            .map_err(|_| StorageError::OperationalError)?;

        Ok(old_status_keys.chain(new_status_keys))
    }
}

impl ReadOnlyInvocationStatusTable for PartitionStoreTransaction<'_> {
    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<InvocationStatus> {
        self.assert_partition_key(invocation_id)?;
        try_migrate_and_get_invocation_status(self, invocation_id)
    }
}

impl InvocationStatusTable for PartitionStoreTransaction<'_> {
    async fn put_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
        status: &InvocationStatus,
    ) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        put_invocation_status(self, invocation_id, status)
    }

    async fn delete_invocation_status(&mut self, invocation_id: &InvocationId) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        delete_invocation_status(self, invocation_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let expected_invocation_id = InvocationId::mock_random();

        let key = create_invocation_status_key(&expected_invocation_id).serialize();

        let actual_invocation_id = invocation_id_from_key_bytes(&mut key.freeze()).unwrap();

        assert_eq!(actual_invocation_id, expected_invocation_id);
    }
}
