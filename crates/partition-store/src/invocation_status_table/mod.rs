// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::{ControlFlow, RangeInclusive};

use futures::Stream;
use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
use tokio_stream::StreamExt;

use restate_rocksdb::{Priority, RocksDbPerfGuard};
use restate_storage_api::invocation_status_table::{
    InvocationLite, InvocationStatus, InvocationStatusAccessor, InvocationStatusDiscriminants,
    InvocationStatusTable, InvocationStatusV1, InvokedInvocationStatusLite,
    ReadOnlyInvocationStatusTable, ScanInvocationStatusTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{Result, StorageError, Transaction};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};

use crate::TableScan::FullScanPartitionKeyRange;
use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::scan::TableScan;
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess, TableKind, break_on_err};

// TODO remove this once we remove the old InvocationStatus
define_table_key!(
    TableKind::InvocationStatus,
    KeyKind::InvocationStatusV1,
    InvocationStatusKeyV1(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid
    )
);

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
    storage.delete_key(&create_invocation_status_key(invocation_id))
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

const MIGRATION_BATCH_SIZE: usize = 1000;

pub(crate) async fn run_invocation_status_v1_migration(storage: &mut PartitionStore) -> Result<()> {
    let partition_key_range = storage.partition_key_range().clone();

    let mut iterator = storage
        .run_iterator(
            "invocation-status-v1-migration",
            Priority::High,
            FullScanPartitionKeyRange::<InvocationStatusKeyV1>(partition_key_range),
            |(mut old_key, mut old_value)| {
                Ok((
                    InvocationStatusKeyV1::deserialize_from(&mut old_key)?,
                    InvocationStatusV1::decode(&mut old_value)?,
                ))
            },
        )
        .map_err(|_| StorageError::OperationalError)?;

    let mut tx = storage.transaction();
    let mut batch_size = 0;
    while let Some(res) = iterator.next().await {
        let (key, value) = res?;
        put_invocation_status(
            &mut tx,
            &InvocationId::from_parts(*key.partition_key_ok_or()?, *key.invocation_uuid_ok_or()?),
            &value.0,
        )?;
        tx.delete_key(&key)?;

        batch_size += 1;
        if batch_size >= MIGRATION_BATCH_SIZE {
            tx.commit().await?;
            batch_size = 0;
            tx = storage.transaction();
        }
    }
    tx.commit().await?;

    Ok(())
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

pub type ScanInvocationStatusAccessor<'a> = InvocationStatusAccessor<
    &'a InvocationStatusV2Lazy<'a>,
    &'a InvocationStatusV2Lazy<'a>,
    &'a InvocationStatusV2Lazy<'a>,
>;

impl ScanInvocationStatusTable for PartitionStore {
    type PreFlightInvocationMetadataAccessor<'a> = &'a InvocationStatusV2Lazy<'a>;
    type InFlightInvocationMetadataAccessor<'a> = &'a InvocationStatusV2Lazy<'a>;
    type CompletedInvocationMetadataAccessor<'a> = &'a InvocationStatusV2Lazy<'a>;

    fn scan_invoked_invocations(
        &self,
    ) -> Result<impl Stream<Item = Result<InvokedInvocationStatusLite>> + Send> {
        Ok(self
            .run_iterator(
                "scan-all-invoked",
                Priority::High,
                FullScanPartitionKeyRange::<InvocationStatusKey>(
                    self.partition_key_range().clone(),
                ),
                read_invoked_full_invocation_id,
            )
            .map_err(|_| StorageError::OperationalError)?
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
        self.run_iterator(
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
        .map_err(|_| StorageError::OperationalError)
    }

    fn for_each_invocation_status<
        E: Into<anyhow::Error>,
        F: for<'a> FnMut(
                (
                    InvocationId,
                    InvocationStatusAccessor<
                        Self::PreFlightInvocationMetadataAccessor<'a>,
                        Self::InFlightInvocationMetadataAccessor<'a>,
                        Self::CompletedInvocationMetadataAccessor<'a>,
                    >,
                ),
            ) -> ControlFlow<std::result::Result<(), E>>
            + Send
            + Sync
            + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        let new_status_keys = self
            .iterator_for_each(
                "df-for-each-invocation-status",
                Priority::Low,
                TableScan::FullScanPartitionKeyRange::<InvocationStatusKey>(range.clone()),
                {
                    move |(mut key, mut value)| {
                        let state_key =
                            break_on_err(InvocationStatusKey::deserialize_from(&mut key))?;

                        if value.len() < std::mem::size_of::<u8>() {
                            return ControlFlow::Break(Err(StorageError::Conversion(restate_types::storage::StorageDecodeError::ReadingCodec(format!(
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

                        let inv_status_v2 = break_on_err(restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy::decode(value).map_err(|e| StorageError::Conversion(e.into())))?;


                        let accessor = break_on_err(
                            inv_status_v2
                                .accessor()
                                .map_err(|err| StorageError::Conversion(err.into())),
                        )?;

                        let (partition_key, invocation_uuid) =
                            break_on_err(state_key.into_inner_ok_or())?;

                        let result = f((
                            InvocationId::from_parts(partition_key, invocation_uuid),
                            accessor,
                        ));

                        result.map_break(|result| {
                            result.map_err(|err| StorageError::Conversion(err.into()))
                        })
                    }
                },
            )
            .map_err(|_| StorageError::OperationalError)?;

        Ok(new_status_keys)
    }
}

impl ReadOnlyInvocationStatusTable for PartitionStoreTransaction<'_> {
    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<InvocationStatus> {
        self.assert_partition_key(invocation_id)?;
        get_invocation_status(self, invocation_id)
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
