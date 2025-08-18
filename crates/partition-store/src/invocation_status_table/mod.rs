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
use std::sync::Arc;

use futures::{FutureExt, Stream};
use tokio_stream::StreamExt;

use restate_rocksdb::{Priority, RocksDbPerfGuard};
use restate_storage_api::invocation_status_table::{
    FilterInvocationStatus, FilterInvocationTime, InvocationCreationTime, InvocationLite,
    InvocationModificationTime, InvocationStatus, InvocationStatusDiscriminants,
    InvocationStatusTable, InvocationStatusV1, InvokedInvocationStatusLite,
    ReadOnlyInvocationStatusTable, ScanInvocationStatusTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{Result, StorageError, Transaction};
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

fn break_on_err<T, E>(r: std::result::Result<T, E>) -> ControlFlow<std::result::Result<(), E>, T> {
    match r {
        Ok(val) => ControlFlow::Continue(val),
        Err(err) => ControlFlow::Break(Err(err)),
    }
}

impl ScanInvocationStatusTable for PartitionStore {
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
        F: FnMut((InvocationId, InvocationStatus)) -> ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        filters: Option<FilterInvocationStatus>,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        // these two iterators can run concurrently in theory.
        // in practice, there are not typically any keys in the first iterator, but rust rightfully forces us to use a mutex to protect the FnMut
        // the intent here is that iterators race to produce the first row, the winner gets an owned lock guard which it then holds until its done iterating
        // and then the next iterator is able to get a lock guard.

        let f = Arc::new(tokio::sync::Mutex::new(f));

        let old_status_keys = self
            .iterator_for_each(
                "df-for-each-invocation-status-v1",
                Priority::Low,
                TableScan::FullScanPartitionKeyRange::<InvocationStatusKeyV1>(range.clone()),
                {
                    let f = f.clone();
                    let mut f_locked = None;
                    move |(mut key, mut value)| {
                        let state_key =
                            break_on_err(InvocationStatusKeyV1::deserialize_from(&mut key))?;
                        let state_value = break_on_err(InvocationStatusV1::decode(&mut value))?;

                        let (partition_key, invocation_uuid) =
                            break_on_err(state_key.into_inner_ok_or())?;

                        let f = f_locked.get_or_insert_with(|| f.clone().blocking_lock_owned());

                        f((
                            InvocationId::from_parts(partition_key, invocation_uuid),
                            state_value.0,
                        ))
                        .map_break(Ok)
                    }
                },
            )
            .map_err(|_| StorageError::OperationalError)?;

        let new_status_keys = match filters {
            Some(FilterInvocationStatus::Time(
                FilterInvocationTime::Modification,
                mut modification_time_filter,
            )) => self
                .iterator_for_each(
                    "df-for-each-invocation-status",
                    Priority::Low,
                    TableScan::FullScanPartitionKeyRange::<InvocationStatusKey>(range.clone()),
                    {
                        let f = f.clone();
                        let mut f_locked = None;

                        move |(mut key, mut value)| {
                            let mut value_for_filter = value;
                            let modification_time = break_on_err(
                                InvocationModificationTime::decode(&mut value_for_filter),
                            )?;

                            if !modification_time_filter(modification_time.0) {
                                return ControlFlow::Continue(());
                            }

                            let state_key =
                                break_on_err(InvocationStatusKey::deserialize_from(&mut key))?;
                            let state_value = break_on_err(InvocationStatus::decode(&mut value))?;

                            let (partition_key, invocation_uuid) =
                                break_on_err(state_key.into_inner_ok_or())?;

                            let f = f_locked.get_or_insert_with(|| f.clone().blocking_lock_owned());

                            f((
                                InvocationId::from_parts(partition_key, invocation_uuid),
                                state_value,
                            ))
                            .map_break(Ok)
                        }
                    },
                )
                .map_err(|_| StorageError::OperationalError)?
                .left_future(),
            Some(FilterInvocationStatus::Time(
                FilterInvocationTime::Creation,
                mut creation_time_filter,
            )) => self
                .iterator_for_each(
                    "df-for-each-invocation-status",
                    Priority::Low,
                    TableScan::FullScanPartitionKeyRange::<InvocationStatusKey>(range.clone()),
                    {
                        let f = f.clone();
                        let mut f_locked = None;

                        move |(mut key, mut value)| {
                            let mut value_for_filter = value;
                            let creation_time = break_on_err(InvocationCreationTime::decode(
                                &mut value_for_filter,
                            ))?;

                            if !creation_time_filter(creation_time.0) {
                                return ControlFlow::Continue(());
                            }

                            let state_key =
                                break_on_err(InvocationStatusKey::deserialize_from(&mut key))?;
                            let state_value = break_on_err(InvocationStatus::decode(&mut value))?;

                            let (partition_key, invocation_uuid) =
                                break_on_err(state_key.into_inner_ok_or())?;

                            let f = f_locked.get_or_insert_with(|| f.clone().blocking_lock_owned());

                            f((
                                InvocationId::from_parts(partition_key, invocation_uuid),
                                state_value,
                            ))
                            .map_break(Ok)
                        }
                    },
                )
                .map_err(|_| StorageError::OperationalError)?
                .left_future()
                .right_future(),
            None => self
                .iterator_for_each(
                    "df-for-each-invocation-status",
                    Priority::Low,
                    TableScan::FullScanPartitionKeyRange::<InvocationStatusKey>(range.clone()),
                    {
                        let f = f.clone();
                        let mut f_locked = None;

                        move |(mut key, mut value)| {
                            let state_key =
                                break_on_err(InvocationStatusKey::deserialize_from(&mut key))?;
                            let state_value = break_on_err(InvocationStatus::decode(&mut value))?;

                            let (partition_key, invocation_uuid) =
                                break_on_err(state_key.into_inner_ok_or())?;

                            let f = f_locked.get_or_insert_with(|| f.clone().blocking_lock_owned());

                            f((
                                InvocationId::from_parts(partition_key, invocation_uuid),
                                state_value,
                            ))
                            .map_break(Ok)
                        }
                    },
                )
                .map_err(|_| StorageError::OperationalError)?
                .right_future()
                .right_future(),
        };

        Ok(async {
            old_status_keys.await?;
            new_status_keys.await?;

            Ok(())
        })
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
