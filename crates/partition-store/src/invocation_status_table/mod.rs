// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::keys::{define_table_key, KeyKind, TableKey};
use crate::owned_iter::OwnedIterator;
use crate::TableScan::FullScanPartitionKeyRange;
use crate::{PartitionStore, TableKind, TableScanIterationDecision};
use crate::{PartitionStoreTransaction, StorageAccess};
use futures::Stream;
use futures_util::stream;
use restate_rocksdb::RocksDbPerfGuard;
use restate_storage_api::invocation_status_table::{
    InvocationLite, InvocationStatus, InvocationStatusDiscriminants, InvocationStatusTable,
    InvocationStatusV1, InvokedOrKilledInvocationStatusLite, ReadOnlyInvocationStatusTable,
};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};
use restate_types::storage::StorageCodec;
use std::ops::RangeInclusive;
use tracing::trace;

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
) {
    match status {
        InvocationStatus::Free => {
            storage.delete_key(&create_invocation_status_key(invocation_id));
        }
        _ => {
            storage.put_kv(create_invocation_status_key(invocation_id), status);
        }
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
        );
        storage.delete_key(&v1_key);
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

fn delete_invocation_status<S: StorageAccess>(storage: &mut S, invocation_id: &InvocationId) {
    // TODO remove this once we remove the old InvocationStatus
    storage.delete_key(&create_invocation_status_key_v1(invocation_id));
    storage.delete_key(&create_invocation_status_key(invocation_id));
}

fn invoked_or_killed_invocations<S: StorageAccess>(
    storage: &mut S,
    partition_key_range: RangeInclusive<PartitionKey>,
) -> Vec<Result<InvokedOrKilledInvocationStatusLite>> {
    let _x = RocksDbPerfGuard::new("invoked-invocations");
    let mut invocations = storage.for_each_key_value_in_place(
        FullScanPartitionKeyRange::<InvocationStatusKeyV1>(partition_key_range.clone()),
        |mut k, mut v| {
            let result = read_invoked_v1_full_invocation_id(&mut k, &mut v).transpose();
            if let Some(res) = result {
                TableScanIterationDecision::Emit(res)
            } else {
                TableScanIterationDecision::Continue
            }
        },
    );
    invocations.extend(storage.for_each_key_value_in_place(
        FullScanPartitionKeyRange::<InvocationStatusKey>(partition_key_range),
        |mut k, mut v| {
            let result = read_invoked_or_killed_status_lite(&mut k, &mut v).transpose();
            if let Some(res) = result {
                TableScanIterationDecision::Emit(res)
            } else {
                TableScanIterationDecision::Continue
            }
        },
    ));

    invocations
}

fn all_invocation_status<S: StorageAccess>(
    storage: &S,
    range: RangeInclusive<PartitionKey>,
) -> impl Stream<Item = Result<(InvocationId, InvocationStatus)>> + Send + '_ {
    stream::iter(
        OwnedIterator::new(storage.iterator_from(
            FullScanPartitionKeyRange::<InvocationStatusKeyV1>(range.clone()),
        ))
        .map(|(mut key, mut value)| {
            let state_key = InvocationStatusKeyV1::deserialize_from(&mut key)?;
            let state_value = StorageCodec::decode::<InvocationStatusV1, _>(&mut value)
                .map_err(|err| StorageError::Conversion(err.into()))?;

            let (partition_key, invocation_uuid) = state_key.into_inner_ok_or()?;
            Ok((
                InvocationId::from_parts(partition_key, invocation_uuid),
                state_value.0,
            ))
        })
        .chain(
            OwnedIterator::new(storage.iterator_from(FullScanPartitionKeyRange::<
                InvocationStatusKey,
            >(range.clone())))
            .map(|(mut key, mut value)| {
                let state_key = InvocationStatusKey::deserialize_from(&mut key)?;
                let state_value = StorageCodec::decode::<InvocationStatus, _>(&mut value)
                    .map_err(|err| StorageError::Conversion(err.into()))?;

                let (partition_key, invocation_uuid) = state_key.into_inner_ok_or()?;
                Ok((
                    InvocationId::from_parts(partition_key, invocation_uuid),
                    state_value,
                ))
            }),
        ),
    )
}

// TODO remove this once we remove the old InvocationStatus
fn read_invoked_v1_full_invocation_id(
    mut k: &mut &[u8],
    v: &mut &[u8],
) -> Result<Option<InvokedOrKilledInvocationStatusLite>> {
    let invocation_id = invocation_id_from_v1_key_bytes(&mut k)?;
    let invocation_status = StorageCodec::decode::<InvocationStatusV1, _>(v)
        .map_err(|err| StorageError::Generic(err.into()))?;
    if let InvocationStatus::Invoked(invocation_meta) = invocation_status.0 {
        Ok(Some(InvokedOrKilledInvocationStatusLite {
            invocation_id,
            invocation_target: invocation_meta.invocation_target,
            is_invoked: true,
        }))
    } else {
        Ok(None)
    }
}

fn read_invoked_or_killed_status_lite(
    mut k: &mut &[u8],
    v: &mut &[u8],
) -> Result<Option<InvokedOrKilledInvocationStatusLite>> {
    let invocation_id = invocation_id_from_key_bytes(&mut k)?;
    let invocation_status = StorageCodec::decode::<InvocationLite, _>(v)
        .map_err(|err| StorageError::Generic(err.into()))?;
    if let InvocationStatusDiscriminants::Invoked = invocation_status.status {
        Ok(Some(InvokedOrKilledInvocationStatusLite {
            invocation_id,
            invocation_target: invocation_status.invocation_target,
            is_invoked: true,
        }))
    } else if let InvocationStatusDiscriminants::Killed = invocation_status.status {
        Ok(Some(InvokedOrKilledInvocationStatusLite {
            invocation_id,
            invocation_target: invocation_status.invocation_target,
            is_invoked: false,
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
        self.assert_partition_key(invocation_id);
        get_invocation_status(self, invocation_id)
    }

    fn all_invoked_or_killed_invocations(
        &mut self,
    ) -> impl Stream<Item = Result<InvokedOrKilledInvocationStatusLite>> + Send {
        stream::iter(invoked_or_killed_invocations(
            self,
            self.partition_key_range().clone(),
        ))
    }

    fn all_invocation_statuses(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(InvocationId, InvocationStatus)>> + Send {
        all_invocation_status(self, range)
    }
}

impl<'a> ReadOnlyInvocationStatusTable for PartitionStoreTransaction<'a> {
    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<InvocationStatus> {
        self.assert_partition_key(invocation_id);
        try_migrate_and_get_invocation_status(self, invocation_id)
    }

    fn all_invoked_or_killed_invocations(
        &mut self,
    ) -> impl Stream<Item = Result<InvokedOrKilledInvocationStatusLite>> + Send {
        stream::iter(invoked_or_killed_invocations(
            self,
            self.partition_key_range().clone(),
        ))
    }

    fn all_invocation_statuses(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(InvocationId, InvocationStatus)>> + Send {
        all_invocation_status(self, range)
    }
}

impl<'a> InvocationStatusTable for PartitionStoreTransaction<'a> {
    async fn put_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
        status: &InvocationStatus,
    ) {
        self.assert_partition_key(invocation_id);
        put_invocation_status(self, invocation_id, status)
    }

    async fn delete_invocation_status(&mut self, invocation_id: &InvocationId) {
        self.assert_partition_key(invocation_id);
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
