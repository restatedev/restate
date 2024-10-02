// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
    InvocationStatus, InvocationStatusTable, InvocationStatusV1, ReadOnlyInvocationStatusTable,
};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};
use restate_types::invocation::InvocationTarget;
use restate_types::storage::StorageCodec;
use std::ops::RangeInclusive;

// TODO remove this once we remove the old InvocationStatus
define_table_key!(
    TableKind::InvocationStatus,
    KeyKind::InvocationStatus,
    InvocationStatusKeyV1(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid
    )
);

define_table_key!(
    TableKind::InvocationStatus,
    KeyKind::InvocationStatusV2,
    InvocationStatusKeyV2(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid
    )
);

fn create_invocation_status_key_v2(invocation_id: &InvocationId) -> InvocationStatusKeyV2 {
    InvocationStatusKeyV2::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
}

fn invocation_id_from_v2_key_bytes<B: bytes::Buf>(bytes: &mut B) -> crate::Result<InvocationId> {
    let mut key = InvocationStatusKeyV2::deserialize_from(bytes)?;
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
            storage.delete_key(&create_invocation_status_key_v2(invocation_id));
        }
        _ => {
            storage.put_kv(create_invocation_status_key_v2(invocation_id), status);
        }
    }
}

fn get_invocation_status<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<InvocationStatus> {
    let _x = RocksDbPerfGuard::new("get-invocation-status");

    storage
        .get_value::<_, InvocationStatus>(create_invocation_status_key_v2(invocation_id))
        .map(|opt| opt.unwrap_or(InvocationStatus::Free))
}

fn delete_invocation_status<S: StorageAccess>(storage: &mut S, invocation_id: &InvocationId) {
    storage.delete_key(&create_invocation_status_key_v2(invocation_id));
}

fn invoked_invocations<S: StorageAccess>(
    storage: &mut S,
    partition_key_range: RangeInclusive<PartitionKey>,
) -> Vec<Result<(InvocationId, InvocationTarget)>> {
    let _x = RocksDbPerfGuard::new("invoked-invocations");
    storage.for_each_key_value_in_place(
        FullScanPartitionKeyRange::<InvocationStatusKeyV2>(partition_key_range),
        |mut k, mut v| {
            let result = read_invoked_neo_full_invocation_id(&mut k, &mut v).transpose();
            if let Some(res) = result {
                TableScanIterationDecision::Emit(res)
            } else {
                TableScanIterationDecision::Continue
            }
        },
    )
}

fn all_invocation_status<S: StorageAccess>(
    storage: &S,
    range: RangeInclusive<PartitionKey>,
) -> impl Stream<Item = Result<(InvocationId, InvocationStatus)>> + Send + '_ {
    stream::iter(
        OwnedIterator::new(storage.iterator_from(
            FullScanPartitionKeyRange::<InvocationStatusKeyV2>(range.clone()),
        ))
        .map(|(mut key, mut value)| {
            let state_key = InvocationStatusKeyV2::deserialize_from(&mut key)?;
            let state_value = StorageCodec::decode::<InvocationStatus, _>(&mut value)
                .map_err(|err| StorageError::Conversion(err.into()))?;

            let (partition_key, invocation_uuid) = state_key.into_inner_ok_or()?;
            Ok((
                InvocationId::from_parts(partition_key, invocation_uuid),
                state_value,
            ))
        }),
    )
}

fn read_invoked_neo_full_invocation_id(
    mut k: &mut &[u8],
    v: &mut &[u8],
) -> Result<Option<(InvocationId, InvocationTarget)>> {
    // TODO this can be improved by simply parsing InvocationTarget and the Status enum
    let invocation_id = invocation_id_from_v2_key_bytes(&mut k)?;
    let invocation_status = StorageCodec::decode::<InvocationStatus, _>(v)
        .map_err(|err| StorageError::Generic(err.into()))?;
    if let InvocationStatus::Invoked(invocation_meta) = invocation_status {
        Ok(Some((invocation_id, invocation_meta.invocation_target)))
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

    fn all_invoked_invocations(
        &mut self,
    ) -> impl Stream<Item = Result<(InvocationId, InvocationTarget)>> + Send {
        stream::iter(invoked_invocations(
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
        get_invocation_status(self, invocation_id)
    }

    fn all_invoked_invocations(
        &mut self,
    ) -> impl Stream<Item = Result<(InvocationId, InvocationTarget)>> + Send {
        stream::iter(invoked_invocations(
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

pub async fn run_neo_invocation_status_migration(
    storage: &mut PartitionStoreTransaction<'_>,
) -> Result<()> {
    let invocation_statuses: Result<Vec<_>> = OwnedIterator::new(storage.iterator_from(
        FullScanPartitionKeyRange::<InvocationStatusKeyV1>(PartitionKey::MIN..=PartitionKey::MAX),
    ))
    .map(|(mut old_key, mut old_value)| {
        let k = InvocationStatusKeyV1::deserialize_from(&mut old_key)?;
        let v = StorageCodec::decode::<InvocationStatusV1, _>(&mut old_value)
            .map_err(|err| StorageError::Generic(err.into()))?;

        Ok((k, v))
    })
    .collect();

    for (key, value) in invocation_statuses? {
        put_invocation_status(
            storage,
            &InvocationId::from_parts(*key.partition_key_ok_or()?, *key.invocation_uuid_ok_or()?),
            &value.0,
        );
        storage.delete_key(&key);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let expected_invocation_id = InvocationId::mock_random();

        let key = create_invocation_status_key_v2(&expected_invocation_id).serialize();

        let actual_invocation_id = invocation_id_from_v2_key_bytes(&mut key.freeze()).unwrap();

        assert_eq!(actual_invocation_id, expected_invocation_id);
    }
}
