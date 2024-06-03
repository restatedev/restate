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
use crate::{RocksDBTransaction, StorageAccess};
use futures::Stream;
use futures_util::stream;
use restate_rocksdb::RocksDbPerfGuard;
use restate_storage_api::invocation_status_table::{
    InvocationStatus, InvocationStatusTable, ReadOnlyInvocationStatusTable,
};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};
use restate_types::invocation::InvocationTarget;
use restate_types::storage::StorageCodec;
use std::ops::RangeInclusive;

define_table_key!(
    TableKind::InvocationStatus,
    KeyKind::InvocationStatus,
    InvocationStatusKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid
    )
);

fn write_invocation_status_key(invocation_id: &InvocationId) -> InvocationStatusKey {
    InvocationStatusKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
}

fn invocation_id_from_bytes<B: bytes::Buf>(bytes: &mut B) -> crate::Result<InvocationId> {
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
    status: InvocationStatus,
) {
    let key = write_invocation_status_key(invocation_id);
    if status == InvocationStatus::Free {
        storage.delete_key(&key);
    } else {
        storage.put_kv(key, status);
    }
}

fn get_invocation_status<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<InvocationStatus> {
    let _x = RocksDbPerfGuard::new("get-invocation-status");
    let key = write_invocation_status_key(invocation_id);

    storage
        .get_value::<_, InvocationStatus>(key)
        .map(|value| value.unwrap_or(InvocationStatus::Free))
}

fn delete_invocation_status<S: StorageAccess>(storage: &mut S, invocation_id: &InvocationId) {
    let key = write_invocation_status_key(invocation_id);
    storage.delete_key(&key);
}

fn invoked_invocations<S: StorageAccess>(
    storage: &mut S,
    partition_key_range: RangeInclusive<PartitionKey>,
) -> Vec<Result<(InvocationId, InvocationTarget)>> {
    let _x = RocksDbPerfGuard::new("invoked-invocations");
    storage.for_each_key_value_in_place(
        FullScanPartitionKeyRange::<InvocationStatusKey>(partition_key_range),
        |mut k, mut v| {
            let result = read_invoked_full_invocation_id(&mut k, &mut v).transpose();
            if let Some(res) = result {
                TableScanIterationDecision::Emit(res)
            } else {
                TableScanIterationDecision::Continue
            }
        },
    )
}

fn read_invoked_full_invocation_id(
    mut k: &mut &[u8],
    v: &mut &[u8],
) -> Result<Option<(InvocationId, InvocationTarget)>> {
    let invocation_id = invocation_id_from_bytes(&mut k)?;
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
        get_invocation_status(self, invocation_id)
    }

    fn invoked_invocations(
        &mut self,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(InvocationId, InvocationTarget)>> + Send {
        stream::iter(invoked_invocations(self, partition_key_range))
    }
}

impl<'a> ReadOnlyInvocationStatusTable for RocksDBTransaction<'a> {
    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<InvocationStatus> {
        get_invocation_status(self, invocation_id)
    }

    // TODO once the invoker uses only InvocationId, we can remove returning the fid here.
    fn invoked_invocations(
        &mut self,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(InvocationId, InvocationTarget)>> + Send {
        stream::iter(invoked_invocations(self, partition_key_range))
    }
}

impl<'a> InvocationStatusTable for RocksDBTransaction<'a> {
    async fn put_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
        status: InvocationStatus,
    ) {
        put_invocation_status(self, invocation_id, status)
    }

    async fn delete_invocation_status(&mut self, invocation_id: &InvocationId) {
        delete_invocation_status(self, invocation_id)
    }
}

#[derive(Clone, Debug)]
pub struct OwnedInvocationStatusRow {
    pub partition_key: PartitionKey,
    pub invocation_uuid: InvocationUuid,
    pub invocation_status: InvocationStatus,
}

impl PartitionStore {
    pub fn all_invocation_status(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Iterator<Item = OwnedInvocationStatusRow> + '_ {
        let iter = self.iterator_from(FullScanPartitionKeyRange::<InvocationStatusKey>(range));
        OwnedIterator::new(iter).map(|(mut key, mut value)| {
            let state_key = InvocationStatusKey::deserialize_from(&mut key).unwrap();
            let state_value = StorageCodec::decode::<InvocationStatus, _>(&mut value).unwrap();
            OwnedInvocationStatusRow {
                partition_key: state_key.partition_key.unwrap(),
                invocation_uuid: state_key.invocation_uuid.unwrap(),
                invocation_status: state_value,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let expected_invocation_id = InvocationId::mock_random();

        let key = write_invocation_status_key(&expected_invocation_id).serialize();

        let actual_invocation_id = invocation_id_from_bytes(&mut key.freeze()).unwrap();

        assert_eq!(actual_invocation_id, expected_invocation_id);
    }
}
