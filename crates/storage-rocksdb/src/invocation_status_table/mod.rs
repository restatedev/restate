// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::codec::ProtoValue;
use crate::keys::{define_table_key, TableKey};
use crate::owned_iter::OwnedIterator;
use crate::TableScan::PartitionKeyRange;
use crate::{RocksDBStorage, TableKind, TableScanIterationDecision};
use crate::{RocksDBTransaction, StorageAccess};
use futures::Stream;
use futures_util::stream;
use prost::Message;
use restate_storage_api::invocation_status_table::{
    InvocationStatus, InvocationStatusTable, ReadOnlyInvocationStatusTable,
};
use restate_storage_api::{Result, StorageError};
use restate_storage_proto::storage;
use restate_types::identifiers::{FullInvocationId, PartitionKey};
use restate_types::identifiers::{InvocationId, InvocationUuid, WithPartitionKey};
use std::ops::RangeInclusive;

define_table_key!(
    TableKind::InvocationStatus,
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
    Ok(InvocationId::new(
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
        let value = ProtoValue(storage::v1::InvocationStatus::from(status));
        storage.put_kv(key, value);
    }
}

fn get_invocation_status<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<InvocationStatus> {
    let key = write_invocation_status_key(invocation_id);

    storage.get_blocking(key, move |_, v| {
        if v.is_none() {
            return Ok(InvocationStatus::Free);
        }
        let v = v.unwrap();
        let proto = storage::v1::InvocationStatus::decode(v)
            .map_err(|err| StorageError::Generic(err.into()))?;
        InvocationStatus::try_from(proto).map_err(StorageError::from)
    })
}

fn delete_invocation_status<S: StorageAccess>(storage: &mut S, invocation_id: &InvocationId) {
    let key = write_invocation_status_key(invocation_id);
    storage.delete_key(&key);
}

fn invoked_invocations<S: StorageAccess>(
    storage: &mut S,
    partition_key_range: RangeInclusive<PartitionKey>,
) -> Vec<Result<FullInvocationId>> {
    storage.for_each_key_value_in_place(
        PartitionKeyRange::<InvocationStatusKey>(partition_key_range),
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
) -> Result<Option<FullInvocationId>> {
    let invocation_id = invocation_id_from_bytes(&mut k)?;
    let proto = storage::v1::InvocationStatus::decode(v)
        .map_err(|err| StorageError::Generic(err.into()))?;
    let invocation_status = InvocationStatus::try_from(proto).map_err(StorageError::from)?;
    if let InvocationStatus::Invoked(invocation_meta) = invocation_status {
        Ok(Some(FullInvocationId::combine(
            invocation_meta.service_id,
            invocation_id,
        )))
    } else {
        Ok(None)
    }
}

impl ReadOnlyInvocationStatusTable for RocksDBStorage {
    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<InvocationStatus> {
        get_invocation_status(self, invocation_id)
    }

    fn invoked_invocations(
        &mut self,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<FullInvocationId>> + Send {
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
    ) -> impl Stream<Item = Result<FullInvocationId>> + Send {
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

impl RocksDBStorage {
    pub fn all_invocation_status(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Iterator<Item = OwnedInvocationStatusRow> + '_ {
        let iter = self.iterator_from(PartitionKeyRange::<InvocationStatusKey>(range));
        OwnedIterator::new(iter).map(|(mut key, value)| {
            let state_key = InvocationStatusKey::deserialize_from(&mut key).unwrap();
            let state_value = storage::v1::InvocationStatus::decode(value).unwrap();
            let state_value = InvocationStatus::try_from(state_value).unwrap();
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
