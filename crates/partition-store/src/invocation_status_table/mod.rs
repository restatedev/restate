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
    CompletedInvocation, InFlightInvocationMetadata, InboxedInvocation, InvocationStatus,
    InvocationStatusTable, NeoInvocationStatus, PreFlightInvocationMetadata,
    ReadOnlyInvocationStatusTable, SourceTable,
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
    InvocationStatusKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid
    )
);

// TODO remove this once we remove the old InvocationStatus
fn create_invocation_status_key(invocation_id: &InvocationId) -> InvocationStatusKey {
    InvocationStatusKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
}

define_table_key!(
    TableKind::InvocationStatus,
    KeyKind::NeoInvocationStatus,
    NeoInvocationStatusKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid
    )
);

fn create_neo_invocation_status_key(invocation_id: &InvocationId) -> NeoInvocationStatusKey {
    NeoInvocationStatusKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
}

// TODO remove this once we remove the old InvocationStatus
fn invocation_id_from_old_key_bytes<B: bytes::Buf>(bytes: &mut B) -> crate::Result<InvocationId> {
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

fn invocation_id_from_neo_key_bytes<B: bytes::Buf>(bytes: &mut B) -> crate::Result<InvocationId> {
    let mut key = NeoInvocationStatusKey::deserialize_from(bytes)?;
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
    match &status {
        InvocationStatus::Inboxed(InboxedInvocation {
            metadata: PreFlightInvocationMetadata { source_table, .. },
            ..
        })
        | InvocationStatus::Invoked(InFlightInvocationMetadata { source_table, .. })
        | InvocationStatus::Suspended {
            metadata: InFlightInvocationMetadata { source_table, .. },
            ..
        }
        | InvocationStatus::Completed(CompletedInvocation { source_table, .. }) => {
            match source_table {
                // TODO remove this once we remove the old InvocationStatus
                SourceTable::Old => {
                    storage.put_kv(create_invocation_status_key(invocation_id), status);
                }
                SourceTable::New => {
                    storage.put_kv(
                        create_neo_invocation_status_key(invocation_id),
                        NeoInvocationStatus(status),
                    );
                }
            }
        }
        InvocationStatus::Scheduled { .. } => {
            // The scheduled variant is only on the NeoInvocationStatus
            storage.put_kv(
                create_neo_invocation_status_key(invocation_id),
                NeoInvocationStatus(status),
            );
        }
        InvocationStatus::Free => {
            // TODO remove this once we remove the old InvocationStatus
            storage.delete_key(&create_invocation_status_key(invocation_id));
            storage.delete_key(&create_neo_invocation_status_key(invocation_id));
        }
    }
}

fn get_invocation_status<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<InvocationStatus> {
    let _x = RocksDbPerfGuard::new("get-invocation-status");

    // Try read the old one first
    if let Some(s) = storage
        .get_value::<_, NeoInvocationStatus>(create_neo_invocation_status_key(invocation_id))?
    {
        return Ok(s.0);
    }
    // TODO remove this once we remove the old InvocationStatus
    storage
        .get_value::<_, InvocationStatus>(create_invocation_status_key(invocation_id))
        .map(|value| value.unwrap_or(InvocationStatus::Free))
}

fn delete_invocation_status<S: StorageAccess>(storage: &mut S, invocation_id: &InvocationId) {
    // TODO remove this once we remove the old InvocationStatus
    storage.delete_key(&create_invocation_status_key(invocation_id));
    storage.delete_key(&create_neo_invocation_status_key(invocation_id));
}

fn invoked_invocations<S: StorageAccess>(
    storage: &mut S,
    partition_key_range: RangeInclusive<PartitionKey>,
) -> Vec<Result<(InvocationId, InvocationTarget)>> {
    let _x = RocksDbPerfGuard::new("invoked-invocations");
    let mut invocations = storage.for_each_key_value_in_place(
        FullScanPartitionKeyRange::<InvocationStatusKey>(partition_key_range.clone()),
        |mut k, mut v| {
            let result = read_invoked_full_invocation_id(&mut k, &mut v).transpose();
            if let Some(res) = result {
                TableScanIterationDecision::Emit(res)
            } else {
                TableScanIterationDecision::Continue
            }
        },
    );
    invocations.extend(storage.for_each_key_value_in_place(
        FullScanPartitionKeyRange::<NeoInvocationStatusKey>(partition_key_range),
        |mut k, mut v| {
            let result = read_invoked_neo_full_invocation_id(&mut k, &mut v).transpose();
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
            FullScanPartitionKeyRange::<InvocationStatusKey>(range.clone()),
        ))
        .map(|(mut key, mut value)| {
            let state_key = InvocationStatusKey::deserialize_from(&mut key)?;
            let state_value = StorageCodec::decode::<InvocationStatus, _>(&mut value)
                .map_err(|err| StorageError::Conversion(err.into()))?;

            let (partition_key, invocation_uuid) = state_key.into_inner_ok_or()?;
            Ok((
                InvocationId::from_parts(partition_key, invocation_uuid),
                state_value,
            ))
        })
        .chain(
            OwnedIterator::new(storage.iterator_from(FullScanPartitionKeyRange::<
                NeoInvocationStatusKey,
            >(range.clone())))
            .map(|(mut key, mut value)| {
                let state_key = NeoInvocationStatusKey::deserialize_from(&mut key)?;
                let state_value = StorageCodec::decode::<NeoInvocationStatus, _>(&mut value)
                    .map_err(|err| StorageError::Conversion(err.into()))?
                    .0;

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
fn read_invoked_full_invocation_id(
    mut k: &mut &[u8],
    v: &mut &[u8],
) -> Result<Option<(InvocationId, InvocationTarget)>> {
    let invocation_id = invocation_id_from_old_key_bytes(&mut k)?;
    let invocation_status = StorageCodec::decode::<InvocationStatus, _>(v)
        .map_err(|err| StorageError::Generic(err.into()))?;
    if let InvocationStatus::Invoked(invocation_meta) = invocation_status {
        Ok(Some((invocation_id, invocation_meta.invocation_target)))
    } else {
        Ok(None)
    }
}

fn read_invoked_neo_full_invocation_id(
    mut k: &mut &[u8],
    v: &mut &[u8],
) -> Result<Option<(InvocationId, InvocationTarget)>> {
    // TODO this can be improved by simply parsing InvocationTarget and the Status enum
    let invocation_id = invocation_id_from_neo_key_bytes(&mut k)?;
    let invocation_status = StorageCodec::decode::<NeoInvocationStatus, _>(v)
        .map_err(|err| StorageError::Generic(err.into()))?
        .0;
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

    fn all_invocation_statuses(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(InvocationId, InvocationStatus)>> + Send {
        all_invocation_status(self, range)
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

    fn all_invocation_statuses(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(InvocationId, InvocationStatus)>> + Send {
        all_invocation_status(self, range)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let expected_invocation_id = InvocationId::mock_random();

        let key = create_invocation_status_key(&expected_invocation_id).serialize();

        let actual_invocation_id = invocation_id_from_old_key_bytes(&mut key.freeze()).unwrap();

        assert_eq!(actual_invocation_id, expected_invocation_id);
    }
}
