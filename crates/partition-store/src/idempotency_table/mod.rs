// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::{ControlFlow, RangeInclusive};

use bytes::Bytes;
use bytestring::ByteString;

use restate_rocksdb::Priority;
use restate_storage_api::idempotency_table::{
    IdempotencyMetadata, IdempotencyTable, ReadOnlyIdempotencyTable, ScanIdempotencyTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{IdempotencyId, PartitionKey, WithPartitionKey};

use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::scan::{ScanDirection, TableScan};
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess, TableKind, break_on_err};

define_table_key!(
    TableKind::Idempotency,
    KeyKind::Idempotency,
    IdempotencyKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: Bytes,
        service_handler: ByteString,
        idempotency_key: ByteString
    )
);

#[inline]
fn create_key(idempotency_id: &IdempotencyId) -> IdempotencyKey {
    IdempotencyKey {
        partition_key: idempotency_id.partition_key(),
        service_name: idempotency_id.service_name.clone(),
        service_key: idempotency_id
            .service_key
            .as_ref()
            .cloned()
            .unwrap_or_default()
            .into_bytes(),

        service_handler: idempotency_id.service_handler.clone(),
        idempotency_key: idempotency_id.idempotency_key.clone(),
    }
}

fn get_idempotency_metadata<S: StorageAccess>(
    storage: &mut S,
    idempotency_id: &IdempotencyId,
) -> Result<Option<IdempotencyMetadata>> {
    storage.get_value_proto(create_key(idempotency_id))
}

fn put_idempotency_metadata<S: StorageAccess>(
    storage: &mut S,
    idempotency_id: &IdempotencyId,
    metadata: &IdempotencyMetadata,
) -> Result<()> {
    storage.put_kv_proto(create_key(idempotency_id), metadata)
}

fn delete_idempotency_metadata<S: StorageAccess>(
    storage: &mut S,
    idempotency_id: &IdempotencyId,
) -> Result<()> {
    let key = create_key(idempotency_id);
    storage.delete_key(&key)
}

impl ReadOnlyIdempotencyTable for PartitionStore {
    async fn get_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
    ) -> Result<Option<IdempotencyMetadata>> {
        self.assert_partition_key(idempotency_id)?;
        get_idempotency_metadata(self, idempotency_id)
    }
}

impl ScanIdempotencyTable for PartitionStore {
    fn for_each_idempotency_metadata<
        F: FnMut((IdempotencyId, IdempotencyMetadata)) -> ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        self.iterator_for_each(
            "df-idempotency",
            Priority::Low,
            TableScan::FullScanPartitionKeyRange::<IdempotencyKey>(range),
            ScanDirection::Forward,
            move |(mut key, mut value)| {
                let key = break_on_err(IdempotencyKey::deserialize_from(&mut key))?;
                let idempotency_metadata = break_on_err(IdempotencyMetadata::decode(&mut value))?;

                let idempotency_id = IdempotencyId::new(
                    key.service_name,
                    Some(break_on_err(
                        ByteString::try_from(key.service_key)
                            .map_err(|e| StorageError::Generic(e.into())),
                    )?),
                    key.service_handler,
                    key.idempotency_key,
                );

                f((idempotency_id, idempotency_metadata)).map_break(Ok)
            },
        )
        .map_err(|_| StorageError::OperationalError)
    }
}

impl ReadOnlyIdempotencyTable for PartitionStoreTransaction<'_> {
    async fn get_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
    ) -> Result<Option<IdempotencyMetadata>> {
        self.assert_partition_key(idempotency_id)?;
        get_idempotency_metadata(self, idempotency_id)
    }
}

impl IdempotencyTable for PartitionStoreTransaction<'_> {
    async fn put_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
        metadata: &IdempotencyMetadata,
    ) -> Result<()> {
        self.assert_partition_key(idempotency_id)?;
        put_idempotency_metadata(self, idempotency_id, metadata)
    }

    async fn delete_idempotency_metadata(&mut self, idempotency_id: &IdempotencyId) -> Result<()> {
        self.assert_partition_key(idempotency_id)?;
        delete_idempotency_metadata(self, idempotency_id)
    }
}
