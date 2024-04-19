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
use crate::scan::TableScan;
use crate::{RocksDBStorage, TableKind};
use crate::{RocksDBTransaction, StorageAccess};
use bytes::Bytes;
use bytestring::ByteString;
use futures::Stream;
use futures_util::stream;
use restate_storage_api::idempotency_table::{
    IdempotencyMetadata, IdempotencyTable, ReadOnlyIdempotencyTable,
};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{IdempotencyId, PartitionKey, WithPartitionKey};
use restate_types::storage::StorageCodec;
use std::ops::RangeInclusive;

define_table_key!(
    TableKind::Idempotency,
    KeyKind::Idempotency,
    IdempotencyKey(
        partition_key: PartitionKey,
        component_name: ByteString,
        component_key: Bytes,
        component_handler: ByteString,
        idempotency_key: ByteString
    )
);

fn create_key(idempotency_id: &IdempotencyId) -> IdempotencyKey {
    IdempotencyKey::default()
        .partition_key(idempotency_id.partition_key())
        .component_name(idempotency_id.component_name.clone())
        .component_key(
            idempotency_id
                .component_key
                .as_ref()
                .cloned()
                .unwrap_or_default(),
        )
        .component_handler(idempotency_id.component_handler.clone())
        .idempotency_key(idempotency_id.idempotency_key.clone())
}

fn get_idempotency_metadata<S: StorageAccess>(
    storage: &mut S,
    idempotency_id: &IdempotencyId,
) -> Result<Option<IdempotencyMetadata>> {
    storage.get_value(create_key(idempotency_id))
}

fn all_idempotency_metadata<S: StorageAccess>(
    storage: &mut S,
    range: RangeInclusive<PartitionKey>,
) -> impl Stream<Item = Result<(IdempotencyId, IdempotencyMetadata)>> + Send + '_ {
    let iter = storage.iterator_from(TableScan::PartitionKeyRange::<IdempotencyKey>(range));
    stream::iter(OwnedIterator::new(iter).map(|(mut k, mut v)| {
        let key = IdempotencyKey::deserialize_from(&mut k)?;
        let idempotency_metadata = StorageCodec::decode::<IdempotencyMetadata, _>(&mut v)
            .map_err(|err| StorageError::Generic(err.into()))?;

        Ok((
            IdempotencyId::new(
                key.component_name_ok_or()?.clone(),
                key.component_key.clone(),
                key.component_handler_ok_or()?.clone(),
                key.idempotency_key_ok_or()?.clone(),
            ),
            idempotency_metadata,
        ))
    }))
}

fn put_idempotency_metadata<S: StorageAccess>(
    storage: &mut S,
    idempotency_id: &IdempotencyId,
    metadata: IdempotencyMetadata,
) {
    storage.put_kv(create_key(idempotency_id), metadata);
}

fn delete_idempotency_metadata<S: StorageAccess>(storage: &mut S, idempotency_id: &IdempotencyId) {
    let key = create_key(idempotency_id);
    storage.delete_key(&key);
}

impl ReadOnlyIdempotencyTable for RocksDBStorage {
    async fn get_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
    ) -> Result<Option<IdempotencyMetadata>> {
        get_idempotency_metadata(self, idempotency_id)
    }

    fn all_idempotency_metadata(
        &mut self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(IdempotencyId, IdempotencyMetadata)>> + Send {
        all_idempotency_metadata(self, range)
    }
}

impl<'a> ReadOnlyIdempotencyTable for RocksDBTransaction<'a> {
    async fn get_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
    ) -> Result<Option<IdempotencyMetadata>> {
        get_idempotency_metadata(self, idempotency_id)
    }

    fn all_idempotency_metadata(
        &mut self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(IdempotencyId, IdempotencyMetadata)>> + Send {
        all_idempotency_metadata(self, range)
    }
}

impl<'a> IdempotencyTable for RocksDBTransaction<'a> {
    async fn put_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
        metadata: IdempotencyMetadata,
    ) {
        put_idempotency_metadata(self, idempotency_id, metadata)
    }

    async fn delete_idempotency_metadata(&mut self, idempotency_id: &IdempotencyId) {
        delete_idempotency_metadata(self, idempotency_id)
    }
}
