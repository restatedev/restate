// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::owned_iter::OwnedIterator;
use crate::protobuf_types::PartitionStoreProtobufValue;
use crate::scan::TableScan;
use crate::{PartitionStore, TableKind};
use crate::{PartitionStoreTransaction, StorageAccess};
use bytes::Bytes;
use bytestring::ByteString;
use futures::Stream;
use futures_util::stream;
use futures_util::future::Either;
use restate_storage_api::idempotency_table::{
    IdempotencyMetadata, IdempotencyTable, ReadOnlyIdempotencyTable,
};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{IdempotencyId, PartitionKey, WithPartitionKey};
use std::ops::RangeInclusive;

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

impl PartitionStoreProtobufValue for IdempotencyMetadata {
    type ProtobufType = crate::protobuf_types::v1::IdempotencyMetadata;
}

fn create_key(idempotency_id: &IdempotencyId) -> IdempotencyKey {
    IdempotencyKey::default()
        .partition_key(idempotency_id.partition_key())
        .service_name(idempotency_id.service_name.clone())
        .service_key(
            idempotency_id
                .service_key
                .as_ref()
                .cloned()
                .unwrap_or_default()
                .into_bytes(),
        )
        .service_handler(idempotency_id.service_handler.clone())
        .idempotency_key(idempotency_id.idempotency_key.clone())
}

fn get_idempotency_metadata<S: StorageAccess>(
    storage: &mut S,
    idempotency_id: &IdempotencyId,
) -> Result<Option<IdempotencyMetadata>> {
    storage.get_value(create_key(idempotency_id))
}

fn all_idempotency_metadata<S: StorageAccess>(
    storage: &S,
    range: RangeInclusive<PartitionKey>,
) -> Result<impl Stream<Item = Result<(IdempotencyId, IdempotencyMetadata)>> + Send + use<'_, S>> {
    let iter = storage.iterator_from(TableScan::FullScanPartitionKeyRange::<IdempotencyKey>(
        range,
    ))?;
    Ok(stream::iter(OwnedIterator::new(iter).map(
        |(mut k, mut v)| {
            let key = IdempotencyKey::deserialize_from(&mut k)?;
            let idempotency_metadata = IdempotencyMetadata::decode(&mut v)?;

            Ok((
                IdempotencyId::new(
                    key.service_name_ok_or()?.clone(),
                    key.service_key
                        .clone()
                        .map(|b| {
                            ByteString::try_from(b).map_err(|e| StorageError::Generic(e.into()))
                        })
                        .transpose()?,
                    key.service_handler_ok_or()?.clone(),
                    key.idempotency_key_ok_or()?.clone(),
                ),
                idempotency_metadata,
            ))
        },
    )))
}

fn put_idempotency_metadata<S: StorageAccess>(
    storage: &mut S,
    idempotency_id: &IdempotencyId,
    metadata: &IdempotencyMetadata,
) -> Result<()> {
    storage.put_kv(create_key(idempotency_id), metadata)
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
        if self.is_idempotency_table_disabled() {
            return Ok(None);
        }
        
        self.assert_partition_key(idempotency_id)?;
        get_idempotency_metadata(self, idempotency_id)
    }

    fn all_idempotency_metadata(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = Result<(IdempotencyId, IdempotencyMetadata)>> + Send> {
        if self.is_idempotency_table_disabled() {
            Ok(Either::Left(stream::empty()))
        } else {
            Ok(Either::Right(all_idempotency_metadata(self, range)?))
        }
    }
}

impl ReadOnlyIdempotencyTable for PartitionStoreTransaction<'_> {
    async fn get_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
    ) -> Result<Option<IdempotencyMetadata>> {
        if self.is_idempotency_table_disabled() {
            return Ok(None);
        }
        
        self.assert_partition_key(idempotency_id)?;
        get_idempotency_metadata(self, idempotency_id)
    }

    fn all_idempotency_metadata(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = Result<(IdempotencyId, IdempotencyMetadata)>> + Send> {
        if self.is_idempotency_table_disabled() {
            Ok(Either::Left(stream::empty()))
        } else {
            Ok(Either::Right(all_idempotency_metadata(self, range)?))
        }
    }
}

impl IdempotencyTable for PartitionStoreTransaction<'_> {
    async fn put_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
        metadata: &IdempotencyMetadata,
    ) -> Result<()> {
        if self.is_idempotency_table_disabled() {
            return Ok(());
        }
        
        self.assert_partition_key(idempotency_id)?;
        put_idempotency_metadata(self, idempotency_id, metadata)
    }

    async fn delete_idempotency_metadata(&mut self, idempotency_id: &IdempotencyId) -> Result<()> {
        if self.is_idempotency_table_disabled() {
            return Ok(());
        }
        
        self.assert_partition_key(idempotency_id)?;
        delete_idempotency_metadata(self, idempotency_id)
    }
}
