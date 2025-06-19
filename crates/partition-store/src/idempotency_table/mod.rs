// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use bytestring::ByteString;

use restate_storage_api::Result;
use restate_storage_api::idempotency_table::{
    IdempotencyMetadata, IdempotencyTable, ReadOnlyIdempotencyTable,
};
use restate_types::identifiers::{IdempotencyId, PartitionKey, WithPartitionKey};

use crate::keys::{KeyKind, define_table_key};
use crate::protobuf_types::PartitionStoreProtobufValue;
use crate::{PartitionStore, TableKind};
use crate::{PartitionStoreTransaction, StorageAccess};

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
        self.assert_partition_key(idempotency_id)?;
        get_idempotency_metadata(self, idempotency_id)
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
