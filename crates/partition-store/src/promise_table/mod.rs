// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use bytes::Bytes;
use bytestring::ByteString;
use futures::Stream;

use restate_rocksdb::{Priority, RocksDbPerfGuard};
use restate_storage_api::promise_table::{
    OwnedPromiseRow, Promise, PromiseTable, ReadOnlyPromiseTable, ScanPromiseTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{PartitionKey, ServiceId, WithPartitionKey};

use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::scan::TableScan;
use crate::{
    PartitionStore, PartitionStoreTransaction, StorageAccess, TableKind, TableScanIterationDecision,
};

define_table_key!(
    TableKind::Promise,
    KeyKind::Promise,
    PromiseKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: Bytes,
        key: ByteString
    )
);

fn create_key(service_id: &ServiceId, key: &ByteString) -> PromiseKey {
    PromiseKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.as_bytes().clone())
        .key(key.clone())
}

fn get_promise<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    key: &ByteString,
) -> Result<Option<Promise>> {
    let _x = RocksDbPerfGuard::new("get-promise");
    storage.get_value(create_key(service_id, key))
}

fn put_promise<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    key: &ByteString,
    metadata: &Promise,
) -> Result<()> {
    storage.put_kv(create_key(service_id, key), metadata)
}

fn delete_all_promises<S: StorageAccess>(storage: &mut S, service_id: &ServiceId) -> Result<()> {
    let prefix_key = PromiseKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.as_bytes().clone());

    let keys = storage.for_each_key_value_in_place(
        TableScan::SinglePartitionKeyPrefix(service_id.partition_key(), prefix_key),
        |k, _| TableScanIterationDecision::Emit(Ok(Bytes::copy_from_slice(k))),
    )?;

    for k in keys {
        let key = k?;
        storage.delete_cf(TableKind::Promise, key)?;
    }
    Ok(())
}

impl ReadOnlyPromiseTable for PartitionStore {
    async fn get_promise(
        &mut self,
        service_id: &ServiceId,
        key: &ByteString,
    ) -> Result<Option<Promise>> {
        self.assert_partition_key(service_id)?;
        get_promise(self, service_id, key)
    }
}

impl ScanPromiseTable for PartitionStore {
    fn scan_promises(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = Result<OwnedPromiseRow>> + Send> {
        self.run_iterator(
            "df-promise",
            Priority::Low,
            TableScan::FullScanPartitionKeyRange::<PromiseKey>(range),
            |(mut k, mut v)| {
                let key = PromiseKey::deserialize_from(&mut k)?;
                let metadata = Promise::decode(&mut v)?;

                let (partition_key, service_name, service_key, promise_key) =
                    key.into_inner_ok_or()?;

                Ok(OwnedPromiseRow {
                    service_id: ServiceId::with_partition_key(
                        partition_key,
                        service_name,
                        ByteString::try_from(service_key)
                            .map_err(|e| anyhow::anyhow!("Cannot convert to string {e}"))?,
                    ),
                    key: promise_key,
                    metadata,
                })
            },
        )
        .map_err(|_| StorageError::OperationalError)
    }
}

impl ReadOnlyPromiseTable for PartitionStoreTransaction<'_> {
    async fn get_promise(
        &mut self,
        service_id: &ServiceId,
        key: &ByteString,
    ) -> Result<Option<Promise>> {
        self.assert_partition_key(service_id)?;
        get_promise(self, service_id, key)
    }
}

impl PromiseTable for PartitionStoreTransaction<'_> {
    async fn put_promise(
        &mut self,
        service_id: &ServiceId,
        key: &ByteString,
        promise: &Promise,
    ) -> Result<()> {
        self.assert_partition_key(service_id)?;
        put_promise(self, service_id, key, promise)
    }

    async fn delete_all_promises(&mut self, service_id: &ServiceId) -> Result<()> {
        self.assert_partition_key(service_id)?;
        delete_all_promises(self, service_id)
    }
}
