// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use restate_core::io::IoExecutor;
use restate_rocksdb::RocksDbReadPerfGuard;
use restate_storage_api::promise_table::{
    OwnedPromiseRow, Promise, ReadPromiseTable, WritePromiseTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::scan::{RowConsumer, ScanOutcome};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{PartitionKey, ServiceId, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_types::{Scope, ServiceName};
use restate_util_string::ReString;

use crate::features::StorageFeatures;
use crate::keys::{DecodeTableKey, KeyKind, OptionalScope, define_table_key};
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

define_table_key!(
    TableKind::Promise,
    KeyKind::ScopedPromise,
    ScopedPromiseKey(
        partition_key: PartitionKey,
        scope: Option<Scope> => OptionalScope,
        service_name: ServiceName,
        service_key: ReString,
        key: ReString,
    )
);

#[inline]
fn create_key(service_id: &ServiceId, key: &ByteString) -> PromiseKey {
    PromiseKey {
        partition_key: service_id.partition_key(),
        service_name: service_id.service_name.clone(),
        service_key: service_id.key.as_bytes().clone(),
        key: key.clone(),
    }
}

/// Returns `true` if the call should use the scoped promise table for all queries.
#[inline]
fn use_scoped_promise(storage_features: StorageFeatures, service_id: &ServiceId) -> bool {
    storage_features.is_migrated_to_scoped_promise_table || service_id.scope.is_some()
}

fn get_promise<S: StorageAccess>(
    storage: &mut S,
    storage_features: StorageFeatures,
    service_id: &ServiceId,
    key: &ByteString,
) -> Result<Option<Promise>> {
    let _x = RocksDbReadPerfGuard::new("get-promise");
    if use_scoped_promise(storage_features, service_id) {
        // todo(tillrohrmann) remove once ServiceId uses ServiceName and ReString internally
        let service_name = ServiceName::new(&service_id.service_name);
        let service_key = ReString::new(&service_id.key);
        let key = ReString::new(key);

        storage.get_value_proto(
            ScopedPromiseKeyRef::builder()
                .partition_key(&service_id.partition_key())
                .scope(&service_id.scope)
                .service_name(&service_name)
                .service_key(&service_key)
                .key(&key)
                .into_complete()
                .expect("to be complete"),
        )
    } else {
        storage.get_value_proto(create_key(service_id, key))
    }
}

fn put_promise<S: StorageAccess>(
    storage: &mut S,
    storage_features: StorageFeatures,
    service_id: &ServiceId,
    key: &ByteString,
    metadata: &Promise,
) -> Result<()> {
    if use_scoped_promise(storage_features, service_id) {
        // todo(tillrohrmann) remove once ServiceId uses ServiceName and ReString internally
        let service_name = ServiceName::new(&service_id.service_name);
        let service_key = ReString::new(&service_id.key);
        let key = ReString::new(key);

        storage.put_kv_proto(
            ScopedPromiseKeyRef::builder()
                .partition_key(&service_id.partition_key())
                .scope(&service_id.scope)
                .service_name(&service_name)
                .service_key(&service_key)
                .key(&key)
                .into_complete()
                .expect("to be complete"),
            metadata,
        )
    } else {
        storage.put_kv_proto(create_key(service_id, key), metadata)
    }
}

fn delete_all_promises<S: StorageAccess>(
    storage: &mut S,
    storage_features: StorageFeatures,
    service_id: &ServiceId,
) -> Result<()> {
    if use_scoped_promise(storage_features, service_id) {
        // todo(tillrohrmann) remove once ServiceId uses ServiceName and ReString internally
        let service_name = ServiceName::new(&service_id.service_name);
        let service_key = ReString::new(&service_id.key);
        let partition_key = service_id.partition_key();

        let prefix_key = ScopedPromiseKeyRef::builder()
            .partition_key(&partition_key)
            .scope(&service_id.scope)
            .service_name(&service_name)
            .service_key(&service_key);

        // Right now the WBWI does not support range deletions :-(
        // That's why we need to iterate over the individual promises.
        let keys = storage.for_each_key_value_in_place(TableScan::Prefix(prefix_key), |k, _| {
            TableScanIterationDecision::Emit(Ok(Box::from(k)))
        })?;

        for k in keys {
            let key = k?;
            storage.delete_cf(TableKind::Promise, key)?;
        }
    } else {
        let partition_key = service_id.partition_key();
        let prefix_key = PromiseKeyRef::builder()
            .partition_key(&partition_key)
            .service_name(&service_id.service_name)
            .service_key(service_id.key.as_bytes());

        let keys = storage.for_each_key_value_in_place(TableScan::Prefix(prefix_key), |k, _| {
            TableScanIterationDecision::Emit(Ok(Box::from(k)))
        })?;

        for k in keys {
            let key = k?;
            storage.delete_cf(TableKind::Promise, key)?;
        }
    }
    Ok(())
}

impl ReadPromiseTable for PartitionStore {
    async fn get_promise(
        &mut self,
        service_id: &ServiceId,
        key: &ByteString,
    ) -> Result<Option<Promise>> {
        self.assert_partition_key(service_id)?;
        get_promise(self, self.storage_features(), service_id, key)
    }
}

impl PartitionStore {
    /// Scans the promises in `range` on `executor`, for data-fusion queries.
    pub fn for_each_promise<F: for<'a> RowConsumer<Item<'a> = OwnedPromiseRow>>(
        &self,
        executor: &(impl IoExecutor + Sync),
        range: KeyRange,
        f: F,
    ) -> Result<impl Future<Output = Result<ScanOutcome<F>>> + Send> {
        // Only scan the legacy unscoped table while we may still hold data there.
        // After migration the range was deleted, so the scoped scan covers everything.
        let migrated = self.storage_features().is_migrated_to_scoped_promise_table;

        // The scans run one after the other, each handing the consumer to the next.
        Ok(async move {
            let f = if migrated {
                f
            } else {
                match self
                    .iterator_for_each(
                        "df-promise",
                        executor,
                        TableScan::ScanPartitionKeyRange::<PromiseKey>(range),
                        f,
                        async |f: &mut F, (mut k, mut v): (&[u8], &[u8])| {
                            let key = PromiseKey::deserialize_from(&mut k)?;
                            let metadata = Promise::decode(&mut v)?;
                            let (partition_key, service_name, service_key, promise_key) =
                                key.split();
                            let service_id = ServiceId::with_partition_key(
                                partition_key,
                                service_name,
                                ByteString::try_from(service_key).map_err(|e| {
                                    StorageError::Generic(anyhow::anyhow!(
                                        "Cannot convert to string {e}"
                                    ))
                                })?,
                            );
                            f.consume(OwnedPromiseRow {
                                service_id,
                                key: promise_key,
                                metadata,
                            })
                            .await
                        },
                    )
                    .map_err(|_| StorageError::AdmissionClosed)?
                    .await?
                {
                    ScanOutcome::Exhausted(f) => f,
                    ScanOutcome::Stopped(f) => return Ok(ScanOutcome::Stopped(f)),
                }
            };

            self.iterator_for_each(
                "df-promise-scoped",
                executor,
                TableScan::ScanPartitionKeyRange::<ScopedPromiseKey>(range),
                f,
                async |f: &mut F, (mut k, mut v): (&[u8], &[u8])| {
                    let key = ScopedPromiseKey::deserialize_from(&mut k)?;
                    let metadata = Promise::decode(&mut v)?;
                    let (_partition_key, scope, service_name, service_key, promise_key) =
                        key.split();
                    let service_id = ServiceId::new(
                        scope,
                        ByteString::from(service_name.as_str()),
                        ByteString::from(service_key.as_str()),
                    );
                    f.consume(OwnedPromiseRow {
                        service_id,
                        key: ByteString::from(promise_key.as_str()),
                        metadata,
                    })
                    .await
                },
            )
            .map_err(|_| StorageError::AdmissionClosed)?
            .await
        })
    }
}

impl ReadPromiseTable for PartitionStoreTransaction<'_> {
    async fn get_promise(
        &mut self,
        service_id: &ServiceId,
        key: &ByteString,
    ) -> Result<Option<Promise>> {
        self.assert_partition_key(service_id)?;
        get_promise(self, self.storage_features(), service_id, key)
    }
}

impl WritePromiseTable for PartitionStoreTransaction<'_> {
    fn put_promise(
        &mut self,
        service_id: &ServiceId,
        key: &ByteString,
        promise: &Promise,
    ) -> Result<()> {
        self.assert_partition_key(service_id)?;
        put_promise(self, self.storage_features(), service_id, key, promise)
    }

    fn delete_all_promises(&mut self, service_id: &ServiceId) -> Result<()> {
        self.assert_partition_key(service_id)?;
        delete_all_promises(self, self.storage_features(), service_id)
    }
}
