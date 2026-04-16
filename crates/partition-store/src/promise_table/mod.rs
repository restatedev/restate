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
use std::sync::Arc;

use crate::keys::{DecodeTableKey, KeyKind, define_table_key};
use crate::scan::TableScan;
use crate::{
    PartitionStore, PartitionStoreTransaction, StorageAccess, TableKind,
    TableScanIterationDecision, break_on_err,
};
use restate_rocksdb::{Priority, RocksDbPerfGuard};
use restate_storage_api::promise_table::{
    OwnedPromiseRow, Promise, ReadPromiseTable, ScanPromiseTable, WritePromiseTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{PartitionKey, ServiceId, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_types::{Scope, ServiceName};
use restate_util_string::ReString;

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
        scope: Option<Scope>,
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

fn get_promise<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    key: &ByteString,
) -> Result<Option<Promise>> {
    let _x = RocksDbPerfGuard::new("get-promise");
    // todo(tillrohrmann) make dependent on migration status once we migrate old promises to the new scoped table
    if service_id.scope.is_some() {
        // todo(tillrohrmann) remove once ServiceId uses ServiceName and ReString internally
        let service_name = ServiceName::new(&service_id.service_name);
        let service_key = ReString::new_owned(&service_id.key);
        let key = ReString::new_owned(key);

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
    service_id: &ServiceId,
    key: &ByteString,
    metadata: &Promise,
) -> Result<()> {
    // todo(tillrohrmann) make dependent on migration status once we migrate old promises to the new scoped table
    if service_id.scope.is_some() {
        // todo(tillrohrmann) remove once ServiceId uses ServiceName and ReString internally
        let service_name = ServiceName::new(&service_id.service_name);
        let service_key = ReString::new_owned(&service_id.key);
        let key = ReString::new_owned(key);

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

fn delete_all_promises<S: StorageAccess>(storage: &mut S, service_id: &ServiceId) -> Result<()> {
    // todo(tillrohrmann) make dependent on migration status once we migrate old promises to the new scoped table
    if service_id.scope.is_some() {
        // todo(tillrohrmann) remove once ServiceId uses ServiceName and ReString internally
        let service_name = ServiceName::new(&service_id.service_name);
        let service_key = ReString::new_owned(&service_id.key);
        let partition_key = service_id.partition_key();

        let prefix_key = ScopedPromiseKeyRef::builder()
            .partition_key(&partition_key)
            .scope(&service_id.scope)
            .service_name(&service_name)
            .service_key(&service_key);

        // Right now the WBWI does not support range deletions :-(
        // That's why we need to iterate over the individual promises.
        let keys = storage.for_each_key_value_in_place(
            TableScan::SinglePartitionKeyPrefix(service_id.partition_key(), prefix_key),
            |k, _| TableScanIterationDecision::Emit(Ok(Box::from(k))),
        )?;

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

        let keys = storage.for_each_key_value_in_place(
            TableScan::SinglePartitionKeyPrefix(service_id.partition_key(), prefix_key),
            |k, _| TableScanIterationDecision::Emit(Ok(Box::from(k))),
        )?;

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
        get_promise(self, service_id, key)
    }
}

impl ScanPromiseTable for PartitionStore {
    fn for_each_promise<
        F: FnMut(OwnedPromiseRow) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: KeyRange,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        // Share callback between two sequential scans via Arc<Mutex<>>
        // (needed because iterator_for_each requires 'static closures).
        // No contention: scans are awaited sequentially.
        let f = Arc::new(parking_lot::Mutex::new(f));

        // todo(tillrohrmann) remove once we migrated the unscoped promises to the scoped promises table
        let f_unscoped = Arc::clone(&f);
        let unscoped = self
            .iterator_for_each(
                "df-promise",
                Priority::Low,
                TableScan::FullScanPartitionKeyRange::<PromiseKey>(range),
                move |(mut k, mut v)| {
                    let key = break_on_err(PromiseKey::deserialize_from(&mut k))?;
                    let metadata = break_on_err(Promise::decode(&mut v))?;
                    let (partition_key, service_name, service_key, promise_key) = key.split();
                    let service_id = ServiceId::with_partition_key(
                        partition_key,
                        service_name,
                        break_on_err(ByteString::try_from(service_key).map_err(|e| {
                            StorageError::Generic(anyhow::anyhow!("Cannot convert to string {e}"))
                        }))?,
                    );
                    f_unscoped.lock()(OwnedPromiseRow {
                        service_id,
                        key: promise_key,
                        metadata,
                    })
                    .map_break(Ok)
                },
            )
            .map_err(|_| StorageError::OperationalError)?;

        let f_scoped = f;
        let scoped = self
            .iterator_for_each(
                "df-promise-scoped",
                Priority::Low,
                TableScan::FullScanPartitionKeyRange::<ScopedPromiseKey>(range),
                move |(mut k, mut v)| {
                    let key = break_on_err(ScopedPromiseKey::deserialize_from(&mut k))?;
                    let metadata = break_on_err(Promise::decode(&mut v))?;
                    let (_partition_key, scope, service_name, service_key, promise_key) =
                        key.split();
                    let service_id = ServiceId::new(
                        scope,
                        ByteString::from(service_name.as_str()),
                        ByteString::from(service_key.as_str()),
                    );
                    f_scoped.lock()(OwnedPromiseRow {
                        service_id,
                        key: ByteString::from(promise_key.as_str()),
                        metadata,
                    })
                    .map_break(Ok)
                },
            )
            .map_err(|_| StorageError::OperationalError)?;

        Ok(async move {
            unscoped.await?;
            scoped.await?;
            Ok(())
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
        get_promise(self, service_id, key)
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
        put_promise(self, service_id, key, promise)
    }

    fn delete_all_promises(&mut self, service_id: &ServiceId) -> Result<()> {
        self.assert_partition_key(service_id)?;
        delete_all_promises(self, service_id)
    }
}
