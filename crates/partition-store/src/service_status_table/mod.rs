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
use crate::{PartitionStore, TableKind};
use crate::{RocksDBTransaction, StorageAccess};
use bytestring::ByteString;
use restate_storage_api::service_status_table::{
    ReadOnlyVirtualObjectStatusTable, VirtualObjectStatus, VirtualObjectStatusTable,
};
use restate_storage_api::Result;
use restate_types::identifiers::WithPartitionKey;
use restate_types::identifiers::{PartitionKey, ServiceId};
use restate_types::storage::StorageCodec;
use std::ops::RangeInclusive;

define_table_key!(
    TableKind::ServiceStatus,
    KeyKind::ServiceStatus,
    ServiceStatusKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: ByteString
    )
);

fn write_status_key(service_id: &ServiceId) -> ServiceStatusKey {
    ServiceStatusKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone())
}

fn put_virtual_object_status<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    status: VirtualObjectStatus,
) {
    let key = ServiceStatusKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone());
    if status == VirtualObjectStatus::Unlocked {
        storage.delete_key(&key);
    } else {
        storage.put_kv(key, status);
    }
}

fn get_virtual_object_status<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
) -> Result<VirtualObjectStatus> {
    let key = ServiceStatusKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone());

    storage
        .get_value(key)
        .map(|value| value.unwrap_or(VirtualObjectStatus::Unlocked))
}

fn delete_virtual_object_status<S: StorageAccess>(storage: &mut S, service_id: &ServiceId) {
    let key = write_status_key(service_id);
    storage.delete_key(&key);
}

impl ReadOnlyVirtualObjectStatusTable for PartitionStore {
    async fn get_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<VirtualObjectStatus> {
        get_virtual_object_status(self, service_id)
    }
}

impl<'a> ReadOnlyVirtualObjectStatusTable for RocksDBTransaction<'a> {
    async fn get_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<VirtualObjectStatus> {
        get_virtual_object_status(self, service_id)
    }
}

impl<'a> VirtualObjectStatusTable for RocksDBTransaction<'a> {
    async fn put_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
        status: VirtualObjectStatus,
    ) {
        put_virtual_object_status(self, service_id, status)
    }

    async fn delete_virtual_object_status(&mut self, service_id: &ServiceId) {
        delete_virtual_object_status(self, service_id)
    }
}

#[derive(Clone, Debug)]
pub struct OwnedVirtualObjectStatusRow {
    pub partition_key: PartitionKey,
    pub name: ByteString,
    pub key: ByteString,
    pub status: VirtualObjectStatus,
}

impl PartitionStore {
    pub fn all_virtual_object_status(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Iterator<Item = OwnedVirtualObjectStatusRow> + '_ {
        let iter = self.iterator_from(FullScanPartitionKeyRange::<ServiceStatusKey>(range));
        OwnedIterator::new(iter).map(|(mut key, mut value)| {
            let state_key = ServiceStatusKey::deserialize_from(&mut key).unwrap();
            let state_value = StorageCodec::decode::<VirtualObjectStatus, _>(&mut value).unwrap();
            OwnedVirtualObjectStatusRow {
                partition_key: state_key.partition_key.unwrap(),
                name: state_key.service_name.unwrap(),
                key: state_key.service_key.unwrap(),
                status: state_value,
            }
        })
    }
}
