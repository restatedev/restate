// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytestring::ByteString;

use restate_rocksdb::RocksDbReadPerfGuard;
use restate_storage_api::Result;
use restate_storage_api::service_status_table::{
    ReadVirtualObjectStatusTable, VirtualObjectStatus, WriteVirtualObjectStatusTable,
};
use restate_types::identifiers::{PartitionKey, ServiceId, WithPartitionKey};

use crate::keys::{KeyKind, define_table_key};
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess, TableKind};

define_table_key!(
    TableKind::ServiceStatus,
    KeyKind::ServiceStatus,
    ServiceStatusKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: ByteString
    )
);

#[inline]
fn write_status_key(service_id: &ServiceId) -> ServiceStatusKey {
    ServiceStatusKey {
        partition_key: service_id.partition_key(),
        service_name: service_id.service_name.clone(),
        service_key: service_id.key.clone(),
    }
}

fn put_virtual_object_status<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    status: &VirtualObjectStatus,
) -> Result<()> {
    let key = write_status_key(service_id);
    if *status == VirtualObjectStatus::Unlocked {
        storage.delete_key(&key)
    } else {
        storage.put_kv_proto(key, status)
    }
}

fn get_virtual_object_status<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
) -> Result<VirtualObjectStatus> {
    let _x = RocksDbReadPerfGuard::new("get-virtual-obj-status");
    let key = write_status_key(service_id);

    storage
        .get_value_proto(key)
        .map(|value| value.unwrap_or(VirtualObjectStatus::Unlocked))
}

fn delete_virtual_object_status<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
) -> Result<()> {
    let key = write_status_key(service_id);
    storage.delete_key(&key)
}

impl ReadVirtualObjectStatusTable for PartitionStore {
    async fn get_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<VirtualObjectStatus> {
        self.assert_partition_key(service_id)?;
        get_virtual_object_status(self, service_id)
    }
}

impl ReadVirtualObjectStatusTable for PartitionStoreTransaction<'_> {
    async fn get_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<VirtualObjectStatus> {
        self.assert_partition_key(service_id)?;
        get_virtual_object_status(self, service_id)
    }
}

impl WriteVirtualObjectStatusTable for PartitionStoreTransaction<'_> {
    fn put_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
        status: &VirtualObjectStatus,
    ) -> Result<()> {
        self.assert_partition_key(service_id)?;
        put_virtual_object_status(self, service_id, status)
    }

    fn delete_virtual_object_status(&mut self, service_id: &ServiceId) -> Result<()> {
        self.assert_partition_key(service_id)?;
        delete_virtual_object_status(self, service_id)
    }
}
