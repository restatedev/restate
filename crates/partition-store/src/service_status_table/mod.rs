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

use bytestring::ByteString;
use futures::Stream;

use restate_rocksdb::{Priority, RocksDbPerfGuard};
use restate_storage_api::service_status_table::{
    ReadOnlyVirtualObjectStatusTable, ScanVirtualObjectStatusTable, VirtualObjectStatus,
    VirtualObjectStatusTable,
};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::WithPartitionKey;
use restate_types::identifiers::{PartitionKey, ServiceId};

use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::protobuf_types::PartitionStoreProtobufValue;
use crate::scan::TableScan;
use crate::{PartitionStore, TableKind};
use crate::{PartitionStoreTransaction, StorageAccess};

define_table_key!(
    TableKind::ServiceStatus,
    KeyKind::ServiceStatus,
    ServiceStatusKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: ByteString
    )
);

impl PartitionStoreProtobufValue for VirtualObjectStatus {
    type ProtobufType = crate::protobuf_types::v1::VirtualObjectStatus;
}

fn write_status_key(service_id: &ServiceId) -> ServiceStatusKey {
    ServiceStatusKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone())
}

fn put_virtual_object_status<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    status: &VirtualObjectStatus,
) -> Result<()> {
    let key = ServiceStatusKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone());
    if *status == VirtualObjectStatus::Unlocked {
        storage.delete_key(&key)
    } else {
        storage.put_kv(key, status)
    }
}

fn get_virtual_object_status<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
) -> Result<VirtualObjectStatus> {
    let _x = RocksDbPerfGuard::new("get-virtual-obj-status");
    let key = ServiceStatusKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone());

    storage
        .get_value(key)
        .map(|value| value.unwrap_or(VirtualObjectStatus::Unlocked))
}

fn delete_virtual_object_status<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
) -> Result<()> {
    let key = write_status_key(service_id);
    storage.delete_key(&key)
}

impl ReadOnlyVirtualObjectStatusTable for PartitionStore {
    async fn get_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<VirtualObjectStatus> {
        self.assert_partition_key(service_id)?;
        get_virtual_object_status(self, service_id)
    }
}

impl ScanVirtualObjectStatusTable for PartitionStore {
    fn scan_virtual_object_statuses(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = Result<(ServiceId, VirtualObjectStatus)>> + Send> {
        self.run_iterator(
            "df-vo-status",
            Priority::Low,
            TableScan::FullScanPartitionKeyRange::<ServiceStatusKey>(range),
            |(mut key, mut value)| {
                let state_key = ServiceStatusKey::deserialize_from(&mut key)?;
                let state_value = VirtualObjectStatus::decode(&mut value)?;

                let (partition_key, service_name, service_key) = state_key.into_inner_ok_or()?;

                Ok((
                    ServiceId::from_parts(partition_key, service_name, service_key),
                    state_value,
                ))
            },
        )
        .map_err(|_| StorageError::OperationalError)
    }
}

impl ReadOnlyVirtualObjectStatusTable for PartitionStoreTransaction<'_> {
    async fn get_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<VirtualObjectStatus> {
        self.assert_partition_key(service_id)?;
        get_virtual_object_status(self, service_id)
    }
}

impl VirtualObjectStatusTable for PartitionStoreTransaction<'_> {
    async fn put_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
        status: &VirtualObjectStatus,
    ) -> Result<()> {
        self.assert_partition_key(service_id)?;
        put_virtual_object_status(self, service_id, status)
    }

    async fn delete_virtual_object_status(&mut self, service_id: &ServiceId) -> Result<()> {
        self.assert_partition_key(service_id)?;
        delete_virtual_object_status(self, service_id)
    }
}
