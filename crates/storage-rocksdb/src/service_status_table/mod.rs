// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::codec::ProtoValue;
use crate::keys::{define_table_key, TableKey};
use crate::owned_iter::OwnedIterator;
use crate::TableScan::PartitionKeyRange;
use crate::{RocksDBStorage, TableKind};
use crate::{RocksDBTransaction, StorageAccess};
use bytes::Bytes;
use bytestring::ByteString;
use prost::Message;
use restate_storage_api::service_status_table::{
    ReadOnlyVirtualObjectStatusTable, VirtualObjectStatus, VirtualObjectStatusTable,
};
use restate_storage_api::{Result, StorageError};
use restate_storage_proto::storage;
use restate_types::identifiers::{InvocationId, InvocationUuid, WithPartitionKey};
use restate_types::identifiers::{PartitionKey, ServiceId};
use std::ops::RangeInclusive;

define_table_key!(
    TableKind::ServiceStatus,
    ServiceStatusKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: Bytes
    )
);

fn write_status_key(service_id: &ServiceId) -> ServiceStatusKey {
    ServiceStatusKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone())
}

fn to_service_status(
    partition_key: PartitionKey,
    pb_status: storage::v1::ServiceStatus,
) -> Result<VirtualObjectStatus> {
    let invocation_uuid = InvocationUuid::try_from(pb_status).map_err(StorageError::from)?;
    Ok(VirtualObjectStatus::Locked(InvocationId::new(
        partition_key,
        invocation_uuid,
    )))
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
        let value = ProtoValue(storage::v1::ServiceStatus::from(status));
        storage.put_kv(key, value);
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

    storage.get_blocking(key, move |_, v| {
        if v.is_none() {
            return Ok(VirtualObjectStatus::Unlocked);
        }
        let v = v.unwrap();
        let proto = storage::v1::ServiceStatus::decode(v)
            .map_err(|err| StorageError::Generic(err.into()))?;
        to_service_status(service_id.partition_key(), proto)
    })
}

fn delete_virtual_object_status<S: StorageAccess>(storage: &mut S, service_id: &ServiceId) {
    let key = write_status_key(service_id);
    storage.delete_key(&key);
}

impl ReadOnlyVirtualObjectStatusTable for RocksDBStorage {
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
    pub key: Bytes,
    pub status: VirtualObjectStatus,
}

impl RocksDBStorage {
    pub fn all_virtual_object_status(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Iterator<Item = OwnedVirtualObjectStatusRow> + '_ {
        let iter = self.iterator_from(PartitionKeyRange::<ServiceStatusKey>(range));
        OwnedIterator::new(iter).map(|(mut key, value)| {
            let state_key = ServiceStatusKey::deserialize_from(&mut key).unwrap();
            let state_value = storage::v1::ServiceStatus::decode(value).unwrap();
            let state_value =
                to_service_status(state_key.partition_key.unwrap(), state_value).unwrap();
            OwnedVirtualObjectStatusRow {
                partition_key: state_key.partition_key.unwrap(),
                name: state_key.service_name.unwrap(),
                key: state_key.service_key.unwrap(),
                status: state_value,
            }
        })
    }
}
