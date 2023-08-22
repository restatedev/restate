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
use crate::scan::TableScan;
use crate::TableKind::Status;
use crate::TableScan::PartitionKeyRange;
use crate::{GetFuture, PutFuture, RocksDBTransaction};
use crate::{RocksDBStorage, TableScanIterationDecision};
use bytes::Bytes;
use bytestring::ByteString;
use futures_util::FutureExt;
use prost::Message;
use restate_storage_api::status_table::{InvocationStatus, StatusTable};
use restate_storage_api::{ready, GetStream, StorageError};
use restate_storage_proto::storage;
use restate_types::identifiers::{InvocationUuid, FullInvocationId, WithPartitionKey};
use restate_types::identifiers::{PartitionKey, ServiceId};
use std::ops::RangeInclusive;
use tokio_stream::StreamExt;
use uuid::Uuid;

define_table_key!(
    Status,
    StatusKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: Bytes
    )
);

fn write_status_key(service_id: &ServiceId) -> StatusKey {
    StatusKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone())
}

fn status_key_from_bytes(mut bytes: Bytes) -> crate::Result<ServiceId> {
    let key = StatusKey::deserialize_from(&mut bytes)?;
    let partition_key = key.partition_key_ok_or().cloned()?;
    Ok(ServiceId::with_partition_key(
        partition_key,
        key.service_name_ok_or().cloned()?,
        key.service_key_ok_or().cloned()?,
    ))
}

impl StatusTable for RocksDBTransaction {
    fn put_invocation_status(
        &mut self,
        service_id: &ServiceId,
        status: InvocationStatus,
    ) -> PutFuture {
        let key = StatusKey::default()
            .partition_key(service_id.partition_key())
            .service_name(service_id.service_name.clone())
            .service_key(service_id.key.clone());
        if status == InvocationStatus::Free {
            self.delete_key(&key);
            return ready();
        }

        let value = ProtoValue(storage::v1::InvocationStatus::from(status));

        self.put_kv(key, value);

        ready()
    }

    fn get_invocation_status(
        &mut self,
        service_id: &ServiceId,
    ) -> GetFuture<Option<InvocationStatus>> {
        let key = StatusKey::default()
            .partition_key(service_id.partition_key())
            .service_name(service_id.service_name.clone())
            .service_key(service_id.key.clone());

        self.get_blocking(key, move |_, v| {
            if v.is_none() {
                return Ok(None);
            }
            let v = v.unwrap();
            let proto = storage::v1::InvocationStatus::decode(v)
                .map_err(|err| StorageError::Generic(err.into()))?;
            InvocationStatus::try_from(proto)
                .map_err(StorageError::from)
                .map(Some)
        })
    }

    fn get_invocation_status_from(
        &mut self,
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
    ) -> GetFuture<Option<(ServiceId, InvocationStatus)>> {
        let key = StatusKey::default().partition_key(partition_key);

        let mut stream = self.for_each_key_value(TableScan::KeyPrefix(key), move |k, v| {
            let invocation_status = match decode_status(v) {
                Ok(invocation_status)
                    if invocation_status.invocation_uuid() == Some(invocation_uuid) =>
                {
                    invocation_status
                }
                Ok(_) => {
                    return TableScanIterationDecision::Continue;
                }
                Err(err) => {
                    return TableScanIterationDecision::BreakWith(Err(err));
                }
            };
            TableScanIterationDecision::BreakWith(
                status_key_from_bytes(Bytes::copy_from_slice(k)).map(|id| (id, invocation_status)),
            )
        });

        async move { stream.next().await.transpose() }.boxed()
    }

    fn delete_invocation_status(&mut self, service_id: &ServiceId) -> PutFuture {
        let key = write_status_key(service_id);

        self.delete_key(&key);

        ready()
    }

    fn invoked_invocations(
        &mut self,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> GetStream<FullInvocationId> {
        self.for_each_key_value(
            PartitionKeyRange::<StatusKey>(partition_key_range),
            |k, v| {
                let result = decode_status_key_value(k, v).transpose();
                if let Some(res) = result {
                    TableScanIterationDecision::Emit(res)
                } else {
                    TableScanIterationDecision::Continue
                }
            },
        )
    }
}

#[derive(Clone, Debug)]
pub struct OwnedStatusRow {
    pub partition_key: PartitionKey,
    pub service: ByteString,
    pub service_key: Bytes,
    pub invocation_status: InvocationStatus,
}

impl RocksDBStorage {
    pub fn all_status(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Iterator<Item = OwnedStatusRow> + '_ {
        let iter = self.iterator_from(PartitionKeyRange::<StatusKey>(range));
        OwnedIterator::new(iter).map(|(mut key, value)| {
            let state_key = StatusKey::deserialize_from(&mut key).unwrap();
            let state_value = storage::v1::InvocationStatus::decode(value).unwrap();
            let state_value = InvocationStatus::try_from(state_value).unwrap();
            OwnedStatusRow {
                partition_key: state_key.partition_key.unwrap(),
                service: state_key.service_name.unwrap(),
                service_key: state_key.service_key.unwrap(),
                invocation_status: state_value,
            }
        })
    }
}

fn decode_status(v: &[u8]) -> crate::Result<InvocationStatus> {
    let proto = storage::v1::InvocationStatus::decode(v)
        .map_err(|error| StorageError::Generic(error.into()))?;
    InvocationStatus::try_from(proto).map_err(StorageError::from)
}

fn decode_status_key_value(k: &[u8], v: &[u8]) -> crate::Result<Option<FullInvocationId>> {
    let status = storage::v1::InvocationStatus::decode(v)
        .map_err(|error| StorageError::Generic(error.into()))?;
    if let Some(storage::v1::invocation_status::Status::Invoked(
        storage::v1::invocation_status::Invoked {
            invocation_uuid, ..
        },
    )) = status.status
    {
        let service_id = status_key_from_bytes(Bytes::copy_from_slice(k))?;
        let uuid = Uuid::from_slice(&invocation_uuid)
            .map_err(|error| StorageError::Generic(error.into()))?;
        Ok(Some(FullInvocationId::with_service_id(service_id, uuid)))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::keys::TableKey;
    use crate::status_table::{status_key_from_bytes, write_status_key};
    use restate_types::identifiers::{ServiceId, WithPartitionKey};

    #[test]
    fn round_trip() {
        let key =
            write_status_key(&ServiceId::with_partition_key(1337, "svc-1", "key-1")).serialize();

        let service_id = status_key_from_bytes(key.freeze()).unwrap();

        assert_eq!(service_id.partition_key(), 1337);
        assert_eq!(service_id.service_name, "svc-1");
        assert_eq!(service_id.key, "key-1");
    }
}
