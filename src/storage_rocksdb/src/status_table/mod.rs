use crate::codec::ProtoValue;
use crate::keys::{define_table_key, TableKey};
use crate::TableKind::Status;
use crate::TableScan::PartitionKeyRange;
use crate::TableScanIterationDecision;
use crate::{GetFuture, PutFuture, RocksDBTransaction};
use bytes::Bytes;
use bytestring::ByteString;
use prost::Message;
use restate_common::types::{InvocationStatus, PartitionKey, ServiceId, ServiceInvocationId};
use restate_storage_api::status_table::StatusTable;
use restate_storage_api::{ready, GetStream, StorageError};
use restate_storage_proto::storage;
use std::ops::RangeInclusive;
use uuid::Uuid;

define_table_key!(
    Status,
    StatusKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: Bytes
    )
);

fn write_status_key(partition_key: PartitionKey, service_id: &ServiceId) -> StatusKey {
    StatusKey::default()
        .partition_key(partition_key)
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone())
}

fn status_key_from_bytes(mut bytes: Bytes) -> crate::Result<(PartitionKey, ServiceId)> {
    let key = StatusKey::deserialize_from(&mut bytes)?;
    let partition_key = key.partition_key_ok_or().cloned()?;
    Ok((
        partition_key,
        ServiceId::new(
            key.service_name_ok_or().cloned()?,
            key.service_key_ok_or().cloned()?,
        ),
    ))
}

impl StatusTable for RocksDBTransaction {
    fn put_invocation_status(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        status: InvocationStatus,
    ) -> PutFuture {
        let key = StatusKey::default()
            .partition_key(partition_key)
            .service_name(service_id.service_name.clone())
            .service_key(service_id.key.clone());

        let value = ProtoValue(storage::v1::InvocationStatus::from(status));

        self.put_kv(key, value);

        ready()
    }

    fn get_invocation_status(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
    ) -> GetFuture<Option<InvocationStatus>> {
        let key = StatusKey::default()
            .partition_key(partition_key)
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

    fn delete_invocation_status(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
    ) -> PutFuture {
        let key = write_status_key(partition_key, service_id);

        self.delete_key(&key);

        ready()
    }

    fn invoked_invocations(
        &mut self,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> GetStream<ServiceInvocationId> {
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

fn decode_status_key_value(k: &[u8], v: &[u8]) -> crate::Result<Option<ServiceInvocationId>> {
    let status = storage::v1::InvocationStatus::decode(v)
        .map_err(|error| StorageError::Generic(error.into()))?;
    if let Some(storage::v1::invocation_status::Status::Invoked(
        storage::v1::invocation_status::Invoked { invocation_id, .. },
    )) = status.status
    {
        let (_, service_id) = status_key_from_bytes(Bytes::copy_from_slice(k))?;
        let uuid = Uuid::from_slice(&invocation_id)
            .map_err(|error| StorageError::Generic(error.into()))?;
        Ok(Some(ServiceInvocationId::new(
            service_id.service_name,
            service_id.key,
            uuid,
        )))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::keys::TableKey;
    use crate::status_table::{status_key_from_bytes, write_status_key};
    use restate_common::types::ServiceId;

    #[test]
    fn round_trip() {
        let key = write_status_key(1337, &ServiceId::new("svc-1", "key-1")).serialize();

        let (partition_key, service_id) = status_key_from_bytes(key.freeze()).unwrap();

        assert_eq!(partition_key, 1337);
        assert_eq!(service_id.service_name, "svc-1");
        assert_eq!(service_id.key, "key-1");
    }
}
