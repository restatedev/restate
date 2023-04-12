use crate::composite_keys::{end_key_successor, read_delimited, write_delimited};
use crate::Result;
use crate::TableKind::Status;
use crate::{write_proto_infallible, GetFuture, PutFuture, RocksDBStorage, RocksDBTransaction};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use prost::Message;
use restate_common::types::{InvocationStatus, PartitionKey, ServiceId, ServiceInvocationId};
use restate_storage_api::status_table::StatusTable;
use restate_storage_api::{ready, GetStream, StorageError};
use restate_storage_proto::storage;
use std::ops::RangeInclusive;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

pub struct StatusKeyComponents {
    pub partition_key: Option<PartitionKey>,
    pub service_name: Option<ByteString>,
    pub service_key: Option<Bytes>,
}

impl StatusKeyComponents {
    pub(crate) fn to_bytes(&self, bytes: &mut BytesMut) -> Option<()> {
        self.partition_key
            .map(|partition_key| bytes.put_u64(partition_key))?;
        self.service_name
            .as_ref()
            .map(|s| write_delimited(s, bytes))?;
        self.service_key.as_ref().map(|s| write_delimited(s, bytes))
    }

    pub(crate) fn from_bytes(bytes: &mut Bytes) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            partition_key: bytes.has_remaining().then(|| bytes.get_u64()),
            service_name: bytes
                .has_remaining()
                .then(|| {
                    read_delimited(bytes)
                        // SAFETY: this is safe since the service name was constructed from a ByteString.
                        .map(|bytes| unsafe { ByteString::from_bytes_unchecked(bytes) })
                })
                .transpose()?,
            service_key: bytes
                .has_remaining()
                .then(|| read_delimited(bytes))
                .transpose()?,
        })
    }
}

fn write_status_key(key: &mut BytesMut, partition_key: PartitionKey, service_id: &ServiceId) {
    key.put_u64(partition_key);
    write_delimited(&service_id.service_name, key);
    write_delimited(&service_id.key, key);
}

fn status_key_from_bytes(mut bytes: Bytes) -> crate::Result<(PartitionKey, ServiceId)> {
    let partition_key = bytes.get_u64();
    let service_name = read_delimited(&mut bytes)?;
    let service_key = read_delimited(&mut bytes)?;

    // SAFETY: this is safe since the service name was constructed from a ByteString.
    let service_name_bs = unsafe { ByteString::from_bytes_unchecked(service_name) };

    Ok((partition_key, ServiceId::new(service_name_bs, service_key)))
}

impl StatusTable for RocksDBTransaction {
    fn put_invocation_status(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        status: InvocationStatus,
    ) -> PutFuture {
        write_status_key(self.key_buffer(), partition_key, service_id);
        write_proto_infallible(
            self.value_buffer(),
            storage::v1::InvocationStatus::from(status),
        );

        self.put_kv_buffer(Status);

        ready()
    }

    fn get_invocation_status(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
    ) -> GetFuture<Option<InvocationStatus>> {
        write_status_key(self.key_buffer(), partition_key, service_id);
        let key = self.clone_key_buffer();

        self.spawn_blocking(move |db| {
            let proto = db.get_proto::<storage::v1::InvocationStatus>(Status, key)?;
            proto
                .map(InvocationStatus::try_from)
                .transpose()
                .map_err(StorageError::from)
        })
    }

    fn delete_invocation_status(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
    ) -> PutFuture {
        write_status_key(self.key_buffer(), partition_key, service_id);

        self.delete_key_buffer(Status);

        ready()
    }

    fn invoked_invocations(
        &mut self,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> GetStream<ServiceInvocationId> {
        self.spawn_background_scan(move |db, tx| {
            find_all_invocation_with_invoked_status(db, tx, partition_key_range)
        })
    }
}

fn find_all_invocation_with_invoked_status(
    db: RocksDBStorage,
    tx: Sender<crate::Result<ServiceInvocationId>>,
    partition_key_range: RangeInclusive<PartitionKey>,
) {
    let start_key = partition_key_range.start().to_be_bytes();
    // compute the end key. Since we are dealing here with partition keys, they can
    // cover the entire keyspace from 0..2^64.
    // in particular the last partition will have its end key equal to that.
    let mut iterator = match end_key_successor(*partition_key_range.end()) {
        None => db.range_iterator(Status, start_key..),
        Some(end_key) => db.range_iterator(Status, start_key..end_key),
    };
    iterator.seek(start_key);
    while let Some((k, v)) = iterator.item() {
        let out = match decode_status_key_value(k, v) {
            Ok(None) => {
                iterator.next();
                continue;
            }
            Ok(Some(sid)) => Ok(sid),
            Err(err) => Err(err),
        };

        if tx.blocking_send(out).is_err() {
            break;
        }
        iterator.next();
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
    use crate::status_table::{status_key_from_bytes, write_status_key};
    use bytes::BytesMut;
    use restate_common::types::ServiceId;

    #[test]
    fn round_trip() {
        let mut key = BytesMut::new();
        write_status_key(&mut key, 1337, &ServiceId::new("svc-1", "key-1"));

        let (partition_key, service_id) = status_key_from_bytes(key.freeze()).unwrap();

        assert_eq!(partition_key, 1337);
        assert_eq!(service_id.service_name, "svc-1");
        assert_eq!(service_id.key, "key-1");
    }
}
