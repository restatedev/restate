use crate::composite_keys::{read_delimited, write_delimited};
use crate::Result;
use crate::TableKind::Timers;
use crate::{write_proto_infallible, PutFuture, RocksDBTransaction};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use prost::Message;
use restate_common::types::{PartitionId, Timer, TimerKey};
use restate_storage_api::timer_table::TimerTable;
use restate_storage_api::{ready, GetStream, StorageError};
use restate_storage_proto::storage;
use uuid::Uuid;

pub struct TimersKeyComponents {
    pub partition_id: Option<PartitionId>,
    pub timestamp: Option<u64>,
    pub service_name: Option<ByteString>,
    pub service_key: Option<Bytes>,
    pub invocation_id: Option<Bytes>,
    pub journal_index: Option<u32>,
}

impl TimersKeyComponents {
    pub(crate) fn to_bytes(&self, bytes: &mut BytesMut) -> Option<()> {
        self.partition_id
            .map(|partition_id| bytes.put_u64(partition_id))?;
        self.timestamp.map(|timestamp| bytes.put_u64(timestamp))?;
        self.service_name
            .as_ref()
            .map(|s| write_delimited(s, bytes))?;
        self.service_key
            .as_ref()
            .map(|s| write_delimited(s, bytes))?;
        self.invocation_id
            .as_ref()
            .map(|s| write_delimited(s, bytes))?;
        self.journal_index
            .map(|journal_index| bytes.put_u32(journal_index))
    }

    pub(crate) fn from_bytes(bytes: &mut Bytes) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            partition_id: bytes.has_remaining().then(|| bytes.get_u64()),
            timestamp: bytes.has_remaining().then(|| bytes.get_u64()),
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
            invocation_id: bytes
                .has_remaining()
                .then(|| read_delimited(bytes))
                .transpose()?,
            journal_index: bytes.has_remaining().then(|| bytes.get_u32()),
        })
    }
}

#[inline]
fn write_timer_key(key: &mut BytesMut, partition_id: PartitionId, timer_key: &TimerKey) {
    key.put_u64(partition_id);
    key.put_u64(timer_key.timestamp);
    write_delimited(
        &timer_key.service_invocation_id.service_id.service_name,
        key,
    );
    write_delimited(&timer_key.service_invocation_id.service_id.key, key);
    write_delimited(timer_key.service_invocation_id.invocation_id, key);
    key.put_u32(timer_key.journal_index);
}

#[inline]
fn exclusive_start_key_range(
    partition_id: PartitionId,
    timer_key: Option<&TimerKey>,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = BytesMut::new();
    key.put_u64(partition_id);

    if let Some(timer_key) = timer_key {
        key.put_u64(timer_key.timestamp);
        write_delimited(
            &timer_key.service_invocation_id.service_id.service_name,
            &mut key,
        );
        write_delimited(&timer_key.service_invocation_id.service_id.key, &mut key);
        write_delimited(timer_key.service_invocation_id.invocation_id, &mut key);
        key.put_u32(timer_key.journal_index + 1);
    }

    let lower_bound = key.to_vec();
    let upper_bound = (partition_id + 1).to_be_bytes().to_vec();

    (lower_bound, upper_bound)
}

#[inline]
fn timer_key_from_key_slice(slice: &[u8]) -> crate::Result<TimerKey> {
    // we must copy the slice to own it, since we need to return an owned TimerKey.
    // but, we peal off various values using the same owned copy and these
    // values share the same underlying memory via reference counting (see Bytes for more details)
    let mut key = Bytes::copy_from_slice(slice);
    let _partition_id = key.get_u64();
    let timestamp = key.get_u64();
    let service_name = read_delimited(&mut key)?;
    let service_key = read_delimited(&mut key)?;
    let invocation_id = read_delimited(&mut key)?;
    let invocation_uuid =
        Uuid::from_slice(&invocation_id).map_err(|error| StorageError::Generic(error.into()))?;
    let journal_index = key.get_u32();
    let timer_key = TimerKey {
        service_invocation_id: restate_common::types::ServiceInvocationId::new(
            unsafe { ByteString::from_bytes_unchecked(service_name) },
            service_key,
            invocation_uuid,
        ),
        journal_index,
        timestamp,
    };

    Ok(timer_key)
}

impl TimerTable for RocksDBTransaction {
    fn add_timer(&mut self, partition_id: PartitionId, key: &TimerKey, timer: Timer) -> PutFuture {
        write_timer_key(self.key_buffer(), partition_id, key);
        write_proto_infallible(self.value_buffer(), storage::v1::Timer::from(timer));

        self.put_kv_buffer(Timers);

        ready()
    }

    fn delete_timer(&mut self, partition_id: PartitionId, key: &TimerKey) -> PutFuture {
        write_timer_key(self.key_buffer(), partition_id, key);

        self.delete_key_buffer(Timers);

        ready()
    }

    fn next_timers_greater_than(
        &mut self,
        partition_id: PartitionId,
        exclusive_start: Option<&TimerKey>,
        limit: usize,
    ) -> GetStream<(TimerKey, Timer)> {
        let (lower_bound, upper_bound) = exclusive_start_key_range(partition_id, exclusive_start);
        self.spawn_background_scan(move |db, tx| {
            let mut iterator = db.range_iterator(Timers, (lower_bound.clone())..upper_bound);
            iterator.seek(lower_bound);

            let mut produced = 0;
            while let Some((k, v)) = iterator.item() {
                let res = decode_timer_key_value(k, v);

                if tx.blocking_send(res).is_err() {
                    break;
                }
                produced += 1;
                if produced == limit {
                    break;
                }
                iterator.next();
            }
        })
    }
}

fn decode_timer_key_value(k: &[u8], v: &[u8]) -> crate::Result<(TimerKey, Timer)> {
    let timer_key = timer_key_from_key_slice(k)?;

    let timer = Timer::try_from(
        storage::v1::Timer::decode(v).map_err(|error| StorageError::Generic(error.into()))?,
    )?;

    Ok((timer_key, timer))
}

#[cfg(test)]
mod tests {
    use crate::timer_table::{
        exclusive_start_key_range, timer_key_from_key_slice, write_timer_key,
    };
    use bytes::BytesMut;
    use restate_common::types::{ServiceInvocationId, TimerKey};
    use uuid::Uuid;

    #[test]
    fn round_trip() {
        let uuid = Uuid::try_parse("018756fa-3f7f-7854-a76b-42c59a3d7f2c").expect("invalid uuid");
        let key = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 1448,
            timestamp: 87654321,
        };

        let mut key_bytes = BytesMut::new();
        write_timer_key(&mut key_bytes, 1337, &key);
        let got = timer_key_from_key_slice(&key_bytes).expect("should not fail");

        assert_eq!(got, key);
    }

    #[test]
    fn test_lexicographical_sorting_by_timestamp() {
        let uuid = Uuid::try_parse("018756fa-3f7f-7854-a76b-42c59a3d7f2c").expect("invalid uuid");

        let a = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 0,
            timestamp: 300,
        };
        let b = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 0,
            timestamp: 301,
        };
        assert_in_range(a, b);
    }

    #[test]
    fn test_lexicographical_sorting_by_svc() {
        let uuid = Uuid::try_parse("018756fa-3f7f-7854-a76b-42c59a3d7f2c").expect("invalid uuid");
        let a = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 0,
            timestamp: 300,
        };
        let b = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-2", "key-1", uuid),
            journal_index: 0,
            timestamp: 300,
        };
        assert_in_range(a, b);
        //
        // same timestamp and svc, different key
        //
        let a = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 0,
            timestamp: 300,
        };
        let b = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-2", uuid),
            journal_index: 0,
            timestamp: 300,
        };
        assert_in_range(a, b);
        //
        // same timestamp, svc, key, but different invocation
        //
        let a = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 0,
            timestamp: 300,
        };
        let uuid2 = Uuid::try_parse("018756fa-3f7f-7854-a76b-42c59a3d7f2d").expect("invalid uuid");
        let b = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid2),
            journal_index: 0,
            timestamp: 300,
        };
        assert_in_range(a, b);
        //
        // same everything but journal index
        //
        let a = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 0,
            timestamp: 300,
        };
        let b = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 1,
            timestamp: 300,
        };
        assert_in_range(a, b);
    }

    #[test]
    fn test_lexicographical_sorting_by_svc_keys() {
        let uuid = Uuid::try_parse("018756fa-3f7f-7854-a76b-42c59a3d7f2c").expect("invalid uuid");
        let a = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 0,
            timestamp: 300,
        };
        let b = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-2", uuid),
            journal_index: 0,
            timestamp: 300,
        };
        assert_in_range(a, b);
        //
        // same timestamp, svc, key, but different invocation
        //
        let a = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 0,
            timestamp: 300,
        };
        let uuid2 = Uuid::try_parse("018756fa-3f7f-7854-a76b-42c59a3d7f2d").expect("invalid uuid");
        let b = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid2),
            journal_index: 0,
            timestamp: 300,
        };
        assert_in_range(a, b);
        //
        // same everything but journal index
        //
        let a = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 0,
            timestamp: 300,
        };
        let b = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 1,
            timestamp: 300,
        };
        assert_in_range(a, b);
    }

    #[test]
    fn test_lexicographical_sorting_by_invocation() {
        let uuid = Uuid::try_parse("018756fa-3f7f-7854-a76b-42c59a3d7f2c").expect("invalid uuid");
        let a = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 0,
            timestamp: 300,
        };
        let uuid2 = Uuid::try_parse("018756fa-3f7f-7854-a76b-42c59a3d7f2d").expect("invalid uuid");
        let b = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid2),
            journal_index: 0,
            timestamp: 300,
        };
        assert_in_range(a, b);
    }

    #[test]
    fn test_lexicographical_sorting_by_journal_index() {
        let uuid = Uuid::try_parse("018756fa-3f7f-7854-a76b-42c59a3d7f2c").expect("invalid uuid");
        let a = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 0,
            timestamp: 300,
        };
        let b = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 1,
            timestamp: 300,
        };
        assert_in_range(a, b);
    }

    fn assert_in_range(key_a: TimerKey, key_b: TimerKey) {
        let mut key_a_bytes = BytesMut::new();
        let mut key_b_bytes = BytesMut::new();

        write_timer_key(&mut key_a_bytes, 1, &key_a);
        write_timer_key(&mut key_b_bytes, 1, &key_b);

        assert!(less_than(&key_a_bytes, &key_b_bytes));

        let (low, high) = exclusive_start_key_range(1, Some(&key_a));

        assert!(less_than(key_a_bytes, &low));
        assert!(less_than_or_equal(&low, &key_b_bytes));
        assert!(less_than(&key_b_bytes, high));
    }

    fn less_than(a: impl AsRef<[u8]>, b: impl AsRef<[u8]>) -> bool {
        a.as_ref() < b.as_ref()
    }

    fn less_than_or_equal(a: impl AsRef<[u8]>, b: impl AsRef<[u8]>) -> bool {
        a.as_ref() <= b.as_ref()
    }
}
