use crate::codec::ProtoValue;
use crate::keys::{define_table_key, TableKey};
use crate::TableKind::Timers;
use crate::TableScanIterationDecision::Emit;
use crate::{PutFuture, RocksDBTransaction};
use crate::{Result, TableScan, TableScanIterationDecision};
use bytes::Bytes;
use bytestring::ByteString;
use prost::Message;
use restate_storage_api::timer_table::{Timer, TimerKey, TimerTable};
use restate_storage_api::{ready, GetStream, StorageError};
use restate_storage_proto::storage;
use restate_types::identifiers::PartitionId;
use uuid::Uuid;

define_table_key!(
    Timers,
    TimersKey(
        partition_id: PartitionId,
        timestamp: u64,
        service_name: ByteString,
        service_key: Bytes,
        invocation_id: Bytes,
        journal_index: u32
    )
);

#[inline]
fn write_timer_key(partition_id: PartitionId, timer_key: &TimerKey) -> TimersKey {
    TimersKey::default()
        .partition_id(partition_id)
        .timestamp(timer_key.timestamp)
        .service_name(
            timer_key
                .service_invocation_id
                .service_id
                .service_name
                .clone(),
        )
        .service_key(timer_key.service_invocation_id.service_id.key.clone())
        .invocation_id(
            timer_key // TODO: fix this
                .service_invocation_id
                .invocation_id
                .as_ref()
                .to_vec()
                .into(),
        )
        .journal_index(timer_key.journal_index)
}

#[inline]
fn exclusive_start_key_range(
    partition_id: PartitionId,
    timer_key: Option<&TimerKey>,
) -> TableScan<TimersKey> {
    if let Some(timer_key) = timer_key {
        let mut lower_bound = write_timer_key(partition_id, timer_key);

        let next_index = lower_bound.journal_index.map(|i| i + 1).unwrap_or(1);

        lower_bound.journal_index = Some(next_index);

        let upper_bound = TimersKey::default()
            .partition_id(partition_id)
            .timestamp(u64::MAX);

        TableScan::KeyRangeInclusive(lower_bound, upper_bound)
    } else {
        TableScan::Partition(partition_id)
    }
}

#[inline]
fn timer_key_from_key_slice(slice: &[u8]) -> Result<TimerKey> {
    let mut key = Bytes::copy_from_slice(slice);
    let key = TimersKey::deserialize_from(&mut key)?;
    if !key.is_complete() {
        return Err(StorageError::DataIntegrityError);
    }
    let invocation_uuid = Uuid::from_slice(key.invocation_id_ok_or()?.as_ref())
        .map_err(|error| StorageError::Generic(error.into()))?;
    let timer_key = TimerKey {
        service_invocation_id: restate_types::invocation::ServiceInvocationId::new(
            key.service_name.unwrap(),
            key.service_key.unwrap(),
            invocation_uuid,
        ),
        journal_index: key.journal_index.unwrap(),
        timestamp: key.timestamp.unwrap(),
    };

    Ok(timer_key)
}

impl TimerTable for RocksDBTransaction {
    fn add_timer(&mut self, partition_id: PartitionId, key: &TimerKey, timer: Timer) -> PutFuture {
        let key = write_timer_key(partition_id, key);
        let value = ProtoValue(storage::v1::Timer::from(timer));

        self.put_kv(key, value);

        ready()
    }

    fn delete_timer(&mut self, partition_id: PartitionId, key: &TimerKey) -> PutFuture {
        let key = write_timer_key(partition_id, key);

        self.delete_key(&key);

        ready()
    }

    fn next_timers_greater_than(
        &mut self,
        partition_id: PartitionId,
        exclusive_start: Option<&TimerKey>,
        limit: usize,
    ) -> GetStream<(TimerKey, Timer)> {
        let scan = exclusive_start_key_range(partition_id, exclusive_start);
        let mut produced = 0;
        self.for_each_key_value(scan, move |k, v| {
            if produced >= limit {
                return TableScanIterationDecision::Break;
            }
            produced += 1;
            let res = decode_seq_timer_key_value(k, v);
            Emit(res)
        })
    }
}

fn decode_seq_timer_key_value(k: &[u8], v: &[u8]) -> Result<(TimerKey, Timer)> {
    let timer_key = timer_key_from_key_slice(k)?;

    let timer = Timer::try_from(
        storage::v1::Timer::decode(v).map_err(|error| StorageError::Generic(error.into()))?,
    )?;

    Ok((timer_key, timer))
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_types::invocation::ServiceInvocationId;

    #[test]
    fn round_trip() {
        let uuid = Uuid::try_parse("018756fa-3f7f-7854-a76b-42c59a3d7f2c").expect("invalid uuid");
        let key = TimerKey {
            service_invocation_id: ServiceInvocationId::new("svc-1", "key-1", uuid),
            journal_index: 1448,
            timestamp: 87654321,
        };

        let key_bytes = write_timer_key(1337, &key).serialize();
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
        let key_a_bytes = write_timer_key(1, &key_a).serialize();
        let key_b_bytes = write_timer_key(1, &key_b).serialize();

        assert!(less_than(&key_a_bytes, &key_b_bytes));

        let (low, high) = match exclusive_start_key_range(1, Some(&key_a)) {
            TableScan::KeyRangeInclusive(low, high) => (low, high),
            _ => panic!(""),
        };
        let low = low.serialize();
        let high = high.serialize();

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
