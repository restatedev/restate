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
use crate::RocksDBTransaction;
use crate::TableKind::Timers;
use crate::TableScanIterationDecision::Emit;
use crate::{TableScan, TableScanIterationDecision};
use futures::Stream;
use prost::Message;
use restate_storage_api::timer_table::{Timer, TimerKey, TimerTable};
use restate_storage_api::{Result, StorageError};
use restate_storage_proto::storage;
use restate_types::identifiers::{InvocationUuid, PartitionId};

define_table_key!(
    Timers,
    TimersKey(
        partition_id: PartitionId,
        timestamp: u64,
        invocation_id: InvocationUuid,
        journal_index: u32
    )
);

#[inline]
fn write_timer_key(partition_id: PartitionId, timer_key: &TimerKey) -> TimersKey {
    TimersKey::default()
        .partition_id(partition_id)
        .timestamp(timer_key.timestamp)
        .invocation_id(timer_key.invocation_uuid)
        .journal_index(timer_key.journal_index)
}

#[inline]
fn timer_key_from_key_slice(slice: &[u8]) -> Result<TimerKey> {
    let mut buf = std::io::Cursor::new(slice);
    let key = TimersKey::deserialize_from(&mut buf)?;
    if !key.is_complete() {
        return Err(StorageError::DataIntegrityError);
    }
    let timer_key = TimerKey {
        invocation_uuid: key.invocation_id.unwrap(),
        journal_index: key.journal_index.unwrap(),
        timestamp: key.timestamp.unwrap(),
    };

    Ok(timer_key)
}

fn decode_seq_timer_key_value(k: &[u8], v: &[u8]) -> Result<(TimerKey, Timer)> {
    let timer_key = timer_key_from_key_slice(k)?;

    let timer = Timer::try_from(
        storage::v1::Timer::decode(v).map_err(|error| StorageError::Generic(error.into()))?,
    )?;

    Ok((timer_key, timer))
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

impl<'a> TimerTable for RocksDBTransaction<'a> {
    async fn add_timer(&mut self, partition_id: PartitionId, key: &TimerKey, timer: Timer) {
        let key = write_timer_key(partition_id, key);
        let value = ProtoValue(storage::v1::Timer::from(timer));

        self.put_kv(key, value);
    }

    async fn delete_timer(&mut self, partition_id: PartitionId, key: &TimerKey) {
        let key = write_timer_key(partition_id, key);

        self.delete_key(&key);
    }

    fn next_timers_greater_than(
        &mut self,
        partition_id: PartitionId,
        exclusive_start: Option<&TimerKey>,
        limit: usize,
    ) -> impl Stream<Item = Result<(TimerKey, Timer)>> + Send {
        let scan = exclusive_start_key_range(partition_id, exclusive_start);
        let mut produced = 0;
        self.for_each_key_value_in_place(scan, move |k, v| {
            if produced >= limit {
                return TableScanIterationDecision::Break;
            }
            produced += 1;
            let res = decode_seq_timer_key_value(k, v);
            Emit(res)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn round_trip() {
        let uuid = Uuid::try_parse("018756fa-3f7f-7854-a76b-42c59a3d7f2c").expect("invalid uuid");
        let key = TimerKey {
            invocation_uuid: uuid.into(),
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
            invocation_uuid: uuid.into(),
            journal_index: 0,
            timestamp: 300,
        };
        let b = TimerKey {
            invocation_uuid: uuid.into(),
            journal_index: 0,
            timestamp: 301,
        };
        assert_in_range(a, b);
    }

    #[test]
    fn test_lexicographical_sorting_by_invocation() {
        let uuid = Uuid::try_parse("018756fa-3f7f-7854-a76b-42c59a3d7f2c").expect("invalid uuid");
        let a = TimerKey {
            invocation_uuid: uuid.into(),
            journal_index: 0,
            timestamp: 300,
        };
        let uuid2 = Uuid::try_parse("018756fa-3f7f-7854-a76b-42c59a3d7f2d").expect("invalid uuid");
        let b = TimerKey {
            invocation_uuid: uuid2.into(),
            journal_index: 0,
            timestamp: 300,
        };
        assert_in_range(a, b);
    }

    #[test]
    fn test_lexicographical_sorting_by_journal_index() {
        let uuid = Uuid::try_parse("018756fa-3f7f-7854-a76b-42c59a3d7f2c").expect("invalid uuid");
        let a = TimerKey {
            invocation_uuid: uuid.into(),
            journal_index: 0,
            timestamp: 300,
        };
        let b = TimerKey {
            invocation_uuid: uuid.into(),
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
