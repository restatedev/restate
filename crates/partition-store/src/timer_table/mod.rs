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
use crate::TableKind::Timers;
use crate::TableScanIterationDecision::Emit;
use crate::{PartitionStore, RocksDBTransaction, StorageAccess};
use crate::{TableScan, TableScanIterationDecision};
use futures::Stream;
use futures_util::stream;
use restate_storage_api::timer_table::{Timer, TimerKey, TimerKind, TimerTable};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{InvocationUuid, PartitionId};
use restate_types::storage::StorageCodec;

define_table_key!(
    Timers,
    KeyKind::Timers,
    TimersKey(
        partition_id: PartitionId,
        timestamp: u64,
        kind: TimerKind,
    )
);

#[inline]
fn write_timer_key(partition_id: PartitionId, timer_key: &TimerKey) -> TimersKey {
    TimersKey::default()
        .partition_id(partition_id)
        .timestamp(timer_key.timestamp)
        .kind(timer_key.kind.clone())
}

#[inline]
fn timer_key_from_key_slice(slice: &[u8]) -> Result<TimerKey> {
    let mut buf = std::io::Cursor::new(slice);
    let key = TimersKey::deserialize_from(&mut buf)?;
    if !key.is_complete() {
        return Err(StorageError::DataIntegrityError);
    }
    let timer_key = TimerKey {
        timestamp: key.timestamp.unwrap(),
        kind: key.kind.unwrap(),
    };

    Ok(timer_key)
}

fn decode_seq_timer_key_value(k: &[u8], mut v: &[u8]) -> Result<(TimerKey, Timer)> {
    let timer_key = timer_key_from_key_slice(k)?;

    let timer = StorageCodec::decode::<Timer, _>(&mut v)
        .map_err(|error| StorageError::Generic(error.into()))?;

    Ok((timer_key, timer))
}

#[inline]
fn exclusive_start_key_range(
    partition_id: PartitionId,
    timer_key: Option<&TimerKey>,
) -> TableScan<TimersKey> {
    if let Some(timer_key) = timer_key {
        let next_timer_key = match timer_key.kind {
            TimerKind::Invocation { invocation_uuid } => {
                let invocation_uuid_value: u128 = invocation_uuid.into();
                TimerKey {
                    timestamp: timer_key.timestamp,
                    kind: TimerKind::Invocation {
                        invocation_uuid: InvocationUuid::from(
                            invocation_uuid_value
                                .checked_add(1)
                                .expect("invocation_uuid should be smaller than u128::MAX"),
                        ),
                    },
                }
            }
            TimerKind::Journal {
                invocation_uuid,
                journal_index,
            } => TimerKey {
                timestamp: timer_key.timestamp,
                kind: TimerKind::Journal {
                    invocation_uuid,
                    journal_index: journal_index
                        .checked_add(1)
                        .expect("journal index should be smaller than u64::MAX"),
                },
            },
        };

        let lower_bound = write_timer_key(partition_id, &next_timer_key);

        let upper_bound = TimersKey::default()
            .partition_id(partition_id)
            .timestamp(u64::MAX);

        TableScan::KeyRangeInclusiveInSinglePartition(partition_id, lower_bound, upper_bound)
    } else {
        TableScan::SinglePartition(partition_id)
    }
}

fn add_timer<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    key: &TimerKey,
    timer: Timer,
) {
    let key = write_timer_key(partition_id, key);

    storage.put_kv(key, timer);
}

fn delete_timer<S: StorageAccess>(storage: &mut S, partition_id: PartitionId, key: &TimerKey) {
    let key = write_timer_key(partition_id, key);
    storage.delete_key(&key);
}

fn next_timers_greater_than<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    exclusive_start: Option<&TimerKey>,
    limit: usize,
) -> Vec<Result<(TimerKey, Timer)>> {
    let scan = exclusive_start_key_range(partition_id, exclusive_start);
    let mut produced = 0;
    storage.for_each_key_value_in_place(scan, move |k, v| {
        if produced >= limit {
            return TableScanIterationDecision::Break;
        }
        produced += 1;
        let res = decode_seq_timer_key_value(k, v);
        Emit(res)
    })
}

impl TimerTable for PartitionStore {
    async fn add_timer(&mut self, partition_id: PartitionId, key: &TimerKey, timer: Timer) {
        add_timer(self, partition_id, key, timer)
    }

    async fn delete_timer(&mut self, partition_id: PartitionId, key: &TimerKey) {
        delete_timer(self, partition_id, key)
    }

    fn next_timers_greater_than(
        &mut self,
        partition_id: PartitionId,
        exclusive_start: Option<&TimerKey>,
        limit: usize,
    ) -> impl Stream<Item = Result<(TimerKey, Timer)>> + Send {
        stream::iter(next_timers_greater_than(
            self,
            partition_id,
            exclusive_start,
            limit,
        ))
    }
}

impl<'a> TimerTable for RocksDBTransaction<'a> {
    async fn add_timer(&mut self, partition_id: PartitionId, key: &TimerKey, timer: Timer) {
        add_timer(self, partition_id, key, timer)
    }

    async fn delete_timer(&mut self, partition_id: PartitionId, key: &TimerKey) {
        delete_timer(self, partition_id, key)
    }

    fn next_timers_greater_than(
        &mut self,
        partition_id: PartitionId,
        exclusive_start: Option<&TimerKey>,
        limit: usize,
    ) -> impl Stream<Item = Result<(TimerKey, Timer)>> + Send {
        stream::iter(next_timers_greater_than(
            self,
            partition_id,
            exclusive_start,
            limit,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timer_table::TimerKey;
    use rand::Rng;
    use restate_storage_api::timer_table::TimerKindDiscriminants;
    use restate_types::identifiers::InvocationUuid;
    use strum::VariantArray;

    const FIXTURE_INVOCATION: InvocationUuid =
        InvocationUuid::from_parts(1706027034946, 12345678900001);

    #[test]
    fn round_trip_journal_kind() {
        let key = TimerKey {
            kind: TimerKind::Journal {
                invocation_uuid: FIXTURE_INVOCATION,
                journal_index: 1448,
            },
            timestamp: 87654321,
        };

        let key_bytes = write_timer_key(PartitionId::from(1337), &key).serialize();
        let got = timer_key_from_key_slice(&key_bytes).expect("should not fail");

        assert_eq!(got, key);
    }

    #[test]
    fn round_trip_invocation_kind() {
        let key = TimerKey {
            kind: TimerKind::Invocation {
                invocation_uuid: FIXTURE_INVOCATION,
            },
            timestamp: 87654321,
        };

        let key_bytes = write_timer_key(PartitionId::from(1337), &key).serialize();
        let got = timer_key_from_key_slice(&key_bytes).expect("should not fail");

        assert_eq!(got, key);
    }

    #[test]
    fn test_lexicographical_sorting_by_timestamp() {
        let kinds = [
            TimerKind::Journal {
                invocation_uuid: FIXTURE_INVOCATION,
                journal_index: 0,
            },
            TimerKind::Invocation {
                invocation_uuid: FIXTURE_INVOCATION,
            },
        ];

        for first_kind in &kinds {
            for second_kind in &kinds {
                let a = TimerKey {
                    kind: first_kind.clone(),
                    timestamp: 300,
                };
                let b = TimerKey {
                    kind: second_kind.clone(),
                    timestamp: 301,
                };
                assert_in_range(a, b);
            }
        }
    }

    #[test]
    fn test_lexicographical_sorting_by_invocation_journal_kind() {
        // Higher random part should be sorted correctly in bytes
        let a = TimerKey {
            kind: TimerKind::Journal {
                invocation_uuid: FIXTURE_INVOCATION,
                journal_index: 0,
            },
            timestamp: 300,
        };
        let b = TimerKey {
            kind: TimerKind::Journal {
                invocation_uuid: FIXTURE_INVOCATION.increment_random(),
                journal_index: 0,
            },
            timestamp: 300,
        };
        assert_in_range(a.clone(), b);

        // Also ensure that higher timestamp is sorted correctly
        let b = TimerKey {
            kind: TimerKind::Journal {
                invocation_uuid: FIXTURE_INVOCATION.increment_timestamp(),
                journal_index: 0,
            },
            timestamp: 300,
        };
        assert_in_range(a, b);
    }

    #[test]
    fn test_lexicographical_sorting_by_invocation_invocation_kind() {
        // Higher random part should be sorted correctly in bytes
        let a = TimerKey {
            kind: TimerKind::Invocation {
                invocation_uuid: FIXTURE_INVOCATION,
            },
            timestamp: 300,
        };
        let b = TimerKey {
            kind: TimerKind::Invocation {
                invocation_uuid: FIXTURE_INVOCATION.increment_random(),
            },
            timestamp: 300,
        };
        assert_in_range(a.clone(), b);

        // Also ensure that higher timestamp is sorted correctly
        let b = TimerKey {
            kind: TimerKind::Invocation {
                invocation_uuid: FIXTURE_INVOCATION.increment_timestamp(),
            },
            timestamp: 300,
        };
        assert_in_range(a, b);
    }

    #[test]
    fn test_lexicographical_sorting_by_journal_index() {
        let a = TimerKey {
            kind: TimerKind::Journal {
                invocation_uuid: FIXTURE_INVOCATION,
                journal_index: 0,
            },
            timestamp: 300,
        };
        let b = TimerKey {
            kind: TimerKind::Journal {
                invocation_uuid: FIXTURE_INVOCATION,
                journal_index: 1,
            },
            timestamp: 300,
        };
        assert_in_range(a, b);
    }

    #[test]
    fn test_lexicographical_sorting_journal_invocation_kind() {
        let a = TimerKey {
            kind: TimerKind::Invocation {
                invocation_uuid: FIXTURE_INVOCATION,
            },
            timestamp: 300,
        };

        let b = TimerKey {
            kind: TimerKind::Journal {
                invocation_uuid: FIXTURE_INVOCATION,
                journal_index: 0,
            },
            timestamp: 300,
        };

        assert_in_range(a, b);
    }

    #[track_caller]
    fn assert_in_range(key_a: TimerKey, key_b: TimerKey) {
        assert!(key_a < key_b);

        let key_a_bytes = write_timer_key(PartitionId::from(1), &key_a).serialize();
        let key_b_bytes = write_timer_key(PartitionId::from(1), &key_b).serialize();

        assert!(less_than(&key_a_bytes, &key_b_bytes));

        let (low, high) = match exclusive_start_key_range(PartitionId::from(1), Some(&key_a)) {
            TableScan::KeyRangeInclusiveInSinglePartition(p, low, high) if *p == 1 => (low, high),
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

    #[test]
    fn timer_key_order_is_the_same_as_binary_order() {
        let mut timer_keys: Vec<_> = (0..100).map(|idx| (idx, random_timer_key())).collect();
        let mut binary_timer_keys: Vec<_> = timer_keys
            .iter()
            .map(|(idx, key)| (*idx, write_timer_key(PartitionId::from(1), key).serialize()))
            .collect();

        timer_keys.sort_by(|(_, key), (_, other_key)| key.cmp(other_key));
        binary_timer_keys.sort_by(|(_, key), (_, other_key)| key.cmp(other_key));

        let timer_keys_sort_order: Vec<_> = timer_keys.iter().map(|(idx, _)| *idx).collect();
        let binary_timer_keys_sort_order: Vec<_> =
            binary_timer_keys.iter().map(|(idx, _)| *idx).collect();

        assert_eq!(
            timer_keys_sort_order, binary_timer_keys_sort_order,
            "In-memory and binary order need to be equal. Failing timers: {timer_keys:?}"
        );
    }

    pub fn random_timer_key() -> TimerKey {
        let kind = {
            match TimerKindDiscriminants::VARIANTS
                [rand::thread_rng().gen_range(0..TimerKindDiscriminants::VARIANTS.len())]
            {
                TimerKindDiscriminants::Invocation => TimerKind::Invocation {
                    invocation_uuid: InvocationUuid::new(),
                },
                TimerKindDiscriminants::Journal => TimerKind::Journal {
                    invocation_uuid: InvocationUuid::new(),
                    journal_index: rand::thread_rng().gen_range(0..2 ^ 16),
                },
            }
        };

        TimerKey {
            kind,
            timestamp: rand::thread_rng().gen_range(0..2 ^ 16),
        }
    }
}
