// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::Stream;
use futures_util::stream;

use restate_rocksdb::RocksDbPerfGuard;
use restate_storage_api::Result;
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::timer_table::{
    ReadTimerTable, Timer, TimerKey, TimerKeyKind, WriteTimerTable,
};
use restate_types::identifiers::{InvocationUuid, PartitionId};

use crate::TableKind::Timers;
use crate::TableScanIterationDecision::Emit;
use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::{
    PaddedPartitionId, PartitionStore, PartitionStoreTransaction, StorageAccess, TableScan,
    TableScanIterationDecision,
};

define_table_key!(
    Timers,
    KeyKind::Timers,
    TimersKey(
        partition_id: PaddedPartitionId,
        timestamp: u64,
        kind: TimerKeyKind,
    )
);

#[inline]
fn write_timer_key(partition_id: PartitionId, timer_key: &TimerKey) -> TimersKey {
    TimersKey {
        partition_id: partition_id.into(),
        timestamp: timer_key.timestamp,
        kind: timer_key.kind.clone(),
    }
}

#[inline]
fn timer_key_from_key_slice(mut buf: &[u8]) -> Result<TimerKey> {
    TimersKey::deserialize_from(&mut buf).map(|tk| TimerKey {
        timestamp: tk.timestamp,
        kind: tk.kind,
    })
}

fn decode_seq_timer_key_value(k: &[u8], mut v: &[u8]) -> Result<(TimerKey, Timer)> {
    let timer_key = timer_key_from_key_slice(k)?;

    let timer = Timer::decode(&mut v)?;

    Ok((timer_key, timer))
}

#[inline]
fn exclusive_start_key_range(
    partition_id: PartitionId,
    timer_key: Option<&TimerKey>,
) -> TableScan<TimersKeyBuilder> {
    if let Some(timer_key) = timer_key {
        let next_timer_key = match timer_key.kind {
            TimerKeyKind::NeoInvoke { invocation_uuid } => {
                let incremented_invocation_uuid = increment_invocation_uuid(invocation_uuid);
                TimerKey {
                    timestamp: timer_key.timestamp,
                    kind: TimerKeyKind::NeoInvoke {
                        invocation_uuid: incremented_invocation_uuid,
                    },
                }
            }
            TimerKeyKind::Invoke { invocation_uuid } => {
                let incremented_invocation_uuid = increment_invocation_uuid(invocation_uuid);
                TimerKey {
                    timestamp: timer_key.timestamp,
                    kind: TimerKeyKind::Invoke {
                        invocation_uuid: incremented_invocation_uuid,
                    },
                }
            }
            TimerKeyKind::CompleteJournalEntry {
                invocation_uuid,
                journal_index,
            } => TimerKey {
                timestamp: timer_key.timestamp,
                kind: TimerKeyKind::CompleteJournalEntry {
                    invocation_uuid,
                    journal_index: journal_index
                        .checked_add(1)
                        .expect("journal index should be smaller than u64::MAX"),
                },
            },
            TimerKeyKind::CleanInvocationStatus { invocation_uuid } => {
                let incremented_invocation_uuid = increment_invocation_uuid(invocation_uuid);
                TimerKey {
                    timestamp: timer_key.timestamp,
                    kind: TimerKeyKind::CleanInvocationStatus {
                        invocation_uuid: incremented_invocation_uuid,
                    },
                }
            }
        };

        let lower_bound = write_timer_key(partition_id, &next_timer_key);

        let upper_bound = TimersKey::builder()
            .partition_id(partition_id.into())
            .timestamp(u64::MAX);

        TableScan::KeyRangeInclusiveInSinglePartition(
            partition_id,
            lower_bound.into_builder(),
            upper_bound,
        )
    } else {
        TableScan::SinglePartition(partition_id)
    }
}

fn increment_invocation_uuid(invocation_uuid: InvocationUuid) -> InvocationUuid {
    let invocation_uuid_value: u128 = invocation_uuid.into();
    InvocationUuid::from(
        invocation_uuid_value
            .checked_add(1)
            .expect("invocation_uuid should be smaller than u128::MAX"),
    )
}

fn add_timer<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    key: &TimerKey,
    timer: &Timer,
) -> Result<()> {
    let key = write_timer_key(partition_id, key);

    storage.put_kv_proto(key, timer)
}

fn delete_timer<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    key: &TimerKey,
) -> Result<()> {
    let key = write_timer_key(partition_id, key);
    storage.delete_key(&key)
}

fn next_timers_greater_than<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    exclusive_start: Option<&TimerKey>,
    limit: usize,
) -> Result<Vec<Result<(TimerKey, Timer)>>> {
    let _x = RocksDbPerfGuard::new("get-next-timers");
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

impl ReadTimerTable for PartitionStore {
    fn next_timers_greater_than(
        &mut self,
        exclusive_start: Option<&TimerKey>,
        limit: usize,
    ) -> Result<impl Stream<Item = Result<(TimerKey, Timer)>> + Send> {
        Ok(stream::iter(next_timers_greater_than(
            self,
            self.partition_id(),
            exclusive_start,
            limit,
        )?))
    }
}

impl ReadTimerTable for PartitionStoreTransaction<'_> {
    fn next_timers_greater_than(
        &mut self,
        exclusive_start: Option<&TimerKey>,
        limit: usize,
    ) -> Result<impl Stream<Item = Result<(TimerKey, Timer)>> + Send> {
        Ok(stream::iter(next_timers_greater_than(
            self,
            self.partition_id(),
            exclusive_start,
            limit,
        )?))
    }
}

impl WriteTimerTable for PartitionStoreTransaction<'_> {
    fn put_timer(&mut self, key: &TimerKey, timer: &Timer) -> Result<()> {
        add_timer(self, self.partition_id(), key, timer)
    }

    fn delete_timer(&mut self, key: &TimerKey) -> Result<()> {
        delete_timer(self, self.partition_id(), key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::TableKeyPrefix;
    use crate::timer_table::TimerKey;
    use rand::Rng;
    use restate_storage_api::timer_table::TimerKeyKindDiscriminants;
    use restate_types::identifiers::InvocationUuid;
    use restate_types::invocation::InvocationTarget;
    use strum::VariantArray;

    const FIXTURE_INVOCATION: InvocationUuid = InvocationUuid::from_u128(12345678900001);

    #[test]
    fn round_trip_complete_journal_entry_kind() {
        let key = TimerKey {
            kind: TimerKeyKind::CompleteJournalEntry {
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
    fn round_trip_invoke_kind() {
        let key = TimerKey {
            kind: TimerKeyKind::Invoke {
                invocation_uuid: FIXTURE_INVOCATION,
            },
            timestamp: 87654321,
        };

        let key_bytes = write_timer_key(PartitionId::from(1337), &key).serialize();
        let got = timer_key_from_key_slice(&key_bytes).expect("should not fail");

        assert_eq!(got, key);
    }

    #[test]
    fn round_trip_clean_invocation_status_kind() {
        let key = TimerKey {
            kind: TimerKeyKind::CleanInvocationStatus {
                invocation_uuid: FIXTURE_INVOCATION,
            },
            timestamp: 87654321,
        };

        let key_bytes = write_timer_key(PartitionId::from(1337), &key).serialize();
        let got = timer_key_from_key_slice(&key_bytes).expect("should not fail");

        assert_eq!(got, key);
    }

    #[test]
    fn round_trip_clean_neo_invoke() {
        let key = TimerKey {
            kind: TimerKeyKind::NeoInvoke {
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
            TimerKeyKind::CompleteJournalEntry {
                invocation_uuid: FIXTURE_INVOCATION,
                journal_index: 0,
            },
            TimerKeyKind::Invoke {
                invocation_uuid: FIXTURE_INVOCATION,
            },
            TimerKeyKind::CleanInvocationStatus {
                invocation_uuid: FIXTURE_INVOCATION,
            },
            TimerKeyKind::NeoInvoke {
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
                assert_in_range(&a, &b);
            }
        }
    }

    #[test]
    fn test_lexicographical_sorting_by_invocation_uuid_complete_journal_entry_kind() {
        // Higher random part should be sorted correctly in bytes
        let a = TimerKey {
            kind: TimerKeyKind::CompleteJournalEntry {
                invocation_uuid: FIXTURE_INVOCATION,
                journal_index: 0,
            },
            timestamp: 300,
        };
        let b = TimerKey {
            kind: TimerKeyKind::CompleteJournalEntry {
                invocation_uuid: InvocationUuid::from_u128(u128::from(FIXTURE_INVOCATION) + 1),
                journal_index: 0,
            },
            timestamp: 300,
        };
        assert_in_range(&a, &b);
    }

    #[test]
    fn test_lexicographical_sorting_by_invocation_uuid_invoke_kind() {
        // Higher random part should be sorted correctly in bytes
        let a = TimerKey {
            kind: TimerKeyKind::Invoke {
                invocation_uuid: FIXTURE_INVOCATION,
            },
            timestamp: 300,
        };
        let b = TimerKey {
            kind: TimerKeyKind::Invoke {
                invocation_uuid: InvocationUuid::from_u128(u128::from(FIXTURE_INVOCATION) + 1),
            },
            timestamp: 300,
        };
        assert_in_range(&a, &b);
    }

    #[test]
    fn test_lexicographical_sorting_by_invocation_uuid_clean_invocation_status_kind() {
        // Higher random part should be sorted correctly in bytes
        let a = TimerKey {
            kind: TimerKeyKind::CleanInvocationStatus {
                invocation_uuid: FIXTURE_INVOCATION,
            },
            timestamp: 300,
        };
        let b = TimerKey {
            kind: TimerKeyKind::CleanInvocationStatus {
                invocation_uuid: InvocationUuid::from_u128(u128::from(FIXTURE_INVOCATION) + 1),
            },
            timestamp: 300,
        };
        assert_in_range(&a, &b);
    }

    #[test]
    fn test_lexicographical_sorting_by_invocation_uuid_neo_invoke_kind() {
        // Higher random part should be sorted correctly in bytes
        let a = TimerKey {
            kind: TimerKeyKind::NeoInvoke {
                invocation_uuid: FIXTURE_INVOCATION,
            },
            timestamp: 300,
        };
        let b = TimerKey {
            kind: TimerKeyKind::NeoInvoke {
                invocation_uuid: InvocationUuid::from_u128(u128::from(FIXTURE_INVOCATION) + 1),
            },
            timestamp: 300,
        };
        assert_in_range(&a, &b);
    }

    #[test]
    fn test_lexicographical_sorting_by_journal_index() {
        let a = TimerKey {
            kind: TimerKeyKind::CompleteJournalEntry {
                invocation_uuid: FIXTURE_INVOCATION,
                journal_index: 0,
            },
            timestamp: 300,
        };
        let b = TimerKey {
            kind: TimerKeyKind::CompleteJournalEntry {
                invocation_uuid: FIXTURE_INVOCATION,
                journal_index: 1,
            },
            timestamp: 300,
        };
        assert_in_range(&a, &b);
    }

    #[test]
    fn test_lexicographical_sorting_timer_kind() {
        let a = TimerKey {
            kind: TimerKeyKind::Invoke {
                invocation_uuid: FIXTURE_INVOCATION,
            },
            timestamp: 300,
        };

        let b = TimerKey {
            kind: TimerKeyKind::CompleteJournalEntry {
                invocation_uuid: FIXTURE_INVOCATION,
                journal_index: 0,
            },
            timestamp: 300,
        };

        let c = TimerKey {
            kind: TimerKeyKind::CleanInvocationStatus {
                invocation_uuid: FIXTURE_INVOCATION,
            },
            timestamp: 300,
        };

        assert_in_range(&a, &b);
        assert_in_range(&b, &c);
    }

    #[track_caller]
    fn assert_in_range(key_a: &TimerKey, key_b: &TimerKey) {
        assert!(key_a < key_b);

        let key_a_bytes = write_timer_key(PartitionId::from(1), key_a).serialize();
        let key_b_bytes = write_timer_key(PartitionId::from(1), key_b).serialize();

        assert!(less_than(&key_a_bytes, &key_b_bytes));

        let (low, high) = match exclusive_start_key_range(PartitionId::from(1), Some(key_a)) {
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
            match TimerKeyKindDiscriminants::VARIANTS
                [rand::rng().random_range(0..TimerKeyKindDiscriminants::VARIANTS.len())]
            {
                TimerKeyKindDiscriminants::Invoke => TimerKeyKind::Invoke {
                    invocation_uuid: InvocationUuid::mock_generate(
                        &InvocationTarget::mock_service(),
                    ),
                },
                TimerKeyKindDiscriminants::NeoInvoke => TimerKeyKind::NeoInvoke {
                    invocation_uuid: InvocationUuid::mock_random(),
                },
                TimerKeyKindDiscriminants::CompleteJournalEntry => {
                    TimerKeyKind::CompleteJournalEntry {
                        invocation_uuid: InvocationUuid::mock_random(),
                        journal_index: rand::rng().random_range(0..2 ^ 16),
                    }
                }
                TimerKeyKindDiscriminants::CleanInvocationStatus => {
                    TimerKeyKind::CleanInvocationStatus {
                        invocation_uuid: InvocationUuid::mock_random(),
                    }
                }
            }
        };

        TimerKey {
            kind,
            timestamp: rand::rng().random_range(0..2 ^ 16),
        }
    }
}
