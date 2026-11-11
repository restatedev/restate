// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The vqueue stage tables read as the `_idx_entry_by_queue` index.
//!
//! Each stage has its own key kind: `kind | queue | has_lock | run_at | seq |
//! entry_id`, where the queue ID's partition key forms the header. Every suffix
//! field has a fixed width, so all of them are declared as ordered fields: queue,
//! lock, run-time, sequence, and entry-ID restrictions position the iterator, and a
//! finite canonical ID restricts its sequence and entry-ID components. `has_lock`
//! is stored descending (`ReverseBoolCodec`); `next_at` predicates translate to the
//! raw run-time slot's preimage (`RestateEpochMillisCodec`). Rows are decoded with
//! those strict field codecs. The stage value (status, metadata, statistics at the
//! last transition) is not read.

use std::ops::ControlFlow;

use restate_clock::RoughTimestamp;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::Stage;
use restate_storage_query_api::schema::RecordProgress;
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, SelectionPrecision, ValuePredicate};
use restate_storage_query_api::tables::index::{
    EntryByQueue, EntryByQueueClause, EntryByQueueColumn, EntryByQueueView,
};
use restate_storage_query_api::{PreparedRead, RowSink, SchemaBinding, Table};
use restate_types::vqueues::{EntryId, Seq, VQueueId};

use crate::keys::FieldCodec;
use crate::keys::codecs::{
    EntryIdCodec, RestateEpochMillisCodec, ReverseBoolCodec, SeqCodec, VQueueIdCodec,
    VQueueIdSuffixCodec,
};
use crate::keys::header::{
    KeySchema, PartitionKeyHeader, PartitionKeyed, prepare_partition_keyed_with,
};
use crate::keys::selection::codec::FieldPredicateCodec;
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{DecodeTableKey, EncodeTableKeyPrefix, KeyDecode, KeyDecoder, KeyKind};
use crate::table::PhysicalTable;
use crate::{Result, break_on_err};

use super::inbox::{FinishedKey, InboxKey, PausedKey, RunningKey, SuspendedKey};

/// Prepares a read of one stage's slice of the index.
fn prepare<K: PartitionKeyed>(
    selection: &Selection<EntryByQueue>,
    stage: Stage,
) -> Result<(KeySelection<K>, SelectionPrecision)> {
    if let Selection::Predicates(columns) = selection {
        let excluded = columns
            .for_column(EntryByQueueColumn::Stage)
            .iter()
            .any(|clause| {
                matches!(clause, EntryByQueueClause::Stage(predicate)
                    if !Domain::from(predicate).contains(&stage))
            });
        if excluded {
            return Ok((KeySelection::empty(), SelectionPrecision::Exact));
        }
    }
    // A finite canonical ID set fixes its components; the canonical predicate
    // stays residual for the pairing.
    let canonical = match selection {
        Selection::Predicates(columns) => columns
            .for_column(EntryByQueueColumn::CanonicalId)
            .iter()
            .filter_map(|clause| match clause {
                EntryByQueueClause::CanonicalId(predicate) => Some(Domain::from(predicate)),
                _ => None,
            })
            .fold(Domain::all(), Domain::intersect)
            .into_values(),
        _ => None,
    };
    prepare_partition_keyed_with(
        selection,
        &[
            EntryByQueueColumn::PartitionKey,
            EntryByQueueColumn::VqueueId,
            EntryByQueueColumn::EntryId,
            EntryByQueueColumn::CanonicalId,
        ],
        |clause| match clause {
            EntryByQueueClause::PartitionKey(predicate) => Some(predicate.into()),
            EntryByQueueClause::VqueueId(predicate) => {
                Some(Domain::from(predicate).map_values(|id| id.partition_key()))
            }
            EntryByQueueClause::EntryId(predicate) => {
                Some(Domain::from(predicate).map_values(|id| id.partition_key()))
            }
            EntryByQueueClause::CanonicalId(predicate) => {
                Some(Domain::from(predicate).map_values(|id| id.partition_key()))
            }
            _ => None,
        },
        |clause, literals| match clause {
            EntryByQueueClause::VqueueId(predicate) => {
                VQueueIdSuffixCodec::try_prepare_value(predicate, literals)
            }
            EntryByQueueClause::HasLock(predicate) => {
                ReverseBoolCodec::try_prepare_value(predicate, literals)
            }
            EntryByQueueClause::NextAt(predicate) => {
                RestateEpochMillisCodec::try_prepare_value(predicate, literals)
            }
            EntryByQueueClause::SequenceNumber(predicate) => {
                SeqCodec::try_prepare_value(predicate, literals)
            }
            EntryByQueueClause::EntryId(predicate) => {
                EntryIdCodec::try_prepare_value(predicate, literals)
            }
            _ => Ok(None),
        },
        |tag, literals| {
            let Some(ids) = &canonical else {
                return Ok(None);
            };
            match tag {
                "sequence_number" => SeqCodec::try_prepare_value(
                    &ValuePredicate::In(ids.iter().map(|id| id.seq().as_u64()).collect()),
                    literals,
                ),
                "entry_id" => EntryIdCodec::try_prepare_value(
                    &ValuePredicate::In(ids.iter().map(|id| id.to_base_entry_id()).collect()),
                    literals,
                ),
                _ => Ok(None),
            }
        },
    )
}

/// Decodes a stage key with the declared field codecs: exact length, strict lock
/// and entry-kind bytes. The run time keeps the stage key's `RoughTimestamp`
/// decoding: floored to seconds, saturated at its maximum.
pub(super) fn decode_stage_key(
    key: &[u8],
    kind: KeyKind,
) -> Result<(VQueueId, bool, RoughTimestamp, Seq, EntryId)> {
    let mut rest = key
        .strip_prefix(kind.as_bytes())
        .ok_or(StorageError::DataIntegrityError)?;
    let qid = VQueueIdCodec::decode_field(&mut rest)?;
    let has_lock = ReverseBoolCodec::decode_field(&mut rest)?;
    let raw_run_at = RestateEpochMillisCodec::decode_field(&mut rest)?;
    let seq = SeqCodec::decode_field(&mut rest)?;
    let entry_id = EntryIdCodec::decode_field(&mut rest)?;
    if !rest.is_empty() {
        return Err(StorageError::DataIntegrityError);
    }
    let seconds = (raw_run_at / 1_000).min(u64::from(u32::MAX)) as u32;
    Ok((qid, has_lock, RoughTimestamp::new(seconds), seq, entry_id))
}

macro_rules! stage_index {
    ($(#[$meta:meta])* $table:ident, $binding:ident, $key:ty, $stage:expr) => {
        $(#[$meta])*
        pub enum $table {}

        impl Table for $table {
            type Record<'a> = (KeyDecoder<'a, $key>, &'a [u8]);
            type Scan = KeySelection<$key>;
        }

        impl PhysicalTable for $table {
            type Key = $key;
        }

        impl KeySchema for $key {
            type Header = PartitionKeyHeader<Self>;

            fn decoder(key: &[u8]) -> Result<KeyDecoder<'_, Self>> {
                // The queue ID includes the partition key. Retain the full key.
                Ok(KeyDecoder::from_payload(key))
            }
        }

        impl KeyDecode for $key {
            fn decode(source: &mut &[u8]) -> Result<Self> {
                Self::deserialize_from(source)
            }
        }

        impl OrderedKeySchema for $key {
            const KEY_KIND: KeyKind = <$key as EncodeTableKeyPrefix>::KEY_KIND;
            const FIELDS: &'static [OrderedKeyField] = &[
                OrderedKeyField::new::<VQueueIdSuffixCodec>("vqueue_id"),
                OrderedKeyField::new::<ReverseBoolCodec>("has_lock"),
                OrderedKeyField::new::<RestateEpochMillisCodec>("next_at"),
                OrderedKeyField::new::<SeqCodec>("sequence_number"),
                OrderedKeyField::new::<EntryIdCodec>("entry_id"),
            ];
        }

        pub enum $binding {}

        impl SchemaBinding for $binding {
            type Schema = EntryByQueue;
            type Table = $table;
            type Materializer = ();

            fn materialize_next(
                record: <Self::Table as Table>::Record<'_>,
                _: &mut (),
                emit: &mut impl RowSink<Self::Schema>,
            ) -> Result<RecordProgress> {
                if let ControlFlow::Break(result) = Self::materialize_projected(record, emit) {
                    result?;
                }
                Ok(RecordProgress::Done)
            }

            fn prepare(selection: &Selection<EntryByQueue>) -> Result<PreparedRead<$table>> {
                let (request, precision) = prepare(selection, $stage)?;
                Ok(PreparedRead::new(request, precision))
            }

            fn materialize(
                (key, _): <Self::Table as Table>::Record<'_>,
                emit: &mut impl for<'a> FnMut(EntryByQueueView<'a>) -> ControlFlow<Result<()>>,
            ) -> ControlFlow<Result<()>> {
                let (qid, has_lock, run_at, seq, entry_id) = break_on_err(decode_stage_key(
                    key.remaining,
                    <$key as EncodeTableKeyPrefix>::KEY_KIND,
                ))?;
                emit(EntryByQueueView {
                    stage: $stage,
                    vqueue_id: &qid,
                    has_lock,
                    next_at: run_at.as_unix_millis(),
                    canonical_id: entry_id.to_base_id(qid.partition_key()).canonicalize(seq),
                })
            }
        }
    };
}

stage_index!(
    /// Entries in the inbox stage.
    InboxIndexTable, InboxIndexBinding, InboxKey, Stage::Inbox
);
stage_index!(
    /// Entries in the running stage.
    RunningIndexTable, RunningIndexBinding, RunningKey, Stage::Running
);
stage_index!(
    /// Entries in the suspended stage.
    SuspendedIndexTable, SuspendedIndexBinding, SuspendedKey, Stage::Suspended
);
stage_index!(
    /// Entries in the paused stage.
    PausedIndexTable, PausedIndexBinding, PausedKey, Stage::Paused
);
stage_index!(
    /// Entries in the finished stage.
    FinishedIndexTable, FinishedIndexBinding, FinishedKey, Stage::Finished
);

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;

    use restate_storage_api::vqueue_table::EntryKey;
    use restate_storage_query_api::selection::ValuePredicate;
    use restate_types::sharding::KeyRange;
    use restate_types::time::MillisSinceEpoch;
    use restate_types::vqueues::EntryKind;

    use crate::table::navigate;

    use super::super::inbox::encode_stage_key;
    use super::*;

    /// Inbox keys of queue `7/q` with explicit raw run-time slots, sorted.
    fn raw_keys(entries: &[(u64, u64)]) -> Vec<([u8; 61], u64)> {
        let qid = VQueueId::custom(7, "q");
        let mut keys: Vec<_> = entries
            .iter()
            .map(|&(seq, raw)| {
                let entry_id = EntryId::new(EntryKind::Invocation, [seq as u8; 16]);
                let entry_key = EntryKey::new(false, MillisSinceEpoch::new(0), seq, entry_id);
                let mut key = encode_stage_key(Stage::Inbox, &qid, &entry_key);
                key[28..36].copy_from_slice(&raw.to_be_bytes());
                (key, seq)
            })
            .collect();
        keys.sort_unstable();
        keys
    }

    #[test]
    fn run_times_and_canonical_ids_position_stage_reads() {
        // Four sub-second raw slots per second from second 10, plus a saturated tail.
        let max = u64::from(RoughTimestamp::MAX.as_u32()) * 1_000;
        let mut entries: Vec<_> = (0..32)
            .map(|seq| (seq, (10 + seq / 4) * 1_000 + (seq % 4) * 250))
            .collect();
        entries.extend([
            (100, max),
            (101, max + 999),
            (102, max + 5_000),
            (103, u64::MAX),
        ]);
        let fixture = raw_keys(&entries);
        let keys: Vec<_> = fixture.iter().map(|(key, _)| key).collect();
        let qid = VQueueId::custom(7, "q");
        let second = |s: u32| RoughTimestamp::new(s).as_unix_millis();
        let visit = |clauses: Vec<EntryByQueueClause>| {
            let selection = clauses.into_iter().fold(Selection::All, Selection::and);
            let prepared = InboxIndexBinding::prepare(&selection).unwrap();
            let (matches, visited) =
                navigate(Arc::clone(prepared.request()), KeyRange::FULL, &keys);
            let mut seqs: Vec<_> = matches.into_iter().map(|index| fixture[index].1).collect();
            seqs.sort_unstable();
            (seqs, visited)
        };
        let in_queue = |predicate: ValuePredicate<MillisSinceEpoch>| {
            vec![
                EntryByQueueClause::VqueueId(ValuePredicate::Equal(qid.clone())),
                EntryByQueueClause::HasLock(ValuePredicate::Equal(false)),
                EntryByQueueClause::NextAt(predicate),
            ]
        };

        assert_eq!(
            visit(in_queue(ValuePredicate::Equal(second(12)))),
            (vec![8, 9, 10, 11], 4),
            "a_second_covers_its_sub_second_slots"
        );
        let fractional = MillisSinceEpoch::new(second(12).as_u64() + 500);
        assert_eq!(
            visit(in_queue(ValuePredicate::Equal(fractional))),
            (vec![], 0),
            "fractional_equality_is_empty"
        );
        let (seqs, visited) = visit(in_queue(ValuePredicate::In(vec![second(10), second(15)])));
        assert_eq!(seqs, [0, 1, 2, 3, 20, 21, 22, 23]);
        assert!(visited <= 9, "in_lists_seek_over_gaps: {visited}");
        assert_eq!(
            visit(in_queue(ValuePredicate::Range {
                lower: Bound::Excluded(second(11)),
                upper: Bound::Included(MillisSinceEpoch::new(second(12).as_u64() + 300)),
            })),
            (vec![8, 9, 10, 11], 4),
            "range_bounds_round_inward"
        );
        assert_eq!(
            visit(in_queue(ValuePredicate::Equal(
                RoughTimestamp::MAX.as_unix_millis()
            ))),
            (vec![100, 101, 102, 103], 4),
            "the_maximum_covers_the_saturated_tail"
        );
        let mut later = in_queue(ValuePredicate::Equal(second(12)));
        later.push(EntryByQueueClause::SequenceNumber(ValuePredicate::Equal(
            10,
        )));
        let (seqs, visited) = visit(later);
        assert_eq!(seqs, [10]);
        assert!(visited <= 4, "sequences_seek_within_the_second: {visited}");

        // A canonical ID navigates like its explicit sequence and entry components.
        let canonical = EntryId::new(EntryKind::Invocation, [10; 16])
            .to_base_id(7)
            .canonicalize(Seq::new(10));
        let queue = || EntryByQueueClause::VqueueId(ValuePredicate::Equal(qid.clone()));
        let by_canonical = visit(vec![
            queue(),
            EntryByQueueClause::CanonicalId(ValuePredicate::Equal(canonical)),
        ]);
        let by_components = visit(vec![
            queue(),
            EntryByQueueClause::SequenceNumber(ValuePredicate::Equal(10)),
            EntryByQueueClause::EntryId(ValuePredicate::Equal(canonical.to_base_entry_id())),
        ]);
        assert_eq!(by_canonical.0, [10]);
        assert_eq!(
            by_canonical, by_components,
            "canonical_ids_restrict_their_components"
        );

        // Neighbors sharing the unconstrained lock and run time are skipped.
        let shared = raw_keys(&(0..64).map(|seq| (seq, 20_000)).collect::<Vec<_>>());
        let keys: Vec<_> = shared.iter().map(|(key, _)| key).collect();
        let selection = Selection::All
            .and(queue())
            .and(EntryByQueueClause::CanonicalId(ValuePredicate::Equal(
                canonical,
            )));
        let prepared = InboxIndexBinding::prepare(&selection).unwrap();
        let (matches, visited) = navigate(Arc::clone(prepared.request()), KeyRange::FULL, &keys);
        assert_eq!(
            matches
                .iter()
                .map(|index| shared[*index].1)
                .collect::<Vec<_>>(),
            [10]
        );
        assert!(visited <= 3, "canonical_ids_seek_past_neighbors: {visited}");
    }

    #[test]
    fn stage_rows_decode_strictly() {
        let materialize = |key: &[u8]| {
            let mut rows = Vec::new();
            let result =
                InboxIndexBinding::materialize((KeyDecoder::from_payload(key), &[]), &mut |row| {
                    rows.push((row.has_lock, row.next_at, row.canonical_id.seq().as_u64()));
                    ControlFlow::Continue(())
                });
            (matches!(result, ControlFlow::Continue(())), rows)
        };
        let (key, _) = raw_keys(&[(3, 12_750)])[0];
        let (ok, rows) = materialize(&key);
        assert!(ok);
        assert_eq!(
            rows,
            [(false, RoughTimestamp::new(12).as_unix_millis(), 3)],
            "sub_second_slots_floor_to_seconds"
        );
        let mut lock = key;
        lock[27] = 2;
        let mut kind = key;
        kind[44] = 0;
        for (label, malformed) in [
            ("lock_byte", &lock[..]),
            ("reserved_entry_kind", &kind[..]),
            ("truncated_entry_id", &key[..60]),
            ("truncated_after_queue", &key[..27]),
        ] {
            let mut errors = Vec::new();
            let result = InboxIndexBinding::materialize(
                (KeyDecoder::from_payload(malformed), &[]),
                &mut |_| ControlFlow::Continue(()),
            );
            if let ControlFlow::Break(Err(StorageError::DataIntegrityError)) = result {
                errors.push(label);
            }
            assert_eq!(
                errors,
                [label],
                "malformed_keys_are_integrity_errors: {label}"
            );
        }
    }

    #[test]
    fn queues_locks_and_sequences_position_stage_reads() {
        // 64 queues of one scope under partition key 7, four entries each; half of
        // each queue's entries hold their lock.
        let queue = |n: u32| VQueueId::custom(7, format!("q{n}"));
        let mut keys: Vec<_> = (0..64)
            .flat_map(|n| (0..4u64).map(move |seq| (n, seq)))
            .map(|(n, seq)| {
                let entry_id = EntryId::new(EntryKind::Invocation, [seq as u8; 16]);
                let entry_key = EntryKey::new(
                    seq % 2 == 0,
                    MillisSinceEpoch::new(1_744_010_000_000 + seq * 1_000),
                    seq,
                    entry_id,
                );
                encode_stage_key(Stage::Inbox, &queue(n), &entry_key)
            })
            .collect();
        keys.sort_unstable();
        let visit = |clauses: Vec<EntryByQueueClause>| {
            let selection = clauses.into_iter().fold(Selection::All, Selection::and);
            let prepared = InboxIndexBinding::prepare(&selection).unwrap();
            let (matches, visited) =
                navigate(Arc::clone(prepared.request()), KeyRange::FULL, &keys);
            (matches.len(), visited)
        };
        let q5 = || EntryByQueueClause::VqueueId(ValuePredicate::Equal(queue(5)));

        assert_eq!(visit(vec![q5()]), (4, 4), "a_queue_reads_only_its_entries");
        // Lock holders sort first in a queue; the bounds enclose exactly them.
        assert_eq!(
            visit(vec![
                q5(),
                EntryByQueueClause::HasLock(ValuePredicate::Equal(true))
            ]),
            (2, 2),
            "lock_holders_lead_their_queue"
        );
        // The unconstrained run time between lock and sequence costs one visit to
        // the other unlocked entry before seeking.
        assert_eq!(
            visit(vec![
                q5(),
                EntryByQueueClause::HasLock(ValuePredicate::Equal(false)),
                EntryByQueueClause::SequenceNumber(ValuePredicate::Equal(3)),
            ]),
            (1, 2),
            "a_queue_position_seeks_past_the_run_time"
        );
        assert_eq!(
            visit(vec![
                q5(),
                EntryByQueueClause::Stage(ValuePredicate::Equal(Stage::Running))
            ]),
            (0, 0),
            "other_stages_read_nothing"
        );
    }
}
