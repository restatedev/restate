// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Physical reads stay inside their snapshot's owner and table, keep payloads
//! opaque, and navigate suffix predicates across key headers.

use std::num::NonZeroUsize;
use std::ops::{ControlFlow, RangeBounds};
use std::sync::Arc;

use bytes::{BufMut, Bytes};
use tokio_util::sync::CancellationToken;

use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::scan::{RowConsumer, ScanOutcome};
use restate_storage_query_api::scan::{Scan, Table};
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_types::partition_table::Partition;
use restate_types::sharding::{KeyRange, PartitionId};

use crate::keys::codecs::U64Codec;
use crate::keys::header::{HeaderBounds, KeyHeader, KeySchema, PartitionKeyHeader};
use crate::keys::macros::define_ordered_key;
use crate::keys::selection::{KeySelection, PrepareKeySelection};
use crate::keys::{EncodeTableKey, KeyDecoder, KeyEncode, KeyKind};
use crate::state_table::{ScopedStateKey, ScopedStateTable, StateKey, StateTable};
use crate::{PartitionStoreManager, QueryIoExecutor, ReadSnapshot, Result};

use super::{PhysicalTable, ReadMetadata};

// Selection-only metadata: neither this schema nor the physical table needs
// a logical schema, statistics, or an Arrow builder.
restate_storage_query_api::define_schema! {
    pub Columns { sequence: u64 }
}

/// Collects decoded keys and opaque payloads, stopping after `limit` rows, if set.
struct Rows<K> {
    records: Vec<(K, Vec<u8>)>,
    limit: Option<usize>,
}

impl<K: KeySchema + Send> RowConsumer for Rows<K> {
    type Item<'a> = (KeyDecoder<'a, K>, &'a [u8]);

    async fn consume(&mut self, (key, payload): Self::Item<'_>) -> Result<ControlFlow<()>> {
        self.records.push((key.decode_all()?, payload.to_vec()));
        Ok(if self.limit == Some(self.records.len()) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        })
    }
}

async fn read<T: PhysicalTable>(
    snapshot: &ReadSnapshot,
    executor: &QueryIoExecutor,
    request: &Arc<KeySelection<T::Key>>,
    limit: Option<usize>,
) -> ScanOutcome<Rows<T::Key>>
where
    T::Key: Send,
{
    snapshot
        .scan::<T, _>(
            executor,
            "physical-table-test",
            Arc::clone(request),
            Rows {
                records: Vec::new(),
                limit,
            },
        )
        .unwrap()
        .await
        .unwrap()
}

/// Collects keys and checks that each payload remains the fixture's `b"\0\xff"`.
async fn read_keys<T: PhysicalTable>(
    snapshot: &ReadSnapshot,
    executor: &QueryIoExecutor,
    request: &Arc<KeySelection<T::Key>>,
) -> Vec<T::Key>
where
    T::Key: Send,
{
    let ScanOutcome::Exhausted(rows) = read::<T>(snapshot, executor, request, None).await else {
        panic!("an unlimited read must exhaust its selection");
    };
    rows.records
        .into_iter()
        .map(|(key, payload)| {
            assert_eq!(payload, b"\0\xff", "legacy_values_remain_uninterpreted");
            key
        })
        .collect()
}

#[restate_core::test]
async fn legacy_state_reads_intersect_implicit_ownership_without_ordered_suffixes() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let executor = QueryIoExecutor::new(NonZeroUsize::new(1).unwrap());
    // Reuse requests across owners. A later FULL restriction cannot widen one.
    // Cover both owner edges, byte and maximum-key boundaries, and an empty 2..=1.
    let requests: Vec<_> = [
        None,
        Some(KeyRange::FULL),
        Some(KeyRange::new(0, 19)),
        Some(KeyRange::new(19, 21)),
        Some(KeyRange::new(21, 39)),
        Some(KeyRange::new(40, 41)),
        Some(KeyRange::new(255, 256)),
        Some(KeyRange::new(u64::MAX, u64::MAX)),
        Some(KeyRange::new(2, 1)),
    ]
    .into_iter()
    .map(|scope| {
        let mut state = KeySelection::<StateKey>::all();
        let mut scoped = KeySelection::<ScopedStateKey>::all();
        if let Some(scope) = scope {
            state = state.restrict_scope(scope).restrict_scope(KeyRange::FULL);
            scoped = scoped.restrict_scope(scope).restrict_scope(KeyRange::FULL);
        }
        (scope, Arc::new(state), Arc::new(scoped))
    })
    .collect();

    let mut state_rows = Vec::new();
    let mut scoped_rows = Vec::new();
    // Neighbors of owner edges 20/40, the 255/256 byte boundary, and the maximum key.
    for partition_key in [0, 19, 20, 21, 39, 40, 41, 255, 256, u64::MAX - 1, u64::MAX] {
        for name in ["b", "aa"] {
            let state = StateKey {
                partition_key,
                service_name: name.into(),
                service_key: "object".into(),
                state_key: Bytes::from_static(b"state"),
            };
            let scoped = ScopedStateKey {
                partition_key,
                scope: None,
                service_name: name.into(),
                service_key: "object".into(),
                state_key: Bytes::from_static(b"state"),
            };
            let mut bytes = Vec::new();
            state.serialize_to(&mut bytes);
            state_rows.push((bytes, state));
            let mut bytes = Vec::new();
            scoped.serialize_to(&mut bytes);
            scoped_rows.push((bytes, scoped));
        }
    }
    state_rows.sort_by(|(a, _), (b, _)| a.cmp(b));
    scoped_rows.sort_by(|(a, _), (b, _)| a.cmp(b));
    assert_eq!(state_rows[0].1.service_name, "b", "legacy_byte_order");

    for (id, owner) in [
        KeyRange::new(20, 40),
        KeyRange::new(0, 0),
        KeyRange::new(u64::MAX - 1, u64::MAX),
    ]
    .into_iter()
    .enumerate()
    {
        let mut store = manager
            .open(
                &Partition::new(PartitionId::from(u16::try_from(id + 1).unwrap()), owner),
                None,
            )
            .await
            .unwrap();
        let mut tx = store.transaction();
        // Out-of-owner keys share the CF, so merely choosing the local database
        // cannot satisfy the ownership check.
        for (bytes, _) in &state_rows {
            tx.raw_put_cf(KeyKind::State, bytes, b"\0\xff");
        }
        for (bytes, _) in &scoped_rows {
            tx.raw_put_cf(KeyKind::ScopedState, bytes, b"\0\xff");
        }
        tx.commit().await.unwrap();
        drop(tx);
        let (snapshot, closed) = store.query_read(CancellationToken::new()).unwrap();

        for (requested, state, scoped) in &requests {
            let effective = owner.intersect(&requested.unwrap_or(KeyRange::FULL));
            let contains = |key| effective.is_some_and(|range| range.contains(&key));
            assert_eq!(
                read_keys::<StateTable>(&snapshot, &executor, state).await,
                state_rows
                    .iter()
                    .filter(|(_, key)| contains(key.partition_key))
                    .map(|(_, key)| key.clone())
                    .collect::<Vec<_>>(),
                "state: owner={owner}, requested={requested:?}",
            );
            assert_eq!(
                read_keys::<ScopedStateTable>(&snapshot, &executor, scoped).await,
                scoped_rows
                    .iter()
                    .filter(|(_, key)| contains(key.partition_key))
                    .map(|(_, key)| key.clone())
                    .collect::<Vec<_>>(),
                "scoped state: owner={owner}, requested={requested:?}",
            );
        }
        drop(snapshot);
        closed.await;
    }
}

// An ordered suffix behind a varying partition-key header; it shares only the
// legacy header codec, not the state table's suffix encoding.
fn range_prefix<B: BufMut>(partition_key: &u64, buffer: &mut B) {
    buffer.put_slice(KeyKind::State.as_bytes());
    buffer.put_u64(*partition_key);
}

define_ordered_key!(
    RangedKey,
    key_kind: KeyKind::State,
    context: u64,
    start: range_prefix,
    fields { sequence: U64Codec }
    schema: Columns
);

impl KeySchema for RangedKey {
    type Header = PartitionKeyHeader<StateKey>;
}

enum RangedTable {}

impl Table for RangedTable {
    type Record<'a> = (KeyDecoder<'a, RangedKey>, &'a [u8]);
    type Scan = KeySelection<RangedKey>;
}

impl PhysicalTable for RangedTable {
    type Key = RangedKey;
}

#[restate_core::test]
async fn suffix_exhaustion_does_not_end_a_scan_across_ownership_values() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(
            &Partition::new(PartitionId::MIN, KeyRange::new(10, 20)),
            None,
        )
        .await
        .unwrap();
    let mut tx = store.transaction();
    for (partition_key, sequences) in [
        (9, &[1][..]),
        (10, &[2, 3]),
        (11, &[1, 3]),
        (20, &[1, 4]),
        (21, &[1]),
    ] {
        for &sequence in sequences {
            let mut key = Vec::new();
            RangedKey::prefix(partition_key, &mut key).sequence(sequence);
            tx.raw_put_cf(KeyKind::State, key, partition_key.to_be_bytes());
        }
    }
    tx.commit().await.unwrap();
    drop(tx);
    let request = Arc::new(
        RangedKey::prepare_candidates(
            &Selection::All.and(ColumnsClause::Sequence(ValuePredicate::Equal(1))),
        )
        .unwrap()
        .restrict_scope(KeyRange::new(9, 20)),
    );
    let executor = QueryIoExecutor::new(NonZeroUsize::new(1).unwrap());
    let (snapshot, closed) = store.query_read(CancellationToken::new()).unwrap();
    let result = read::<RangedTable>(&snapshot, &executor, &request, None).await;
    assert!(matches!(result, ScanOutcome::Exhausted(_)));
    assert_eq!(
        result
            .into_inner()
            .records
            .into_iter()
            .map(|(key, payload)| (
                u64::from_be_bytes(payload.try_into().unwrap()),
                key.sequence
            ))
            .collect::<Vec<_>>(),
        [(11, 1), (20, 1)],
        "suffix_mismatch_in_first_group_preserves_later_owned_matches"
    );
    drop(snapshot);
    closed.await;
}

#[test]
fn ordered_suffixes_position_reads_under_pinned_and_varying_headers() {
    let mut rows = Vec::new();
    for partition_key in [9, 10, 11, 12, 21] {
        for sequence in 0..100 {
            let mut key = Vec::new();
            RangedKey::prefix(partition_key, &mut key).sequence(sequence);
            rows.push(((partition_key, sequence), key));
        }
    }
    let owner = KeyRange::new(10, 20);
    let keys: Vec<_> = rows.iter().map(|(_, key)| key).collect();
    let navigate = |request, owner| {
        let (matches, visited) = super::navigate(request, owner, &keys);
        let rows: Vec<_> = matches.into_iter().map(|index| rows[index].0).collect();
        (rows, visited)
    };
    let request = |sequences: &[u64], scope: Option<KeyRange>| {
        let predicate = ValuePredicate::In(sequences.to_vec());
        let request =
            RangedKey::prepare_candidates(&Selection::All.and(ColumnsClause::Sequence(predicate)))
                .unwrap();
        match scope {
            Some(scope) => request.restrict_scope(scope),
            None => request,
        }
    };
    let pairs = |keys: &[u64], sequences: &[u64]| {
        keys.iter()
            .flat_map(|&key| sequences.iter().map(move |&sequence| (key, sequence)))
            .collect::<Vec<_>>()
    };

    let (matches, visited) = navigate(request(&[50, 60], Some(KeyRange::new(11, 11))), owner);
    assert_eq!(matches, pairs(&[11], &[50, 60]));
    assert_eq!(visited, 3, "pinned_header_positions_between_values");

    let (matches, visited) = navigate(request(&[50, 60], Some(KeyRange::new(10, 12))), owner);
    assert_eq!(matches, pairs(&[10, 11, 12], &[50, 60]));
    assert_eq!(
        visited, 15,
        "varying_headers_seek_within_and_between_headers"
    );

    let (matches, visited) = navigate(request(&[99], None), owner);
    assert_eq!(matches, pairs(&[10, 11, 12], &[99]));
    assert!(visited <= 6, "suffix_only_restriction_positions: {visited}");
    assert_eq!(
        navigate(request(&[50], Some(KeyRange::new(9, 9))), owner),
        (Vec::new(), 0),
        "pinned_header_outside_owner_is_unowned"
    );
}

/// Writes this fixture table's prefix: its key kind followed by table ID `ID`.
fn prefix<const ID: u64, B: BufMut>(buffer: &mut B) {
    buffer.put_slice(KeyKind::ScopedState.as_bytes());
    buffer.put_u64(ID);
}

define_ordered_key!(
    RecordKey,
    key_kind: KeyKind::ScopedState,
    context: (),
    start: |_: &(), buffer| prefix::<1, _>(buffer),
    fields { sequence: U64Codec }
    schema: Columns
);

// The key macro emits a public RecordKey, whose KeySchema exposes this header.
pub enum RawHeader {}

impl KeyHeader for RawHeader {
    type Scope = ();
    const KEY_KIND: KeyKind = KeyKind::ScopedState;
    const LENGTH: usize = 10;

    fn scope(_metadata: &ReadMetadata<'_>) -> Result<()> {
        Ok(())
    }

    fn bounds((): ()) -> Option<HeaderBounds> {
        let mut bytes = Vec::new();
        prefix::<1, _>(&mut bytes);
        Some(HeaderBounds::prefix(Self::KEY_KIND, Bytes::from(bytes)))
    }
}

impl KeySchema for RecordKey {
    type Header = RawHeader;
}

enum RawTable {}

impl Table for RawTable {
    type Record<'a> = (KeyDecoder<'a, RecordKey>, &'a [u8]);
    type Scan = KeySelection<RecordKey>;
}

impl PhysicalTable for RawTable {
    type Key = RecordKey;
}

#[restate_core::test]
async fn raw_scans_preserve_payloads_table_boundaries_and_independent_cursors() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let fixtures: &[(u64, &[u8])] = &[(1, b""), (2, &[0]), (3, &[0xff]), (4, b"opaque")];
    let mut tx = store.transaction();
    for &(sequence, payload) in fixtures {
        let mut key = Vec::new();
        prefix::<1, _>(&mut key);
        RecordKey { sequence }.encode(&mut key);
        tx.raw_put_cf(KeyKind::ScopedState, key, payload);
    }
    let mut other = Vec::new();
    prefix::<2, _>(&mut other);
    RecordKey { sequence: 1 }.encode(&mut other);
    tx.raw_put_cf(KeyKind::ScopedState, other, b"other table");
    tx.commit().await.unwrap();
    drop(tx);

    let executor = QueryIoExecutor::new(NonZeroUsize::new(1).unwrap());
    let (snapshot, closed) = store.query_read(CancellationToken::new()).unwrap();
    let all = Arc::new(RecordKey::prepare_candidates(&Selection::All).unwrap());
    // Stop one execution early, then execute the immutable request twice. The
    // returned consumer carries its rows and the distinct completion reason.
    for limit in [Some(1), None, None] {
        let result = read::<RawTable>(&snapshot, &executor, &all, limit).await;
        assert_eq!(matches!(result, ScanOutcome::Stopped(_)), limit.is_some());
        assert_eq!(
            result
                .into_inner()
                .records
                .into_iter()
                .map(|(key, payload)| (key.sequence, payload))
                .collect::<Vec<_>>(),
            fixtures
                .iter()
                .take(limit.unwrap_or(fixtures.len()))
                .map(|(key, payload)| (*key, payload.to_vec()))
                .collect::<Vec<_>>(),
            "raw_payloads_without_crossing_tables"
        );
    }

    let selected = Arc::new(
        RecordKey::prepare_candidates(
            &Selection::All.and(ColumnsClause::Sequence(ValuePredicate::In(vec![1, 3]))),
        )
        .unwrap(),
    );
    let result = read::<RawTable>(&snapshot, &executor, &selected, None).await;
    assert!(matches!(result, ScanOutcome::Exhausted(_)));
    assert_eq!(
        result
            .into_inner()
            .records
            .into_iter()
            .map(|(key, _)| key.sequence)
            .collect::<Vec<_>>(),
        [1, 3],
        "physical_key_predicates_are_enforced_before_emission"
    );
    drop(snapshot);
    closed.await;
}
