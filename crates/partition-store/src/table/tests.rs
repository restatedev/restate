// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;
use std::sync::{Arc, mpsc};

use bytes::{BufMut, Bytes};

use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
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
use crate::{PartitionStore, PartitionStoreManager};

use super::PhysicalTable;

// Selection-only metadata: neither this schema nor the physical table implements
// LogicalSchema, Stat, or any Arrow row/builder contract.
restate_storage_query_api::define_schema! {
    pub Columns { sequence: u64 }
}

define_ordered_key!(
    RecordKey,
    key_kind: KeyKind::ScopedState,
    context: (),
    start: |_: &(), buffer| prefix::<1, _>(buffer),
    fields { sequence: U64Codec }
    schema: Columns
);

struct RawTable;
pub enum RawHeader {}

fn prefix<const ID: u64, B: BufMut>(buffer: &mut B) {
    buffer.put_slice(KeyKind::ScopedState.as_bytes());
    buffer.put_u64(ID);
}

async fn read_keys<T: PhysicalTable>(
    store: &PartitionStore,
    request: &Arc<KeySelection<T::Key>>,
) -> Vec<T::Key>
where
    T::Key: Send,
{
    let (tx, rx) = mpsc::channel();
    <PartitionStore as Scan<T>>::scan(store, Arc::clone(request), move |(key, payload)| {
        assert_eq!(payload, b"\0\xff", "legacy_values_remain_uninterpreted");
        tx.send(key.decode_all().unwrap()).unwrap();
        ControlFlow::Continue(())
    })
    .unwrap()
    .await
    .unwrap();
    rx.into_iter().collect()
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_state_reads_intersect_implicit_ownership_without_ordered_suffixes() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    // A prepared request is reused across owners. None exercises the implicit
    // restriction alone; the final FULL restriction must never widen an earlier one.
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
    assert_eq!(
        state_rows[0].1.service_name, "b",
        "legacy_byte_order_is_not_string_order"
    );

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
        // Deliberately put out-of-owner keys in the same CF: selecting the local
        // database alone must not accidentally satisfy the ownership assertion.
        for (bytes, _) in &state_rows {
            tx.raw_put_cf(KeyKind::State, bytes, b"\0\xff");
        }
        for (bytes, _) in &scoped_rows {
            tx.raw_put_cf(KeyKind::ScopedState, bytes, b"\0\xff");
        }
        tx.commit().await.unwrap();
        drop(tx);

        for (requested, state, scoped) in &requests {
            let effective = owner.intersect(&requested.unwrap_or(KeyRange::FULL));
            let contains =
                |key| effective.is_some_and(|range| range.start() <= key && key <= range.end());
            assert_eq!(
                read_keys::<StateTable>(&store, state).await,
                state_rows
                    .iter()
                    .filter(|(_, key)| contains(key.partition_key))
                    .map(|(_, key)| key.clone())
                    .collect::<Vec<_>>(),
                "state_scan_is_exactly_query_intersect_owner: owner={owner}, requested={requested:?}",
            );
            assert_eq!(
                read_keys::<ScopedStateTable>(&store, scoped).await,
                scoped_rows
                    .iter()
                    .filter(|(_, key)| contains(key.partition_key))
                    .map(|(_, key)| key.clone())
                    .collect::<Vec<_>>(),
                "scoped_state_scan_is_exactly_query_intersect_owner: owner={owner}, requested={requested:?}",
            );
        }
    }
}

// An ordered suffix behind a varying partition-key header. This deliberately
// shares only the legacy header codec, not the state table's suffix encoding.
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

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
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
    let (tx, rx) = mpsc::channel();
    <PartitionStore as Scan<RangedTable>>::scan(&store, request, move |(key, payload)| {
        tx.send((
            u64::from_be_bytes(payload.try_into().unwrap()),
            key.decode_sequence().unwrap(),
        ))
        .unwrap();
        ControlFlow::Continue(())
    })
    .unwrap()
    .await
    .unwrap();
    assert_eq!(
        rx.into_iter().collect::<Vec<_>>(),
        [(11, 1), (20, 1)],
        "suffix_mismatch_in_first_group_preserves_later_owned_matches"
    );
}

impl KeyHeader for RawHeader {
    type Scope = ();
    const KEY_KIND: KeyKind = KeyKind::ScopedState;
    const LENGTH: usize = 10;

    fn bounds((): ()) -> Option<HeaderBounds> {
        let mut bytes = Vec::new();
        prefix::<1, _>(&mut bytes);
        Some(HeaderBounds::prefix(Self::KEY_KIND, Bytes::from(bytes)))
    }
}

impl KeySchema for RecordKey {
    type Header = RawHeader;
}

impl Table for RawTable {
    type Record<'a> = (KeyDecoder<'a, RecordKey>, &'a [u8]);
    type Scan = KeySelection<RecordKey>;
}

impl PhysicalTable for RawTable {
    type Key = RecordKey;
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_scans_preserve_payloads_table_boundaries_and_independent_cursors() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let fixtures: &[(u64, &[u8])] = &[(1, b""), (2, &[0]), (3, &[0xff]), (4, b"opaque")];
    let mut transaction = store.transaction();
    for &(sequence, payload) in fixtures {
        let mut key = Vec::new();
        prefix::<1, _>(&mut key);
        RecordKey { sequence }.encode(&mut key);
        transaction.raw_put_cf(KeyKind::ScopedState, key, payload);
    }
    let mut other = Vec::new();
    prefix::<2, _>(&mut other);
    RecordKey { sequence: 1 }.encode(&mut other);
    transaction.raw_put_cf(KeyKind::ScopedState, other, b"other table");
    transaction.commit().await.unwrap();
    drop(transaction);

    let all = Arc::new(RecordKey::prepare_candidates(&Selection::All).unwrap());
    // Stop one execution early, then execute the same immutable request twice.
    for limit in [Some(1), None, None] {
        let (tx, rx) = mpsc::channel();
        let mut emitted = 0;
        <PartitionStore as Scan<RawTable>>::scan(
            &store,
            Arc::clone(&all),
            move |(key, payload)| {
                tx.send((key.decode_sequence().unwrap(), payload.to_vec()))
                    .unwrap();
                emitted += 1;
                if limit == Some(emitted) {
                    ControlFlow::Break(Ok(()))
                } else {
                    ControlFlow::Continue(())
                }
            },
        )
        .unwrap()
        .await
        .unwrap();
        assert_eq!(
            rx.into_iter().collect::<Vec<_>>(),
            fixtures
                .iter()
                .take(limit.unwrap_or(fixtures.len()))
                .map(|(key, payload)| (*key, payload.to_vec()))
                .collect::<Vec<_>>(),
            "raw_payloads_include_empty_zero_and_malformed_values_without_crossing_tables"
        );
    }

    let selected = Arc::new(
        RecordKey::prepare_candidates(
            &Selection::All.and(ColumnsClause::Sequence(ValuePredicate::In(vec![1, 3]))),
        )
        .unwrap(),
    );
    let (tx, rx) = mpsc::channel();
    <PartitionStore as Scan<RawTable>>::scan(&store, selected, move |(key, _)| {
        tx.send(key.decode_sequence().unwrap()).unwrap();
        ControlFlow::Continue(())
    })
    .unwrap()
    .await
    .unwrap();
    assert_eq!(
        rx.into_iter().collect::<Vec<_>>(),
        [1, 3],
        "physical_key_predicates_are_enforced_before_emission"
    );
}
