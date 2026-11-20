// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::ops::Bound;

use futures::FutureExt;
use tokio_util::sync::CancellationToken;

use restate_clock::UniqueTimestamp;
use restate_clock::time::MillisSinceEpoch;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, Stage, Status, WriteVQueueTable,
};
use restate_storage_query_api::Table;
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::index::{
    EntryByServiceCreated as Index, EntryByServiceCreatedClause as IndexClause,
};
use restate_types::config::Configuration;
use restate_types::identifiers::PartitionId;
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, VQueueId};
use restate_util_string::ReString;

use crate::keys::FieldCodec;
use crate::keys::codecs::CanonicalEntryIdCodec;
use crate::keys::selection::PrepareKeySelection;
use crate::vqueue_table::{EntryByServiceCreated, EntryByServiceCreatedKey};
use crate::walk::engine;
use crate::{PartitionStore, PartitionStoreManager};

use super::*;

/// (service, handler, millis after the base, id byte); `None` is a state mutation.
const ENTRIES: &[(&str, Option<&str>, u64, u8)] = &[
    ("A", None, 50, 1),
    ("A", Some("h1"), 100, 2),
    ("A", Some("h1"), 300, 3),
    // The same millisecond as the previous entry: a tie broken by the ID.
    ("A", Some("h1"), 300, 4),
    ("A", Some("h1"), 500, 5),
    ("A", Some("h2"), 200, 6),
    ("A", Some("h2"), 400, 7),
    ("B", Some("h1"), 150, 8),
    ("B", Some("h1"), 350, 9),
    ("C", Some("h3"), 250, 10),
];

const BASE: u64 = 1_744_010_000_000;

type Row = (String, Option<String>, Reverse<UniqueTimestamp>, Vec<u8>);

fn row(key: EntryByServiceCreatedKey) -> Row {
    let mut id = Vec::new();
    CanonicalEntryIdCodec::encode_field(&key.canonical_id, &mut id);
    (
        key.target_service_name.to_string(),
        key.target_handler_name.map(|handler| handler.to_string()),
        key.created_at,
        id,
    )
}

async fn store_with_entries() -> PartitionStore {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let mut config = Configuration::default();
    config.common.experimental.set_track_entry_indexes(true);
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .unwrap();
    engine::run_all(&mut store, 3).await.unwrap();

    let qid = VQueueId::custom(7, "q");
    let mut tx = store.transaction();
    for &(service, handler, millis, byte) in ENTRIES {
        let (target, kind) = match handler {
            Some(handler) => (
                EntryTargetRef::Service {
                    scope: None,
                    service,
                    handler,
                },
                EntryKind::Invocation,
            ),
            None => (
                EntryTargetRef::VirtualObject {
                    scope: None,
                    service,
                    key: "k",
                    handler: HandlerRef::StateMutation,
                },
                EntryKind::StateMutation,
            ),
        };
        let created =
            UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(BASE + millis));
        let key = EntryKey::new(
            false,
            created.to_unix_millis(),
            u64::from(byte),
            EntryId::new(kind, [byte; 16]),
        );
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &target,
            },
            EntryStateRef {
                stage: Stage::Inbox,
                status: Status::New,
                entry_key: &key,
                metadata: &EntryMetadata::default(),
                stats: &EntryStatistics::new(created, key.run_at()),
            },
        );
    }
    tx.commit().await.unwrap();
    store
}

fn limits(max_runs: usize, buffer: usize) -> OrderedRunsLimits {
    OrderedRunsLimits {
        max_runs,
        max_discovery_keys: 1_000,
        buffer: NonZeroUsize::new(buffer).unwrap(),
        step_budget: NonZeroUsize::new(64).unwrap(),
        bucket_fields: 0,
    }
}

/// Every row the merge of `T` yields, in order, and the keys it examined after
/// discovery.
fn merged_from<T: PhysicalTable<Key = EntryByServiceCreatedKey>>(
    snapshot: &ReadSnapshot,
    selection: Selection<Index>,
    direction: ReadDirection,
    limits: OrderedRunsLimits,
) -> Option<(Vec<Row>, u64)> {
    let request = Arc::new(EntryByServiceCreatedKey::prepare_candidates(&selection).unwrap());
    let OrderedRunsDiscovery::Ready(mut runs) = snapshot
        .ordered_runs::<T>(request, 2, direction, limits, Box::new(()))
        .unwrap()
    else {
        return None;
    };
    let discovered = runs.examined();
    let mut rows = Vec::new();
    loop {
        match runs.advance().unwrap() {
            CursorStep::Record => rows.push(row(runs.current().unwrap().0.decode_all().unwrap())),
            CursorStep::Skipped => {}
            CursorStep::Done => break,
        }
    }
    Some((rows, runs.examined() - discovered))
}

fn merged(
    snapshot: &ReadSnapshot,
    selection: Selection<Index>,
    direction: ReadDirection,
    limits: OrderedRunsLimits,
) -> Option<Vec<Row>> {
    merged_from::<EntryByServiceCreated>(snapshot, selection, direction, limits)
        .map(|(rows, _)| rows)
}

/// The rows `filter` admits, sorted by creation time (newest first) and ID, or
/// in reverse.
fn expected_in(
    direction: ReadDirection,
    filter: impl Fn(&str, Option<&str>, u64) -> bool,
) -> Vec<Row> {
    let mut rows = expected(filter);
    if direction == ReadDirection::Backward {
        rows.reverse();
    }
    rows
}

/// The rows `filter` admits, sorted by creation time (newest first) and ID.
fn expected(filter: impl Fn(&str, Option<&str>, u64) -> bool) -> Vec<Row> {
    let mut rows: Vec<Row> = ENTRIES
        .iter()
        .filter(|(service, handler, millis, _)| filter(service, *handler, *millis))
        .map(|&(service, handler, millis, byte)| {
            let kind = if handler.is_some() {
                EntryKind::Invocation
            } else {
                EntryKind::StateMutation
            };
            let created =
                UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(BASE + millis));
            let mut id = Vec::new();
            let key = EntryKey::new(
                false,
                created.to_unix_millis(),
                u64::from(byte),
                EntryId::new(kind, [byte; 16]),
            );
            CanonicalEntryIdCodec::encode_field(&key.to_canonical_entry_id(7), &mut id);
            (
                service.to_owned(),
                handler.map(str::to_owned),
                Reverse(created),
                id,
            )
        })
        .collect();
    rows.sort_by(|left, right| (&left.2, &left.3).cmp(&(&right.2, &right.3)));
    rows
}

/// The runs of one service, of one handler across services, and of a time window
/// merge into creation order, newest first, with ties broken by ID, whatever
/// the buffer size; backwards, into the exact reverse, oldest first, reading a
/// window's runs only within the window. Too many runs stop discovery before
/// anything is read.
#[restate_core::test]
async fn runs_merge_into_creation_order() {
    let store = store_with_entries().await;
    let (snapshot, _closed) = store.query_read(CancellationToken::new()).unwrap();
    let service = |name: &str| IndexClause::TargetServiceName(ValuePredicate::Equal(name.into()));
    let handler =
        |name: &str| IndexClause::TargetHandlerName(ValuePredicate::Equal(ReString::from(name)));
    let window = || {
        IndexClause::CreatedAt(ValuePredicate::Range {
            lower: Bound::Included(MillisSinceEpoch::new(BASE + 150)),
            upper: Bound::Excluded(MillisSinceEpoch::new(BASE + 400)),
        })
    };

    for (direction, buffer) in [ReadDirection::Forward, ReadDirection::Backward]
        .into_iter()
        .flat_map(|direction| [1, 2, 64].map(|buffer| (direction, buffer)))
    {
        // Service A: the state mutation's run, h1 and h2.
        assert_eq!(
            merged(
                &snapshot,
                Selection::All.and(service("A")),
                direction,
                limits(8, buffer)
            ),
            Some(expected_in(direction, |s, _, _| s == "A")),
            "service only, {direction:?}, buffer {buffer}"
        );
        // Handler h1 across services A and B.
        assert_eq!(
            merged(
                &snapshot,
                Selection::All.and(handler("h1")),
                direction,
                limits(8, buffer)
            ),
            Some(expected_in(direction, |_, h, _| h == Some("h1"))),
            "handler only, {direction:?}, buffer {buffer}"
        );
        // Both, within a time window.
        assert_eq!(
            merged(
                &snapshot,
                Selection::All
                    .and(service("A"))
                    .and(handler("h1"))
                    .and(window()),
                direction,
                limits(8, buffer)
            ),
            Some(expected_in(direction, |s, h, t| s == "A"
                && h == Some("h1")
                && (150..400).contains(&t))),
            "both and window, {direction:?}, buffer {buffer}"
        );
        // Service A within the window: h1's run, with a key on either side of
        // the window, and h2's, with one newer. Backwards, each run reads back
        // from the window's end to its first match, examining only matches.
        let (rows, examined) = merged_from::<EntryByServiceCreated>(
            &snapshot,
            Selection::All.and(service("A")).and(window()),
            direction,
            limits(8, buffer),
        )
        .unwrap();
        assert_eq!(
            rows,
            expected_in(direction, |s, _, t| s == "A" && (150..400).contains(&t)),
            "service and window, {direction:?}, buffer {buffer}"
        );
        if direction == ReadDirection::Backward {
            assert_eq!(examined, rows.len() as u64, "buffer {buffer}");
        }
        // No restriction: every run.
        assert_eq!(
            merged(&snapshot, Selection::All, direction, limits(8, buffer)),
            Some(expected_in(direction, |_, _, _| true)),
            "everything, {direction:?}, buffer {buffer}"
        );
    }
    // Five runs exceed a limit of four, and nothing is read; so does discovery
    // that would evaluate more keys than allowed.
    let few_keys = OrderedRunsLimits {
        max_discovery_keys: 2,
        ..limits(8, 2)
    };
    for direction in [ReadDirection::Forward, ReadDirection::Backward] {
        assert_eq!(
            merged(&snapshot, Selection::All, direction, limits(4, 2)),
            None
        );
        assert_eq!(merged(&snapshot, Selection::All, direction, few_keys), None);
    }
}

/// Charges what a read holds, by step.
#[derive(Clone, Default)]
struct Charged(Arc<std::sync::Mutex<(usize, usize, Option<usize>)>>);

impl Charged {
    /// (charged now, most charged at once)
    fn now_and_peak(&self) -> (usize, usize) {
        let state = self.0.lock().unwrap();
        (state.0, state.1)
    }

    fn with_limit(limit: usize) -> Self {
        let charged = Self::default();
        charged.0.lock().unwrap().2 = Some(limit);
        charged
    }
}

impl RetainedMemory for Charged {
    fn grow(&mut self, bytes: usize) -> Result<()> {
        let mut state = self.0.lock().unwrap();
        if state.2.is_some_and(|limit| state.0 + bytes > limit) {
            return Err(restate_storage_api::StorageError::Generic(anyhow::anyhow!(
                "memory limit"
            )));
        }
        state.0 += bytes;
        state.1 = state.1.max(state.0);
        Ok(())
    }

    fn shrink(&mut self, bytes: usize) {
        self.0.lock().unwrap().0 -= bytes;
    }
}

fn service_a() -> Arc<KeySelection<EntryByServiceCreatedKey>> {
    Arc::new(
        EntryByServiceCreatedKey::prepare_candidates(&Selection::<Index>::All.and(
            IndexClause::TargetServiceName(ValuePredicate::Equal("A".into())),
        ))
        .unwrap(),
    )
}

/// What `runs` holds, measured from the capacities of its containers and the
/// lengths of the bytes it copied rather than from what it charged.
fn held<T: PhysicalTable>(runs: &OrderedRuns<T>) -> usize {
    let Some(merge) = &runs.inner else {
        return 0;
    };
    let per_run: usize = merge
        .runs
        .iter()
        .map(|run| {
            run.prefix.len()
                + run.resume.as_ref().map_or(0, |resume| resume.len())
                + run.floor.as_ref().map_or(0, |floor| floor.len())
                + run.records.capacity() * size_of::<Record>()
                + run.records.iter().map(Record::bytes).sum::<usize>()
        })
        .sum();
    let heads: usize = merge
        .heads
        .iter()
        .map(|Reverse(head)| head.record.bytes())
        .sum();
    size_of::<Merge<T::Key>>()
        + merge.runs.capacity() * size_of::<Run>()
        + per_run
        + merge.heads.capacity() * size_of::<Reverse<Head>>()
        + heads
        + runs.current.as_ref().map_or(0, |head| head.record.bytes())
        + merge.selection.retained_bytes()
        // The snapshot cursor's copy of the bounds and the iterator's, charged
        // as a bound rather than measured.
        + 3 * merge.cursor.scan().bound_bytes()
}

/// Between steps, a read is charged exactly what it holds, at the capacity it
/// allocated, and all of it is released when the read is exhausted, dropped,
/// or doesn't start. A charge that fails stops the read.
#[restate_core::test]
async fn merges_charge_what_they_hold_and_release_it() {
    let store = store_with_entries().await;
    let (snapshot, _closed) = store.query_read(CancellationToken::new()).unwrap();
    let start = |charged: &Charged, direction| {
        snapshot.ordered_runs::<EntryByServiceCreated>(
            service_a(),
            2,
            direction,
            limits(8, 1),
            Box::new(charged.clone()),
        )
    };

    // Service A has three runs and seven records of about 70 bytes each, read
    // either way.
    for direction in [ReadDirection::Forward, ReadDirection::Backward] {
        let charged = Charged::default();
        let OrderedRunsDiscovery::Ready(mut runs) = start(&charged, direction).unwrap() else {
            panic!("three runs are within the limit");
        };
        let (discovered, _) = charged.now_and_peak();
        assert_eq!(
            (runs.retained_bytes(), discovered),
            (held(&runs), held(&runs))
        );
        let mut records = 0;
        while runs.advance().unwrap() != CursorStep::Done {
            let (now, _) = charged.now_and_peak();
            assert_eq!((runs.retained_bytes(), now), (held(&runs), held(&runs)));
            records += 1;
        }
        assert_eq!(records, 7);
        let (now, peak) = charged.now_and_peak();
        assert_eq!(
            (now, runs.retained_bytes()),
            (0, 0),
            "an exhausted read holds nothing"
        );
        assert!(peak > discovered);
    }

    // Two creation times: the selection seeks past rejected keys, growing its
    // seek storage. Across service A's runs, it seeks during discovery; within
    // h1 alone, whose first key matches, only while reading ahead, and
    // backwards, it steps over them.
    let created_at = || {
        IndexClause::CreatedAt(ValuePredicate::In(vec![
            MillisSinceEpoch::new(BASE + 500),
            MillisSinceEpoch::new(BASE + 100),
        ]))
    };
    let service_a = || {
        Selection::<Index>::All
            .and(IndexClause::TargetServiceName(ValuePredicate::Equal(
                "A".into(),
            )))
            .and(created_at())
    };
    let h1 = || IndexClause::TargetHandlerName(ValuePredicate::Equal(ReString::from("h1")));
    for (selection, direction) in [
        (service_a(), ReadDirection::Forward),
        (service_a(), ReadDirection::Backward),
        (service_a().and(h1()), ReadDirection::Forward),
        (service_a().and(h1()), ReadDirection::Backward),
    ] {
        let request = Arc::new(EntryByServiceCreatedKey::prepare_candidates(&selection).unwrap());
        let charged = Charged::default();
        let OrderedRunsDiscovery::Ready(mut runs) = snapshot
            .ordered_runs::<EntryByServiceCreated>(
                request,
                2,
                direction,
                limits(8, 1),
                Box::new(charged.clone()),
            )
            .unwrap()
        else {
            panic!("the runs are within the limit");
        };
        let mut records = 0;
        loop {
            assert_eq!(
                (runs.retained_bytes(), charged.now_and_peak().0),
                (held(&runs), held(&runs))
            );
            match runs.advance().unwrap() {
                CursorStep::Record => records += 1,
                CursorStep::Skipped => {}
                CursorStep::Done => break,
            }
        }
        assert_eq!((records, charged.now_and_peak().0), (2, 0));
    }

    // Dropping an unfinished read releases what it holds.
    let charged = Charged::default();
    let OrderedRunsDiscovery::Ready(mut runs) = start(&charged, ReadDirection::Backward).unwrap()
    else {
        panic!("three runs are within the limit");
    };
    assert_eq!(runs.advance().unwrap(), CursorStep::Record);
    assert!(charged.now_and_peak().0 > 0);
    drop(runs);
    assert_eq!(charged.now_and_peak().0, 0);

    // Neither does one that finds no run: here, a service without entries.
    let charged = Charged::default();
    let request = Arc::new(
        EntryByServiceCreatedKey::prepare_candidates(&Selection::<Index>::All.and(
            IndexClause::TargetServiceName(ValuePredicate::Equal("Z".into())),
        ))
        .unwrap(),
    );
    let OrderedRunsDiscovery::Ready(mut runs) = snapshot
        .ordered_runs::<EntryByServiceCreated>(
            request,
            2,
            ReadDirection::Forward,
            limits(8, 1),
            Box::new(charged.clone()),
        )
        .unwrap()
    else {
        panic!("no runs are within the limit");
    };
    assert_eq!((charged.now_and_peak().0, runs.retained_bytes()), (0, 0));
    assert_eq!(runs.advance().unwrap(), CursorStep::Done);

    // A read that stops at discovery holds nothing either.
    let charged = Charged::default();
    let request = Arc::new(EntryByServiceCreatedKey::prepare_candidates(&Selection::All).unwrap());
    let discovery = snapshot
        .ordered_runs::<EntryByServiceCreated>(
            request,
            2,
            ReadDirection::Forward,
            limits(4, 1),
            Box::new(charged.clone()),
        )
        .unwrap();
    assert!(matches!(
        discovery,
        OrderedRunsDiscovery::TooManyRuns { examined } if examined > 0
    ));
    assert_eq!(charged.now_and_peak().0, 0);

    // A charge over the limit stops the read, at discovery or while reading ahead.
    let discovered = {
        let charged = Charged::default();
        let _runs = start(&charged, ReadDirection::Forward).unwrap();
        charged.now_and_peak().0
    };
    assert!(start(&Charged::with_limit(10), ReadDirection::Forward).is_err());
    let charged_at_limit = Charged::with_limit(discovered + 10);
    let OrderedRunsDiscovery::Ready(mut runs) =
        start(&charged_at_limit, ReadDirection::Forward).unwrap()
    else {
        panic!("discovery fits the limit");
    };
    assert!(runs.advance().is_err());
    assert_eq!(
        (
            charged_at_limit.now_and_peak().0,
            runs.retained_bytes(),
            held(&runs)
        ),
        (0, 0, 0),
        "a failed step releases everything"
    );
}

/// A table whose partition owns only the entries with an even ID byte, as if the
/// others were left over from a split.
enum EvenOnly {}

impl Table for EvenOnly {
    type Record<'a> = (KeyDecoder<'a, EntryByServiceCreatedKey>, &'a [u8]);
    type Scan = KeySelection<EntryByServiceCreatedKey>;
}

impl PhysicalTable for EvenOnly {
    type Key = EntryByServiceCreatedKey;

    fn owns_record(
        key: KeyDecoder<'_, EntryByServiceCreatedKey>,
        _: &crate::table::ReadMetadata<'_>,
    ) -> Result<bool> {
        let id = row(key.decode_all()?).3;
        Ok(id[id.len() - 1].is_multiple_of(2))
    }
}

/// Reading ahead examines at most the step budget of keys per step, including
/// the keys the partition doesn't own, and still returns every owned record in
/// order: a step that runs out of budget is skipped and the next continues.
#[restate_core::test]
async fn reading_ahead_stays_within_the_step_budget() {
    let store = store_with_entries().await;
    let (snapshot, _closed) = store.query_read(CancellationToken::new()).unwrap();
    let budget = OrderedRunsLimits {
        step_budget: NonZeroUsize::MIN,
        ..limits(8, 2)
    };
    let even = |id: &[u8]| id[id.len() - 1].is_multiple_of(2);
    for direction in [ReadDirection::Forward, ReadDirection::Backward] {
        let OrderedRunsDiscovery::Ready(mut runs) = snapshot
            .ordered_runs::<EvenOnly>(service_a(), 2, direction, budget, Box::new(()))
            .unwrap()
        else {
            panic!("three runs are within the limit");
        };
        let mut rows = Vec::new();
        let mut skipped = 0;
        loop {
            let before = runs.examined();
            let step = runs.advance().unwrap();
            assert!(runs.examined() - before <= 1, "one key per step");
            match step {
                CursorStep::Record => {
                    rows.push(row(runs.current().unwrap().0.decode_all().unwrap()))
                }
                CursorStep::Skipped => skipped += 1,
                CursorStep::Done => break,
            }
        }
        let expected: Vec<_> = expected_in(direction, |s, _, _| s == "A")
            .into_iter()
            .filter(|row| even(&row.3))
            .collect();
        assert_eq!(rows, expected, "{direction:?}");
        assert!(skipped > 0);
    }
}

/// An exhausted merge releases the snapshot even while the cursor is alive; an
/// unfinished one holds it. Once the read is cancelled, neither the next step
/// nor the current record can be read, and the failed step releases everything.
#[restate_core::test]
async fn merges_release_the_snapshot_and_stop_on_cancellation() {
    let store = store_with_entries().await;
    let start = |snapshot: &ReadSnapshot| {
        let OrderedRunsDiscovery::Ready(runs) = snapshot
            .ordered_runs::<EntryByServiceCreated>(
                service_a(),
                2,
                ReadDirection::Forward,
                limits(8, 1),
                Box::new(()),
            )
            .unwrap()
        else {
            panic!("three runs are within the limit");
        };
        runs
    };

    let (snapshot, closed) = store.query_read(CancellationToken::new()).unwrap();
    let mut runs = start(&snapshot);
    drop(snapshot);
    let mut closed = std::pin::pin!(closed);
    while runs.advance().unwrap() != CursorStep::Done {}
    assert!(
        closed.as_mut().now_or_never().is_some(),
        "exhausted merges release"
    );
    drop(runs);

    let cancel = CancellationToken::new();
    let (snapshot, closed) = store.query_read(cancel.clone()).unwrap();
    let mut runs = start(&snapshot);
    drop(snapshot);
    let mut closed = std::pin::pin!(closed);
    assert_eq!(runs.advance().unwrap(), CursorStep::Record);
    assert!(runs.current().is_ok());
    assert!(
        closed.as_mut().now_or_never().is_none(),
        "unfinished merges hold"
    );
    cancel.cancel();
    assert!(
        runs.current().is_err(),
        "a cancelled read hides its current record"
    );
    assert!(runs.advance().is_err(), "a cancelled read stops");
    assert!(
        closed.as_mut().now_or_never().is_some(),
        "the failed step releases the snapshot"
    );
    assert_eq!(runs.retained_bytes(), 0);
}

/// A store whose entries are `(service, millis after BASE, id byte)`, each an
/// invocation of handler `h`, with the entry indexes tracked.
async fn store_with(entries: &[(&str, u64, u8)]) -> PartitionStore {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let mut config = Configuration::default();
    config.common.experimental.set_track_entry_indexes(true);
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .unwrap();
    engine::run_all(&mut store, 3).await.unwrap();
    let qid = VQueueId::custom(7, "q");
    let mut tx = store.transaction();
    for &(service, millis, byte) in entries {
        let target = EntryTargetRef::Service {
            scope: None,
            service,
            handler: "h",
        };
        let created =
            UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(BASE + millis));
        let key = EntryKey::new(
            false,
            created.to_unix_millis(),
            u64::from(byte),
            EntryId::new(EntryKind::Invocation, [byte; 16]),
        );
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &target,
            },
            EntryStateRef {
                stage: Stage::Inbox,
                status: Status::New,
                entry_key: &key,
                metadata: &EntryMetadata::default(),
                stats: &EntryStatistics::new(created, key.run_at()),
            },
        );
    }
    tx.commit().await.unwrap();
    store
}

/// `entry_by_created/1` read with its hour as a bucket and runs of an hour and a
/// service: `(service, millis after BASE)` in merge order, or the error the read
/// ended with.
fn bucketed(
    snapshot: &ReadSnapshot,
    direction: ReadDirection,
    limits: OrderedRunsLimits,
) -> Option<std::result::Result<Vec<(String, u64)>, StorageError>> {
    bucketed_with(snapshot, KeySelection::all(), direction, limits)
}

/// [`bucketed`] with `selection`.
fn bucketed_with(
    snapshot: &ReadSnapshot,
    selection: KeySelection<crate::vqueue_table::EntryByCreatedKey>,
    direction: ReadDirection,
    limits: OrderedRunsLimits,
) -> Option<std::result::Result<Vec<(String, u64)>, StorageError>> {
    let OrderedRunsDiscovery::Ready(mut runs) = snapshot
        .ordered_runs::<crate::vqueue_table::EntryByCreated>(
            Arc::new(selection),
            2,
            direction,
            limits,
            Box::new(()),
        )
        .unwrap()
    else {
        return None;
    };
    let mut rows = Vec::new();
    loop {
        match runs.advance() {
            Ok(CursorStep::Record) => {
                let key = runs.current().unwrap().0.decode_all().unwrap();
                let millis = key.created_at.0.to_unix_millis().as_u64() - BASE;
                rows.push((key.target_service_name.to_string(), millis));
            }
            Ok(CursorStep::Skipped) => {}
            Ok(CursorStep::Done) => return Some(Ok(rows)),
            Err(error) => return Some(Err(error)),
        }
    }
}

/// With the hour as a bucket, a read discovers one hour's runs at a time: three
/// services over four hours and a fourth in one of them are thirteen runs, too
/// many for a read that merges four, yet bucket by bucket it returns every entry
/// newest first, or oldest first backwards. A read that merges three fails at
/// the hour with four: it has returned the other hours' rows already, so it
/// can't read another way.
#[restate_core::test]
async fn bucketed_reads_discover_one_bucket_at_a_time() {
    const HOUR: u64 = 3_600_000;
    let mut entries = Vec::new();
    let mut byte = 0;
    for hour in 0..4 {
        for (offset, service) in [(10, "A"), (30, "B"), (20, "C"), (40, "A")] {
            byte += 1;
            entries.push((service, hour * HOUR + offset, byte));
        }
    }
    entries.push(("D", HOUR + 50, 99));
    let store = store_with(&entries).await;
    let (snapshot, _closed) = store.query_read(CancellationToken::new()).unwrap();
    let four = OrderedRunsLimits {
        bucket_fields: 1,
        ..limits(4, 2)
    };
    let mut expected: Vec<_> = entries
        .iter()
        .map(|&(service, millis, _)| (service.to_owned(), millis))
        .collect();
    expected.sort_by_key(|&(_, millis)| Reverse(millis));
    let unbucketed = OrderedRunsLimits {
        bucket_fields: 0,
        ..four
    };
    let three = OrderedRunsLimits {
        max_runs: 3,
        ..four
    };
    for direction in [ReadDirection::Forward, ReadDirection::Backward] {
        let rows = bucketed(&snapshot, direction, four).unwrap().unwrap();
        assert_eq!(
            rows,
            expected_in_order(&expected, direction),
            "{direction:?}"
        );
        // Without buckets, the thirteen runs are too many to discover.
        assert!(bucketed(&snapshot, direction, unbucketed).is_none());
        // Three runs a bucket: the hours without D fit, the one with it doesn't.
        let ended = bucketed(&snapshot, direction, three).unwrap();
        assert!(ended.is_err(), "{direction:?}: {ended:?}");
    }
}

fn expected_in_order(
    newest_first: &[(String, u64)],
    direction: ReadDirection,
) -> Vec<(String, u64)> {
    let mut rows = newest_first.to_vec();
    if direction == ReadDirection::Backward {
        rows.reverse();
    }
    rows
}

/// Backwards, a bucket without a match stays a bucket: oldest first, service
/// A in hours 0 and 2 and only B in hour 1, the read of A returns each of its
/// entries once, without reading from hour 1 into the hour it returned.
#[restate_core::test]
async fn buckets_without_a_match_are_skipped_in_both_directions() {
    use crate::vqueue_table::EntryByCreatedKey;

    const HOUR: u64 = 3_600_000;
    let store = store_with(&[("A", 10, 1), ("B", HOUR + 10, 2), ("A", 2 * HOUR + 10, 3)]).await;
    let (snapshot, _closed) = store.query_read(CancellationToken::new()).unwrap();
    let only_a = || {
        EntryByCreatedKey::prepare_candidates(&Selection::All.and(IndexClause::TargetServiceName(
            ValuePredicate::Equal("A".into()),
        )))
        .unwrap()
    };
    let limits = OrderedRunsLimits {
        bucket_fields: 1,
        ..limits(4, 2)
    };
    let newest_first = vec![("A".to_owned(), 2 * HOUR + 10), ("A".to_owned(), 10)];
    // A budget of one key a step pauses the later buckets' discovery, which
    // continues in the next steps with the same rows.
    let one_key = OrderedRunsLimits {
        step_budget: NonZeroUsize::MIN,
        ..limits
    };
    for direction in [ReadDirection::Forward, ReadDirection::Backward] {
        for limits in [limits, one_key] {
            let rows = bucketed_with(&snapshot, only_a(), direction, limits)
                .unwrap()
                .unwrap();
            assert_eq!(
                rows,
                expected_in_order(&newest_first, direction),
                "{direction:?}"
            );
        }
        assert!(
            skipped_steps(&snapshot, only_a(), direction, one_key) > 0,
            "{direction:?}"
        );
    }
}

/// How many steps of the bucketed read report [`CursorStep::Skipped`].
fn skipped_steps(
    snapshot: &ReadSnapshot,
    selection: KeySelection<crate::vqueue_table::EntryByCreatedKey>,
    direction: ReadDirection,
    limits: OrderedRunsLimits,
) -> usize {
    let OrderedRunsDiscovery::Ready(mut runs) = snapshot
        .ordered_runs::<crate::vqueue_table::EntryByCreated>(
            Arc::new(selection),
            2,
            direction,
            limits,
            Box::new(()),
        )
        .unwrap()
    else {
        panic!("too many runs");
    };
    let mut skipped = 0;
    loop {
        match runs.advance().unwrap() {
            CursorStep::Record => {}
            CursorStep::Skipped => skipped += 1,
            CursorStep::Done => return skipped,
        }
    }
}

/// Bucketed reads charge what they hold and release all of it: when done, when
/// a later bucket is refused memory, which fails the read after it returned the
/// first bucket's rows, and when cancelled between buckets.
#[restate_core::test]
async fn bucketed_reads_charge_refuse_and_cancel_like_others() {
    const HOUR: u64 = 3_600_000;
    let store = store_with(&[
        ("A", 2 * HOUR + 10, 1),
        ("A", HOUR + 10, 2),
        ("B", HOUR + 20, 3),
        ("C", HOUR + 30, 4),
        ("D", HOUR + 40, 5),
    ])
    .await;
    let limits = OrderedRunsLimits {
        bucket_fields: 1,
        ..limits(8, 2)
    };
    let open = |snapshot: &ReadSnapshot, direction, charged: &Charged| {
        let OrderedRunsDiscovery::Ready(runs) = snapshot
            .ordered_runs::<crate::vqueue_table::EntryByCreated>(
                Arc::new(KeySelection::all()),
                2,
                direction,
                limits,
                Box::new(charged.clone()),
            )
            .unwrap()
        else {
            panic!("the first bucket's runs are within the limit");
        };
        runs
    };
    let (snapshot, _closed) = store.query_read(CancellationToken::new()).unwrap();

    // Done: all five rows, and everything charged released.
    let mut peak = 0;
    for direction in [ReadDirection::Forward, ReadDirection::Backward] {
        let charged = Charged::default();
        let mut runs = open(&snapshot, direction, &charged);
        let mut rows = 0;
        while let step = runs.advance().unwrap()
            && step != CursorStep::Done
        {
            rows += usize::from(step == CursorStep::Record);
        }
        assert_eq!((rows, charged.now_and_peak().0), (5, 0), "{direction:?}");
        if direction == ReadDirection::Forward {
            peak = charged.now_and_peak().1;
        }
    }

    // Refused: newest first, the first hour's one run fits and returns its
    // row; the next hour's four runs don't, which fails the read.
    let charged = Charged::with_limit(peak - 1);
    let mut runs = open(&snapshot, ReadDirection::Forward, &charged);
    let mut rows = 0;
    let failed = loop {
        match runs.advance() {
            Ok(CursorStep::Record) => rows += 1,
            Ok(CursorStep::Skipped) => {}
            Ok(CursorStep::Done) => break false,
            Err(_) => break true,
        }
    };
    assert!(failed && rows == 1, "failed: {failed}, rows: {rows}");
    assert_eq!((runs.retained_bytes(), charged.now_and_peak().0), (0, 0));

    // Cancelled between buckets: the next step fails and releases everything.
    let cancel = CancellationToken::new();
    let (snapshot, _closed) = store.query_read(cancel.clone()).unwrap();
    let charged = Charged::default();
    let mut runs = open(&snapshot, ReadDirection::Forward, &charged);
    assert_eq!(runs.advance().unwrap(), CursorStep::Record);
    cancel.cancel();
    assert!(runs.advance().is_err());
    assert_eq!((runs.retained_bytes(), charged.now_and_peak().0), (0, 0));
}
