// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::ControlFlow;

use bytes::{Bytes, BytesMut};
use rocksdb::ReadOptions;

use restate_rocksdb::{IterAction, Priority, RocksDbManager};
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::{Stage, Status};
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::stats::service_stats::{
    ServiceStats as Target, ServiceStatsClause as Clause,
};
use restate_types::partition_table::Partition;
use restate_types::sharding::{KeyRange, PartitionId};
use restate_types::vqueues::EntryKind;
use restate_util_string::encoded_mem_cmp_str;

use crate::keys::KeyKind;
use crate::keys::header::InstanceScope;
use crate::keys::selection::KeyMatch;
use crate::scan::PhysicalScan;
use crate::stats::aggregated::test_util::read_stats;
use crate::stats::aggregated::{AggregatedStatsMut, StageStatus, complete_gauges, ready_scope};
use crate::stats::{Stat, encode_header};
use crate::{PartitionStore, PartitionStoreManager};

use super::{ServiceStats, ServiceStatsKey};

type Row = (&'static str, EntryKind, Option<&'static str>);
const ROWS: &[Row] = &[
    ("alpha", EntryKind::Invocation, None),
    ("alpha", EntryKind::Invocation, Some("")),
    ("alpha", EntryKind::Invocation, Some("a")),
    ("alpha", EntryKind::Invocation, Some("a\0")),
    ("alpha", EntryKind::Invocation, Some("abc")),
    ("alpha", EntryKind::Invocation, Some("z")),
    ("alpha", EntryKind::Invocation, Some("é")),
    ("alpha", EntryKind::StateMutation, None),
    ("alphabet", EntryKind::Invocation, Some("abc")),
    ("beta", EntryKind::Invocation, Some("abc")),
];

fn encoded_key(scope: InstanceScope, row: &Row) -> Vec<u8> {
    let mut key = Vec::new();
    ServiceStats::encode_key(
        scope,
        ServiceStatsKey::borrowed(row.0, row.2, row.1),
        &mut key,
    );
    key
}

fn prefix(scope: InstanceScope) -> BytesMut {
    let mut bytes = BytesMut::new();
    encode_header::<ServiceStats, _>(scope, &mut bytes);
    bytes
}

fn alpha() -> Selection<Target> {
    Selection::default().and(Clause::ServiceName(ValuePredicate::Equal("alpha".into())))
}

type Case = (Selection<Target>, fn(&Row) -> bool);

fn cases() -> Vec<Case> {
    vec![
        (Selection::All, |_| true),
        (Selection::Predicates(Default::default()), |_| true),
        (Selection::Empty, |_| false),
        (alpha(), |row| row.0 == "alpha"),
        (
            Selection::default().and(Clause::ServiceName(ValuePredicate::In(vec![
                "beta".into(),
                "alpha".into(),
                "alpha".into(),
            ]))),
            |row| matches!(row.0, "alpha" | "beta"),
        ),
        (
            Selection::default()
                .and(Clause::ServiceName(ValuePredicate::In(vec![
                    "beta".into(),
                    "alphax".into(),
                    "alpha".into(),
                    "alpha".into(),
                ])))
                .and(Clause::ServiceName(ValuePredicate::In(vec![
                    "alpha".into(),
                    "beta".into(),
                    "gamma".into(),
                    "zulu".into(),
                ]))),
            |row| matches!(row.0, "alpha" | "beta"),
        ),
        (
            Selection::default().and(Clause::ServiceName(ValuePredicate::Range {
                lower: Excluded("alpha".into()),
                upper: Included("beta".into()),
            })),
            |row| row.0 > "alpha" && row.0 <= "beta",
        ),
        (
            Selection::default()
                .and(Clause::ServiceNameStartsWith("alp".into()))
                .and(Clause::ServiceName(ValuePredicate::Range {
                    lower: Excluded("alpha".into()),
                    upper: Unbounded,
                })),
            |row| row.0.starts_with("alp") && row.0 > "alpha",
        ),
        (
            Selection::default().and(Clause::Handler(ValuePredicate::Equal(None))),
            |row| row.2.is_none(),
        ),
        (
            Selection::default().and(Clause::ServiceName(ValuePredicate::In(vec![
                "alpha".into(),
                "alphaz".into(), // the latter has no stored keys
            ]))),
            |row| row.0 == "alpha",
        ),
        // A finite suffix domain must not exhaust the scan across parent groups.
        (
            Selection::default().and(Clause::Handler(ValuePredicate::In(vec![
                Some("a".into()),
                Some("abc".into()),
            ]))),
            |row| matches!(row.2, Some("a" | "abc")),
        ),
        (
            alpha()
                .and(Clause::Kind(ValuePredicate::Equal(EntryKind::Invocation)))
                .and(Clause::Handler(ValuePredicate::Range {
                    lower: Excluded(None),
                    upper: Included(Some("abc".into())),
                })),
            |row| {
                row.0 == "alpha"
                    && row.1 == EntryKind::Invocation
                    && row.2.is_some_and(|h| h <= "abc")
            },
        ),
        (
            Selection::default()
                .and(Clause::ServiceNameStartsWith("alp".into()))
                .and(Clause::Kind(ValuePredicate::Equal(EntryKind::Invocation)))
                .and(Clause::HandlerStartsWith("a".into())),
            |row| {
                row.0.starts_with("alp")
                    && row.1 == EntryKind::Invocation
                    && row.2.is_some_and(|h| h.starts_with('a'))
            },
        ),
        // A suffix prefix must not stop the scan when a later service can match.
        (
            Selection::default().and(Clause::HandlerStartsWith("a".into())),
            |row| row.2.is_some_and(|h| h.starts_with('a')),
        ),
        (
            Selection::default().and(Clause::HandlerStartsWith("".into())),
            |row| row.2.is_some(),
        ),
        (
            Selection::default().and(Clause::HandlerStartsWith("a\0".into())),
            |row| row.2.is_some_and(|h| h.starts_with("a\0")),
        ),
        (
            Selection::default()
                .and(Clause::ServiceNameStartsWith("a".into()))
                .and(Clause::ServiceNameStartsWith("alp".into())),
            |row| row.0.starts_with("alp"),
        ),
        (
            Selection::default()
                .and(Clause::ServiceNameStartsWith("alp".into()))
                .and(Clause::ServiceNameStartsWith("bet".into())),
            |_| false,
        ),
        (
            alpha()
                .and(Clause::Handler(ValuePredicate::Equal(None)))
                .and(Clause::HandlerStartsWith("".into())),
            |_| false,
        ),
        (
            Selection::default()
                .and(Clause::ServiceName(ValuePredicate::Range {
                    lower: Included("alpha".into()),
                    upper: Included("beta".into()),
                }))
                .and(Clause::ServiceName(ValuePredicate::Range {
                    lower: Excluded("alpha".into()),
                    upper: Excluded("beta".into()),
                })),
            |row| row.0 > "alpha" && row.0 < "beta",
        ),
        (
            alpha().and(Clause::ServiceNameStartsWith("bet".into())),
            |_| false,
        ),
        (
            Selection::default().and(Clause::Kind(ValuePredicate::In(vec![]))),
            |_| false,
        ),
        (
            Selection::default()
                .and(Clause::ServiceName(ValuePredicate::Range {
                    lower: Included("beta".into()),
                    upper: Unbounded,
                }))
                .and(Clause::ServiceName(ValuePredicate::Range {
                    lower: Unbounded,
                    upper: Excluded("alpha".into()),
                })),
            |_| false,
        ),
    ]
}

#[test]
fn compiled_bounds_and_key_checks_agree_with_logical_filters() {
    let partition = InstanceScope {
        partition: PartitionId::MIN,
        id: 64,
    };
    let prefix = prefix(partition);
    let mut keys: Vec<_> = ROWS
        .iter()
        .map(|row| (row, encoded_key(partition, row)))
        .collect();
    keys.sort_by(|(_, a), (_, b)| a.cmp(b));
    for (case, (filter, expected_match)) in cases().into_iter().enumerate() {
        let prepared = ServiceStatsKey::prepare_selection(&filter).unwrap();
        let mut cursor = prepared.into_cursor(&prefix).unwrap();
        let scan = cursor.as_ref().map(|cursor| cursor.scan().clone());
        let in_bounds = |key: &[u8]| scan.as_ref().is_some_and(|scan| scan.contains_key(key));
        let mut actual = Vec::new();
        let mut seek_to: Option<Bytes> = None;
        for (row, key) in &keys {
            if !in_bounds(key)
                || seek_to
                    .as_ref()
                    .is_some_and(|target| key.as_slice() < target.as_ref())
            {
                continue;
            }
            match cursor.as_mut().unwrap().evaluate(key).unwrap() {
                KeyMatch::Match => actual.push(*row),
                KeyMatch::Seek(target) => {
                    assert!(
                        target.as_ref() > key.as_slice() && in_bounds(&target),
                        "case {case}"
                    );
                    seek_to = Some(target);
                }
                KeyMatch::Done => break,
                KeyMatch::Skip => panic!("fixed_header_uses_ordered_seeks"),
            }
        }
        let expected: Vec<_> = keys
            .iter()
            .filter(|(row, _)| expected_match(row))
            .map(|(row, _)| *row)
            .collect();
        assert_eq!(actual, expected, "case {case}");
    }

    // An unconstrained handler stops lower-bound construction. The upper bound
    // stops at the prefix's exclusive byte boundary, before the next service.
    let prepared = ServiceStatsKey::prepare_selection(
        &Selection::default()
            .and(Clause::ServiceNameStartsWith("alp".into()))
            .and(Clause::Kind(ValuePredicate::Equal(EntryKind::Invocation))),
    )
    .unwrap();
    let scan = prepared.scan(&prefix).unwrap().unwrap();
    let PhysicalScan::RangeExclusive(_, _, lower, upper) = scan else {
        panic!("expected range")
    };
    let mut expected_lower = Vec::new();
    ServiceStatsKey::prefix(partition, &mut expected_lower).service_name("alp");
    assert_eq!(lower.as_ref(), expected_lower);
    let mut end = prefix.to_vec();
    end.extend_from_slice(b"alq");
    assert_eq!(upper.as_ref(), end);
    let beta = encoded_key(partition, &ROWS[9]);
    assert_eq!(
        prepared
            .into_cursor(&prefix)
            .unwrap()
            .unwrap()
            .evaluate(&beta)
            .unwrap(),
        KeyMatch::Done
    );

    // A SQL-style >= range starts at the encoded value, not the whole stat table
    // or an equality prefix that would exclude lexicographically later services.
    let filter = Selection::default().and(Clause::ServiceName(ValuePredicate::Range {
        lower: Included("LargeState".into()),
        upper: Unbounded,
    }));
    let scan = ServiceStatsKey::prepare_selection(&filter)
        .unwrap()
        .scan(&prefix)
        .unwrap()
        .unwrap();
    let PhysicalScan::RangeExclusive(_, _, lower, upper) = scan else {
        panic!("expected bounded range")
    };
    expected_lower.clear();
    ServiceStatsKey::prefix(partition, &mut expected_lower).service_name("LargeState");
    assert_eq!(lower.as_ref(), expected_lower);
    end = prefix.to_vec();
    assert!(crate::convert_to_upper_bound(&mut end));
    assert_eq!(upper.as_ref(), end);

    let invalid = Selection::default().and(Clause::Kind(ValuePredicate::Equal(EntryKind::Unknown)));
    assert!(ServiceStatsKey::prepare_selection(&invalid).is_err());
    let mut cursor = ServiceStatsKey::prepare_selection(&alpha())
        .unwrap()
        .into_cursor(&prefix)
        .unwrap()
        .unwrap();
    assert!(cursor.evaluate(&[]).is_err());
    let mut trailing = encoded_key(partition, &ROWS[0]);
    trailing.push(0);
    assert!(cursor.evaluate(&trailing).is_err());
}

async fn scan_keys(
    store: &PartitionStore,
    scope: InstanceScope,
    filter: &Selection<Target>,
) -> crate::Result<Vec<Vec<u8>>> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    read_stats::<ServiceStats, _, _>(store, filter, move |key, counts| {
        assert_eq!(counts.iter().map(|(_, count)| count).sum::<u64>(), 1);
        let key = key.try_full_decode::<ServiceStats>().unwrap();
        let mut encoded = Vec::new();
        ServiceStats::encode_key(scope, key, &mut encoded);
        tx.send(encoded).unwrap();
        ControlFlow::Continue(())
    })?
    .await?;
    let mut keys = Vec::new();
    while let Some(key) = rx.recv().await {
        keys.push(key);
    }
    Ok(keys)
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_stats_filters_run_before_value_materialization() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    complete_gauges(&mut store).await;
    let scope = ready_scope::<ServiceStats>(&store);
    let mut tx = store.transaction();
    for row in ROWS {
        AggregatedStatsMut::new(&mut tx, scope.id).increment_stage_status::<ServiceStats, _>(
            ServiceStatsKey::borrowed(row.0, row.2, row.1),
            StageStatus {
                stage: Stage::Inbox,
                status: Status::New,
            },
        );
    }
    tx.commit().await.unwrap();
    drop(tx);
    for (case, (filter, matches)) in cases().into_iter().enumerate() {
        let mut expected: Vec<_> = ROWS
            .iter()
            .filter(|row| matches(row))
            .map(|row| encoded_key(scope, row))
            .collect();
        expected.sort();
        assert_eq!(
            scan_keys(&store, scope, &filter).await.unwrap(),
            expected,
            "case {case}"
        );
    }

    // Observe the production iterator before residual key filtering as well.
    for filter in std::iter::once(alpha()).chain(
        [
            (Included("alphabet".into()), Unbounded),
            (Excluded("alpha".into()), Unbounded),
            (Unbounded, Included("alpha".into())),
            (Unbounded, Excluded("beta".into())),
        ]
        .into_iter()
        .map(|(lower, upper)| {
            Selection::default().and(Clause::ServiceName(ValuePredicate::Range { lower, upper }))
        }),
    ) {
        let prepared = ServiceStatsKey::prepare_selection(&filter).unwrap();
        let scan = prepared.scan(&prefix(scope)).unwrap().unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        store
            .iterator_for_each_physical(
                "test-stat-bounds",
                Priority::Low,
                ReadOptions::default(),
                scan,
                move |(key, _)| {
                    sender.send(key.to_vec()).unwrap();
                    ControlFlow::Continue(())
                },
            )
            .unwrap()
            .await
            .unwrap();
        let mut visited = Vec::new();
        while let Some(key) = receiver.recv().await {
            visited.push(key);
        }
        assert_eq!(visited, scan_keys(&store, scope, &filter).await.unwrap());
    }

    // This entry lies inside the IN-list envelope but outside its exact set.
    // Its malformed value must never be deserialized for this query.
    let key = encoded_key(scope, &("alphax", EntryKind::Invocation, None));
    let mut tx = store.transaction();
    tx.raw_put_cf(KeyKind::RegisteredStats, key, b"malformed");
    tx.commit().await.unwrap();
    drop(tx);
    let filter = Selection::default().and(Clause::ServiceName(ValuePredicate::In(vec![
        "alpha".into(),
        "beta".into(),
    ])));
    assert_eq!(
        scan_keys(&store, scope, &filter).await.unwrap().len(),
        ROWS.iter()
            .filter(|row| matches!(row.0, "alpha" | "beta"))
            .count()
    );
    assert!(scan_keys(&store, scope, &Selection::All).await.is_err());

    // A malformed leading field lies after the first valid gap key (alphabet).
    // The real stat scan succeeds only if it seeks past the gap, rather than
    // visiting and decoding every key between alpha and beta.
    let fixed = prefix(scope);
    let mut malformed = fixed.to_vec();
    malformed.extend_from_slice(encoded_mem_cmp_str!("alphax").as_bytes());
    malformed.pop(); // remove the string's terminal marker
    let mut tx = store.transaction();
    tx.raw_put_cf(KeyKind::RegisteredStats, malformed, b"malformed");
    tx.commit().await.unwrap();
    drop(tx);

    let filter = filter.and(Clause::ServiceName(ValuePredicate::In(vec![
        "beta".into(),
        "alphax".into(),
        "alpha".into(),
    ])));
    let expected = scan_keys(&store, scope, &filter).await.unwrap();
    assert_eq!(expected.len(), 9);

    // Observe actual RocksDB visits through the controlled adapter. Only the
    // first gap key is visited; the next visit must be beta.
    let prepared = ServiceStatsKey::prepare_selection(&filter).unwrap();
    let mut cursor = prepared.into_cursor(&fixed).unwrap().unwrap();
    let scan = cursor.scan().clone();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    store
        .iterator_controlled_physical(
            "test-stat-seeks",
            Priority::Low,
            ReadOptions::default(),
            scan.clone(),
            move |(key, _)| {
                sender.send(key.to_vec()).unwrap();
                match crate::break_on_err(cursor.evaluate(key))? {
                    KeyMatch::Match => ControlFlow::Continue(IterAction::Next),
                    KeyMatch::Skip => panic!("fixed_header_uses_ordered_seeks"),
                    KeyMatch::Done => ControlFlow::Break(Ok(())),
                    KeyMatch::Seek(target) => ControlFlow::Continue(IterAction::Seek(target)),
                }
            },
        )
        .unwrap()
        .await
        .unwrap();
    let mut visited = Vec::new();
    while let Some(key) = receiver.recv().await {
        visited.push(key);
    }
    let mut expected_visits = expected;
    expected_visits.push(encoded_key(scope, &ROWS[8])); // alphabet
    expected_visits.sort();
    assert_eq!(visited, expected_visits);

    // The adapter rejects equal/backward targets and the exclusive upper bound.
    let PhysicalScan::RangeExclusive(_, _, _, upper) = &scan else {
        panic!("expected range")
    };
    for target in [visited[0].clone(), fixed.to_vec(), upper.to_vec()] {
        let error = store
            .iterator_controlled_physical(
                "test-invalid-seek",
                Priority::Low,
                ReadOptions::default(),
                scan.clone(),
                move |_| ControlFlow::Continue(IterAction::Seek(target.clone().into())),
            )
            .unwrap()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("scan seek must advance"));
    }
}
