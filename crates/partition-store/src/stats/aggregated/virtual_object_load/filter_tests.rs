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

use rocksdb::ReadOptions;

use restate_rocksdb::{IterAction, Priority, RocksDbManager};
use restate_storage_api::Transaction;
use restate_storage_api::filter::{Filter, ValuePredicate};
use restate_storage_api::stats::virtual_object_load::VirtualObjectLoadClause as Clause;
use restate_storage_api::vqueue_table::Stage;
use restate_types::partition_table::Partition;
use restate_types::sharding::{KeyRange, PartitionId};
use restate_types::vqueues::EntryKind;

use crate::PartitionStoreManager;
use crate::keys::filter::KeyMatch;
use crate::scan::PhysicalScan;
use crate::stats::Stat;
use crate::stats::aggregated::AggregatedStatsMut;

use super::{VirtualObjectLoad, VirtualObjectLoadKey};

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn compound_bounds_skip_keys_and_carry_between_services() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let partition = PartitionId::MIN;
    let mut store = manager
        .open(&Partition::new(partition, KeyRange::FULL), None)
        .await
        .unwrap();
    let mut tx = store.transaction();
    for service in ["A", "B", "LargeState", "Z"] {
        // Key comparisons are lexical: "10000" is below "9999".
        for key in ["0001", "10000", "9998", "9999", "9999a", "z"] {
            AggregatedStatsMut::new(&mut tx).increment_stage::<VirtualObjectLoad, _>(
                VirtualObjectLoadKey::borrowed(
                    service,
                    None::<&str>,
                    key,
                    None::<&str>,
                    EntryKind::Invocation,
                    3337,
                ),
                Stage::Inbox,
            );
        }
    }
    tx.commit().await.unwrap();
    drop(tx);

    let encode = |service: &str, key: &str| {
        let mut bytes = Vec::new();
        VirtualObjectLoad::encode_key(
            partition,
            VirtualObjectLoadKey::borrowed(
                service,
                None::<&str>,
                key,
                None::<&str>,
                EntryKind::Invocation,
                3337,
            ),
            &mut bytes,
        );
        bytes
    };
    for (finite_parent, bounded_child, fixed_scope, expected_visits) in [
        (
            true,
            false,
            true,
            vec![
                ("A", "9999a"),
                ("A", "z"),
                ("B", "0001"),
                ("LargeState", "9999a"),
                ("LargeState", "z"),
            ],
        ),
        (
            false,
            false,
            true,
            vec![
                ("A", "9999a"),
                ("A", "z"),
                ("B", "0001"),
                ("B", "9999a"),
                ("B", "z"),
                ("LargeState", "0001"),
                ("LargeState", "9999a"),
                ("LargeState", "z"),
            ],
        ),
        (
            true,
            true,
            true,
            vec![
                ("A", "9999a"),
                ("A", "z"),
                ("LargeState", "9999a"),
                ("LargeState", "z"),
            ],
        ),
        (
            true,
            false,
            false,
            vec![
                ("A", "0001"),
                ("A", "9999a"),
                ("A", "z"),
                ("B", "0001"),
                ("LargeState", "0001"),
                ("LargeState", "9999a"),
                ("LargeState", "z"),
            ],
        ),
        (
            true,
            true,
            false,
            vec![
                ("A", "0001"),
                ("A", "9999a"),
                ("A", "z"),
                ("B", "0001"),
                ("LargeState", "0001"),
                ("LargeState", "9999a"),
                ("LargeState", "z"),
            ],
        ),
    ] {
        let services = if finite_parent {
            ValuePredicate::In(vec!["A".into(), "LargeState".into()])
        } else {
            ValuePredicate::Range {
                lower: Included("A".into()),
                upper: Included("LargeState".into()),
            }
        };
        let mut filter = Filter::default()
            .and(Clause::ServiceName(services))
            .and(Clause::Key(ValuePredicate::Range {
                lower: Excluded("9999".into()),
                upper: if bounded_child {
                    Included("9999a".into())
                } else {
                    Unbounded
                },
            }));
        if fixed_scope {
            filter = filter.and(Clause::Scope(ValuePredicate::Equal(None)));
        }
        let expected: Vec<_> = ["A", "B", "LargeState"]
            .into_iter()
            .filter(|service| !finite_parent || *service != "B")
            .flat_map(|service| {
                ["9999a", "z"]
                    .into_iter()
                    .filter(move |key| !bounded_child || *key == "9999a")
                    .map(move |key| encode(service, key))
            })
            .collect();

        // Exercise the public stats path, including value decoding and callbacks.
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        store
            .scan_virtual_object_load(&filter, move |key, _| {
                let key = key.try_full_decode::<VirtualObjectLoad>().unwrap();
                let mut bytes = Vec::new();
                VirtualObjectLoad::encode_key(partition, key, &mut bytes);
                sender.send(bytes).unwrap();
                ControlFlow::Continue(())
            })
            .unwrap()
            .await
            .unwrap();
        let mut actual = Vec::new();
        while let Some(key) = receiver.recv().await {
            actual.push(key);
        }
        assert_eq!(actual, expected);

        // Observe raw RocksDB visits: with a fixed scope, the initial bound and
        // finite parent jumps include the key's lower bound. Unconstrained scopes
        // and continuous parents need refinement after discovering their value.
        let mut fixed = Vec::new();
        VirtualObjectLoadKey::prefix(partition, &mut fixed);
        let mut cursor = VirtualObjectLoadKey::prepare_filter(&filter)
            .unwrap()
            .into_cursor(&fixed)
            .unwrap()
            .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        store
            .iterator_controlled_physical(
                "test-compound-seeks",
                Priority::Low,
                ReadOptions::default(),
                cursor.scan().clone(),
                move |(key, _)| {
                    sender.send(key.to_vec()).unwrap();
                    match crate::break_on_err(cursor.evaluate(key))? {
                        KeyMatch::Match => ControlFlow::Continue(IterAction::Next),
                        KeyMatch::Seek(target) => ControlFlow::Continue(IterAction::Seek(target)),
                        KeyMatch::Done => ControlFlow::Break(Ok(())),
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
        assert_eq!(
            visited,
            expected_visits
                .into_iter()
                .map(|(service, key)| encode(service, key))
                .collect::<Vec<_>>(),
            "finite_parent={finite_parent}, bounded_child={bounded_child}, fixed_scope={fixed_scope}"
        );
    }

    // LIKE 'Large%' AND scope IS NULL AND key > '9': RocksDB stops before Largf,
    // without relying on cursor filtering or early termination in the callback.
    let keys = ["0001", "10000", "9998", "9999", "9999a", "z"];
    let mut tx = store.transaction();
    for service in ["Large", "Largf"] {
        for key in keys {
            AggregatedStatsMut::new(&mut tx).increment_stage::<VirtualObjectLoad, _>(
                VirtualObjectLoadKey::borrowed(
                    service,
                    None::<&str>,
                    key,
                    None::<&str>,
                    EntryKind::Invocation,
                    3337,
                ),
                Stage::Inbox,
            );
        }
    }
    tx.commit().await.unwrap();
    drop(tx);
    let filter = Filter::default()
        .and(Clause::ServiceNameStartsWith("Large".into()))
        .and(Clause::Scope(ValuePredicate::Equal(None)))
        .and(Clause::Key(ValuePredicate::Range {
            lower: Excluded("9".into()),
            upper: Unbounded,
        }));
    let mut fixed = Vec::new();
    VirtualObjectLoadKey::prefix(partition, &mut fixed);
    let scan = VirtualObjectLoadKey::prepare_filter(&filter)
        .unwrap()
        .scan(&fixed)
        .unwrap()
        .unwrap();
    let PhysicalScan::RangeExclusive(_, _, lower, upper) = &scan else {
        panic!("expected bounded prefix range");
    };
    let mut expected_lower = Vec::new();
    VirtualObjectLoadKey::prefix(partition, &mut expected_lower)
        .service_name("Large")
        .scope(None::<&str>)
        .key("9");
    // The terminal marker is incremented to exclude the exact key's suffix group.
    *expected_lower.last_mut().unwrap() += 1;
    assert_eq!(lower.as_ref(), expected_lower);
    let mut expected_upper = fixed;
    expected_upper.extend_from_slice(b"Largf");
    assert_eq!(upper.as_ref(), expected_upper);

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    store
        .iterator_for_each_physical(
            "test-prefix-upper-bound",
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
    let expected: Vec<_> = ["Large", "LargeState"]
        .into_iter()
        .flat_map(|service| {
            keys.into_iter()
                .filter(move |key| service != "Large" || *key > "9")
                .map(move |key| encode(service, key))
        })
        .collect();
    assert_eq!(visited, expected);
}
