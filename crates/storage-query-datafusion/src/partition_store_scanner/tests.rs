// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! V1 scan edge cases: reaching the LIMIT skips the next layout, an error
//! discards rows not sent yet, and a scan waiting for a slow query leaves its IO
//! thread free for other scans.

use std::num::NonZeroUsize;

use bytes::Bytes;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, col, lit};
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};

use restate_partition_store::journal_table_v2::JournalKey;
use restate_partition_store::keys::selection::KeySelection;
use restate_partition_store::keys::{EncodeTableKey, KeyKind};
use restate_partition_store::state_table::{StateKey, StateTable};
use restate_rocksdb::RocksDbManager;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_api::Transaction;
use restate_storage_api::journal_table::{JournalEntry, WriteJournalTable};
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::journal::{Entry, InputEntry};
use restate_types::partitions::Partition;

use crate::journal::schema::{SysJournalBuilder, SysJournalTable};
use crate::state::schema::StateBuilder;

use super::*;

#[restate_core::test]
async fn journal_stops_before_the_next_layout_and_flushes_only_success() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let io = QueryIoExecutor::new(NonZeroUsize::new(1).unwrap());
    let scanner = SysJournalTable::create_local_scanner(manager, io);
    let invocation = InvocationId::mock_random();
    let mut corrupt = Vec::new();
    JournalKey {
        partition_key: invocation.partition_key(),
        invocation_uuid: invocation.invocation_uuid(),
        journal_index: 0,
    }
    .serialize_to(&mut corrupt);
    let mut tx = store.transaction();
    tx.put_journal_entry(
        &invocation,
        0,
        &JournalEntry::Entry(ProtobufRawEntryCodec::serialize_enriched(Entry::Input(
            InputEntry {
                headers: vec![],
                value: Bytes::new(),
            },
        ))),
    )
    .unwrap();
    tx.raw_put_cf(KeyKind::JournalV2, &corrupt, b"\xff");
    tx.commit().await.unwrap();
    drop(tx);

    let scan = |batch_size, limit| {
        scanner
            .scan_partition(
                PartitionId::MIN,
                KeyRange::FULL,
                SysJournalBuilder::schema(),
                None,
                batch_size,
                limit,
                Time::new(),
            )
            .unwrap()
    };
    let zero = scan(1, Some(0)).collect::<Vec<_>>().await;
    assert!(zero.is_empty());
    let limited = scan(1, Some(1)).collect::<Vec<_>>().await;
    assert_eq!(
        limited.len(),
        1,
        "LIMIT must not read the corrupt second layout"
    );
    assert_eq!(limited[0].as_ref().unwrap().num_rows(), 1);

    // The first layout's one row is buffered. Failing in the second layout must
    // report the error once and discard that row, rather than flushing on Drop.
    let failed = scan(2, None).collect::<Vec<_>>().await;
    assert_eq!(failed.len(), 1);
    assert!(failed[0].is_err());
    let mut tx = store.transaction();
    tx.raw_delete_cf(KeyKind::JournalV2, &corrupt);
    tx.commit().await.unwrap();
    drop(tx);
    let healthy = scan(2, None).collect::<Vec<_>>().await;
    assert_eq!(
        healthy.len(),
        1,
        "successful exhaustion flushes the partial batch"
    );
    assert_eq!(healthy[0].as_ref().unwrap().num_rows(), 1);

    // A SQL filter that fails on a row reports its own error, not a storage
    // error: `CAST('Input' AS BIGINT)` can't succeed. With a batch size of one,
    // the filter fails while the scan reads; with two, on the final flush.
    let schema = SysJournalBuilder::schema();
    let entry_type = col("entry_type", &schema).unwrap();
    let failing: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(CastExpr::new(entry_type, DataType::Int64, None)),
        Operator::Eq,
        lit(1i64),
    ));
    for batch_size in [1, 2] {
        let failed = scanner
            .scan_partition(
                PartitionId::MIN,
                KeyRange::FULL,
                Arc::clone(&schema),
                Some(ScanPredicate::fixed(Arc::clone(&failing))),
                batch_size,
                None,
                Time::new(),
            )
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let error = failed
            .into_iter()
            .find_map(Result::err)
            .unwrap()
            .to_string();
        assert!(
            error.contains("Cast error") && !error.contains("generic storage error"),
            "batch size {batch_size}: {error}"
        );
    }

    // Contract test for initial vs live expressions: even replacing the entire
    // live expression with false must keep the initial ID selection for storage,
    // avoiding this unrelated corrupt record. Today's client preserves static
    // conjuncts in its updates; this exercises the stronger scan contract.
    corrupt.clear();
    JournalKey {
        partition_key: invocation.partition_key().wrapping_add(1),
        invocation_uuid: invocation.invocation_uuid(),
        journal_index: 0,
    }
    .serialize_to(&mut corrupt);
    let mut tx = store.transaction();
    tx.raw_put_cf(KeyKind::JournalV2, &corrupt, b"\xff");
    tx.commit().await.unwrap();
    drop(tx);
    let initial: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        col("id", &schema).unwrap(),
        Operator::Eq,
        lit(ScalarValue::LargeUtf8(Some(invocation.to_string()))),
    ));
    let (predicate, live) = ScanPredicate::updatable(Arc::clone(&initial));
    let whole_expression: Arc<dyn PhysicalExpr> =
        Arc::new(BinaryExpr::new(initial, Operator::And, lit(false)));
    for (next, expected_rows) in [
        (None, 1),
        (Some(whole_expression), 0),
        (Some(lit(false)), 0),
    ] {
        if let Some(next) = next {
            live.update(next).unwrap();
        }
        let batches = scanner
            .scan_partition(
                PartitionId::MIN,
                KeyRange::FULL,
                Arc::clone(&schema),
                Some(predicate.clone()),
                1,
                None,
                Time::new(),
            )
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let rows: usize = batches
            .into_iter()
            .map(|batch| batch.unwrap().num_rows())
            .sum();
        assert_eq!(rows, expected_rows);
    }
}

#[restate_core::test]
async fn blocked_batch_output_releases_the_shared_worker_and_cancels_cleanly() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let mut tx = store.transaction();
    for name in [b"a", b"b"] {
        let mut key = Vec::new();
        StateKey {
            partition_key: 9,
            service_name: "svc".into(),
            service_key: "object".into(),
            state_key: Bytes::copy_from_slice(name),
        }
        .serialize_to(&mut key);
        tx.raw_put_cf(KeyKind::State, key, b"value");
    }
    tx.commit().await.unwrap();
    drop(tx);
    let (snapshot, closed) = store.query_read(CancellationToken::new()).unwrap();
    let io = QueryIoExecutor::new(NonZeroUsize::new(1).unwrap());
    let (tx, mut rx) = mpsc::channel(1);
    let rows = Rows {
        output: BatchSender::<StateBuilder>::new(StateBuilder::schema(), tx, None, 1, None),
        elapsed: ElapsedCompute::new(Time::new()),
    };
    let (reached, blocked) = oneshot::channel();
    let mut reached = Some(reached);
    let mut seen = 0;
    let append = |builder: &mut StateBuilder, (key, value): <StateTable as Table>::Record<'_>| {
        let mut row = builder.row();
        row.partition_key(key.decode_all()?.partition_key);
        row.value(value);
        Ok(())
    };
    let mut read = Box::pin(rows.read::<StateTable>(
        &snapshot,
        &io,
        "blocked-output-test",
        Arc::new(KeySelection::all()),
        move |builder, record| {
            append(builder, record)?;
            seen += 1;
            if seen == 2 {
                reached.take().unwrap().send(()).unwrap();
            }
            Ok(())
        },
    ));
    tokio::select! {
        _ = &mut read => panic!("the second batch has no output room"),
        result = blocked => result.unwrap(),
    }
    let (other_tx, mut other_rx) = mpsc::channel(1);
    let other = Rows {
        output: BatchSender::<StateBuilder>::new(
            StateBuilder::schema(),
            other_tx,
            None,
            1,
            Some(1),
        ),
        elapsed: ElapsedCompute::new(Time::new()),
    };
    assert!(matches!(
        other
            .read::<StateTable>(
                &snapshot,
                &io,
                "other-output-test",
                Arc::new(KeySelection::all()),
                append
            )
            .await
            .unwrap(),
        ScanOutcome::Stopped(_)
    ));
    assert_eq!(other_rx.recv().await.unwrap().unwrap().num_rows(), 1);
    drop(read);
    drop(snapshot);
    closed.await;
    assert_eq!(rx.recv().await.unwrap().unwrap().num_rows(), 1);
    assert!(
        rx.recv().await.is_none(),
        "cancellation discards the blocked batch"
    );
}
