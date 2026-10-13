// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::array::LargeStringArray;
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;

use restate_limiter::LimitKey;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::WriteVQueueTable;
use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
use restate_types::clock::UniqueTimestamp;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::VQueueId;

use crate::mocks::*;

fn meta() -> VQueueMeta {
    let created_at =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_000)).unwrap();
    VQueueMeta::new(created_at, None, LimitKey::None, VQueueLink::None)
}

async fn select_ids(engine: &mut MockQueryEngine, query: &str) -> Vec<String> {
    let records = engine
        .execute(query.to_owned())
        .await
        .unwrap()
        .stream
        .collect::<Vec<datafusion::common::Result<RecordBatch>>>()
        .await
        .remove(0)
        .unwrap();

    let mut ids: Vec<String> = records
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .iter()
        .flatten()
        .map(str::to_string)
        .collect();
    ids.sort();
    ids
}

/// `id IN (...)` is served via the batched multi-get fast path. Only the listed
/// vqueues must come back, even though several other metadata rows share the
/// same partition.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn vqueue_meta_point_query_returns_only_matching_ids() {
    let mut engine = MockQueryEngine::create().await;

    let qids: Vec<VQueueId> = (0..4)
        .map(|i| VQueueId::custom(3337, format!("q{i}")))
        .collect();

    let mut tx = engine.partition_store().transaction();
    for qid in &qids {
        tx.create_vqueue(qid, &meta());
    }
    tx.commit().await.unwrap();
    drop(tx);

    let got = select_ids(
        &mut engine,
        &format!(
            "SELECT id FROM sys_vqueue_meta WHERE id IN ('{}', '{}')",
            qids[0], qids[2]
        ),
    )
    .await;

    let mut expected = vec![qids[0].to_string(), qids[2].to_string()];
    expected.sort();
    assert_eq!(got, expected);
}

/// `id NOT IN (> 3 values)` must fall back to a full partition-key-range scan
/// (the negated list can't become a lookup set) and return the non-excluded
/// rows. Four values is the smallest list that survives as a negated
/// `InListExpr`.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn vqueue_meta_not_in_returns_non_excluded_rows() {
    let mut engine = MockQueryEngine::create().await;

    let qids: Vec<VQueueId> = (0..6)
        .map(|i| VQueueId::custom(3337, format!("q{i}")))
        .collect();

    let mut tx = engine.partition_store().transaction();
    for qid in &qids {
        tx.create_vqueue(qid, &meta());
    }
    tx.commit().await.unwrap();
    drop(tx);

    let excluded = qids[0..4]
        .iter()
        .map(|qid| format!("'{qid}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let got = select_ids(
        &mut engine,
        &format!("SELECT id FROM sys_vqueue_meta WHERE id NOT IN ({excluded})"),
    )
    .await;

    let mut expected = vec![qids[4].to_string(), qids[5].to_string()];
    expected.sort();
    assert_eq!(got, expected);
}
