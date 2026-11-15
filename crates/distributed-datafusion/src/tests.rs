// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::DataFusionError;
use futures::TryStreamExt;

use restate_clock::MockClock;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryKind, EntryMetadata, EntryStateRef, Stage, Status,
    WriteVQueueTable,
};
use restate_storage_query_api::errors::QueryExecutionError;
use restate_types::clock::UniqueTimestamp;
use restate_types::config::QueryEngineOptions;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{EntryId, EntryTargetRef, VQueueId};

use crate::mocks::MockQueryEngine;
use crate::{DataFusionEnv, MetadataTables};

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn offline_storage_and_optional_service_metadata_are_independent() {
    let env = DataFusionEnv::from_options(&QueryEngineOptions::default())
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    let mut engine = MockQueryEngine::create_with_env(env.clone()).await;
    let qid = VQueueId::custom(3337, "offline-entry");
    let now = MillisSinceEpoch::new(1_744_010_000_000);
    let key = EntryKey::new(false, now, 1, EntryId::new(EntryKind::Invocation, [1; 16]));
    let mut tx = engine.partition_store().transaction();
    tx.create_vqueue_entry_status(
        &EntryContext {
            qid: &qid,
            target: &EntryTargetRef::Service {
                scope: None,
                service: "test",
                handler: "handler",
            },
        },
        EntryStateRef {
            stage: Stage::Running,
            status: Status::Started,
            entry_key: &key,
            metadata: &EntryMetadata::default(),
            stats: &EntryStatistics::new(
                UniqueTimestamp::try_from_unix_millis(now).unwrap(),
                key.run_at(),
            ),
        },
    );
    tx.commit().await.unwrap();
    drop(tx);
    let batches: Vec<_> = engine
        .execute("SELECT entry_id FROM sys_vqueue_entry_status")
        .await
        .unwrap()
        .stream
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(
        array_value_to_string(batches[0].column(0), 0).unwrap(),
        key.entry_id().display(qid.partition_key()).to_string()
    );

    let batches: Vec<_> = engine
        .execute("SELECT COUNT(*) FROM sys_scheduler")
        .await
        .unwrap()
        .stream
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        array_value_to_string(batches[0].column(0), 0).unwrap(),
        "0",
        "offline_live_data_is_explicitly_empty"
    );

    let QueryExecutionError::DataFusion(error) = engine
        .execute("SELECT * FROM sys_service")
        .await
        .err()
        .expect("metadata must be absent offline");
    assert!(
        matches!(error.find_root(), DataFusionError::Plan(message)
        if message.contains("sys_service") && message.contains("not found")),
        "{error}"
    );

    env.register_tables(MetadataTables::new(engine.sources().clone()))
        .await
        .unwrap();
    let batches: Vec<_> = engine
        .execute("SELECT * FROM sys_service")
        .await
        .unwrap()
        .stream
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
}
