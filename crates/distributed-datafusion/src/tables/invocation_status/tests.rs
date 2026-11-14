// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::prelude::{SessionContext, col, lit};
use futures::TryStreamExt;
use tokio_util::sync::CancellationToken;

use restate_partition_store::PartitionStoreManager;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, InboxedInvocation, InvocationStatus,
    PreFlightInvocationMetadata, ScheduledInvocation, WriteInvocationStatusTable,
};
use restate_storage_query_api::tables::invocation_status::InvocationStatus as InvocationStatusSchema;
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionId};
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;
use restate_worker_api::CapturedPartitionRead;

use crate::access::PrimaryRead;
use crate::mocks::{differential_sessions, rows};
use crate::table_providers::{PartitionReadContext, ScanPartition};

use super::StatusScanner;

#[restate_core::test]
async fn persisted_invocations_match_v1_for_scans_lookups_and_projections() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let mut tx = store.transaction();
    let ids: Vec<_> = (0..32)
        .map(|index| InvocationId::from_parts(index, InvocationUuid::from_u128(index.into())))
        .collect();
    for (index, id) in ids.iter().enumerate() {
        let status = match index % 6 {
            0 => InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
            1 => InvocationStatus::Completed(CompletedInvocation::mock_neo()),
            2 => InvocationStatus::Paused(InFlightInvocationMetadata::mock()),
            3 => InvocationStatus::Suspended {
                metadata: InFlightInvocationMetadata::mock(),
                awaiting_on: restate_types::journal_v2::UnresolvedFuture::unknown_from_iter([
                    restate_types::journal_v2::NotificationId::for_completion(1),
                    restate_types::journal_v2::NotificationId::SignalIndex(10),
                ]),
            },
            4 => InvocationStatus::Scheduled(ScheduledInvocation {
                metadata: PreFlightInvocationMetadata::mock(),
            }),
            _ => InvocationStatus::Inboxed(InboxedInvocation {
                inbox_sequence_number: 1,
                metadata: PreFlightInvocationMetadata::mock(),
            }),
        };
        tx.put_invocation_status(id, &status).unwrap();
    }
    tx.commit().await.unwrap();
    drop(tx);
    let [old, new] = differential_sessions(&manager).await;
    let mut keys = ids.iter().map(|id| format!("'{id}'")).collect::<Vec<_>>();
    keys.push(format!(
        "'{}'",
        InvocationId::from_parts(100, InvocationUuid::from_u128(100))
    ));
    for sql in [
        "SELECT * FROM sys_invocation_status ORDER BY id".to_owned(),
        "SELECT COUNT(*) FROM sys_invocation_status".into(),
        "SELECT * FROM sys_invocation ORDER BY id".into(),
        "SELECT status, COUNT(*) FROM sys_invocation GROUP BY status ORDER BY status".into(),
        format!("SELECT * FROM sys_invocation_status WHERE id IN ({}) ORDER BY id", keys.join(",")),
        format!("SELECT id, status FROM sys_invocation_status WHERE id = '{}'", ids[5]),
        "SELECT id, status FROM sys_invocation_status WHERE partition_key >= 3 ORDER BY id LIMIT 3 OFFSET 2".into(),
        "SELECT scheduled_start_at FROM sys_invocation_status ORDER BY id".into(),
    ] {
        assert_eq!(rows(new.as_ref(), &sql).await, rows(old.as_ref(), &sql).await, "{sql}");
    }
    assert_eq!(
        rows(new.as_ref(), "SELECT COUNT(*) FROM sys_invocation_status").await,
        [["32"]]
    );
    assert_eq!(
        rows(
            new.as_ref(),
            "SELECT COUNT(*) FROM sys_invocation_status WHERE id = 'invalid'"
        )
        .await,
        [["0"]]
    );

    // SQL routes literal IDs to multi-get; the scan path navigates the same
    // restriction by key. Missing IDs and limits hold on that path too.
    let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
    let context = PartitionReadContext::from_capture(CapturedPartitionRead {
        partition: PartitionId::MIN,
        storage: Some(snapshot),
        live: Default::default(),
    })
    .with_query_io(crate::mocks::query_io());
    let schema = InvocationStatusSchema::arrow_schema();
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
    let missing = InvocationId::from_parts(4, InvocationUuid::from_u128(100));
    let filter = col("id").in_list(
        [ids[3], ids[5], missing]
            .iter()
            .map(|id| lit(ScalarValue::LargeUtf8(Some(id.to_string()))))
            .collect(),
        false,
    );
    let predicate = SessionContext::new()
        .create_physical_expr(filter, &df_schema)
        .unwrap();
    for (limit, expected) in [(None, vec![ids[3], ids[5]]), (Some(1), vec![ids[3]])] {
        let batches = Arc::new(StatusScanner)
            .prepare_read(
                PrimaryRead::Scan,
                schema.clone(),
                Some(Arc::clone(&predicate)),
                limit,
            )
            .unwrap()
            .execute(context.clone(), 8, Default::default())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let found: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch.column_by_name("id").unwrap();
                (0..batch.num_rows()).map(|row| array_value_to_string(column, row).unwrap())
            })
            .collect();
        let expected: Vec<_> = expected.iter().map(ToString::to_string).collect();
        assert_eq!(found, expected, "scan_path_ids_with_limit_{limit:?}");
    }
}
