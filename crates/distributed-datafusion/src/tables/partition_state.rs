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

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use tokio::sync::watch;

use restate_storage_query_api::tables::partition_state::PartitionState;
use restate_types::GenerationalNodeId;
use restate_types::cluster::cluster_state::{LegacyClusterState, PartitionProcessorStatus};
use restate_types::identifiers::PartitionId;
use restate_util_string::ReString;

use crate::table_providers::{GenericTableProvider, Scan};

crate::tables::table_adapter!(pub(crate) PartitionStateTable, PartitionStateBuilder, PartitionState);

impl PartitionStateTable {
    pub(crate) fn create_provider(
        watch: Option<watch::Receiver<Arc<LegacyClusterState>>>,
    ) -> Arc<dyn TableProvider> {
        Arc::new(GenericTableProvider::declared::<Self>(
            Some(Arc::new(PartitionStateScanner { watch })),
            None,
        ))
    }
}

#[derive(Debug)]
struct PartitionStateScanner {
    watch: Option<watch::Receiver<Arc<LegacyClusterState>>>,
}

impl Scan for PartitionStateScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let mut stream = RecordBatchReceiverStream::builder(projection.clone(), 2);
        let tx = stream.tx();
        let state = self.watch.as_ref().map(|watch| watch.borrow().clone());
        stream.spawn(async move {
            // As in V1, a coordinator without a watcher exposes an empty table.
            let Some(state) = state else { return Ok(()) };
            let mut builder = PartitionStateBuilder::new(projection);
            let mut remaining = limit.unwrap_or(usize::MAX);
            for node in state.alive_nodes() {
                for (partition, status) in &node.partitions {
                    if remaining == 0 {
                        break;
                    }
                    append_partition_row(
                        &mut builder,
                        node.generational_node_id,
                        *partition,
                        status,
                    );
                    remaining -= 1;
                    if builder.num_rows() >= batch_size && tx.send(builder.finish()).await.is_err()
                    {
                        return Ok(());
                    }
                }
                if remaining == 0 {
                    break;
                }
            }
            if builder.num_rows() > 0 {
                let _ = tx.send(builder.into_batch()).await;
            }
            Ok(())
        });
        stream.build()
    }
}

fn append_partition_row(
    builder: &mut PartitionStateBuilder,
    node_id: GenerationalNodeId,
    partition_id: PartitionId,
    state: &PartitionProcessorStatus,
) {
    let mut row = builder.row();
    row.partition_id(u32::from(partition_id));
    row.fmt_plain_node_id(node_id.as_plain());
    row.fmt_gen_node_id(node_id);
    row.fmt_target_mode(state.planned_mode);
    row.fmt_effective_mode(state.effective_mode());
    row.updated_at(state.updated_at.as_u64() as i64);
    if let Some(epoch) = state.last_observed_leader_epoch {
        row.leader_epoch(u64::from(epoch));
    }
    if let Some(leader) = &state.last_observed_leader_node {
        row.fmt_leader(leader);
    }
    if let Some(lsn) = state.last_applied_log_lsn {
        row.applied_log_lsn(u64::from(lsn));
    }
    if let Some(ts) = state.last_record_applied_at {
        row.last_record_applied_at(ts.as_u64() as i64);
    }
    if state.is_broken() {
        row.fmt_broken_reason(state.broken_reason);
    } else {
        row.fmt_replay_status(state.replay_status);
    }
    if let Some(lsn) = state.durable_lsn {
        row.durable_log_lsn(u64::from(lsn));
    }
    if let Some(lsn) = state.last_archived_log_lsn {
        row.archived_log_lsn(u64::from(lsn));
    }
    if let Some(lsn) = state.target_tail_lsn {
        row.target_tail_lsn(u64::from(lsn));
    }
    if let Some(version) = state.last_applied_rule_book_version {
        row.applied_rule_book_version(u32::from(version));
    }
    if let Some(version) = state.last_applied_schema_version {
        row.applied_schema_version(u32::from(version));
    }
    if let Some(lsn) = state.byte_offsets_base_lsn {
        row.byte_offsets_base_lsn(u64::from(lsn));
    }
    if let Some(offset) = state.log_byte_offset {
        row.log_byte_offset(offset);
    }
    if let Some(offset) = state.partition_store_byte_offset {
        row.partition_store_byte_offset(offset);
    }
    if row.is_enabled_features_defined() {
        row.enabled_features(
            state
                .enabled_features
                .enabled_names()
                .map(|name| Some(ReString::from_static(name)))
                .collect(),
        );
    }
    if row.is_enabled_storage_features_defined() {
        row.enabled_storage_features(
            state
                .enabled_storage_features
                .iter()
                .cloned()
                .map(Some)
                .collect(),
        );
    }
}
