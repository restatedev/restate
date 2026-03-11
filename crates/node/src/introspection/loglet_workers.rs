// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Local scanner implementation for the `loglet_workers` DataFusion table.
//!
//! This scanner queries active loglet workers on-demand via their introspection
//! channels and joins the responses with cached loglet metadata from the
//! [`LogletStateMap`].

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;

use restate_core::Metadata;
use restate_log_server::{ActiveWorkerMap, LogletState, LogletStateMap, LogletWorkerState};
use restate_storage_query_datafusion::Scan;
use restate_storage_query_datafusion::loglet_worker::LogletWorkersBuilder;
use restate_storage_query_datafusion::table_util::Builder;
use restate_types::GenerationalNodeId;
use restate_types::logs::LogletId;

/// Creates a local scanner for `loglet_workers` from log-server in-memory state.
///
/// This scanner queries active workers on-demand via their introspection
/// channels and joins the responses with cached loglet metadata.
pub(crate) fn create_local_scanner(
    state_map: LogletStateMap,
    active_worker_map: ActiveWorkerMap,
    metadata: Metadata,
) -> Arc<dyn Scan> {
    Arc::new(LogletWorkersScanner {
        state_map,
        active_worker_map,
        metadata,
    })
}

struct LogletWorkersScanner {
    state_map: LogletStateMap,
    active_worker_map: ActiveWorkerMap,
    metadata: Metadata,
}

impl Debug for LogletWorkersScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LogletWorkersScanner")
    }
}

impl Scan for LogletWorkersScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        _batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let state_map = self.state_map.clone();
        let active_worker_map = self.active_worker_map.clone();
        let my_node_id = self.metadata.my_node_id();
        let schema = projection.clone();

        let fut = async move {
            // Query all active workers for their live state (on-demand).
            let worker_states = active_worker_map.get_all_worker_states().await;
            // Get cached loglet metadata.
            let loglet_states = state_map.snapshot().await;

            let mut builder = LogletWorkersBuilder::new(schema.clone());

            for (count, (loglet_id, loglet_state)) in loglet_states.iter().enumerate() {
                if limit.is_some_and(|l| count >= l) {
                    break;
                }

                let worker_state = worker_states.get(loglet_id);
                append_loglet_worker_row(
                    &mut builder,
                    my_node_id,
                    *loglet_id,
                    loglet_state,
                    worker_state,
                );
            }

            builder.finish()
        };

        Box::pin(RecordBatchStreamAdapter::new(projection, stream::once(fut)))
    }
}

fn append_loglet_worker_row(
    builder: &mut LogletWorkersBuilder,
    node_id: GenerationalNodeId,
    loglet_id: LogletId,
    loglet_state: &LogletState,
    worker_state: Option<&LogletWorkerState>,
) {
    let mut row = builder.row();

    row.fmt_plain_node_id(node_id.as_plain());
    row.fmt_gen_node_id(node_id);
    row.fmt_loglet_id(loglet_id);
    if row.is_log_id_defined() {
        row.log_id(u32::from(loglet_id.log_id()));
    }
    if row.is_segment_index_defined() {
        row.segment_index(u32::from(loglet_id.segment_index()));
    }

    // Fields from LogletState (always available)
    let tail = loglet_state.local_tail();
    row.local_tail(*tail.offset());
    row.is_sealed(tail.is_sealed());
    row.trim_point(u32::from(loglet_state.trim_point()));
    row.known_global_tail(u32::from(loglet_state.known_global_tail()));

    if let Some(seq) = loglet_state.sequencer()
        && row.is_sequencer_defined()
    {
        row.fmt_sequencer(seq);
    }

    // Fields from active worker state
    let is_active = worker_state.is_some();
    row.is_active(is_active);

    if let Some(ws) = worker_state {
        row.staging_local_tail(u32::from(ws.staging_local_tail));
        row.seal_enqueued(ws.seal_enqueued);
        row.accepting_writes(ws.accepting_writes);
        row.pending_stores(ws.pending_stores);
        row.pending_repair_stores(ws.pending_repair_stores);
        row.pending_seals(ws.pending_seals);
        row.pending_trims(ws.pending_trims);
        row.pending_tail_waiters(ws.pending_tail_waiters);
        row.last_request_at(ws.last_request_at.as_u64() as i64);
    }
    // row is dropped here, which appends null for any unset projected columns
}
