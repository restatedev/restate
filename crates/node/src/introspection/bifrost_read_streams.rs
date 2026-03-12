// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Local scanner implementation for the `bifrost_read_streams` DataFusion table.
//!
//! This scanner reads the [`ActiveReadStreamRegistry`] snapshot from the local
//! bifrost instance and produces Arrow record batches for fan-out SQL queries.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;

use restate_bifrost::read_stream_registry::{ActiveReadStreamRegistry, ReadStreamState};
use restate_core::Metadata;
use restate_storage_query_datafusion::Scan;
use restate_storage_query_datafusion::bifrost_read_stream::BifrostReadStreamsBuilder;
use restate_storage_query_datafusion::table_util::Builder;
use restate_types::GenerationalNodeId;
use restate_types::logs::{Lsn, SequenceNumber};

/// Creates a local scanner for `bifrost_read_streams` from the bifrost read
/// stream registry.
pub(crate) fn create_local_scanner(
    registry: ActiveReadStreamRegistry,
    metadata: Metadata,
) -> Arc<dyn Scan> {
    Arc::new(BifrostReadStreamsScanner { registry, metadata })
}

struct BifrostReadStreamsScanner {
    registry: ActiveReadStreamRegistry,
    metadata: Metadata,
}

impl Debug for BifrostReadStreamsScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BifrostReadStreamsScanner")
    }
}

impl Scan for BifrostReadStreamsScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        _batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let snapshot = self.registry.snapshot();
        let my_node_id = self.metadata.my_node_id();
        let schema = projection.clone();

        let fut = async move {
            let mut builder = BifrostReadStreamsBuilder::new(schema.clone());

            for (count, state) in snapshot.iter().enumerate() {
                if limit.is_some_and(|l| count >= l) {
                    break;
                }
                append_row(&mut builder, my_node_id, state);
            }

            builder.finish()
        };

        Box::pin(RecordBatchStreamAdapter::new(projection, stream::once(fut)))
    }
}

fn append_row(
    builder: &mut BifrostReadStreamsBuilder,
    node_id: GenerationalNodeId,
    state: &ReadStreamState,
) {
    let mut row = builder.row();

    row.fmt_plain_node_id(node_id.as_plain());
    row.fmt_gen_node_id(node_id);
    row.log_id(u32::from(state.log_id));
    row.read_pointer(u64::from(state.read_pointer));
    row.end_lsn(u64::from(state.end_lsn));
    row.fmt_state(state.state);

    if let Some(tail) = state.safe_known_tail {
        row.safe_known_tail(u64::from(tail));
    }

    if let Some(seg) = state.current_segment {
        row.current_segment(u32::from(seg));
    }

    if let Some(loglet_id) = state.loglet_id {
        row.fmt_loglet_id(loglet_id);
    }

    row.is_tailing(state.end_lsn == Lsn::MAX);
}
