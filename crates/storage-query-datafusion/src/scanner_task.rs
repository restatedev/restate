// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::Context;
use dashmap::DashMap;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::metrics::Time;
use tokio::sync::{mpsc, watch};
use tokio_stream::StreamExt as TokioStreamExt;
use tracing::{debug, warn};

use restate_core::network::{Oneshot, Reciprocal};
use restate_core::{TaskCenter, TaskKind};
use restate_types::GenerationalNodeId;
use restate_types::net::remote_query_scanner::{
    RemoteQueryScannerNextResult, RemoteQueryScannerOpen, RemoteQueryScannerPredicate,
    ScannerBatch, ScannerFailure, ScannerId,
};

use crate::context::QueryContext;
use crate::remote_fragment::RemoteFragment;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::{decode_expr, decode_schema, encode_record_batch};

const SCANNER_EXPIRATION: Duration = Duration::from_secs(60);

pub(crate) struct NextRequest {
    pub reciprocal: Reciprocal<Oneshot<RemoteQueryScannerNextResult>>,
    pub next_predicate: Option<RemoteQueryScannerPredicate>,
}

/// Control-plane handle retained by the server map. Its cancellation signal
/// wakes the scanner task even while that task is polling a pipeline-breaking
/// fragment rather than waiting for another request.
pub(crate) struct ScannerHandle {
    requests: mpsc::UnboundedSender<NextRequest>,
    cancellation: watch::Sender<bool>,
}

pub(super) type ScannerMap = DashMap<ScannerId, ScannerHandle, ahash::RandomState>;

impl ScannerHandle {
    pub(crate) fn send(
        &self,
        request: NextRequest,
    ) -> Result<(), mpsc::error::SendError<NextRequest>> {
        self.requests.send(request)
    }

    pub(crate) fn cancel(&self) {
        let _ = self.cancellation.send(true);
    }
}

/// Owns one remote scanner's pull stream, dynamic predicate, and cancellation
/// lifecycle from a successful `Open` until EOF, failure, close, or peer death.
pub(crate) struct ScannerTask {
    peer: GenerationalNodeId,
    scanner_id: ScannerId,
    stream: SendableRecordBatchStream,
    rx: mpsc::UnboundedReceiver<NextRequest>,
    cancellation: watch::Receiver<bool>,
    scanners: Weak<ScannerMap>,
    ctx: Arc<TaskContext>,
    schema: SchemaRef,
    dynamic_filter: Option<Arc<DynamicFilterPhysicalExpr>>,
}

impl ScannerTask {
    /// Spawns the scanner task and registers the scanner in the scanners map.
    pub(crate) fn spawn(
        scanner_id: ScannerId,
        query_context: &QueryContext,
        remote_scanner_manager: &RemoteScannerManager,
        peer: GenerationalNodeId,
        scanners: &Arc<ScannerMap>,
        request: RemoteQueryScannerOpen,
    ) -> anyhow::Result<bool> {
        if let Some(expected_owner) = request.expected_partition_owner {
            remote_scanner_manager
                .validate_local_partition_owner(request.partition_id, expected_owner)?;
        }

        let scanner = remote_scanner_manager
            .local_partition_scanner(&request.table)
            .context("not registered scanner for a table")?;
        let schema = decode_schema(&request.projection_schema_bytes).context("bad schema bytes")?;
        let ctx = query_context.task_ctx();

        let predicate = request
            .predicate
            .map(|predicate| decode_expr(&ctx, &schema, &predicate.serialized_physical_expression))
            .transpose()?;

        let schema = Arc::new(schema);

        let fragment = request.fragment.as_ref().and_then(|wire| {
            match RemoteFragment::from_wire(wire, &ctx, &schema) {
                Ok(fragment) => fragment,
                Err(error) => {
                    warn!("Declining remote fragment for scanner {scanner_id}: {error}");
                    None
                }
            }
        });

        let dynamic_filter = predicate
            .as_ref()
            .map(|pred| Arc::new(DynamicFilterPhysicalExpr::new(Vec::new(), Arc::clone(pred))));

        let stream = scanner.scan_partition(
            request.partition_id,
            request.range,
            schema.clone(),
            dynamic_filter
                .as_ref()
                .map(|filter| filter.clone() as Arc<dyn PhysicalExpr>),
            usize::try_from(request.batch_size).expect("batch_size to fit in a usize"),
            request
                .limit
                .map(|limit| usize::try_from(limit).expect("limit to fit in a usize")),
            Time::new(),
        )?;
        let (stream, fragment_applied) = if let Some(fragment) = fragment {
            match fragment.execute_recoverable(stream, Arc::clone(&ctx)) {
                Ok(stream) => (stream, true),
                Err(failure) => {
                    warn!(
                        "Declining remote fragment for scanner {scanner_id} after setup failed: {}",
                        failure.error
                    );
                    (failure.input, false)
                }
            }
        } else {
            (stream, false)
        };

        let (requests, rx) = mpsc::unbounded_channel();
        let (cancel, cancellation) = watch::channel(false);
        let mut task = Self {
            peer,
            scanner_id,
            stream,
            rx,
            cancellation,
            scanners: Arc::downgrade(scanners),
            ctx,
            schema,
            dynamic_filter,
        };

        scanners.insert(
            scanner_id,
            ScannerHandle {
                requests,
                cancellation: cancel,
            },
        );

        // make sure we add before we spawn.
        TaskCenter::spawn_unmanaged(TaskKind::DfScanner, "df-scanner-task", async move {
            task.run().await
        })?;

        Ok(fragment_applied)
    }

    async fn run(&mut self) {
        // Monitor the cluster state of the scanner peer to ensure we dispose the scanner if the
        // node was observed as dead.
        let mut peer_watch =
            TaskCenter::with_current(|tc| tc.cluster_state().watch(self.peer.as_plain()));

        let mut watch_fut = std::pin::pin!(
            peer_watch.conditional_wait_for(self.peer.generation(), |state| !state.is_alive())
        );
        let mut shutdown = std::pin::pin!(async {
            tokio::select! {
                _ = &mut watch_fut => {
                    debug!("Removing scanner because peer {} is dead", self.peer);
                }
                _ = self.cancellation.changed() => {
                    debug!("Remote scanner {} was cancelled", self.scanner_id);
                }
            }
        });

        loop {
            let request = tokio::select! {
                _ = &mut shutdown => return,
                maybe_request = self.rx.recv() => {
                    let Some(request) = maybe_request else {
                        return;
                    };
                    request
                }
                () = tokio::time::sleep(SCANNER_EXPIRATION) => {
                    warn!("Removing scanner due to a long inactivity {}", self.scanner_id);
                    return;
                }
            };

            if let Some(next_predicate) = request.next_predicate
                && let Err(e) = apply_next_predicate(
                    self.dynamic_filter.as_ref(),
                    &self.ctx,
                    &self.schema,
                    &next_predicate,
                )
            {
                warn!("Failed to apply next predicate: {e}");
            }

            // connection/request has been closed, don't bother with driving the stream.
            // The scanner will be dropped because we want to make sure that we don't get spurious
            // next messages from the client after.
            if request.reciprocal.is_closed() {
                return;
            }

            // The filtering is done inside the partition stream. Poll it only
            // while this scanner remains live, prioritizing cancellation.
            let next_batch = tokio::select! {
                biased;
                _ = &mut shutdown => return,
                next_batch = self.stream.next() => next_batch,
            };

            let record_batch = match next_batch {
                Some(Ok(record_batch)) => record_batch,
                Some(Err(e)) => {
                    warn!("Error while scanning {}: {e}", self.scanner_id);
                    request
                        .reciprocal
                        .send(RemoteQueryScannerNextResult::Failure(ScannerFailure {
                            scanner_id: self.scanner_id,
                            message: e.to_string(),
                        }));
                    return;
                }
                None => {
                    request
                        .reciprocal
                        .send(RemoteQueryScannerNextResult::NoMoreRecords(self.scanner_id));
                    return;
                }
            };

            match encode_record_batch(&self.stream.schema(), record_batch) {
                Ok(record_batch) => {
                    request
                        .reciprocal
                        .send(RemoteQueryScannerNextResult::NextBatch(ScannerBatch {
                            scanner_id: self.scanner_id,
                            record_batch,
                        }))
                }
                Err(e) => {
                    warn!("Error while encoding batch {}: {e}", self.scanner_id);
                    request
                        .reciprocal
                        .send(RemoteQueryScannerNextResult::Failure(ScannerFailure {
                            scanner_id: self.scanner_id,
                            message: e.to_string(),
                        }));
                    return;
                }
            }
        }
    }
}

fn apply_next_predicate(
    dynamic_filter: Option<&Arc<DynamicFilterPhysicalExpr>>,
    ctx: &TaskContext,
    schema: &SchemaRef,
    predicate: &RemoteQueryScannerPredicate,
) -> datafusion::common::Result<()> {
    let predicate = decode_expr(ctx, schema, &predicate.serialized_physical_expression)?;
    if let Some(dynamic_filter) = dynamic_filter {
        dynamic_filter.update(predicate)?;
    }
    Ok(())
}

impl Drop for ScannerTask {
    fn drop(&mut self) {
        if let Some(scanners) = self.scanners.upgrade() {
            let _ = scanners.remove(&self.scanner_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{BooleanArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::scalar::ScalarValue;

    use super::*;
    use crate::encode_expr;

    fn greater_than(column: &Arc<dyn PhysicalExpr>, value: i64) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::clone(column),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(value)))),
        ))
    }

    #[test]
    fn next_predicate_updates_the_server_dynamic_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let column = Arc::new(Column::new("value", 0)) as Arc<dyn PhysicalExpr>;
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&column)],
            greater_than(&column, 10),
        ));
        let update = RemoteQueryScannerPredicate {
            serialized_physical_expression: encode_expr(&greater_than(&column, 20))
                .expect("encode predicate update"),
        };

        apply_next_predicate(
            Some(&dynamic_filter),
            &TaskContext::default(),
            &schema,
            &update,
        )
        .expect("apply predicate update");

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![19, 20, 21]))])
                .expect("predicate input");
        let values = dynamic_filter
            .evaluate(&batch)
            .expect("updated predicate evaluation")
            .into_array(batch.num_rows())
            .expect("updated predicate values");
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean predicate values"),
            &BooleanArray::from(vec![false, false, true])
        );
    }
}
