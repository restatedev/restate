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
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::PhysicalExpr;
use tokio::sync::mpsc;
use tokio_stream::StreamExt as TokioStreamExt;
use tracing::{debug, warn};

use restate_core::network::{Oneshot, Reciprocal};
use restate_core::{TaskCenter, TaskKind};
use restate_types::GenerationalNodeId;
use restate_types::net::remote_query_scanner::{
    RemoteQueryScannerNextResult, RemoteQueryScannerOpen, RemoteQueryScannerPredicate,
    ScannerBatch, ScannerCompleted, ScannerFailure, ScannerId,
};

use crate::context::QueryContext;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::remote_query_scanner_server::ScannerMap;
use crate::scan_metrics::ScanMetrics;
use crate::{decode_expr, decode_schema, encode_record_batch};

const SCANNER_EXPIRATION: Duration = Duration::from_secs(60);

pub(crate) struct NextRequest {
    pub reciprocal: Reciprocal<Oneshot<RemoteQueryScannerNextResult>>,
    pub next_predicate: Option<RemoteQueryScannerPredicate>,
}

pub(crate) type ScannerHandle = mpsc::UnboundedSender<NextRequest>;

// Tracks a single scanner's lifecycle running in [`RemoteQueryScannerServer`]
pub(crate) struct ScannerTask {
    peer: GenerationalNodeId,
    scanner_id: ScannerId,
    stream: SendableRecordBatchStream,
    rx: mpsc::UnboundedReceiver<NextRequest>,
    scanners: Weak<ScannerMap>,
    ctx: Arc<TaskContext>,
    schema: SchemaRef,
    dynamic_filter: Option<Arc<DynamicFilterPhysicalExpr>>,
    metrics: ScanMetrics,
    collect_metrics: bool,
}

impl ScannerTask {
    /// Spawns the scanner task and registers the scanner in the scanners map.
    pub fn spawn(
        scanner_id: ScannerId,
        query_context: &QueryContext,
        remote_scanner_manager: &RemoteScannerManager,
        peer: GenerationalNodeId,
        scanners: &Arc<ScannerMap>,
        request: RemoteQueryScannerOpen,
    ) -> anyhow::Result<()> {
        let scanner = remote_scanner_manager
            .local_partition_scanner(&request.table)
            .context("not registered scanner for a table")?;
        let schema = decode_schema(&request.projection_schema_bytes).context("bad schema bytes")?;
        let ctx = query_context.task_ctx();

        let access_predicate = request
            .predicate
            .map(|predicate| decode_expr(&ctx, &schema, &predicate.serialized_physical_expression))
            .transpose()?;

        let schema = Arc::new(schema);

        let dynamic_filter = access_predicate
            .as_ref()
            .map(|pred| Arc::new(DynamicFilterPhysicalExpr::new(Vec::new(), Arc::clone(pred))));

        let metrics = ScanMetrics::remote(request.partition_id);
        let stream = scanner.scan_partition(
            request.partition_id,
            request.range,
            schema.clone(),
            dynamic_filter
                .as_ref()
                .map(|filter| filter.clone() as Arc<dyn PhysicalExpr>),
            access_predicate,
            usize::try_from(request.batch_size).expect("batch_size to fit in a usize"),
            request
                .limit
                .map(|limit| usize::try_from(limit).expect("limit to fit in a usize")),
            metrics.clone(),
        )?;

        let (tx, rx) = mpsc::unbounded_channel();
        let mut task = Self {
            peer,
            scanner_id,
            stream,
            rx,
            scanners: Arc::downgrade(scanners),
            ctx,
            schema,
            dynamic_filter,
            metrics,
            collect_metrics: request.collect_metrics,
        };

        scanners.insert(scanner_id, tx);

        // make sure we add before we spawn.
        TaskCenter::spawn_unmanaged(TaskKind::DfScanner, "df-scanner-task", async move {
            task.run().await
        })?;

        Ok(())
    }

    async fn run(&mut self) {
        // Monitor the cluster state of the scanner peer to ensure we dispose the scanner if the
        // node was observed as dead.
        let mut peer_watch =
            TaskCenter::with_current(|tc| tc.cluster_state().watch(self.peer.as_plain()));

        let mut watch_fut = std::pin::pin!(
            peer_watch.conditional_wait_for(self.peer.generation(), |state| !state.is_alive())
        );

        loop {
            let request = tokio::select! {
                _ = &mut watch_fut => {
                    // peer is dead, dispose the scanner
                    debug!("Removing scanner due to peer {} being dead", self.peer);
                    return;
                }
                maybe_request = self.rx.recv() => {
                    match maybe_request {
                            Some(request) => request,
                            None => {
                                // scanner has been closed.
                                return;
                            }
                        }
                }
                () = tokio::time::sleep(SCANNER_EXPIRATION) => {
                    warn!("Removing scanner due to a long inactivity {}", self.scanner_id);
                    return;
                }
            };

            if let Some(next_predicate) = request.next_predicate {
                match decode_expr(
                    &self.ctx,
                    &self.schema,
                    &next_predicate.serialized_physical_expression,
                ) {
                    Ok(next_predicate) => {
                        if let Some(dynamic_filter) = &self.dynamic_filter
                            && let Err(e) = dynamic_filter.update(next_predicate)
                        {
                            warn!("Failed to update dynamic filter: {e}");
                        }
                    }
                    Err(e) => {
                        warn!("Failed to decode next predicate: {e}")
                    }
                }
            }

            // connection/request has been closed, don't bother with driving the stream.
            // The scanner will be dropped because we want to make sure that we don't get spurious
            // next messages from the client after.
            if request.reciprocal.is_closed() {
                return;
            }

            let response = self.next_response().await;
            let more = matches!(response, RemoteQueryScannerNextResult::NextBatch(_));
            request.reciprocal.send(response);
            if !more {
                return;
            }
        }
    }

    async fn next_response(&mut self) -> RemoteQueryScannerNextResult {
        // The local scan publishes its final counters before exposing EOF.
        let result = match self.stream.next().await {
            Some(Ok(batch)) => encode_record_batch(&self.stream.schema(), batch),
            Some(Err(error)) => Err(error),
            None => {
                return if self.collect_metrics {
                    RemoteQueryScannerNextResult::Completed(ScannerCompleted {
                        scanner_id: self.scanner_id,
                        metrics: self.metrics.snapshot(),
                    })
                } else {
                    RemoteQueryScannerNextResult::NoMoreRecords(self.scanner_id)
                };
            }
        };
        let metrics = self.collect_metrics.then(|| self.metrics.snapshot());
        match result {
            Ok(record_batch) => RemoteQueryScannerNextResult::NextBatch(ScannerBatch {
                scanner_id: self.scanner_id,
                record_batch,
                metrics,
            }),
            Err(error) => {
                warn!("Error while scanning/encoding {}: {error}", self.scanner_id);
                RemoteQueryScannerNextResult::Failure(ScannerFailure {
                    scanner_id: self.scanner_id,
                    message: error.to_string(),
                    metrics,
                })
            }
        }
    }
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
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use restate_types::sharding::PartitionId;

    use super::*;

    fn task(
        collect_metrics: bool,
        batches: Vec<datafusion::common::Result<RecordBatch>>,
    ) -> ScannerTask {
        let schema = Arc::new(Schema::empty());
        let (_, rx) = mpsc::unbounded_channel();
        ScannerTask {
            peer: GenerationalNodeId::new(1, 1),
            scanner_id: ScannerId(GenerationalNodeId::new(1, 1), 1),
            stream: Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::iter(batches),
            )),
            rx,
            scanners: Weak::new(),
            ctx: Arc::new(TaskContext::default()),
            schema,
            dynamic_filter: None,
            metrics: ScanMetrics::remote(PartitionId::MIN),
            collect_metrics,
        }
    }

    #[tokio::test]
    async fn progress_completion_empty_and_failed_replies_respect_capabilities() {
        for enabled in [false, true] {
            let mut task = task(
                enabled,
                vec![Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))],
            );
            task.metrics.iterator_metrics().mark_supported();
            task.metrics.record_emitted();
            let (reply, received) = Reciprocal::<Oneshot<RemoteQueryScannerNextResult>>::mock();
            reply.send(task.next_response().await);
            let RemoteQueryScannerNextResult::NextBatch(batch) = received.recv().await else {
                panic!("expected batch")
            };
            assert_eq!(batch.metrics.is_some(), enabled);
            if let Some(metrics) = batch.metrics {
                assert!(metrics.available);
                assert!(!metrics.complete);
                assert_eq!(metrics.records_emitted, 1);
            }
            task.metrics.finish();
            let response = task.next_response().await;
            match response {
                RemoteQueryScannerNextResult::Completed(done) => {
                    assert!(enabled);
                    assert!(done.metrics.complete);
                    assert_eq!(done.metrics.records_emitted, 1);
                }
                RemoteQueryScannerNextResult::NoMoreRecords(_) => assert!(!enabled),
                _ => panic!("expected EOF"),
            }
        }
        let mut empty = task(true, vec![]);
        empty.metrics.iterator_metrics().mark_supported();
        empty.metrics.finish();
        let RemoteQueryScannerNextResult::Completed(done) = empty.next_response().await else {
            panic!("expected measured EOF")
        };
        assert!(done.metrics.available && done.metrics.complete);
        assert_eq!(done.metrics.keys_visited, 0);

        let mut failed = task(
            true,
            vec![Err(datafusion::common::DataFusionError::Execution(
                "failed scan".into(),
            ))],
        );
        failed.metrics.iterator_metrics().mark_supported();
        let RemoteQueryScannerNextResult::Failure(failure) = failed.next_response().await else {
            panic!("expected failure")
        };
        assert!(!failure.metrics.unwrap().complete);
    }
}
