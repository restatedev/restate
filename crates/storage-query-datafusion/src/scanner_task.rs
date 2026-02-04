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
    ScannerBatch, ScannerFailure, ScannerId,
};

use crate::context::QueryContext;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::remote_query_scanner_server::ScannerMap;
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

        let predicate = request
            .predicate
            .map(|predicate| decode_expr(&ctx, &schema, &predicate.serialized_physical_expression))
            .transpose()?;

        let schema = Arc::new(schema);

        let dynamic_filter = predicate
            .as_ref()
            .map(|pred| Arc::new(DynamicFilterPhysicalExpr::new(Vec::new(), Arc::clone(pred))));

        let stream = scanner.scan_partition(
            request.partition_id,
            request.range.clone(),
            schema.clone(),
            dynamic_filter
                .as_ref()
                .map(|filter| filter.clone() as Arc<dyn PhysicalExpr>),
            usize::try_from(request.batch_size).expect("batch_size to fit in a usize"),
            request
                .limit
                .map(|limit| usize::try_from(limit).expect("limit to fit in a usize")),
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

            // The filtering is now done by FilterCoalesceStream inside scan_partition,
            // so we just need to get the next batch from the stream.
            let record_batch = match self.stream.next().await {
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

impl Drop for ScannerTask {
    fn drop(&mut self) {
        if let Some(scanners) = self.scanners.upgrade() {
            let _ = scanners.remove(&self.scanner_id);
        }
    }
}
