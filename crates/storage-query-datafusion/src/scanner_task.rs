// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use datafusion::execution::SendableRecordBatchStream;
use tokio::sync::mpsc;
use tokio_stream::StreamExt as TokioStreamExt;
use tracing::{debug, warn};

use restate_core::network::{Oneshot, Reciprocal};
use restate_core::{TaskCenter, TaskKind};
use restate_types::GenerationalNodeId;
use restate_types::net::remote_query_scanner::{
    RemoteQueryScannerNextResult, RemoteQueryScannerOpen, ScannerBatch, ScannerFailure, ScannerId,
};

use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::remote_query_scanner_server::ScannerMap;
use crate::{decode_schema, encode_record_batch};

const SCANNER_EXPIRATION: Duration = Duration::from_secs(60);

type NextReciprocal = Reciprocal<Oneshot<RemoteQueryScannerNextResult>>;

pub(crate) type ScannerHandle = mpsc::UnboundedSender<NextReciprocal>;

// Tracks a single scanner's lifecycle running in [`RemoteQueryScannerServer`]
pub(crate) struct ScannerTask {
    peer: GenerationalNodeId,
    scanner_id: ScannerId,
    stream: SendableRecordBatchStream,
    rx: mpsc::UnboundedReceiver<NextReciprocal>,
    scanners: Weak<ScannerMap>,
}

impl ScannerTask {
    /// Spawns the scanner task and registers the scanner in the scanners map.
    pub fn spawn(
        scanner_id: ScannerId,
        remote_scanner_manager: &RemoteScannerManager,
        peer: GenerationalNodeId,
        scanners: &Arc<ScannerMap>,
        request: RemoteQueryScannerOpen,
    ) -> anyhow::Result<()> {
        let scanner = remote_scanner_manager
            .local_partition_scanner(&request.table)
            .context("not registered scanner for a table")?;
        let schema = decode_schema(&request.projection_schema_bytes).context("bad schema bytes")?;
        let stream = scanner.scan_partition(
            request.partition_id,
            request.range.clone(),
            Arc::new(schema),
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
            let reciprocal = tokio::select! {
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

            // connection/request has been closed, don't bother with driving the stream.
            // The scanner will be dropped because we want to make sure that we don't get supurious
            // next messages from the client after.
            if reciprocal.is_closed() {
                return;
            }

            let record_batch = match self.stream.next().await {
                Some(Ok(record_batch)) => record_batch,
                Some(Err(e)) => {
                    warn!("Error while scanning {}: {e}", self.scanner_id);

                    reciprocal.send(RemoteQueryScannerNextResult::Failure(ScannerFailure {
                        scanner_id: self.scanner_id,
                        message: e.to_string(),
                    }));
                    return;
                }
                None => {
                    reciprocal.send(RemoteQueryScannerNextResult::NoMoreRecords(self.scanner_id));
                    return;
                }
            };
            match encode_record_batch(&self.stream.schema(), record_batch) {
                Ok(record_batch) => {
                    reciprocal.send(RemoteQueryScannerNextResult::NextBatch(ScannerBatch {
                        scanner_id: self.scanner_id,
                        record_batch,
                    }))
                }
                Err(e) => {
                    reciprocal.send(RemoteQueryScannerNextResult::Failure(ScannerFailure {
                        scanner_id: self.scanner_id,
                        message: e.to_string(),
                    }));
                    break;
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
