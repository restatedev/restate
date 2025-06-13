// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin;
use std::sync::Arc;
use std::time::Duration;

use ahash::HashMap;
use anyhow::Context;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use tokio::time;
use tokio::time::Instant;
use tokio_stream::StreamExt as TokioStreamExt;
use tracing::warn;

use restate_core::network::{
    BackPressureMode, Incoming, MessageRouterBuilder, Rpc, ServiceMessage, ServiceReceiver, Verdict,
};
use restate_core::{cancellation_watcher, my_node_id};
use restate_types::net::RpcRequest;
use restate_types::net::remote_query_scanner::{
    RemoteDataFusionService, RemoteQueryScannerClose, RemoteQueryScannerClosed,
    RemoteQueryScannerNext, RemoteQueryScannerNextResult, RemoteQueryScannerOpen,
    RemoteQueryScannerOpened, ScannerBatch, ScannerFailure, ScannerId,
};

use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::{decode_schema, encode_record_batch};

struct Scanner {
    stream: SendableRecordBatchStream,
    last_accessed: Instant,
}

impl Scanner {
    fn new(
        remote_scanner_manager: RemoteScannerManager,
        request: RemoteQueryScannerOpen,
    ) -> anyhow::Result<Self> {
        let scanner = remote_scanner_manager
            .local_partition_scanner(&request.table)
            .context("not registered scanner for a table")?;
        let schema = decode_schema(&request.projection_schema_bytes).context("bad schema bytes")?;
        let stream = scanner.scan_partition(
            request.partition_id,
            request.range.clone(),
            Arc::new(schema),
        )?;
        Ok(Self {
            stream,
            last_accessed: Instant::now(),
        })
    }

    async fn next_batch(&mut self) -> Result<Option<Vec<u8>>, DataFusionError> {
        self.last_accessed = Instant::now();
        if let Some(res) = self.stream.next().await {
            let record_batch = res?;
            let buf = encode_record_batch(&self.stream.schema(), record_batch)?;
            Ok(Some(buf))
        } else {
            Ok(None)
        }
    }
}

pub struct RemoteQueryScannerServer {
    expire_old_scanners_after: Duration,
    remote_scanner_manager: RemoteScannerManager,
    network_rx: ServiceReceiver<RemoteDataFusionService>,
}

impl RemoteQueryScannerServer {
    pub fn new(
        expire_old_scanners_after: Duration,
        remote_scanner_manager: RemoteScannerManager,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let network_rx = router_builder.register_service(64, BackPressureMode::PushBack);

        Self {
            expire_old_scanners_after,
            remote_scanner_manager,
            network_rx,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let RemoteQueryScannerServer {
            expire_old_scanners_after,
            remote_scanner_manager,
            network_rx,
        } = self;

        let mut shutdown = pin::pin!(cancellation_watcher());
        let mut next_scanner_id = 1u64;
        let mut scanners: HashMap<ScannerId, Scanner> = Default::default();
        let mut interval = time::interval(expire_old_scanners_after);
        let mut network_rx = network_rx.start();

        loop {
            tokio::select! {
                        biased;
                        _ = &mut shutdown => {
                            // stop accepting messages
                            return Ok(());
                        },
                        Some(msg) = network_rx.next() => {
                            match msg {
                                ServiceMessage::Rpc(msg) if msg.msg_type() == RemoteQueryScannerOpen::TYPE => {
                                    let scan_req = msg.into_typed::<RemoteQueryScannerOpen>();
                                    next_scanner_id += 1;
                                    let scanner_id = ScannerId(my_node_id(), next_scanner_id);
                                    Self::on_open(scanner_id, scan_req, &mut scanners, remote_scanner_manager.clone()).await;
                                }
                                ServiceMessage::Rpc(msg) if msg.msg_type() == RemoteQueryScannerNext::TYPE => {
                                    let next_req = msg.into_typed::<RemoteQueryScannerNext>();
                                    let (reciprocal, req) = next_req.split();
                                    let res = Self::on_next(&mut scanners, req).await;
                                    reciprocal.send(res);
                                }
                                ServiceMessage::Rpc(msg) if msg.msg_type() == RemoteQueryScannerClose::TYPE => {
                                    let close_req = msg.into_typed::<RemoteQueryScannerClose>();
                                    let (reciprocal, close_req) = close_req.split();
                                    scanners.remove(&close_req.scanner_id);
                                    let res = RemoteQueryScannerClosed {
                                        scanner_id: close_req.scanner_id,
                                    };
                                    reciprocal.send(res);
                                }
                                msg => { msg.fail(Verdict::MessageUnrecognized); }
                            }
                        },
                       now = interval.tick() => {
                           Self::try_expire_scanners(&now,  expire_old_scanners_after ,&mut scanners);
                       }
            }
        }
    }

    async fn on_open(
        scanner_id: ScannerId,
        scan_req: Incoming<Rpc<RemoteQueryScannerOpen>>,
        scanners: &mut HashMap<ScannerId, Scanner>,
        remote_scanner_manager: RemoteScannerManager,
    ) {
        let (reciprocal, body) = scan_req.split();
        let maybe_scanner = Scanner::new(remote_scanner_manager, body);
        let Ok(scanner) = maybe_scanner else {
            warn!(
                "Unable to create a scanner {}",
                maybe_scanner.err().unwrap()
            );
            let response = RemoteQueryScannerOpened::Failure;
            reciprocal.send(response);
            return;
        };
        scanners.insert(scanner_id, scanner);
        let response = RemoteQueryScannerOpened::Success { scanner_id };
        reciprocal.send(response);
    }

    async fn on_next(
        scanners: &mut HashMap<ScannerId, Scanner>,
        req: RemoteQueryScannerNext,
    ) -> RemoteQueryScannerNextResult {
        let scanner_id = req.scanner_id;
        let Some(scanner) = scanners.get_mut(&scanner_id) else {
            tracing::info!(
                "No such scanner {}. This could be an expired scanner due to a slow scan with no activity.",
                scanner_id
            );
            return RemoteQueryScannerNextResult::NoSuchScanner(scanner_id);
        };
        match scanner.next_batch().await {
            Ok(Some(record_batch)) => RemoteQueryScannerNextResult::NextBatch(ScannerBatch {
                scanner_id,
                record_batch,
            }),
            Ok(None) => {
                scanners.remove(&scanner_id);
                RemoteQueryScannerNextResult::NoMoreRecords(scanner_id)
            }
            Err(e) => {
                scanners.remove(&scanner_id);
                warn!("Error while scanning {}: {}", scanner_id, e);

                RemoteQueryScannerNextResult::Failure(ScannerFailure {
                    scanner_id,
                    message: format!("{e}"),
                })
            }
        }
    }

    fn try_expire_scanners(
        now: &Instant,
        duration: Duration,
        scanners: &mut HashMap<ScannerId, Scanner>,
    ) {
        scanners.retain(|id, scanner| {
            let elapsed = now.saturating_duration_since(scanner.last_accessed);
            let should_retain = elapsed < duration;
            if !should_retain {
                warn!("Removing scanner due to a long inactivity {}", id)
            }
            should_retain
        });
    }
}
