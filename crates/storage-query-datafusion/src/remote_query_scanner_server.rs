// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use crate::context::QueryContext;
use crate::{decode_schema, encode_record_batch};
use ahash::HashMap;
use anyhow::Context;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use tokio::time;
use tokio::time::Instant;
use tokio_stream::StreamExt as TokioStreamExt;
use tracing::warn;

use restate_core::network::{Incoming, MessageRouterBuilder, MessageStream};
use restate_core::{cancellation_watcher, my_node_id};
use restate_types::net::remote_query_scanner::{
    RemoteQueryScannerClose, RemoteQueryScannerClosed, RemoteQueryScannerNext,
    RemoteQueryScannerNextResult, RemoteQueryScannerOpen, RemoteQueryScannerOpened, ScannerId,
};

struct Scanner {
    stream: SendableRecordBatchStream,
    last_accessed: Instant,
}

impl Scanner {
    fn new(ctx: QueryContext, request: RemoteQueryScannerOpen) -> anyhow::Result<Self> {
        let scanner = ctx
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
    query_context: QueryContext,
    open_stream: MessageStream<RemoteQueryScannerOpen>,
    next_stream: MessageStream<RemoteQueryScannerNext>,
    close_stream: MessageStream<RemoteQueryScannerClose>,
}

impl RemoteQueryScannerServer {
    pub fn new(
        expire_old_scanners_after: Duration,
        query_context: QueryContext,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let open_stream = router_builder.subscribe_to_stream(64);
        let next_stream = router_builder.subscribe_to_stream(64);
        let close_stream = router_builder.subscribe_to_stream(64);

        Self {
            expire_old_scanners_after,
            query_context,
            open_stream,
            next_stream,
            close_stream,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let RemoteQueryScannerServer {
            expire_old_scanners_after,
            query_context,
            mut open_stream,
            mut next_stream,
            mut close_stream,
        } = self;

        let mut shutdown = pin::pin!(cancellation_watcher());
        let mut next_scanner_id = 1u64;
        let mut scanners: HashMap<ScannerId, Scanner> = Default::default();
        let mut interval = time::interval(expire_old_scanners_after);

        let node_id = my_node_id();

        loop {
            tokio::select! {
                        biased;
                        _ = &mut shutdown => {
                            // stop accepting messages
                            drop(open_stream);
                            drop(next_stream);
                            drop(close_stream);
                            return Ok(());
                        },
                        Some(scan_req) = open_stream.next() => {
                            next_scanner_id += 1;
                            let scanner_id = ScannerId(node_id, next_scanner_id);
                            Self::on_open(scanner_id, scan_req, &mut scanners, query_context.clone()).await;
                        },
                        Some(next_req) = next_stream.next() => {
                            let (reciprocal, req) = next_req.split();
                            let id = req.scanner_id;
                            let res = Self::on_next(&mut scanners, req).await;
                            let outgoing = reciprocal.prepare(res);
                            if outgoing.send().await.is_err() {
                                    scanners.remove(&id);
                            }
                        },
                        Some(close_req) = close_stream.next() => {
                            let id  = close_req.body().scanner_id;
                            scanners.remove(&id);
                            let res = RemoteQueryScannerClosed {
                                scanner_id: id,
                            };
                            let outgoing = close_req.into_outgoing(res);
                            let _ = outgoing.send().await;
                       },
                       now = interval.tick() => {
                           Self::try_expire_scanners(&now,  expire_old_scanners_after ,&mut scanners);
                       }
            }
        }
    }

    async fn on_open(
        scanner_id: ScannerId,
        scan_req: Incoming<RemoteQueryScannerOpen>,
        scanners: &mut HashMap<ScannerId, Scanner>,
        query_context: QueryContext,
    ) {
        let (reciprocal, body) = scan_req.split();
        let maybe_scanner = Scanner::new(query_context, body);
        let Ok(scanner) = maybe_scanner else {
            warn!(
                "Unable to create a scanner {}",
                maybe_scanner.err().unwrap()
            );
            let response = RemoteQueryScannerOpened::Failure;
            let _ = reciprocal.prepare(response).send().await;
            return;
        };
        scanners.insert(scanner_id, scanner);
        let response = RemoteQueryScannerOpened::Success { scanner_id };
        let outgoing = reciprocal.prepare(response);
        if outgoing.send().await.is_err() {
            scanners.remove(&scanner_id);
        }
    }

    async fn on_next(
        scanners: &mut HashMap<ScannerId, Scanner>,
        req: RemoteQueryScannerNext,
    ) -> RemoteQueryScannerNextResult {
        let scanner_id = req.scanner_id;
        let Some(scanner) = scanners.get_mut(&scanner_id) else {
            tracing::info!( "No such scanner {}. This could be an expired scanner due to a slow scan with no activity.", scanner_id);
            return RemoteQueryScannerNextResult::NoSuchScanner(scanner_id);
        };
        match scanner.next_batch().await {
            Ok(Some(record_batch)) => RemoteQueryScannerNextResult::NextBatch {
                scanner_id,
                record_batch,
            },
            Ok(None) => {
                scanners.remove(&scanner_id);
                RemoteQueryScannerNextResult::NoMoreRecords(scanner_id)
            }
            Err(e) => {
                scanners.remove(&scanner_id);
                warn!("Error while scanning {}: {}", scanner_id, e);

                RemoteQueryScannerNextResult::Failure {
                    scanner_id,
                    message: format!("{}", e),
                }
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
