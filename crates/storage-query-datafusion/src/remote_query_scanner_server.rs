// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use dashmap::DashMap;
use tokio::sync::mpsc;
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
    RemoteQueryScannerOpened, ScannerId,
};

use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::scanner_task::{ScannerHandle, ScannerTask};

pub(super) type ScannerMap = DashMap<ScannerId, ScannerHandle, ahash::RandomState>;

pub struct RemoteQueryScannerServer {
    remote_scanner_manager: RemoteScannerManager,
    network_rx: ServiceReceiver<RemoteDataFusionService>,
}

impl RemoteQueryScannerServer {
    pub fn new(
        remote_scanner_manager: RemoteScannerManager,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let network_rx = router_builder.register_service(64, BackPressureMode::PushBack);

        Self {
            remote_scanner_manager,
            network_rx,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let RemoteQueryScannerServer {
            remote_scanner_manager,
            network_rx,
        } = self;

        let mut shutdown = pin::pin!(cancellation_watcher());
        let mut next_scanner_id = 1u64;
        let scanners: Arc<ScannerMap> = Default::default();
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
                            Self::on_open(scanner_id, scan_req, &scanners, &remote_scanner_manager);
                        }
                        ServiceMessage::Rpc(msg) if msg.msg_type() == RemoteQueryScannerNext::TYPE => {
                            Self::on_next(
                                &scanners,
                                msg.into_typed::<RemoteQueryScannerNext>()
                            );
                        }
                        ServiceMessage::Rpc(msg) if msg.msg_type() == RemoteQueryScannerClose::TYPE => {
                            let close_req = msg.into_typed::<RemoteQueryScannerClose>();
                            let Some((reciprocal, close_req)) = close_req.split() else {
                                continue;
                            };
                            scanners.remove(&close_req.scanner_id);
                            let res = RemoteQueryScannerClosed {
                                scanner_id: close_req.scanner_id,
                            };
                            reciprocal.send(res);
                        }
                        msg => { msg.fail(Verdict::MessageUnrecognized); }
                    }
                },
            }
        }
    }

    fn on_open(
        scanner_id: ScannerId,
        scan_req: Incoming<Rpc<RemoteQueryScannerOpen>>,
        scanners: &Arc<ScannerMap>,
        remote_scanner_manager: &RemoteScannerManager,
    ) {
        let peer = scan_req.peer();
        let Some((reciprocal, body)) = scan_req.split() else {
            return;
        };
        let partition_id = body.partition_id;

        if let Err(e) = ScannerTask::spawn(scanner_id, remote_scanner_manager, peer, scanners, body)
        {
            warn!("Unable to create a scanner in partition {partition_id}:  {e}");
            let response = RemoteQueryScannerOpened::Failure;
            reciprocal.send(response);
            return;
        }

        let response = RemoteQueryScannerOpened::Success { scanner_id };
        reciprocal.send(response);
    }

    fn on_next(scanners: &ScannerMap, req: Incoming<Rpc<RemoteQueryScannerNext>>) {
        let Some((reciprocal, req)) = req.split() else {
            return;
        };
        let scanner_id = req.scanner_id;
        let Some(scanner) = scanners.get(&scanner_id) else {
            tracing::info!(
                "No such scanner {}. This could be an expired scanner due to a slow scan, no activity, or it's a scanner that was closed.",
                scanner_id
            );
            reciprocal.send(RemoteQueryScannerNextResult::NoSuchScanner(scanner_id));
            return;
        };
        // Note: we trust that the task will remove the scanner from the map on Drop and we will not try to
        // do that again here. If we do, we might end up dead-locking the map because we are holding a
        // reference into it (scanner).
        if let Err(mpsc::error::SendError(request)) =
            scanner.send(super::scanner_task::NextRequest {
                reciprocal,
                next_predicate: req.next_predicate,
            })
        {
            tracing::info!(
                "No such scanner {}. This could be an expired scanner due to a slow scan with no activity.",
                scanner_id
            );
            request
                .reciprocal
                .send(RemoteQueryScannerNextResult::NoSuchScanner(scanner_id));
        }
    }
}
