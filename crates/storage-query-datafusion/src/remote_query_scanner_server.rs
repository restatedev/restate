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

type DashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;

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
        let scanners: Arc<DashMap<ScannerId, ScannerHandle>> = Default::default();
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
            }
        }
    }

    fn on_open(
        scanner_id: ScannerId,
        scan_req: Incoming<Rpc<RemoteQueryScannerOpen>>,
        scanners: &Arc<DashMap<ScannerId, ScannerHandle>>,
        remote_scanner_manager: &RemoteScannerManager,
    ) {
        let (reciprocal, body) = scan_req.split();
        let maybe_scanner = ScannerTask::spawn(scanner_id, remote_scanner_manager, scanners, body);
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

    fn on_next(
        scanners: &DashMap<ScannerId, ScannerHandle>,
        req: Incoming<Rpc<RemoteQueryScannerNext>>,
    ) {
        let (reciprocal, req) = req.split();
        let scanner_id = req.scanner_id;
        let Some(scanner) = scanners.get(&scanner_id) else {
            tracing::info!(
                "No such scanner {}. This could be an expired scanner due to a slow scan with no activity.",
                scanner_id
            );
            reciprocal.send(RemoteQueryScannerNextResult::NoSuchScanner(scanner_id));
            return;
        };
        // Note: we trust that the task will remove the scanner from the map on Drop and will not try to
        // that again here. If we do, we might end up dead-locking the map because we are holding a
        // reference into it (scanner).
        if let Err(mpsc::error::SendError(reciprocal)) = scanner.send(reciprocal) {
            tracing::info!(
                "No such scanner {}. This could be an expired scanner due to a slow scan with no activity.",
                scanner_id
            );
            reciprocal.send(RemoteQueryScannerNextResult::NoSuchScanner(scanner_id));
        }
    }
}
