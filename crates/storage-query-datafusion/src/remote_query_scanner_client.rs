// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Debug, Formatter};
use std::ops::RangeInclusive;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use tracing::{debug, info};

use restate_core::network::{NetworkSender, Networking, Swimlane, TransportConnect};
use restate_core::{TaskCenter, TaskCenterFutureExt, TaskKind, task_center};
use restate_types::NodeId;
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::net::remote_query_scanner::{
    RemoteQueryScannerClose, RemoteQueryScannerNext, RemoteQueryScannerNextResult,
    RemoteQueryScannerOpen, RemoteQueryScannerOpened, ScannerBatch, ScannerFailure, ScannerId,
};

use crate::{decode_record_batch, encode_schema};

// ----- rpc service definition -----

#[async_trait]
pub trait RemoteScannerService: Send + Sync + Debug + 'static {
    async fn open(
        &self,
        peer: NodeId,
        req: RemoteQueryScannerOpen,
    ) -> Result<RemoteQueryScannerOpened, DataFusionError>;

    async fn next_batch(
        &self,
        peer: NodeId,
        req: RemoteQueryScannerNext,
    ) -> Result<RemoteQueryScannerNextResult, DataFusionError>;

    fn close(&self, peer: NodeId, req: RemoteQueryScannerClose);
}

// ----- service proxy -----
pub fn create_remote_scanner_service<T: TransportConnect>(
    network: Networking<T>,
) -> Arc<dyn RemoteScannerService> {
    Arc::new(RemoteScannerServiceProxy::new(
        network,
        TaskCenter::current(),
    ))
}

// ----- datafusion remote scan -----

struct RemoteScannerDropGuard {
    target_node_id: NodeId,
    scanner_id: ScannerId,
    closer: Option<Arc<dyn RemoteScannerService>>,
}

impl RemoteScannerDropGuard {
    /// The guard will not close the remote scanner
    pub fn forget(&mut self) {
        self.closer.take();
    }
}

impl Drop for RemoteScannerDropGuard {
    fn drop(&mut self) {
        if let Some(closer) = self.closer.take() {
            let target_node_id = self.target_node_id;
            let scanner_id = self.scanner_id;
            debug!("Closing remote scanner {scanner_id}");
            closer.close(target_node_id, RemoteQueryScannerClose { scanner_id });
        }
    }
}

/// Given an implementation of a remote ScannerService, this function
/// creates a DataFusion [[SendableRecordBatchStream]] that transports
/// record batches via the RemoteScannerService API.
pub fn remote_scan_as_datafusion_stream(
    service: Arc<dyn RemoteScannerService>,
    target_node_id: NodeId,
    partition_id: PartitionId,
    range: RangeInclusive<PartitionKey>,
    table_name: String,
    projection_schema: SchemaRef,
    limit: Option<usize>,
) -> SendableRecordBatchStream {
    let mut builder = RecordBatchReceiverStream::builder(projection_schema.clone(), 1);

    let tx = builder.tx();

    let task = async move {
        //
        // get a scanner id
        //
        let open_request = RemoteQueryScannerOpen {
            partition_id,
            range,
            table: table_name,
            projection_schema_bytes: encode_schema(&projection_schema),
            limit: limit.map(|limit| u64::try_from(limit).expect("limit to fit in a u64")),
        };

        let RemoteQueryScannerOpened::Success { scanner_id } =
            service.open(target_node_id, open_request).await?
        else {
            Err(DataFusionError::Internal(
                "Unable to open a remote scanner".to_string(),
            ))?
        };

        // a drop guard to close the remote scanner
        let mut close_guard = RemoteScannerDropGuard {
            scanner_id,
            target_node_id,
            closer: Some(service.clone()),
        };
        //
        // loop while we have record_batch coming in
        //
        loop {
            let req = RemoteQueryScannerNext { scanner_id };
            info!("fetching next batch from scanner {scanner_id} {target_node_id}");
            let batch = match service.next_batch(target_node_id, req).await {
                Err(e) => {
                    return Err(e);
                }
                Ok(RemoteQueryScannerNextResult::Unknown) => {
                    return Err(DataFusionError::Internal(
                        "Received unknown scanner result".to_owned(),
                    ));
                }
                Ok(RemoteQueryScannerNextResult::NextBatch(ScannerBatch {
                    record_batch, ..
                })) => decode_record_batch(&record_batch)?,
                Ok(RemoteQueryScannerNextResult::Failure(ScannerFailure { message, .. })) => {
                    // assume server closed the scanner before responding
                    close_guard.forget();
                    return Err(DataFusionError::Internal(message));
                }
                Ok(RemoteQueryScannerNextResult::NoMoreRecords(_)) => {
                    // assume server closed the scanner before responding
                    close_guard.forget();
                    return Ok(());
                }
                Ok(RemoteQueryScannerNextResult::NoSuchScanner(_)) => {
                    close_guard.forget();
                    return Err(DataFusionError::Internal("No such scanner. It could have expired due to a long period of inactivity.".to_string()));
                }
            };

            let res = tx.send(Ok(batch)).await;
            if res.is_ok() {
                continue;
            }
            return res
                .map(|_| ())
                .map_err(|e| DataFusionError::External(e.into()));
        }
    };

    builder.spawn(task);
    builder.build()
}

// ----- everything below is the client side implementation details -----

#[derive(Clone)]
struct RemoteScannerServiceProxy<T> {
    networking: Networking<T>,
    task_center: task_center::Handle,
}

impl<T> Debug for RemoteScannerServiceProxy<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemoteScannerServiceProxy")
    }
}

impl<T: TransportConnect> RemoteScannerServiceProxy<T> {
    fn new(networking: Networking<T>, task_center: task_center::Handle) -> Self {
        Self {
            networking,
            task_center,
        }
    }
}

#[async_trait]
impl<T: TransportConnect> RemoteScannerService for RemoteScannerServiceProxy<T> {
    async fn open(
        &self,
        peer: NodeId,
        req: RemoteQueryScannerOpen,
    ) -> Result<RemoteQueryScannerOpened, DataFusionError> {
        self.networking
            .call_rpc(peer, Swimlane::default(), req, None, None)
            .in_tc_as_task(
                &self.task_center,
                TaskKind::InPlace,
                "RemoteScannerServiceProxy::open",
            )
            .await
            .map_err(|e| DataFusionError::External(e.into()))
    }

    async fn next_batch(
        &self,
        peer: NodeId,
        req: RemoteQueryScannerNext,
    ) -> Result<RemoteQueryScannerNextResult, DataFusionError> {
        self.networking
            .call_rpc(peer, Swimlane::default(), req, None, None)
            .in_tc_as_task(
                &self.task_center,
                TaskKind::InPlace,
                "RemoteScannerServiceProxy::next_batch",
            )
            .await
            .map_err(|e| DataFusionError::External(e.into()))
    }

    fn close(&self, peer: NodeId, req: RemoteQueryScannerClose) {
        // why not spawn_unmanaged? because spawn_unmanaged will reject spawns
        // if we are shutting down. Yet, we still want to close the remote scanner
        // if we can. If we already have a connection, we _might_ be able to send
        // the request still.
        let scanner_id = req.scanner_id;
        let target_node_id = peer;
        let networking = self.networking.clone();
        tokio::spawn(
            async move {
                if let Err(e) = networking
                    .call_rpc(peer, Swimlane::default(), req, None, None)
                    .await
                {
                    info!(
                        "Unable to close the scanner {scanner_id} at {target_node_id} due to {e}",
                    );
                }
            }
            .in_tc_as_task(&self.task_center, TaskKind::Disposable, "df-remote-close"),
        );
    }
}
