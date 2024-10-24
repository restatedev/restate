// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use crate::{decode_record_batch, encode_schema};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use restate_core::network::rpc_router::RpcRouter;
use restate_core::network::{Incoming, MessageRouterBuilder, Networking, TransportConnect};
use restate_core::TaskCenter;
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::net::remote_query_scanner::{
    RemoteQueryScannerClose, RemoteQueryScannerClosed, RemoteQueryScannerNext,
    RemoteQueryScannerNextResult, RemoteQueryScannerOpen, RemoteQueryScannerOpened,
};
use restate_types::NodeId;
use tracing::warn;

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

    async fn close(
        &self,
        peer: NodeId,
        req: RemoteQueryScannerClose,
    ) -> Result<RemoteQueryScannerClosed, DataFusionError>;
}

// ----- service proxy -----
pub fn create_remote_scanner_service<T: TransportConnect>(
    network: Networking<T>,
    task_center: TaskCenter,
    router_builder: &mut MessageRouterBuilder,
) -> Arc<dyn RemoteScannerService> {
    Arc::new(RemoteScannerServiceProxy::new(
        network,
        task_center,
        router_builder,
    ))
}

// ----- datafusion remote scan -----

/// Given an implementation of a remote ScannerService, this function
/// creates a Datafusion's [[SendableRecordBatchStream]] that transports
/// record batches via the RemoteScannerService API.
pub fn remote_scan_as_datafusion_stream(
    service: Arc<dyn RemoteScannerService>,
    target_node_id: NodeId,
    partition_id: PartitionId,
    range: RangeInclusive<PartitionKey>,
    table_name: String,
    projection_schema: SchemaRef,
) -> SendableRecordBatchStream {
    let mut builder = RecordBatchReceiverStream::builder(projection_schema.clone(), 2);

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
        };

        let RemoteQueryScannerOpened::Success { scanner_id } =
            service.open(target_node_id, open_request).await?
        else {
            Err(DataFusionError::Internal(
                "Unable to open a remote scanner".to_string(),
            ))?
        };

        let closer = service.clone();
        let close_fn = move || async move {
            if let Err(close_err) = closer
                .close(target_node_id, RemoteQueryScannerClose { scanner_id })
                .await
            {
                warn!(
                    "Unable to close the scanner {} at {} due to {}",
                    scanner_id, target_node_id, close_err
                );
            }
        };

        //
        // loop while we have record_batch coming in
        //
        loop {
            let req = RemoteQueryScannerNext { scanner_id };
            let batch = match service.next_batch(target_node_id, req).await {
                Err(e) => {
                    // RPC error. let's try to close the scanner.
                    close_fn().await;
                    return Err(e);
                }
                Ok(RemoteQueryScannerNextResult::NextBatch { record_batch, .. }) => {
                    decode_record_batch(&record_batch)?
                }
                Ok(RemoteQueryScannerNextResult::Failure { message, .. }) => {
                    // assume server closed the scanner before responding
                    return Err(DataFusionError::Internal(message));
                }
                Ok(RemoteQueryScannerNextResult::NoMoreRecords(_)) => {
                    // assume server closed the scanner before responding
                    return Ok(());
                }
                Ok(RemoteQueryScannerNextResult::NoSuchScanner(_)) => {
                    return Err(DataFusionError::Internal("No such scanner. It could have expired due to a long period of inactivity.".to_string()));
                }
            };

            let res = tx.send(Ok(batch)).await;
            if res.is_ok() {
                continue;
            }
            // tx is closed. which means datafusion is not interested in our records anymore
            // let us be good citizens and also close the remote scanner.
            // TODO(igal) consider spawning close in the background.
            close_fn().await;

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
    task_center: TaskCenter,
    open_rpc: RpcRouter<RemoteQueryScannerOpen>,
    next_rpc: RpcRouter<RemoteQueryScannerNext>,
    close_rpc: RpcRouter<RemoteQueryScannerClose>,
}

impl<T> Debug for RemoteScannerServiceProxy<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemoteScannerServiceProxy")
    }
}

impl<T: TransportConnect> RemoteScannerServiceProxy<T> {
    fn new(
        networking: Networking<T>,
        task_center: TaskCenter,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        Self {
            open_rpc: RpcRouter::new(router_builder),
            next_rpc: RpcRouter::new(router_builder),
            close_rpc: RpcRouter::new(router_builder),
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
        self.task_center
            .run_in_scope("RemoteScannerServiceProxy::open", None, async {
                self.open_rpc
                    .call(&self.networking, peer, req)
                    .await
                    .map_err(|e| DataFusionError::External(e.into()))
                    .map(Incoming::into_body)
            })
            .await
    }

    async fn next_batch(
        &self,
        peer: NodeId,
        req: RemoteQueryScannerNext,
    ) -> Result<RemoteQueryScannerNextResult, DataFusionError> {
        self.task_center
            .run_in_scope("RemoteScannerServiceProxy::next_batch", None, async {
                self.next_rpc
                    .call(&self.networking, peer, req)
                    .await
                    .map_err(|e| DataFusionError::External(e.into()))
                    .map(Incoming::into_body)
            })
            .await
    }

    async fn close(
        &self,
        peer: NodeId,
        req: RemoteQueryScannerClose,
    ) -> Result<RemoteQueryScannerClosed, DataFusionError> {
        self.task_center
            .run_in_scope("RemoteScannerServiceProxy::close", None, async {
                self.close_rpc
                    .call(&self.networking, peer, req)
                    .await
                    .map_err(|e| DataFusionError::External(e.into()))
                    .map(Incoming::into_body)
            })
            .await
    }
}
