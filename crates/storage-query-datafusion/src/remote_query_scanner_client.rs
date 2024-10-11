// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{decode_record_batch, encode_schema};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use restate_core::network::rpc_router::RpcRouter;
use restate_core::network::{MessageRouterBuilder, Networking, TransportConnect};
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::net::remote_query_scanner::{
    RemoteQueryScannerClose, RemoteQueryScannerClosed, RemoteQueryScannerNext,
    RemoteQueryScannerNextResult, RemoteQueryScannerOpen, RemoteQueryScannerOpened,
};
use restate_types::NodeId;
use std::fmt::{Debug, Formatter};
use std::ops::RangeInclusive;
use std::sync::Arc;
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
    router_builder: &mut MessageRouterBuilder,
) -> Arc<dyn RemoteScannerService> {
    Arc::new(RemoteScannerServiceProxy::new(network, router_builder))
}

// ----- datafusion remote scan -----

/// Given an implementation of a remote ScannerService, this function
/// creates a Datafusin's [[SendableRecordBatchStream]] that transports
/// record batches via the RemoteScannerService API.
pub fn remote_scan_as_datafusion_stream(
    svc: Arc<dyn RemoteScannerService>,
    target_node_id: NodeId,
    partition_id: PartitionId,
    range: RangeInclusive<PartitionKey>,
    table_name: String,
    projection_schema: SchemaRef,
) -> SendableRecordBatchStream {
    RemoteScannerHandle::remote_scan(
        svc,
        target_node_id,
        partition_id,
        range,
        table_name,
        projection_schema,
    )
}

// ----- everything below is the client side implementation details -----

#[derive(Clone)]
struct RemoteScannerServiceProxy<T> {
    networking: Networking<T>,
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
    fn new(networking: Networking<T>, router_builder: &mut MessageRouterBuilder) -> Self {
        Self {
            open_rpc: RpcRouter::new(router_builder),
            next_rpc: RpcRouter::new(router_builder),
            close_rpc: RpcRouter::new(router_builder),
            networking,
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
        let res = self
            .open_rpc
            .call(&self.networking, peer, req)
            .await
            .map_err(|e| DataFusionError::External(e.into()))?
            .into_body();
        Ok(res)
    }

    async fn next_batch(
        &self,
        peer: NodeId,
        req: RemoteQueryScannerNext,
    ) -> Result<RemoteQueryScannerNextResult, DataFusionError> {
        let res = self
            .next_rpc
            .call(&self.networking, peer, req)
            .await
            .map_err(|e| DataFusionError::External(e.into()))?
            .into_body();
        Ok(res)
    }

    async fn close(
        &self,
        peer: NodeId,
        req: RemoteQueryScannerClose,
    ) -> Result<RemoteQueryScannerClosed, DataFusionError> {
        let res = self
            .close_rpc
            .call(&self.networking, peer, req)
            .await
            .map_err(|e| DataFusionError::External(e.into()))?
            .into_body();
        Ok(res)
    }
}

// ----- RemoteScannerHandle ---

/// Represents an RPC based scanner
struct RemoteScannerHandle;

impl RemoteScannerHandle {
    fn remote_scan(
        svc: Arc<dyn RemoteScannerService>,
        target: NodeId,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        table: String,
        schema: SchemaRef,
    ) -> SendableRecordBatchStream {
        let mut builder = RecordBatchReceiverStream::builder(schema.clone(), 2);

        let tx = builder.tx();

        let task = async move {
            //
            // get a scanner id
            //
            let open_request = RemoteQueryScannerOpen {
                partition_id,
                range,
                table,
                projection_schema_bytes: encode_schema(&schema),
            };

            let RemoteQueryScannerOpened::Success { scanner_id } =
                svc.open(target, open_request).await?
            else {
                Err(DataFusionError::Internal(
                    "Unable to open a remote scanner".to_string(),
                ))?
            };

            //
            // loop while we have record_batch coming in
            //
            loop {
                let req = RemoteQueryScannerNext { scanner_id };
                let batch = match svc.next_batch(target, req).await? {
                    RemoteQueryScannerNextResult::NextBatch { record_batch, .. } => {
                        decode_record_batch(&record_batch)?
                    }
                    RemoteQueryScannerNextResult::Failure(_) => {
                        return Err(DataFusionError::Internal(
                            "Error during scanning".to_string(),
                        ));
                    }
                    RemoteQueryScannerNextResult::NoMoreRecords(_) => {
                        // assume server closed the server before responding
                        return Ok(());
                    }
                    RemoteQueryScannerNextResult::NoSuchScanner(_) => {
                        return Err(DataFusionError::Internal("No such scanner".to_string()));
                    }
                };

                let res = tx.send(Ok(batch)).await;
                if res.is_ok() {
                    continue;
                }
                //
                // tx is closed. which means datafusion is not interested in our records anymore
                // let us be good citizens and also close the remote scanner.
                return res
                    .map(|_| ())
                    .map_err(|e| DataFusionError::External(e.into()));
            }
        };

        builder.spawn(task);
        builder.build()
    }
}
