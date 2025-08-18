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
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use tracing::debug;

use restate_core::network::{Connection, NetworkSender, Networking, Swimlane, TransportConnect};
use restate_core::{TaskCenter, TaskCenterFutureExt, TaskKind, task_center};
use restate_types::NodeId;
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::net::remote_query_scanner::{
    RemoteQueryScannerClose, RemoteQueryScannerNext, RemoteQueryScannerNextResult,
    RemoteQueryScannerOpen, RemoteQueryScannerOpened, RemoteQueryScannerPredicate, ScannerBatch,
    ScannerFailure, ScannerId,
};

use crate::{decode_record_batch, encode_expr, encode_schema};

#[derive(Debug, Clone)]
pub struct RemoteScanner {
    scanner_id: ScannerId,
    connection: Option<Connection>,
}

impl RemoteScanner {
    pub fn new(scanner_id: ScannerId, connection: Connection) -> Self {
        Self {
            scanner_id,
            connection: Some(connection),
        }
    }

    async fn next_batch(&self) -> Result<RemoteQueryScannerNextResult, DataFusionError> {
        let Some(ref connection) = self.connection else {
            return Err(DataFusionError::Internal(
                "connection used after forget()".to_string(),
            ));
        };
        let peer = connection.peer();
        let permit = connection.reserve().await.ok_or_else(|| {
            DataFusionError::External(
                anyhow::anyhow!(
                    "remote scanner {} connection lost to {peer}",
                    self.scanner_id
                )
                .into(),
            )
        })?;

        let reply = permit
            .send_rpc(
                RemoteQueryScannerNext {
                    scanner_id: self.scanner_id,
                },
                None,
            )
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;

        reply.await.map_err(|e| DataFusionError::External(e.into()))
    }

    /// The scanner will not auto close the remote scanner on drop
    pub fn forget(mut self) {
        self.connection.take();
    }
}

impl Drop for RemoteScanner {
    fn drop(&mut self) {
        let scanner_id = self.scanner_id;
        if let Some(connection) = self.connection.take() {
            tokio::spawn(async move {
                let Some(permit) = connection.reserve().await else {
                    return;
                };
                debug!(
                    "Closing remote scanner {scanner_id} remotely for {}",
                    connection.peer()
                );
                // Ideally, this should be a unary call, but to maintain compatibility
                // with previous version we keep this as rpc.
                // todo (lo-pri): migrate this to a unary call.
                let Ok(reply) = permit.send_rpc(RemoteQueryScannerClose { scanner_id }, None)
                else {
                    return;
                };

                let _ = reply.await;
            });
        }
    }
}

// ----- rpc service definition -----

#[async_trait]
pub trait RemoteScannerService: Send + Sync + Debug + 'static {
    async fn open(
        &self,
        peer: NodeId,
        req: RemoteQueryScannerOpen,
    ) -> Result<RemoteScanner, DataFusionError>;
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

/// Given an implementation of a remote ScannerService, this function
/// creates a DataFusion [[SendableRecordBatchStream]] that transports
/// record batches via the RemoteScannerService API.
#[allow(clippy::too_many_arguments)]
pub fn remote_scan_as_datafusion_stream(
    service: Arc<dyn RemoteScannerService>,
    target_node_id: NodeId,
    partition_id: PartitionId,
    range: RangeInclusive<PartitionKey>,
    table_name: String,
    projection_schema: SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    batch_size: usize,
    limit: Option<usize>,
) -> SendableRecordBatchStream {
    let mut builder = RecordBatchReceiverStream::builder(projection_schema.clone(), 1);

    let tx = builder.tx();

    let task = async move {
        let initial_predicate = match &predicate {
            Some(predicate) => Some(RemoteQueryScannerPredicate {
                serialized_physical_expression: encode_expr(predicate)?,
            }),
            None => None,
        };

        //
        // get a scanner id
        //
        let open_request = RemoteQueryScannerOpen {
            partition_id,
            range,
            table: table_name,
            projection_schema_bytes: encode_schema(&projection_schema),
            limit: limit.map(|limit| u64::try_from(limit).expect("limit to fit in a u64")),
            predicate: initial_predicate,
            batch_size: u64::try_from(batch_size).expect("batch_size to fit in a u64"),
        };

        // RemoteScanner will auto close on drop. Please call forget() if you don't need this
        // behaviour.
        let remote_scanner = service.open(target_node_id, open_request).await?;
        // loop while we have record_batch coming in
        //
        loop {
            let batch = match remote_scanner.next_batch().await {
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
                    remote_scanner.forget();
                    return Err(DataFusionError::Internal(message));
                }
                Ok(RemoteQueryScannerNextResult::NoMoreRecords(_)) => {
                    // assume server closed the scanner before responding
                    remote_scanner.forget();
                    return Ok(());
                }
                Ok(RemoteQueryScannerNextResult::NoSuchScanner(_)) => {
                    remote_scanner.forget();
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
    ) -> Result<RemoteScanner, DataFusionError> {
        let connection = self
            .networking
            .get_connection(peer, Swimlane::default())
            .in_tc_as_task(
                &self.task_center,
                TaskKind::InPlace,
                "RemoteScannerServiceProxy::open",
            )
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;

        let permit = connection.reserve().await.ok_or_else(|| {
            DataFusionError::External(
                anyhow::anyhow!("cannot open remote scanner; connection lost to {peer}").into(),
            )
        })?;

        let reply = permit
            .send_rpc(req, None)
            // codec error is an internal error
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;

        match reply.await {
            Ok(RemoteQueryScannerOpened::Success { scanner_id }) => {
                Ok(RemoteScanner::new(scanner_id, connection))
            }
            Ok(RemoteQueryScannerOpened::Failure) => Err(DataFusionError::Internal(
                "Unable to open a remote scanner".to_string(),
            )),
            Err(e) => Err(DataFusionError::External(e.into())),
        }
    }
}
