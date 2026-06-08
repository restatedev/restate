// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr_common::physical_expr::snapshot_generation;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use tracing::debug;

use restate_core::network::{Connection, NetworkSender, Networking, Swimlane, TransportConnect};
use restate_core::{TaskCenter, TaskCenterFutureExt, TaskKind, task_center};
use restate_types::NodeId;
use restate_types::identifiers::PartitionId;
use restate_types::net::remote_query_scanner::{
    RemoteQueryScannerClose, RemoteQueryScannerNext, RemoteQueryScannerNextResult,
    RemoteQueryScannerOpen, RemoteQueryScannerOpened, RemoteQueryScannerPredicate, ScannerBatch,
    ScannerFailure, ScannerId,
};
use restate_types::sharding::KeyRange;

use crate::{decode_record_batch, encode_expr, encode_schema};

#[derive(derive_more::Debug)]
pub struct RemoteScanner {
    scanner_id: ScannerId,
    connection: Option<Connection>,
}

impl RemoteScanner {
    /// Constructs a scanner that owns `connection` for the purpose of sending
    /// `Close` on drop. Use this to install a drop-guard *before* sending
    /// `Open`: if the caller's future is cancelled (or the proxy returns
    /// `Err`) after `Open` reaches the wire, the existing `Drop` impl emits
    /// `Close` so the server doesn't keep an orphan scanner until TTL.
    pub fn new(scanner_id: ScannerId, connection: Connection) -> Self {
        Self {
            scanner_id,
            connection: Some(connection),
        }
    }

    async fn next_batch(
        &mut self,
        next_predicate: Option<RemoteQueryScannerPredicate>,
    ) -> Result<RemoteQueryScannerNextResult, DataFusionError> {
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
                    next_predicate,
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
///
/// `scanner_id` is allocated by the caller (typically via
/// [`RemoteScannerManager::allocate_scanner_id`]) so the server can adopt the
/// caller's id instead of minting its own.
#[allow(clippy::too_many_arguments)]
pub fn remote_scan_as_datafusion_stream(
    service: Arc<dyn RemoteScannerService>,
    target_node_id: NodeId,
    scanner_id: ScannerId,
    partition_id: PartitionId,
    range: KeyRange,
    table_name: String,
    projection_schema: SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    batch_size: usize,
    limit: Option<usize>,
) -> SendableRecordBatchStream {
    let mut builder = RecordBatchReceiverStream::builder(projection_schema.clone(), 1);

    let tx = builder.tx();

    let task = async move {
        // get a snapshot of the initial predicate
        let mut predicate_generation = predicate.as_ref().map(snapshot_generation).unwrap_or(0);

        let initial_predicate = match &predicate {
            Some(predicate) => Some(RemoteQueryScannerPredicate {
                serialized_physical_expression: encode_expr(predicate)?,
            }),
            None => None,
        };

        let open_request = RemoteQueryScannerOpen {
            scanner_id: Some(scanner_id),
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
        let mut remote_scanner = service.open(target_node_id, open_request).await?;
        // loop while we have record_batch coming in
        //
        loop {
            let next_predicate = next_predicate(&mut predicate_generation, predicate.as_ref())?;

            let batch = match remote_scanner.next_batch(next_predicate).await {
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

fn next_predicate(
    predicate_generation: &mut u64,
    predicate: Option<&Arc<dyn PhysicalExpr>>,
) -> Result<Option<RemoteQueryScannerPredicate>, DataFusionError> {
    if *predicate_generation != 0 {
        // generation 0 means the predicate is static (or we never had one)
        let predicate = predicate.ok_or(DataFusionError::Internal(
            "Missing predicate despite non-zero predicate generation".into(),
        ))?;
        let current_predicate_generation = snapshot_generation(predicate);

        if current_predicate_generation != *predicate_generation {
            *predicate_generation = current_predicate_generation;
            Ok(Some(RemoteQueryScannerPredicate {
                serialized_physical_expression: encode_expr(predicate)?,
            }))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
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
            .get_connection(peer, Swimlane::Datafusion)
            .in_tc_as_task(
                &self.task_center,
                TaskKind::InPlace,
                "RemoteScannerServiceProxy::open",
            )
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;

        // We always set the client minted scanner-id
        let scanner_id = req.scanner_id.unwrap();

        // Reserve and send Open. `send_rpc` is synchronous after the permit
        // is in hand — by the time it returns the message is queued on the
        // egress and the server is committed to seeing it.
        let open_permit = connection.reserve().await.ok_or_else(|| {
            DataFusionError::External(
                anyhow::anyhow!("cannot open remote scanner; connection lost to {peer}").into(),
            )
        })?;
        let open_reply = open_permit
            .send_rpc(req, None)
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;

        // From here on we must guarantee a `Close` reaches the server if we
        // don't hand a `RemoteScanner` back to the caller — otherwise the
        // scanner the server is about to create sits orphaned until TTL.
        // Pre-constructing the scanner installs its own `Drop` as the guard;
        // it fires `Close` on cancellation or any `Err` return below.
        // On `Failure` we disarm via `.forget()` so we don't accidentally close a scanner
        // that another caller holds under the same id.
        let mut remote_scanner = RemoteScanner::new(scanner_id, connection.clone());

        match open_reply.await {
            Ok(RemoteQueryScannerOpened::Success { scanner_id }) => {
                // Server is running Restate <v1.7 so we need to respect
                // the returned scanner_id
                if remote_scanner.scanner_id != scanner_id {
                    remote_scanner.forget();
                    remote_scanner = RemoteScanner::new(scanner_id, connection.clone())
                }
                Ok(remote_scanner)
            }
            Ok(RemoteQueryScannerOpened::Failure) => {
                remote_scanner.forget();
                Err(DataFusionError::Internal(
                    "Unable to open a remote scanner".to_string(),
                ))
            }
            Err(e) => Err(DataFusionError::External(e.into())),
        }
    }
}
