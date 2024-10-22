// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Bytes, BytesMut};
use tonic::{async_trait, Request, Response, Status};
use tracing::{debug, info};

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_server::ClusterCtrlSvc;
use restate_admin::cluster_controller::protobuf::{
    ClusterStateRequest, ClusterStateResponse, CreatePartitionSnapshotRequest,
    CreatePartitionSnapshotResponse, DescribeLogRequest, DescribeLogResponse, ListLogsRequest,
    ListLogsResponse, ListNodesRequest, ListNodesResponse, TrimLogRequest,
};
use restate_admin::cluster_controller::ClusterControllerHandle;
use restate_bifrost::{Bifrost, FindTailAttributes};
use restate_metadata_store::MetadataStoreClient;
use restate_types::identifiers::PartitionId;
use restate_types::logs::metadata::Logs;
use restate_types::logs::{LogId, Lsn};
use restate_types::metadata_store::keys::{BIFROST_CONFIG_KEY, NODES_CONFIG_KEY};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::storage::{StorageCodec, StorageEncode};
use restate_types::Versioned;

use crate::network_server::AdminDependencies;

pub struct ClusterCtrlSvcHandler {
    metadata_store_client: MetadataStoreClient,
    controller_handle: ClusterControllerHandle,
    bifrost_handle: Bifrost,
}

impl ClusterCtrlSvcHandler {
    pub fn new(admin_deps: AdminDependencies) -> Self {
        Self {
            controller_handle: admin_deps.cluster_controller_handle,
            metadata_store_client: admin_deps.metadata_store_client,
            bifrost_handle: admin_deps.bifrost_handle,
        }
    }

    async fn get_logs(&self) -> Result<Logs, Status> {
        self.metadata_store_client
            .get::<Logs>(BIFROST_CONFIG_KEY.clone())
            .await
            .map_err(|error| Status::unknown(format!("Failed to get log metadata: {:?}", error)))?
            .ok_or(Status::not_found("Missing log metadata"))
    }
}

#[async_trait]
impl ClusterCtrlSvc for ClusterCtrlSvcHandler {
    async fn get_cluster_state(
        &self,
        _request: Request<ClusterStateRequest>,
    ) -> Result<Response<ClusterStateResponse>, Status> {
        let cluster_state = self
            .controller_handle
            .get_cluster_state()
            .await
            .map_err(|_| Status::aborted("Node is shutting down"))?;

        let resp = ClusterStateResponse {
            cluster_state: Some((*cluster_state).clone().into()),
        };
        Ok(Response::new(resp))
    }

    async fn list_logs(
        &self,
        _request: Request<ListLogsRequest>,
    ) -> Result<Response<ListLogsResponse>, Status> {
        Ok(Response::new(ListLogsResponse {
            logs: serialize_value(self.get_logs().await?),
        }))
    }

    async fn describe_log(
        &self,
        request: Request<DescribeLogRequest>,
    ) -> Result<Response<DescribeLogResponse>, Status> {
        let request = request.into_inner();

        let log_id = LogId::new(request.log_id);
        let logs = self.get_logs().await?;

        let chain = logs
            .chain(&log_id)
            .ok_or(Status::not_found(format!(
                "Log id {} not found",
                request.log_id
            )))?
            .clone();

        let (tail_state, trim_point) = tokio::join!(
            self.bifrost_handle
                .find_tail(log_id, FindTailAttributes::default()),
            self.bifrost_handle.get_trim_point(log_id)
        );

        let tail_state = tail_state
            .map_err(|err| Status::internal(format!("Failed to find log tail: {:?}", err)))?;
        let trim_point = trim_point
            .map_err(|err| Status::internal(format!("Failed to find log trim point: {:?}", err)))?;
        debug!(
            ?log_id,
            ?trim_point,
            ?tail_state,
            "Retrieved log information"
        );

        let mut tail_segment = chain.tail();
        if tail_segment.tail_lsn.is_none() {
            tail_segment.tail_lsn = Some(tail_state.offset());
        }

        chain.iter().last().as_mut().unwrap().tail_lsn = tail_segment.tail_lsn;

        Ok(Response::new(DescribeLogResponse {
            log_id: log_id.into(),
            logs_version: logs.version().into(),
            chain: serialize_value(chain),
            tail_state: match tail_state {
                restate_types::logs::TailState::Open(_) => 1,
                restate_types::logs::TailState::Sealed(_) => 2,
            },
            tail_offset: tail_state.offset().as_u64(),
            trim_point: trim_point.as_u64(),
        }))
    }

    async fn list_nodes(
        &self,
        _request: Request<ListNodesRequest>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        let nodes_config = self
            .metadata_store_client
            .get::<NodesConfiguration>(NODES_CONFIG_KEY.clone())
            .await
            .map_err(|error| {
                Status::unknown(format!(
                    "Failed to get nodes configuration metadata: {:?}",
                    error
                ))
            })?
            .ok_or(Status::not_found("Missing nodes configuration"))?;

        Ok(Response::new(ListNodesResponse {
            nodes_configuration: serialize_value(nodes_config),
        }))
    }

    /// Internal operations API to trigger the log truncation
    async fn trim_log(&self, request: Request<TrimLogRequest>) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let log_id = LogId::from(request.log_id);
        let trim_point = Lsn::from(request.trim_point);
        if let Err(err) = self
            .controller_handle
            .trim_log(log_id, trim_point)
            .await
            .map_err(|_| Status::aborted("Node is shutting down"))?
        {
            info!("Failed trimming the log: {err}");
            return Err(Status::internal(err.to_string()));
        }
        Ok(Response::new(()))
    }

    /// Handles ad-hoc snapshot requests, as sent by `restatectl snapshots create`. This is
    /// implemented as an RPC call within the cluster to a worker node hosting the partition.
    async fn create_partition_snapshot(
        &self,
        request: Request<CreatePartitionSnapshotRequest>,
    ) -> Result<Response<CreatePartitionSnapshotResponse>, Status> {
        let request = request.into_inner();
        let partition_id = PartitionId::from(
            u16::try_from(request.partition_id)
                .map_err(|id| Status::invalid_argument(format!("Invalid partition id: {id}")))?,
        );

        match self
            .controller_handle
            .create_partition_snapshot(partition_id)
            .await
            .map_err(|_| Status::aborted("Node is shutting down"))?
        {
            Err(err) => {
                info!("Failed creating partition snapshot: {err}");
                Err(Status::internal(err.to_string()))
            }
            Ok(snapshot_id) => Ok(Response::new(CreatePartitionSnapshotResponse {
                snapshot_id: snapshot_id.to_string(),
            })),
        }
    }
}

fn serialize_value<T: StorageEncode>(value: T) -> Bytes {
    let mut buf = BytesMut::new();
    StorageCodec::encode(&value, &mut buf).expect("We can always serialize");
    buf.freeze()
}
