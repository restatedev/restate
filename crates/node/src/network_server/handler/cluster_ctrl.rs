// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use restate_core::MetadataWriter;
use tonic::{async_trait, Request, Response, Status};
use tracing::{debug, info};

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_server::ClusterCtrlSvc;
use restate_admin::cluster_controller::protobuf::{
    ClusterStateRequest, ClusterStateResponse, CreatePartitionSnapshotRequest,
    CreatePartitionSnapshotResponse, DescribeLogRequest, DescribeLogResponse, FindTailRequest,
    FindTailResponse, ListLogsRequest, ListLogsResponse, ListNodesRequest, ListNodesResponse,
    SealAndExtendChainRequest, SealAndExtendChainResponse, SealedSegment, TailState,
    TrimLogRequest,
};
use restate_admin::cluster_controller::ClusterControllerHandle;
use restate_bifrost::{Bifrost, BifrostAdmin, Error as BiforstError};
use restate_metadata_store::MetadataStoreClient;
use restate_types::identifiers::PartitionId;
use restate_types::logs::metadata::{Logs, ProviderKind, SegmentIndex};
use restate_types::logs::{LogId, Lsn, SequenceNumber};
use restate_types::metadata_store::keys::{BIFROST_CONFIG_KEY, NODES_CONFIG_KEY};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::storage::{StorageCodec, StorageEncode};
use restate_types::{Version, Versioned};

use crate::network_server::ClusterControllerDependencies;

pub struct ClusterCtrlSvcHandler {
    metadata_store_client: MetadataStoreClient,
    controller_handle: ClusterControllerHandle,
    bifrost_handle: Bifrost,
    metadata_writer: MetadataWriter,
}

impl ClusterCtrlSvcHandler {
    pub fn new(admin_deps: ClusterControllerDependencies) -> Self {
        Self {
            controller_handle: admin_deps.cluster_controller_handle,
            metadata_store_client: admin_deps.metadata_store_client,
            bifrost_handle: admin_deps.bifrost_handle,
            metadata_writer: admin_deps.metadata_writer,
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

        let (trim_point, nodes_config) = tokio::join!(
            self.bifrost_handle.get_trim_point(log_id),
            self.metadata_store_client
                .get::<NodesConfiguration>(NODES_CONFIG_KEY.clone()),
        );

        let trim_point = trim_point
            .map_err(|err| Status::internal(format!("Failed to find log trim point: {:?}", err)))?;
        debug!(?log_id, ?trim_point, "Retrieved log information");

        let nodes_config = nodes_config
            .map_err(|error| {
                Status::unknown(format!(
                    "Failed to get nodes configuration metadata: {:?}",
                    error
                ))
            })?
            .ok_or(Status::not_found("Missing nodes configuration"))?;

        Ok(Response::new(DescribeLogResponse {
            log_id: log_id.into(),
            logs_version: logs.version().into(),
            chain: serialize_value(chain),
            tail_state: 0, // TailState_UNKNOWN
            tail_offset: Lsn::INVALID.as_u64(),
            trim_point: trim_point.as_u64(),
            nodes_configuration: serialize_value(nodes_config),
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

    async fn seal_and_extend_chain(
        &self,
        request: Request<SealAndExtendChainRequest>,
    ) -> Result<Response<SealAndExtendChainResponse>, Status> {
        let admin = BifrostAdmin::new(
            &self.bifrost_handle,
            &self.metadata_writer,
            &self.metadata_store_client,
        );

        let request = request.into_inner();
        let kind: ProviderKind = request
            .provider
            .parse()
            .map_err(|_| Status::invalid_argument("Provider type is not supported"))?;

        let sealed_segment = admin
            .seal_and_extend_chain(
                request.log_id.into(),
                request.segment_index.map(SegmentIndex::from),
                request
                    .min_version
                    .map(Version::from)
                    .unwrap_or_else(|| Version::MIN),
                kind,
                request.params.into(),
            )
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(SealAndExtendChainResponse {
            new_segment_index: sealed_segment.segment_index.next().into(),
            sealed_segment: Some(SealedSegment {
                provider: sealed_segment.provider.to_string(),
                tail_offset: sealed_segment.tail.offset().into(),
                params: sealed_segment.params.to_string(),
            }),
        }))
    }

    async fn find_tail(
        &self,
        request: Request<FindTailRequest>,
    ) -> Result<Response<FindTailResponse>, Status> {
        let request = request.into_inner();
        let log_id: LogId = request.log_id.into();

        let admin = BifrostAdmin::new(
            &self.bifrost_handle,
            &self.metadata_writer,
            &self.metadata_store_client,
        );

        let writable_loglet = admin
            .writeable_loglet(log_id)
            .await
            .map_err(|err| match err {
                BiforstError::UnknownLogId(_) => Status::invalid_argument("Unknown log-id"),
                err => Status::internal(err.to_string()),
            })?;

        let tail_state = tokio::time::timeout(Duration::from_secs(2), writable_loglet.find_tail())
            .await
            .map_err(|_elapsed| Status::deadline_exceeded("Timedout finding tail"))?
            .map_err(|err| Status::internal(err.to_string()))?;

        let response = FindTailResponse {
            segment_index: writable_loglet.segment_index().into(),
            tail_state: if tail_state.is_sealed() {
                TailState::Sealed
            } else {
                TailState::Open
            }
            .into(),
            log_id: log_id.into(),
            tail_lsn: tail_state.offset().into(),
        };

        Ok(Response::new(response))
    }
}

fn serialize_value<T: StorageEncode>(value: T) -> Bytes {
    let mut buf = BytesMut::new();
    StorageCodec::encode(&value, &mut buf).expect("We can always serialize");
    buf.freeze()
}
