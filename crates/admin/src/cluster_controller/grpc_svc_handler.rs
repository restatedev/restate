// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Bytes, BytesMut};
use tonic::{Request, Response, Status, async_trait};
use tracing::info;

use restate_bifrost::loglet::FindTailOptions;
use restate_bifrost::{Bifrost, Error as BiforstError};
use restate_core::{Metadata, MetadataWriter};
use restate_types::identifiers::PartitionId;
use restate_types::logs::metadata::{Logs, SegmentIndex};
use restate_types::logs::{LogId, Lsn, SequenceNumber};
use restate_types::metadata_store::keys::{BIFROST_CONFIG_KEY, NODES_CONFIG_KEY};
use restate_types::net::partition_processor_manager::Snapshot;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::protobuf::cluster::ClusterConfiguration;
use restate_types::storage::{StorageCodec, StorageEncode};
use restate_types::{PlainNodeId, Version, Versioned};

use crate::cluster_controller::protobuf::cluster_ctrl_svc_server::ClusterCtrlSvc;
use crate::cluster_controller::protobuf::{
    ClusterStateRequest, ClusterStateResponse, CreatePartitionSnapshotRequest,
    CreatePartitionSnapshotResponse, DescribeLogRequest, DescribeLogResponse, FindTailRequest,
    FindTailResponse, ListLogsRequest, ListLogsResponse, SealAndExtendChainRequest,
    SealAndExtendChainResponse, SealedSegment, TailState, TrimLogRequest,
};

use super::ClusterControllerHandle;
use super::protobuf::{
    GetClusterConfigurationRequest, GetClusterConfigurationResponse,
    SetClusterConfigurationRequest, SetClusterConfigurationResponse,
};
use super::service::ChainExtension;

pub(crate) struct ClusterCtrlSvcHandler {
    controller_handle: ClusterControllerHandle,
    bifrost: Bifrost,
    metadata_writer: MetadataWriter,
}

impl ClusterCtrlSvcHandler {
    pub fn new(
        controller_handle: ClusterControllerHandle,
        bifrost: Bifrost,
        metadata_writer: MetadataWriter,
    ) -> Self {
        Self {
            controller_handle,
            bifrost,
            metadata_writer,
        }
    }

    async fn get_logs(&self) -> Result<Logs, Status> {
        self.metadata_writer
            .metadata_store_client()
            .get::<Logs>(BIFROST_CONFIG_KEY.clone())
            .await
            .map_err(|error| Status::unknown(format!("Failed to get log metadata: {error:?}")))?
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
            self.bifrost.get_trim_point(log_id),
            self.metadata_writer
                .metadata_store_client()
                .get::<NodesConfiguration>(NODES_CONFIG_KEY.clone()),
        );

        let trim_point = trim_point
            .map_err(|err| Status::internal(format!("Failed to find log trim point: {err:?}")))?;

        let nodes_config = nodes_config
            .map_err(|error| {
                Status::unknown(format!(
                    "Failed to get nodes configuration metadata: {error:?}"
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
            .create_partition_snapshot(
                partition_id,
                request.min_target_lsn.map(Into::into),
                request.trim_log,
            )
            .await
            .map_err(|_| Status::aborted("Node is shutting down"))?
        {
            Err(err) => {
                info!("Failed creating partition snapshot: {err}");
                Err(Status::internal(err.to_string()))
            }
            Ok(Snapshot {
                snapshot_id,
                log_id,
                min_applied_lsn,
            }) => Ok(Response::new(CreatePartitionSnapshotResponse {
                snapshot_id: snapshot_id.to_string(),
                log_id: log_id.into(),
                min_applied_lsn: min_applied_lsn.as_u64(),
            })),
        }
    }

    async fn seal_and_extend_chain(
        &self,
        request: Request<SealAndExtendChainRequest>,
    ) -> Result<Response<SealAndExtendChainResponse>, Status> {
        let request = request.into_inner();

        let extension = match request.extension {
            Some(ext) => {
                if !ext.params.is_empty() {
                    // `params`` is no longer supported. It's better to fail loudly
                    // than act unexpectedly on invalid request version
                    return Err(Status::invalid_argument(
                        "Detected a deprecated argument. Please upgrade to the latest version of restatectl tool to ensure compatibility.",
                    ));
                }

                Some(ChainExtension {
                    segment_index_to_seal: ext.segment_index.map(SegmentIndex::from),

                    provider_kind: ext
                        .provider
                        .parse()
                        .map_err(|_| Status::invalid_argument("Provider type is not supported"))?,

                    nodeset: if !ext.nodeset.is_empty() {
                        Some(
                            ext.nodeset
                                .iter()
                                .map(|node_id| PlainNodeId::new(node_id.id))
                                .collect(),
                        )
                    } else {
                        None
                    },
                    sequencer: ext.sequencer.map(Into::into),
                    replication: ext
                        .replication
                        .map(|p| p.try_into())
                        .transpose()
                        .map_err(|_| Status::invalid_argument("Invalid replication property"))?,
                })
            }
            None => None,
        };

        let sealed_segment = self
            .controller_handle
            .seal_and_extend_chain(
                request.log_id.into(),
                request
                    .min_version
                    .map(Version::from)
                    .unwrap_or_else(|| Version::MIN),
                extension,
            )
            .await
            .map_err(|_| Status::aborted("Node is shutting down"))?
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

        let writable_loglet = self
            .bifrost
            .admin()
            .writeable_loglet(log_id)
            .await
            .map_err(|err| match err {
                BiforstError::UnknownLogId(_) => Status::invalid_argument("Unknown log-id"),
                err => Status::internal(err.to_string()),
            })?;

        let tail_state = writable_loglet
            .find_tail(FindTailOptions::default())
            .await
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

    async fn get_cluster_configuration(
        &self,
        _request: tonic::Request<GetClusterConfigurationRequest>,
    ) -> Result<Response<GetClusterConfigurationResponse>, Status> {
        let logs = Metadata::with_current(|m| m.logs_ref());
        let partition_table = Metadata::with_current(|m| m.partition_table_ref());

        let response = GetClusterConfigurationResponse {
            cluster_configuration: Some(ClusterConfiguration {
                num_partitions: u32::from(partition_table.num_partitions()),
                partition_replication: partition_table.partition_replication().clone().into(),
                bifrost_provider: Some(logs.configuration().default_provider.clone().into()),
            }),
        };

        Ok(Response::new(response))
    }

    async fn set_cluster_configuration(
        &self,
        request: Request<SetClusterConfigurationRequest>,
    ) -> Result<Response<SetClusterConfigurationResponse>, Status> {
        let request = request.into_inner();
        let request = request
            .cluster_configuration
            .ok_or_else(|| Status::invalid_argument("cluster_configuration is a required field"))?;

        self.controller_handle
            .update_cluster_configuration(
                request.partition_replication.try_into().map_err(|err| {
                    Status::invalid_argument(format!("invalid partition_replication: {err}"))
                })?,
                request
                    .bifrost_provider
                    .ok_or_else(|| {
                        Status::invalid_argument("default_provider is a required field")
                    })?
                    .try_into()
                    .map_err(|err| {
                        Status::invalid_argument(format!("invalid default_provider: {err}"))
                    })?,
            )
            .await
            .map_err(|_| Status::aborted("Node is shutting down"))?
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(SetClusterConfigurationResponse {}))
    }
}

fn serialize_value<T: StorageEncode>(value: T) -> Bytes {
    let mut buf = BytesMut::new();
    StorageCodec::encode(&value, &mut buf).expect("We can always serialize");
    buf.freeze()
}
