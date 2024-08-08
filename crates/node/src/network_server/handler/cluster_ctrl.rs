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
use tracing::info;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_server::ClusterCtrlSvc;
use restate_admin::cluster_controller::protobuf::{
    ClusterStateRequest, ClusterStateResponse, DescribeLogRequest, DescribeLogResponse,
    ListLogsRequest, ListLogsResponse, TrimLogRequest,
};
use restate_admin::cluster_controller::ClusterControllerHandle;
use restate_bifrost::{Bifrost, FindTailAttributes};
use restate_metadata_store::MetadataStoreClient;
use restate_types::logs::metadata::Logs;
use restate_types::logs::{LogId, Lsn};
use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;
use restate_types::storage::{StorageCodec, StorageEncode};

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

    async fn logs(&self) -> Result<Logs, Status> {
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
            data: serialize_value(self.logs().await?),
        }))
    }

    async fn describe_log(
        &self,
        request: Request<DescribeLogRequest>,
    ) -> Result<Response<DescribeLogResponse>, Status> {
        let request = request.into_inner();

        let log_id = LogId::new(request.log_id);
        let chain = self
            .logs()
            .await?
            .chain(&log_id)
            .ok_or(Status::not_found(format!(
                "Log id {} not found",
                request.log_id
            )))?
            .clone();

        let tail_state = self
            .bifrost_handle
            .find_tail(log_id, FindTailAttributes::default())
            .await
            .map_err(|err| Status::internal(format!("Failed to find tail: {:?}", err)))?;

        info!("{:?} tail: {:?}", log_id, tail_state);

        let mut tail_segment = chain.tail();
        if tail_segment.tail_lsn.is_none() {
            tail_segment.tail_lsn = Some(tail_state.offset());
        }

        chain.iter().last().as_mut().unwrap().tail_lsn = tail_segment.tail_lsn;

        Ok(Response::new(DescribeLogResponse {
            chain: serialize_value(chain),
            tail_state: match tail_state {
                restate_bifrost::TailState::Open(_) => 1,
                restate_bifrost::TailState::Sealed(_) => 2,
            },
            tail_offset: tail_state.offset().as_u64(),
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
}

fn serialize_value<T: StorageEncode>(value: T) -> Bytes {
    let mut buf = BytesMut::new();
    StorageCodec::encode(value, &mut buf).expect("We can always serialize");
    buf.freeze()
}
