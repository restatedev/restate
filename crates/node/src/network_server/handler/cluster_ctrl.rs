// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};

use tonic::{async_trait, Request, Response, Status};
use tracing::info;

use restate_cluster_controller::ClusterControllerHandle;
use restate_cluster_controller::NodeState;
use restate_metadata_store::MetadataStoreClient;
use restate_node_services::cluster_ctrl::cluster_ctrl_svc_server::ClusterCtrlSvc;
use restate_node_services::cluster_ctrl::node_state;
use restate_node_services::cluster_ctrl::AliveNode;
use restate_node_services::cluster_ctrl::DeadNode;
use restate_node_services::cluster_ctrl::{
    ClusterStateRequest, ClusterStateResponse, TrimLogRequest,
};
use restate_types::identifiers::PartitionId;
use restate_types::logs::{LogId, Lsn};
use restate_types::processors::PartitionProcessorStatus;
use restate_types::processors::RunMode;
use restate_types::PlainNodeId;

use crate::network_server::AdminDependencies;

pub struct ClusterCtrlSvcHandler {
    _metadata_store_client: MetadataStoreClient,
    controller_handle: ClusterControllerHandle,
}

impl ClusterCtrlSvcHandler {
    pub fn new(admin_deps: AdminDependencies) -> Self {
        Self {
            controller_handle: admin_deps.cluster_controller_handle,
            _metadata_store_client: admin_deps.metadata_store_client,
        }
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
            .map_err(|_| tonic::Status::aborted("Node is shutting down"))?;

        let resp = ClusterStateResponse {
            last_refreshed: cluster_state
                .last_refreshed
                .and_then(|r| r.elapsed().try_into().ok()),
            nodes_config_version: Some(cluster_state.nodes_config_version.into()),
            nodes: to_protobuf_nodes(&cluster_state.nodes),
        };
        Ok(Response::new(resp))
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

fn to_protobuf_nodes(
    nodes: &BTreeMap<PlainNodeId, NodeState>,
) -> HashMap<u32, restate_node_services::cluster_ctrl::NodeState> {
    fn to_proto(node: &NodeState) -> restate_node_services::cluster_ctrl::NodeState {
        let state = Some(match node {
            NodeState::Alive {
                last_heartbeat_at,
                generation,
                partitions,
            } => {
                let alive_node = AliveNode {
                    last_heartbeat_at: Some((*last_heartbeat_at).into()),
                    generational_node_id: Some((*generation).into()),
                    partitions: to_protobuf_partitions(partitions),
                };
                node_state::State::Alive(alive_node)
            }
            NodeState::Dead { last_seen_alive } => {
                let dead_node = DeadNode {
                    last_seen_alive: last_seen_alive.map(Into::into),
                };
                node_state::State::Dead(dead_node)
            }
        });
        restate_node_services::cluster_ctrl::NodeState { state }
    }

    let mut out = HashMap::with_capacity(nodes.len());
    for (id, node) in nodes {
        out.insert((*id).into(), to_proto(node));
    }
    out
}

fn to_protobuf_partitions(
    pps: &BTreeMap<PartitionId, PartitionProcessorStatus>,
) -> HashMap<u64, restate_node_services::cluster_ctrl::PartitionProcessorStatus> {
    fn to_proto(
        pp: &PartitionProcessorStatus,
    ) -> restate_node_services::cluster_ctrl::PartitionProcessorStatus {
        let mut out = restate_node_services::cluster_ctrl::PartitionProcessorStatus::default();
        out.updated_at = Some(pp.updated_at.into());
        out.planned_mode = match pp.planned_mode {
            RunMode::Leader => restate_node_services::cluster_ctrl::RunMode::Leader as i32,
            RunMode::Follower => restate_node_services::cluster_ctrl::RunMode::Follower as i32,
        };
        out.effective_mode = pp.effective_mode.map(|m| match m {
            RunMode::Leader => restate_node_services::cluster_ctrl::RunMode::Leader as i32,
            RunMode::Follower => restate_node_services::cluster_ctrl::RunMode::Follower as i32,
        });
        out.last_observed_leader_epoch = pp.last_observed_leader_epoch.map(|e| e.into());
        out.last_observed_leader_node = pp.last_observed_leader_node.map(|e| e.into());
        out.last_applied_log_lsn = pp.last_applied_log_lsn.map(|l| l.into());
        out.last_record_applied_at = pp.last_record_applied_at.map(Into::into);
        out.num_skipped_records = pp.num_skipped_records;
        out.replay_status = match pp.replay_status {
            restate_types::processors::ReplayStatus::Starting => {
                restate_node_services::cluster_ctrl::ReplayStatus::Starting as i32
            }
            restate_types::processors::ReplayStatus::Active => {
                restate_node_services::cluster_ctrl::ReplayStatus::Active as i32
            }
            restate_types::processors::ReplayStatus::CatchingUp { target_tail_lsn } => {
                out.target_tail_lsn = Some(target_tail_lsn.into());
                restate_node_services::cluster_ctrl::ReplayStatus::CatchingUp as i32
            }
        };
        out.last_persisted_log_lsn = pp.last_persisted_log_lsn.map(|l| l.into());

        out
    }

    let mut out = HashMap::with_capacity(pps.len());
    for (id, node) in pps {
        out.insert((*id).into(), to_proto(node));
    }
    out
}
