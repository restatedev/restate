// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::cluster::cluster_state::{PartitionProcessorStatus, RunMode};
use crate::identifiers::{InvocationId, PartitionId};
use crate::ingress::{InvocationResponse, SubmittedInvocationNotification};
use crate::invocation::{InvocationQuery, ServiceInvocation};
use crate::net::{define_message, TargetName};

use crate::net::define_rpc;
use crate::Version;

define_rpc! {
    @request = GetProcessorsState,
    @response = ProcessorsStateResponse,
    @request_target = TargetName::GetProcessorsStateRequest,
    @response_target = TargetName::ProcessorsStateResponse,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GetProcessorsState {}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorsStateResponse {
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    pub state: BTreeMap<PartitionId, PartitionProcessorStatus>,
}

define_message! {
    @message = ControlProcessors,
    @target = TargetName::ControlProcessors,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlProcessors {
    pub min_partition_table_version: Version,
    pub commands: Vec<ControlProcessor>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ControlProcessor {
    pub partition_id: PartitionId,
    pub command: ProcessorCommand,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ProcessorCommand {
    Stop,
    Follower,
    Leader,
}

impl From<RunMode> for ProcessorCommand {
    fn from(value: RunMode) -> Self {
        match value {
            RunMode::Leader => ProcessorCommand::Leader,
            RunMode::Follower => ProcessorCommand::Follower,
        }
    }
}

define_rpc! {
    @request = PartitionProcessorRpcRequest,
    @response = Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>,
    @request_target = TargetName::PartitionProcessorRpc,
    @response_target = TargetName::PartitionProcessorRpcResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionProcessorRpcRequest {
    pub partition_id: PartitionId,
    pub request: PartitionProcessorRequestKind,
}

impl PartitionProcessorRpcRequest {
    pub fn get_output(partition_id: PartitionId, invocation_query: InvocationQuery) -> Self {
        Self {
            partition_id,
            request: PartitionProcessorRequestKind::GetOutputResult(invocation_query),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionProcessorRequestKind {
    SubmitInvocation(ServiceInvocation),
    AttachToInvocation(InvocationId),
    GetOutputResult(InvocationQuery),
    SubmitResponse(InvocationResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum PartitionProcessorRpcError {
    #[error("not leader for partition '{0}'")]
    NotLeader(PartitionId),
    #[error("rejecting rpc because too busy")]
    Busy,
    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionProcessorRpcResponse {
    SubmitInvocationNotification(SubmittedInvocationNotification),
    GetOutputResult(GetOutputResult),
    SubmitResponseNotification,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GetOutputResult {
    NotFound,
    NotReady,
    NotSupported,
    Ready(InvocationResponse),
}
