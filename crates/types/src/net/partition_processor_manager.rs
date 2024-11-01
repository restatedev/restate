// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::cluster::cluster_state::RunMode;
use crate::identifiers::{PartitionId, SnapshotId};
use crate::net::define_rpc;
use crate::net::{define_message, TargetName};
use crate::Version;

define_message! {
    @message = ControlProcessors,
    @target = TargetName::ControlProcessors,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlProcessors {
    pub min_partition_table_version: Version,
    pub min_logs_table_version: Version,
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
    @request = CreateSnapshotRequest,
    @response = CreateSnapshotResponse,
    @request_target = TargetName::PartitionCreateSnapshotRequest,
    @response_target = TargetName::PartitionCreateSnapshotResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSnapshotRequest {
    pub partition_id: PartitionId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSnapshotResponse {
    pub result: Result<SnapshotId, SnapshotError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotError {
    SnapshotCreationFailed(String),
}
