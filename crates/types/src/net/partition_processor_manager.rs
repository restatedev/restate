// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::Version;
use crate::identifiers::{PartitionId, SnapshotId};
use crate::logs::{LogId, Lsn};
use crate::net::{ServiceTag, define_service, define_unary_message};
use crate::net::{default_wire_codec, define_rpc};

pub struct PartitionManagerService;

define_service! {
    @service = PartitionManagerService,
    @tag = ServiceTag::PartitionManagerService,
}

define_unary_message! {
    @message = ControlProcessors,
    @service = PartitionManagerService,
}

default_wire_codec!(ControlProcessors);

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
    // Version of the current partition configuration used for creating the command for selecting
    // the leader. Restate <= 1.3.2 does not set the current version attribute.
    #[serde(default = "Version::invalid")]
    pub current_version: Version,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, derive_more::Display)]
pub enum ProcessorCommand {
    // #[deprecated(
    //     since = "1.3.3",
    //     note = "Stopping should happen based on the PartitionReplicaSetStates"
    // )]
    Stop,
    // #[deprecated(
    //     since = "1.3.3",
    //     note = "Starting followers should happen based on the PartitionReplicaSetStates"
    // )]
    Follower,
    Leader,
}

define_rpc! {
    @request = CreateSnapshotRequest,
    @response = CreateSnapshotResponse,
    @service = PartitionManagerService,
}

default_wire_codec!(CreateSnapshotRequest);
default_wire_codec!(CreateSnapshotResponse);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSnapshotRequest {
    pub partition_id: PartitionId,
    pub min_target_lsn: Option<Lsn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSnapshotResponse {
    pub result: Result<Snapshot, SnapshotError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub snapshot_id: SnapshotId,
    pub log_id: LogId,
    pub min_applied_lsn: Lsn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotError {
    SnapshotCreationFailed(String),
}
