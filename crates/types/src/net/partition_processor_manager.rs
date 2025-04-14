// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use prost_dto::{FromProst, IntoProst};
use serde::{Deserialize, Serialize};

use crate::Version;
use crate::cluster::cluster_state::RunMode;
use crate::errors::ConversionError;
use crate::identifiers::{PartitionId, SnapshotId};
use crate::logs::{LogId, Lsn};
use crate::net::define_rpc;
use crate::net::{TargetName, define_message};
use crate::protobuf::net::cluster as proto;

use super::codec::V2Convertible;

define_message! {
    @message = ControlProcessors,
    @target = TargetName::ControlProcessors,
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoProst)]
#[prost(target=crate::protobuf::net::cluster::ControlProcessors)]
pub struct ControlProcessors {
    #[prost(required)]
    pub min_partition_table_version: Version,
    #[prost(required)]
    pub min_logs_table_version: Version,
    pub commands: Vec<ControlProcessor>,
}

impl TryFrom<proto::ControlProcessors> for ControlProcessors {
    type Error = ConversionError;

    fn try_from(value: proto::ControlProcessors) -> Result<Self, Self::Error> {
        Ok(Self {
            min_partition_table_version: value
                .min_partition_table_version
                .ok_or_else(|| ConversionError::missing_field("min_partition_table_version"))?
                .into(),
            min_logs_table_version: value
                .min_logs_table_version
                .ok_or_else(|| ConversionError::missing_field("min_logs_table_version"))?
                .into(),
            commands: {
                let mut commands = Vec::with_capacity(value.commands.len());
                for command in value.commands {
                    commands.push(command.try_into()?);
                }
                commands
            },
        })
    }
}

impl V2Convertible for ControlProcessors {
    type Target = proto::ControlProcessors;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, IntoProst)]
#[prost(target=crate::protobuf::net::cluster::ControlProcessor)]
pub struct ControlProcessor {
    pub partition_id: PartitionId,
    pub command: ProcessorCommand,
}

impl TryFrom<proto::ControlProcessor> for ControlProcessor {
    type Error = ConversionError;

    fn try_from(value: proto::ControlProcessor) -> Result<Self, Self::Error> {
        let proto::ControlProcessor {
            partition_id,
            command,
        } = value;

        Ok(Self {
            partition_id: u16::try_from(partition_id)
                .map_err(|_| ConversionError::invalid_data("partition_id"))?
                .into(),
            command: command.into(),
        })
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    derive_more::Display,
    FromProst,
    IntoProst,
)]
#[prost(target=crate::protobuf::net::cluster::ProcessorCommand)]
pub enum ProcessorCommand {
    Stop,
    Follower,
    Leader,
}

impl ProcessorCommand {
    pub fn as_run_mode(&self) -> Option<RunMode> {
        match self {
            ProcessorCommand::Stop => None,
            ProcessorCommand::Follower => Some(RunMode::Follower),
            ProcessorCommand::Leader => Some(RunMode::Leader),
        }
    }
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
