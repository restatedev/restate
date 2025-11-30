// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, Bytes};
use serde::{Deserialize, Serialize};

use crate::Version;
use crate::identifiers::{PartitionId, SnapshotId};
use crate::logs::{LogId, Lsn};
use crate::net::codec::{
    EncodeError, MigrationCodec, WireDecode, WireEncode, decode_as_bilrost, decode_as_flexbuffers,
    encode_as_bilrost, encode_as_flexbuffers,
};
use crate::net::define_rpc;
use crate::net::{ProtocolVersion, ServiceTag, define_service, define_unary_message};

pub struct PartitionManagerService;

define_service! {
    @service = PartitionManagerService,
    @tag = ServiceTag::PartitionManagerService,
}

define_unary_message! {
    @message = ControlProcessors,
    @service = PartitionManagerService,
}

impl MigrationCodec for ControlProcessors {
    const BILROST_VERSION: ProtocolVersion = ProtocolVersion::V2;
}

#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct ControlProcessors {
    #[bilrost(1)]
    pub min_partition_table_version: Version,
    #[bilrost(2)]
    pub min_logs_table_version: Version,
    #[bilrost(3)]
    pub commands: Vec<ControlProcessor>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, bilrost::Message)]
pub struct ControlProcessor {
    #[bilrost(1)]
    pub partition_id: PartitionId,
    #[bilrost(oneof(2-3))]
    pub command: ProcessorCommand,
    // Version of the current partition configuration used for creating the command for selecting
    // the leader. Restate <= 1.3.2 does not set the current version attribute.
    #[bilrost(4)]
    #[serde(default = "Version::invalid")]
    pub current_version: Version,
}

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, derive_more::Display, bilrost::Oneof,
)]
pub enum ProcessorCommand {
    // #[deprecated(
    //     since = "1.3.3",
    //     note = "Stopping should happen based on the PartitionReplicaSetStates"
    // )]
    #[bilrost(empty)]
    Stop,
    // #[deprecated(
    //     since = "1.3.3",
    //     note = "Starting followers should happen based on the PartitionReplicaSetStates"
    // )]
    #[bilrost(tag(2), message)]
    Follower,
    #[bilrost(tag(3), message)]
    Leader,
}

define_rpc! {
    @request = CreateSnapshotRequest,
    @response = CreateSnapshotResponse,
    @service = PartitionManagerService,
}

impl MigrationCodec for CreateSnapshotRequest {
    const BILROST_VERSION: ProtocolVersion = ProtocolVersion::V2;
}

#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct CreateSnapshotRequest {
    #[bilrost(1)]
    pub partition_id: PartitionId,
    #[bilrost(2)]
    pub min_target_lsn: Option<Lsn>,
}

#[derive(Debug, Clone, bilrost::Oneof)]
pub enum CreateSnapshotStatus {
    #[bilrost(empty)]
    Unknown,
    #[bilrost(tag(1), message)]
    Ok,
    #[bilrost(tag(2), message)]
    Error(String),
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct CreateSnapshotResponse {
    #[bilrost(oneof(1-2))]
    pub status: CreateSnapshotStatus,
    #[bilrost(3)]
    pub snapshot: Option<Snapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct Snapshot {
    #[bilrost(1)]
    pub snapshot_id: SnapshotId,
    #[bilrost(2)]
    pub log_id: LogId,
    #[bilrost(3)]
    pub min_applied_lsn: Lsn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotError {
    SnapshotCreationFailed(String),
}

impl WireEncode for CreateSnapshotResponse {
    fn encode_to_bytes(&self, protocol_version: ProtocolVersion) -> Result<Bytes, EncodeError> {
        match protocol_version {
            ProtocolVersion::V2 => {
                let message_v2 = compat::CreateSnapshotResponse::from(self.clone());
                Ok(Bytes::from(encode_as_flexbuffers(&message_v2)))
            }
            ProtocolVersion::V3 => Ok(encode_as_bilrost(self)),
            _ => Err(EncodeError::IncompatibleVersion {
                type_tag: stringify!(CreateSnapshotResponse),
                min_required: ProtocolVersion::V2,
                actual: protocol_version,
            }),
        }
    }
}

impl WireDecode for CreateSnapshotResponse {
    type Error = anyhow::Error;

    fn try_decode(buf: impl Buf, protocol_version: ProtocolVersion) -> Result<Self, Self::Error> {
        match protocol_version {
            ProtocolVersion::V2 => {
                let message_v2: compat::CreateSnapshotResponse =
                    decode_as_flexbuffers(buf, protocol_version)?;
                Ok(message_v2.into())
            }
            ProtocolVersion::V3 => decode_as_bilrost(buf, protocol_version),
            _ => Err(anyhow::anyhow!(
                "invalid protocol version: {}",
                protocol_version.as_str_name()
            )),
        }
    }
}

mod compat {
    use serde::{Deserialize, Serialize};

    use crate::net::partition_processor_manager::Snapshot;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct CreateSnapshotResponse {
        pub result: Result<Snapshot, SnapshotError>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum SnapshotError {
        SnapshotCreationFailed(String),
    }

    impl From<super::CreateSnapshotResponse> for CreateSnapshotResponse {
        fn from(value: super::CreateSnapshotResponse) -> Self {
            match value.status {
                super::CreateSnapshotStatus::Unknown | super::CreateSnapshotStatus::Ok => Self {
                    result: value.snapshot.ok_or_else(|| {
                        SnapshotError::SnapshotCreationFailed(
                            "invalid snapshot response".to_string(),
                        )
                    }),
                },
                super::CreateSnapshotStatus::Error(e) => Self {
                    result: Err(SnapshotError::SnapshotCreationFailed(e)),
                },
            }
        }
    }

    impl From<CreateSnapshotResponse> for super::CreateSnapshotResponse {
        fn from(value: CreateSnapshotResponse) -> Self {
            match value.result {
                Ok(snapshot) => Self {
                    status: super::CreateSnapshotStatus::Ok,
                    snapshot: Some(snapshot),
                },
                Err(SnapshotError::SnapshotCreationFailed(e)) => Self {
                    status: super::CreateSnapshotStatus::Error(e),
                    snapshot: None,
                },
            }
        }
    }
}
