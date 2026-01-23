// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub(crate) mod handler;

pub mod pb_conversions {
    use restate_metadata_server_grpc::grpc;
    use restate_metadata_server_grpc::grpc::{WriteRequest, WriteRequestKind};
    use restate_types::errors::ConversionError;

    use crate::MetadataServerSummary;

    impl From<MetadataServerSummary> for grpc::StatusResponse {
        fn from(value: MetadataServerSummary) -> Self {
            match value {
                MetadataServerSummary::Starting => grpc::StatusResponse {
                    status: restate_types::protobuf::common::MetadataServerStatus::StartingUp
                        .into(),
                    configuration: None,
                    leader: None,
                    raft: None,
                    snapshot: None,
                },
                MetadataServerSummary::Provisioning => grpc::StatusResponse {
                    status:
                        restate_types::protobuf::common::MetadataServerStatus::AwaitingProvisioning
                            .into(),
                    configuration: None,
                    leader: None,
                    raft: None,
                    snapshot: None,
                },
                MetadataServerSummary::Standby => grpc::StatusResponse {
                    status: restate_types::protobuf::common::MetadataServerStatus::Standby.into(),
                    configuration: None,
                    leader: None,
                    raft: None,
                    snapshot: None,
                },
                MetadataServerSummary::Member {
                    configuration,
                    leader,
                    raft,
                    snapshot,
                } => grpc::StatusResponse {
                    status: restate_types::protobuf::common::MetadataServerStatus::Member.into(),
                    configuration: Some(grpc::MetadataServerConfiguration::from(configuration)),
                    leader: leader.map(u32::from),
                    raft: Some(grpc::RaftSummary::from(raft)),
                    snapshot: snapshot.map(grpc::SnapshotSummary::from),
                },
            }
        }
    }

    impl From<crate::WriteRequest> for WriteRequest {
        fn from(value: crate::WriteRequest) -> Self {
            match value.kind {
                crate::RequestKind::Delete { key, precondition } => Self {
                    request_id: Some(value.request_id.into()),
                    kind: WriteRequestKind::Delete as i32,
                    key: key.into_bytes(),
                    precondition: Some(precondition.into()),
                    value: None,
                },
                crate::RequestKind::Put {
                    key,
                    value: versioned_value,
                    precondition,
                } => Self {
                    request_id: Some(value.request_id.into()),
                    key: key.into_bytes(),
                    kind: WriteRequestKind::Put as i32,
                    precondition: Some(precondition.into()),
                    value: Some(versioned_value.into()),
                },
            }
        }
    }

    impl TryFrom<WriteRequest> for crate::WriteRequest {
        type Error = ConversionError;

        fn try_from(value: WriteRequest) -> Result<Self, Self::Error> {
            let request_id = value
                .request_id
                .ok_or_else(|| ConversionError::missing_field("request_id"))?
                .into();

            let request = match WriteRequestKind::try_from(value.kind)
                .map_err(|_| ConversionError::invalid_data("kind"))?
            {
                WriteRequestKind::Delete => Self {
                    request_id,
                    kind: crate::RequestKind::Delete {
                        key: value
                            .key
                            .try_into()
                            .map_err(|_| ConversionError::invalid_data("key"))?,
                        precondition: value
                            .precondition
                            .ok_or_else(|| ConversionError::missing_field("precondition"))?
                            .try_into()?,
                    },
                },
                WriteRequestKind::Put => Self {
                    request_id,
                    kind: crate::RequestKind::Put {
                        key: value
                            .key
                            .try_into()
                            .map_err(|_| ConversionError::invalid_data("key"))?,
                        value: value
                            .value
                            .ok_or_else(|| ConversionError::missing_field("value"))?
                            .try_into()?,
                        precondition: value
                            .precondition
                            .ok_or_else(|| ConversionError::missing_field("precondition"))?
                            .try_into()?,
                    },
                },
                _ => return Err(ConversionError::InvalidData("kind")),
            };

            Ok(request)
        }
    }
}
