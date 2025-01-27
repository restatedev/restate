// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod client;
pub(crate) mod handler;

tonic::include_proto!("restate.metadata_store_svc");
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("metadata_store_svc");

pub mod pb_conversions {
    use crate::grpc::{
        GetResponse, GetVersionResponse, PreconditionKind, Ulid, WriteRequest, WriteRequestKind,
    };
    use crate::{grpc, MetadataStoreSummary};
    use restate_core::metadata_store::{Precondition, VersionedValue};
    use restate_types::Version;

    #[derive(Debug, thiserror::Error)]
    pub enum ConversionError {
        #[error("missing field '{0}'")]
        MissingField(&'static str),
        #[error("invalid data '{0}'")]
        InvalidData(&'static str),
    }

    impl ConversionError {
        pub fn missing_field(field: &'static str) -> Self {
            ConversionError::MissingField(field)
        }

        pub fn invalid_data(field: &'static str) -> Self {
            ConversionError::InvalidData(field)
        }
    }

    impl TryFrom<GetResponse> for Option<VersionedValue> {
        type Error = ConversionError;

        fn try_from(value: GetResponse) -> Result<Self, Self::Error> {
            if let Some(versioned_value) = value.value {
                Ok(Some(VersionedValue::try_from(versioned_value)?))
            } else {
                Ok(None)
            }
        }
    }

    impl TryFrom<grpc::VersionedValue> for VersionedValue {
        type Error = ConversionError;

        fn try_from(value: grpc::VersionedValue) -> Result<Self, Self::Error> {
            let version = value
                .version
                .ok_or_else(|| ConversionError::missing_field("version"))?;
            Ok(VersionedValue::new(version.into(), value.bytes))
        }
    }

    impl From<GetVersionResponse> for Option<Version> {
        fn from(value: GetVersionResponse) -> Self {
            value.version.map(Into::into)
        }
    }

    impl From<VersionedValue> for grpc::VersionedValue {
        fn from(value: VersionedValue) -> Self {
            grpc::VersionedValue {
                version: Some(value.version.into()),
                bytes: value.value,
            }
        }
    }

    impl From<grpc::Version> for Version {
        fn from(value: grpc::Version) -> Self {
            Version::from(value.value)
        }
    }

    impl From<Version> for grpc::Version {
        fn from(value: Version) -> Self {
            grpc::Version {
                value: value.into(),
            }
        }
    }

    impl From<Precondition> for grpc::Precondition {
        fn from(value: Precondition) -> Self {
            match value {
                Precondition::None => grpc::Precondition {
                    kind: PreconditionKind::None.into(),
                    version: None,
                },
                Precondition::DoesNotExist => grpc::Precondition {
                    kind: PreconditionKind::DoesNotExist.into(),
                    version: None,
                },
                Precondition::MatchesVersion(version) => grpc::Precondition {
                    kind: PreconditionKind::MatchesVersion.into(),
                    version: Some(version.into()),
                },
            }
        }
    }

    impl TryFrom<grpc::Precondition> for Precondition {
        type Error = ConversionError;

        fn try_from(value: grpc::Precondition) -> Result<Self, Self::Error> {
            match value.kind() {
                PreconditionKind::Unknown => {
                    Err(ConversionError::invalid_data("unknown precondition kind"))
                }
                PreconditionKind::None => Ok(Precondition::None),
                PreconditionKind::DoesNotExist => Ok(Precondition::DoesNotExist),
                PreconditionKind::MatchesVersion => Ok(Precondition::MatchesVersion(
                    value
                        .version
                        .ok_or_else(|| ConversionError::missing_field("version"))?
                        .into(),
                )),
            }
        }
    }

    impl From<MetadataStoreSummary> for grpc::StatusResponse {
        fn from(value: MetadataStoreSummary) -> Self {
            match value {
                MetadataStoreSummary::Starting => grpc::StatusResponse {
                    status: restate_types::protobuf::common::MetadataServerStatus::StartingUp
                        .into(),
                    configuration: None,
                    leader: None,
                    raft: None,
                    snapshot: None,
                },
                MetadataStoreSummary::Provisioning => grpc::StatusResponse {
                    status:
                        restate_types::protobuf::common::MetadataServerStatus::AwaitingProvisioning
                            .into(),
                    configuration: None,
                    leader: None,
                    raft: None,
                    snapshot: None,
                },
                MetadataStoreSummary::Standby => grpc::StatusResponse {
                    status: restate_types::protobuf::common::MetadataServerStatus::Standby.into(),
                    configuration: None,
                    leader: None,
                    raft: None,
                    snapshot: None,
                },
                MetadataStoreSummary::Member {
                    configuration,
                    leader,
                    raft,
                    snapshot,
                } => grpc::StatusResponse {
                    status: restate_types::protobuf::common::MetadataServerStatus::Member.into(),
                    configuration: Some(grpc::MetadataStoreConfiguration::from(configuration)),
                    leader: leader.map(grpc::MemberId::from),
                    raft: Some(grpc::RaftSummary::from(raft)),
                    snapshot: snapshot.map(grpc::SnapshotSummary::from),
                },
            }
        }
    }

    impl From<ulid::Ulid> for Ulid {
        fn from(value: ulid::Ulid) -> Self {
            Self {
                high: (value.0 >> 64) as u64,
                low: value.0 as u64,
            }
        }
    }

    impl From<Ulid> for ulid::Ulid {
        fn from(value: Ulid) -> Self {
            Self::from((value.high as u128) << 64 | value.low as u128)
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

#[cfg(test)]
mod test {
    use super::Ulid;

    #[test]
    fn test_ulid_encoding() {
        let id = ulid::Ulid::new();
        let encoded = Ulid::from(id);
        let decoded = ulid::Ulid::from(encoded);

        assert_eq!(id, decoded);
    }
}
