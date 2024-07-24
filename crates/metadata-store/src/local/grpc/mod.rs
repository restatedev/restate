// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod client;
pub mod handler;
pub mod net_util;

pub mod pb_conversions {
    use crate::grpc_svc;
    use crate::grpc_svc::{GetResponse, GetVersionResponse, PreconditionKind};
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

    impl TryFrom<grpc_svc::VersionedValue> for VersionedValue {
        type Error = ConversionError;

        fn try_from(value: grpc_svc::VersionedValue) -> Result<Self, Self::Error> {
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

    impl From<VersionedValue> for grpc_svc::VersionedValue {
        fn from(value: VersionedValue) -> Self {
            grpc_svc::VersionedValue {
                version: Some(value.version.into()),
                bytes: value.value,
            }
        }
    }

    impl From<grpc_svc::Version> for Version {
        fn from(value: grpc_svc::Version) -> Self {
            Version::from(value.value)
        }
    }

    impl From<Version> for grpc_svc::Version {
        fn from(value: Version) -> Self {
            grpc_svc::Version {
                value: value.into(),
            }
        }
    }

    impl From<Precondition> for grpc_svc::Precondition {
        fn from(value: Precondition) -> Self {
            match value {
                Precondition::None => grpc_svc::Precondition {
                    kind: PreconditionKind::None.into(),
                    version: None,
                },
                Precondition::DoesNotExist => grpc_svc::Precondition {
                    kind: PreconditionKind::DoesNotExist.into(),
                    version: None,
                },
                Precondition::MatchesVersion(version) => grpc_svc::Precondition {
                    kind: PreconditionKind::MatchesVersion.into(),
                    version: Some(version.into()),
                },
            }
        }
    }

    impl TryFrom<grpc_svc::Precondition> for Precondition {
        type Error = ConversionError;

        fn try_from(value: grpc_svc::Precondition) -> Result<Self, Self::Error> {
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
}
