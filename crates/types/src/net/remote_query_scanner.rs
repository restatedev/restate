// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Display, Formatter};
use std::ops::RangeInclusive;

use prost_dto::IntoProst;

use super::TargetName;
use super::codec::V2Convertible;
use crate::GenerationalNodeId;
use crate::errors::ConversionError;
use crate::identifiers::{PartitionId, PartitionKey};
use crate::net::define_rpc;
use crate::protobuf::net::query_scanner as proto;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ScannerId(pub GenerationalNodeId, pub u64);

impl From<ScannerId> for proto::ScannerId {
    fn from(value: ScannerId) -> Self {
        Self {
            node_id: Some(value.0.into()),
            scanner_id: value.1,
        }
    }
}

impl TryFrom<proto::ScannerId> for ScannerId {
    type Error = ConversionError;

    fn try_from(value: proto::ScannerId) -> Result<Self, Self::Error> {
        Ok(Self(
            value
                .node_id
                .ok_or_else(|| ConversionError::missing_field("node_id"))?
                .into(),
            value.scanner_id,
        ))
    }
}

impl Display for ScannerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("ScannerId({}, {})", self.0, self.1))
    }
}

// ----- open scanner -----

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteQueryScannerOpen {
    pub partition_id: PartitionId,
    pub range: RangeInclusive<PartitionKey>,
    pub table: String,
    pub projection_schema_bytes: Vec<u8>,
}

impl From<RemoteQueryScannerOpen> for proto::RemoteQueryScannerOpen {
    fn from(value: RemoteQueryScannerOpen) -> Self {
        let RemoteQueryScannerOpen {
            partition_id,
            range,
            table,
            projection_schema_bytes,
        } = value;

        Self {
            partition_id: partition_id.into(),
            start_key: *range.start(),
            end_key: *range.end(),
            table,
            projection_schema: projection_schema_bytes.into(),
        }
    }
}

impl TryFrom<proto::RemoteQueryScannerOpen> for RemoteQueryScannerOpen {
    type Error = ConversionError;

    fn try_from(value: proto::RemoteQueryScannerOpen) -> Result<Self, Self::Error> {
        let proto::RemoteQueryScannerOpen {
            partition_id,
            start_key,
            end_key,
            table,
            projection_schema,
        } = value;

        Ok(Self {
            partition_id: partition_id
                .try_into()
                .map_err(|_| ConversionError::invalid_data("partition_id"))?,
            range: start_key..=end_key,
            table,
            projection_schema_bytes: projection_schema.into(),
        })
    }
}

impl V2Convertible for RemoteQueryScannerOpen {
    type Target = proto::RemoteQueryScannerOpen;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RemoteQueryScannerOpened {
    Success { scanner_id: ScannerId },
    Failure,
}

impl From<RemoteQueryScannerOpened> for proto::RemoteQueryScannerOpened {
    fn from(value: RemoteQueryScannerOpened) -> Self {
        match value {
            RemoteQueryScannerOpened::Failure => Self { scanner_id: None },
            RemoteQueryScannerOpened::Success { scanner_id } => Self {
                scanner_id: Some(scanner_id.into()),
            },
        }
    }
}

impl TryFrom<proto::RemoteQueryScannerOpened> for RemoteQueryScannerOpened {
    type Error = ConversionError;

    fn try_from(value: proto::RemoteQueryScannerOpened) -> Result<Self, Self::Error> {
        let proto::RemoteQueryScannerOpened { scanner_id } = value;

        match scanner_id {
            None => Ok(Self::Failure),
            Some(scanner_id) => Ok(Self::Success {
                scanner_id: scanner_id.try_into()?,
            }),
        }
    }
}

impl V2Convertible for RemoteQueryScannerOpened {
    type Target = proto::RemoteQueryScannerOpened;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

// ----- next batch -----

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, IntoProst)]
#[prost(target=crate::protobuf::net::query_scanner::RemoteQueryScannerNext)]
pub struct RemoteQueryScannerNext {
    #[prost(required)]
    pub scanner_id: ScannerId,
}

impl TryFrom<proto::RemoteQueryScannerNext> for RemoteQueryScannerNext {
    type Error = ConversionError;

    fn try_from(value: proto::RemoteQueryScannerNext) -> Result<Self, Self::Error> {
        let proto::RemoteQueryScannerNext { scanner_id } = value;

        Ok(Self {
            scanner_id: scanner_id
                .ok_or_else(|| ConversionError::missing_field("scanner_id"))?
                .try_into()?,
        })
    }
}

impl V2Convertible for RemoteQueryScannerNext {
    type Target = proto::RemoteQueryScannerNext;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RemoteQueryScannerNextResult {
    NextBatch {
        scanner_id: ScannerId,
        record_batch: Vec<u8>,
    },
    Failure {
        scanner_id: ScannerId,
        message: String,
    },
    NoMoreRecords(ScannerId),
    NoSuchScanner(ScannerId),
}

impl From<RemoteQueryScannerNextResult> for proto::RemoteQueryScannerNextResult {
    fn from(value: RemoteQueryScannerNextResult) -> Self {
        use proto::remote_query_scanner_next_result::Status;

        match value {
            RemoteQueryScannerNextResult::NextBatch {
                scanner_id,
                record_batch,
            } => Self {
                scanner_id: Some(scanner_id.into()),
                status: Status::NextBatch.into(),
                record_batch: Some(record_batch.into()),
                ..Default::default()
            },
            RemoteQueryScannerNextResult::Failure {
                scanner_id,
                message,
            } => Self {
                scanner_id: Some(scanner_id.into()),
                status: Status::Failure.into(),
                message: Some(message),
                ..Default::default()
            },
            RemoteQueryScannerNextResult::NoMoreRecords(scanner_id) => Self {
                scanner_id: Some(scanner_id.into()),
                status: Status::NoMoreRecords.into(),
                ..Default::default()
            },
            RemoteQueryScannerNextResult::NoSuchScanner(scanner_id) => Self {
                scanner_id: Some(scanner_id.into()),
                status: Status::NoSuchScanner.into(),
                ..Default::default()
            },
        }
    }
}

impl TryFrom<proto::RemoteQueryScannerNextResult> for RemoteQueryScannerNextResult {
    type Error = ConversionError;

    fn try_from(value: proto::RemoteQueryScannerNextResult) -> Result<Self, Self::Error> {
        use proto::remote_query_scanner_next_result::Status;

        let scanner_id = value
            .scanner_id
            .ok_or_else(|| ConversionError::missing_field("scanner_id"))?
            .try_into()?;

        match value.status() {
            Status::Unknown => Err(ConversionError::invalid_data("status")),
            Status::NoMoreRecords => Ok(Self::NoMoreRecords(scanner_id)),
            Status::NoSuchScanner => Ok(Self::NoSuchScanner(scanner_id)),
            Status::Failure => Ok(Self::Failure {
                scanner_id,
                message: value
                    .message
                    .ok_or_else(|| ConversionError::missing_field("message"))?,
            }),
            Status::NextBatch => Ok(Self::NextBatch {
                scanner_id,
                record_batch: value
                    .record_batch
                    .ok_or_else(|| ConversionError::missing_field("record_batch"))?
                    .into(),
            }),
        }
    }
}

impl V2Convertible for RemoteQueryScannerNextResult {
    type Target = proto::RemoteQueryScannerNextResult;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

// ----- close scanner -----

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, IntoProst)]
#[prost(target=crate::protobuf::net::query_scanner::RemoteQueryScannerClose)]
pub struct RemoteQueryScannerClose {
    #[prost(required)]
    pub scanner_id: ScannerId,
}

impl TryFrom<proto::RemoteQueryScannerClose> for RemoteQueryScannerClose {
    type Error = ConversionError;

    fn try_from(value: proto::RemoteQueryScannerClose) -> Result<Self, Self::Error> {
        let proto::RemoteQueryScannerClose { scanner_id } = value;

        Ok(Self {
            scanner_id: scanner_id
                .ok_or_else(|| ConversionError::missing_field("scanner_id"))?
                .try_into()?,
        })
    }
}

impl V2Convertible for RemoteQueryScannerClose {
    type Target = proto::RemoteQueryScannerClose;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, IntoProst)]
#[prost(target=crate::protobuf::net::query_scanner::RemoteQueryScannerClosed)]
pub struct RemoteQueryScannerClosed {
    #[prost(required)]
    pub scanner_id: ScannerId,
}

impl TryFrom<proto::RemoteQueryScannerClosed> for RemoteQueryScannerClosed {
    type Error = ConversionError;

    fn try_from(value: proto::RemoteQueryScannerClosed) -> Result<Self, Self::Error> {
        let proto::RemoteQueryScannerClosed { scanner_id } = value;

        Ok(Self {
            scanner_id: scanner_id
                .ok_or_else(|| ConversionError::missing_field("scanner_id"))?
                .try_into()?,
        })
    }
}

impl V2Convertible for RemoteQueryScannerClosed {
    type Target = proto::RemoteQueryScannerClosed;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

// ----- RemoteScanner API API -----

// Scan
define_rpc! {
    @request = RemoteQueryScannerOpen,
    @response = RemoteQueryScannerOpened,
    @request_target = TargetName::RemoteQueryScannerOpen,
    @response_target = TargetName::RemoteQueryScannerOpened,
}

define_rpc! {
    @request = RemoteQueryScannerNext,
    @response = RemoteQueryScannerNextResult,
    @request_target = TargetName::RemoteQueryScannerNext,
    @response_target = TargetName::RemoteQueryScannerNextResult,
}

define_rpc! {
    @request = RemoteQueryScannerClose,
    @response = RemoteQueryScannerClosed,
    @request_target = TargetName::RemoteQueryScannerClose,
    @response_target = TargetName::RemoteQueryScannerClosed,
}
