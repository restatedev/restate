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

use super::TargetName;
use crate::identifiers::{PartitionId, PartitionKey};
use crate::net::define_rpc;
use crate::GenerationalNodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ScannerId(pub GenerationalNodeId, pub u64);

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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RemoteQueryScannerOpened {
    Success { scanner_id: ScannerId },
    Failure,
}

// ----- next batch -----

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteQueryScannerNext {
    pub scanner_id: ScannerId,
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

// ----- close scanner -----

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteQueryScannerClose {
    pub scanner_id: ScannerId,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteQueryScannerClosed {
    pub scanner_id: ScannerId,
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
