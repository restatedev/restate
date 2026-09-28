// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Display, Formatter};

use super::ServiceTag;
use crate::GenerationalNodeId;
use crate::identifiers::PartitionId;
use crate::net::{bilrost_wire_codec, define_rpc, define_service};

pub struct RemoteDataFusionService;
define_service! {
    @service = RemoteDataFusionService,
    @tag = ServiceTag::RemoteDataFusionService,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bilrost::Message)]
pub struct ScannerId(#[bilrost(1)] pub GenerationalNodeId, #[bilrost(2)] pub u64);

impl Display for ScannerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("ScannerId({}, {})", self.0, self.1))
    }
}

// ----- open scanner -----

#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct RemoteQueryScannerOpen {
    #[bilrost(1)]
    pub partition_id: PartitionId,
    #[bilrost(2)]
    pub range: crate::sharding::KeyRange,
    #[bilrost(3)]
    pub table: String,
    #[bilrost(tag(4), encoding(plainbytes))]
    pub projection_schema_bytes: Vec<u8>,
    #[bilrost(tag(5))]
    pub limit: Option<u64>,
    #[bilrost(tag(6))]
    pub batch_size: u64,
    #[bilrost(tag(7))]
    pub predicate: Option<RemoteQueryScannerPredicate>,
    /// Scanner id allocated by the caller; the server adopts this id rather than
    /// minting its own, which lets clients pipeline the first `Next` immediately
    /// after `Open` without waiting for the open reply.
    ///
    /// **Since v1.7**
    ///
    /// todo: make required in v1.8
    #[bilrost(tag(8))]
    pub scanner_id: Option<ScannerId>,
}

#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct RemoteQueryScannerPredicate {
    // We ship the expression passed to scan() over the wire to filter records before sending
    // them back
    // see `encode_expr` / `decode_expr` in storage-query-datafusion/src/lib.rs
    #[bilrost(tag(1), encoding(plainbytes))]
    pub serialized_physical_expression: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message, bilrost::Oneof)]
pub enum RemoteQueryScannerOpened {
    Failure,
    #[bilrost(1)]
    Success {
        // Client must use this scanner_id for all scanner operations.
        // It can be different from the client minted scanner-id in v1.7
        scanner_id: ScannerId,
    },
}

// ----- next batch -----

#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct RemoteQueryScannerNext {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
    #[bilrost(tag(2))]
    pub next_predicate: Option<RemoteQueryScannerPredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct ScannerBatch {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
    #[bilrost(tag(2), encoding(plainbytes))]
    pub record_batch: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct ScannerFailure {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
    #[bilrost(2)]
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message, bilrost::Oneof)]
pub enum RemoteQueryScannerNextResult {
    Unknown,
    #[bilrost(1)]
    NextBatch(ScannerBatch),
    #[bilrost(2)]
    Failure(ScannerFailure),
    #[bilrost(3)]
    NoMoreRecords(ScannerId),
    #[bilrost(4)]
    NoSuchScanner(ScannerId),
}

// ----- close scanner -----

#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct RemoteQueryScannerClose {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
}

#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct RemoteQueryScannerClosed {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
}

// ----- RemoteScanner API API -----

// Scan
define_rpc! {
    @request = RemoteQueryScannerOpen,
    @response = RemoteQueryScannerOpened,
    @service = RemoteDataFusionService,
}

bilrost_wire_codec!(RemoteQueryScannerOpen);
bilrost_wire_codec!(RemoteQueryScannerOpened);

define_rpc! {
    @request = RemoteQueryScannerNext,
    @response = RemoteQueryScannerNextResult,
    @service = RemoteDataFusionService,
}
bilrost_wire_codec!(RemoteQueryScannerNext);
bilrost_wire_codec!(RemoteQueryScannerNextResult);

define_rpc! {
    @request = RemoteQueryScannerClose,
    @response = RemoteQueryScannerClosed,
    @service = RemoteDataFusionService,
}
bilrost_wire_codec!(RemoteQueryScannerClose);
bilrost_wire_codec!(RemoteQueryScannerClosed);
