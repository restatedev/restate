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
    /// Requests progress snapshots and the richer completion reply. Absent for
    /// older clients, which continue to receive NoMoreRecords.
    #[bilrost(tag(9))]
    pub collect_metrics: bool,
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
    #[bilrost(tag(3))]
    pub metrics: Option<ScannerMetrics>,
}

#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct ScannerFailure {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
    #[bilrost(2)]
    pub message: String,
    #[bilrost(tag(3))]
    pub metrics: Option<ScannerMetrics>,
}

/// Cumulative, unsampled accounting for one scanner, not process-wide metrics.
/// Missing reports mean unavailable, not zero work. Progress may lag batched
/// iterator publication; complete reports include all work before scanner EOF.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, bilrost::Message)]
pub struct ScannerMetrics {
    #[bilrost(1)]
    pub iterators: u64,
    #[bilrost(2)]
    pub completed_iterators: u64,
    #[bilrost(3)]
    pub keys_visited: u64,
    #[bilrost(4)]
    pub seeks: u64,
    #[bilrost(5)]
    pub nexts: u64,
    #[bilrost(6)]
    pub prevs: u64,
    /// Bytes presented by iterators, not disk bytes.
    #[bilrost(7)]
    pub bytes_visited: u64,
    #[bilrost(8)]
    pub wall_time_ns: u64,
    /// Native records delivered for Arrow conversion; one may produce several rows.
    #[bilrost(9)]
    pub records_emitted: u64,
    #[bilrost(10)]
    pub available: bool,
    #[bilrost(11)]
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct ScannerCompleted {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
    #[bilrost(2)]
    pub metrics: ScannerMetrics,
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
    /// Sent only to clients that requested metrics when opening the scanner.
    #[bilrost(5)]
    Completed(ScannerCompleted),
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
