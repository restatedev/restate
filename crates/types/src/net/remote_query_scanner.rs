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

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, bilrost::Message,
)]
pub struct ScannerId(#[bilrost(1)] pub GenerationalNodeId, #[bilrost(2)] pub u64);

impl Display for ScannerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("ScannerId({}, {})", self.0, self.1))
    }
}

// ----- open scanner -----

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
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
    #[serde(default)]
    pub limit: Option<u64>,
    #[bilrost(tag(6))]
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,
    #[bilrost(tag(7))]
    #[serde(default)]
    pub predicate: Option<RemoteQueryScannerPredicate>,
    /// Scanner id allocated by the caller; the server adopts this id rather than
    /// minting its own. This lets a client close a cancelled scanner after `Open`
    /// reaches the wire, even if it has not received the open reply yet.
    ///
    /// **Since v1.7**
    ///
    /// todo: make required in v1.8
    #[bilrost(tag(8))]
    pub scanner_id: Option<ScannerId>,
    /// Partition owner selected while the querying node built its physical plan.
    /// The serving node rejects the open if it no longer owns the partition.
    ///
    /// Node-level scans leave this unset because they are not partition-routed.
    ///
    /// **Since v1.8**
    #[bilrost(tag(9))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_partition_owner: Option<GenerationalNodeId>,
    /// Optional physical plan fragment to execute over this partition scan.
    /// The plan contains one custom input leaf whose schema must match
    /// `projection_schema_bytes`; the response acknowledges whether the worker
    /// accepted the fragment before any rows are consumed.
    ///
    /// **Since v1.8**
    #[bilrost(tag(10))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fragment: Option<RemoteQueryScannerFragment>,
}

fn default_batch_size() -> u64 {
    64
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct RemoteQueryScannerPredicate {
    // We ship the expression passed to scan() over the wire to filter records before sending
    // them back
    // see `encode_expr` / `decode_expr` in storage-query-datafusion/src/lib.rs
    #[bilrost(tag(1), encoding(plainbytes))]
    pub serialized_physical_expression: Vec<u8>,
}

/// Versioned DataFusion physical-plan bytes plus the schema the sender expects
/// after execution. Receivers decline incompatible versions or schemas and let
/// the querying node apply the fragment locally.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct RemoteQueryScannerFragment {
    #[bilrost(1)]
    pub format_version: u32,
    #[bilrost(tag(2), encoding(plainbytes))]
    pub serialized_plan: Vec<u8>,
    #[bilrost(tag(3), encoding(plainbytes))]
    pub output_schema_bytes: Vec<u8>,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    bilrost::Message,
    bilrost::Oneof,
)]
pub enum RemoteQueryScannerOpened {
    Failure,
    #[bilrost(1)]
    Success {
        // Client must use this scanner_id for all scanner operations.
        // It can be different from the client minted scanner-id in v1.7
        scanner_id: ScannerId,
    },
    /// The scanner was opened and accepted the requested physical fragment.
    /// When the request included `expected_partition_owner`, this also
    /// acknowledges that the server validated it.
    #[bilrost(tag(2), message)]
    SuccessWithFragment {
        #[bilrost(1)]
        scanner_id: ScannerId,
    },
    /// The scanner was opened without an accepted fragment and the server
    /// validated the requested partition owner.
    #[bilrost(tag(3), message)]
    SuccessWithOwnerValidation {
        #[bilrost(1)]
        scanner_id: ScannerId,
    },
    /// The scanner could not be opened. New clients use the message to surface
    /// ownership fencing and fragment-validation failures to the query caller.
    #[bilrost(tag(4), message)]
    FailureWithMessage {
        #[bilrost(1)]
        message: String,
    },
}

// ----- next batch -----

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct RemoteQueryScannerNext {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
    #[bilrost(tag(2))]
    #[serde(default)]
    pub next_predicate: Option<RemoteQueryScannerPredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct ScannerBatch {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
    #[bilrost(tag(2), encoding(plainbytes))]
    pub record_batch: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct ScannerFailure {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
    #[bilrost(2)]
    pub message: String,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    bilrost::Message,
    bilrost::Oneof,
)]
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct RemoteQueryScannerClose {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
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

#[cfg(test)]
mod test {

    use bilrost::{Message, OwnedMessage};
    use serde::Deserialize;

    use crate::{
        GenerationalNodeId,
        identifiers::PartitionId,
        net::remote_query_scanner::{
            RemoteQueryScannerPredicate, ScannerBatch, ScannerFailure, ScannerId,
        },
        sharding::KeyRange,
    };

    #[derive(
        Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message,
    )]
    struct LegacyRemoteQueryScannerOpen {
        #[bilrost(1)]
        partition_id: PartitionId,
        #[bilrost(2)]
        range: KeyRange,
        #[bilrost(3)]
        table: String,
        #[bilrost(tag(4), encoding(plainbytes))]
        projection_schema_bytes: Vec<u8>,
        #[bilrost(tag(5))]
        #[serde(default)]
        limit: Option<u64>,
        #[bilrost(tag(6))]
        #[serde(default = "super::default_batch_size")]
        batch_size: u64,
        #[bilrost(tag(7))]
        #[serde(default)]
        predicate: Option<RemoteQueryScannerPredicate>,
        #[bilrost(tag(8))]
        scanner_id: Option<ScannerId>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, bilrost::Message, bilrost::Oneof)]
    enum LegacyRemoteQueryScannerOpened {
        Failure,
        #[bilrost(1)]
        Success {
            scanner_id: ScannerId,
        },
    }

    // V1/flexbuffers scanner next result type.
    #[derive(Deserialize)]
    #[allow(dead_code)]
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

    #[test]
    fn backward_compatibility() {
        let scanner_id = ScannerId(GenerationalNodeId::new(10, 20), 100);
        let batch = vec![1, 2, 3, 4];
        let v2 = super::RemoteQueryScannerNextResult::NextBatch(ScannerBatch {
            scanner_id,
            record_batch: batch.clone(),
        });

        let bytes = flexbuffers::to_vec(&v2).expect("to serialize");

        let v1: RemoteQueryScannerNextResult =
            flexbuffers::from_slice(&bytes).expect("to deserialize");

        assert!(
            matches!(v1, RemoteQueryScannerNextResult::NextBatch { scanner_id: id, record_batch} if id == scanner_id && batch == record_batch )
        );

        let v2 = super::RemoteQueryScannerNextResult::Failure(ScannerFailure {
            scanner_id,
            message: "scanner failed successfully!".to_string(),
        });

        let bytes = flexbuffers::to_vec(&v2).expect("to serialize");

        let v1: RemoteQueryScannerNextResult =
            flexbuffers::from_slice(&bytes).expect("to deserialize");

        assert!(
            matches!(v1, RemoteQueryScannerNextResult::Failure { scanner_id: id, message} if id == scanner_id && message == "scanner failed successfully!" )
        )
    }

    #[test]
    fn optional_open_fields_are_backward_compatible() {
        let owner = GenerationalNodeId::new(10, 20);
        let scanner_id = ScannerId(owner, 100);
        let new_request = super::RemoteQueryScannerOpen {
            partition_id: PartitionId::MIN,
            range: KeyRange::FULL,
            table: "sys_inbox".to_owned(),
            projection_schema_bytes: vec![1, 2, 3],
            limit: Some(10),
            batch_size: 128,
            predicate: Some(RemoteQueryScannerPredicate {
                serialized_physical_expression: vec![4, 5, 6],
            }),
            scanner_id: Some(scanner_id),
            expected_partition_owner: Some(owner),
            fragment: Some(super::RemoteQueryScannerFragment {
                format_version: 1,
                serialized_plan: vec![7, 8, 9],
                output_schema_bytes: vec![10, 11, 12],
            }),
        };

        let bilrost_bytes = new_request.encode_to_vec();
        let legacy_request = LegacyRemoteQueryScannerOpen::decode(bilrost_bytes.as_slice())
            .expect("legacy message should skip the new owner field");
        assert_eq!(legacy_request.partition_id, new_request.partition_id);
        assert_eq!(legacy_request.scanner_id, new_request.scanner_id);

        let mut request_without_owner = new_request;
        request_without_owner.expected_partition_owner = None;
        request_without_owner.fragment = None;
        let legacy_request = LegacyRemoteQueryScannerOpen {
            partition_id: request_without_owner.partition_id,
            range: request_without_owner.range,
            table: request_without_owner.table.clone(),
            projection_schema_bytes: request_without_owner.projection_schema_bytes.clone(),
            limit: request_without_owner.limit,
            batch_size: request_without_owner.batch_size,
            predicate: request_without_owner.predicate.clone(),
            scanner_id: request_without_owner.scanner_id,
        };
        let decoded_request =
            super::RemoteQueryScannerOpen::decode(legacy_request.encode_to_vec().as_slice())
                .expect("new message should decode the legacy shape");
        assert_eq!(decoded_request.expected_partition_owner, None);
        assert_eq!(decoded_request.fragment, None);
        assert_eq!(
            request_without_owner.encode_to_vec(),
            legacy_request.encode_to_vec(),
            "a fragment-free request must remain byte-identical to the legacy wire shape"
        );
        assert_eq!(
            flexbuffers::to_vec(&request_without_owner).expect("new request should serialize"),
            flexbuffers::to_vec(&legacy_request).expect("legacy request should serialize"),
        );
    }

    #[test]
    fn fragment_open_negotiation_is_backward_compatible() {
        let scanner_id = ScannerId(GenerationalNodeId::new(10, 20), 100);
        let legacy = LegacyRemoteQueryScannerOpened::Success { scanner_id };
        let decoded = super::RemoteQueryScannerOpened::decode(legacy.encode_to_vec().as_slice())
            .expect("new client should decode an old success response");
        assert_eq!(
            decoded,
            super::RemoteQueryScannerOpened::Success { scanner_id }
        );

        let unchanged = super::RemoteQueryScannerOpened::Success { scanner_id };
        let decoded = LegacyRemoteQueryScannerOpened::decode(unchanged.encode_to_vec().as_slice())
            .expect("old client should decode the unchanged success response");
        assert_eq!(decoded, legacy);

        let owner_validated =
            super::RemoteQueryScannerOpened::SuccessWithOwnerValidation { scanner_id };
        assert_eq!(
            super::RemoteQueryScannerOpened::decode(owner_validated.encode_to_vec().as_slice())
                .expect("new owner acknowledgement should round trip"),
            owner_validated
        );
        let failure = super::RemoteQueryScannerOpened::FailureWithMessage {
            message: "owner changed".to_owned(),
        };
        assert_eq!(
            super::RemoteQueryScannerOpened::decode(failure.encode_to_vec().as_slice())
                .expect("detailed failure should round trip"),
            failure
        );
    }
}
