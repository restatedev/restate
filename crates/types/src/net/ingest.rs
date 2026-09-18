// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;

use restate_encoding::ArcedSlice;

use crate::identifiers::PartitionId;
use crate::logs::{HasRecordKeys, Keys};
use crate::net::partition_processor::PartitionLeaderService;
use crate::net::{RpcRequest, bilrost_wire_codec, define_rpc};

#[derive(Debug, Eq, PartialEq, Clone, bilrost::Message)]
pub struct IngestRecord {
    #[bilrost(1)]
    pub keys: Keys,
    #[bilrost(2)]
    pub record: Bytes,
}

impl IngestRecord {
    pub fn new(keys: Keys, record: Bytes) -> Self {
        Self { keys, record }
    }

    pub fn estimate_size(&self) -> usize {
        size_of::<Keys>() + self.record.len()
    }
}

impl HasRecordKeys for IngestRecord {
    fn record_keys(&self) -> Keys {
        self.keys.clone()
    }
}

#[derive(Debug, bilrost::Message)]
pub struct IngestRequest {
    #[bilrost(tag(1), encoding(ArcedSlice<packed>))]
    pub records: Arc<[IngestRecord]>,
}

impl IngestRequest {
    pub fn estimate_size(&self) -> usize {
        self.records
            .iter()
            .fold(0, |size, item| size + item.estimate_size())
    }
}

bilrost_wire_codec!(IngestRequest);

#[derive(Debug, Clone, bilrost::Oneof, bilrost::Message)]
pub enum ResponseStatus {
    Unknown,
    #[bilrost(tag = 1, message)]
    Ack,
    #[bilrost(tag = 2, message)]
    NotLeader {
        of: PartitionId,
    },
    #[bilrost(tag = 3, message)]
    Internal {
        msg: String,
    },
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct IngestResponse {
    #[bilrost(1)]
    pub status: ResponseStatus,
}

impl From<ResponseStatus> for IngestResponse {
    fn from(status: ResponseStatus) -> Self {
        Self { status }
    }
}

bilrost_wire_codec!(IngestResponse);

define_rpc! {
    @request=IngestRequest,
    @response=IngestResponse,
    @service=PartitionLeaderService,
}

/// [`ReceivedIngestRequest`] must be kept
/// in lockstep with [`IngestRequest`]
/// It uses the same TYPE as [`IngestRequest`]
/// to be able to decode directly to owned Vec
/// on server side.
#[derive(Debug, bilrost::Message)]
pub struct ReceivedIngestRequest {
    #[bilrost(tag(1), encoding(packed))]
    pub records: Vec<IngestRecord>,
}

bilrost_wire_codec!(ReceivedIngestRequest);

/// The [`ReceivedIngestRequest`] uses the same TYPE
/// as [`IngestRequest`] to be able to directly decode
/// received RPC messages to this type.
impl RpcRequest for ReceivedIngestRequest {
    const TYPE: &str = stringify!(IngestRequest);
    type Response = IngestResponse;
    type Service = PartitionLeaderService;
}

#[allow(deprecated)]
pub use dedup_sn_query::{DedupSequenceNrQueryRequest, DedupSequenceNrQueryResponse};

#[allow(deprecated)]
mod dedup_sn_query {
    use crate::{
        message::MessageIndex,
        net::{
            bilrost_wire_codec, define_rpc, ingest::ResponseStatus,
            partition_processor::PartitionLeaderService,
        },
    };

    #[derive(Debug, Clone, bilrost::Message)]
    #[bilrost(reserved_tags(1, 2))]
    #[deprecated]
    pub struct DedupSequenceNrQueryRequest {}

    bilrost_wire_codec!(DedupSequenceNrQueryRequest);

    /// Last sequence number recorded by the partition processor
    #[derive(Debug, Clone, bilrost::Message)]
    #[deprecated]
    pub struct DedupSequenceNrQueryResponse {
        #[bilrost(1)]
        pub status: ResponseStatus,
        #[bilrost(2)]
        pub sequence_number: Option<MessageIndex>,
    }

    bilrost_wire_codec!(DedupSequenceNrQueryResponse);

    define_rpc! {
        @request = DedupSequenceNrQueryRequest,
        @response = DedupSequenceNrQueryResponse,
        @service = PartitionLeaderService,
    }
}
