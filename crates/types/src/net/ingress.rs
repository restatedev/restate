// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::identifiers::PartitionId;
use crate::logs::{HasRecordKeys, Keys};
use crate::net::partition_processor::PartitionLeaderService;
use crate::net::{RpcRequest, bilrost_wire_codec, default_wire_codec, define_rpc};
use crate::storage::{StorageCodec, StorageCodecKind, StorageEncode, StorageEncodeError};

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub enum IngestDedup {
    #[default]
    None,
    Producer {
        producer_id: u128,
        offset: u64,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct IngestRecord {
    pub keys: Keys,
    pub record: Bytes,
}

impl IngestRecord {
    pub fn from_parts<T>(keys: Keys, record: T) -> Self
    where
        T: StorageEncode,
    {
        let mut buf = BytesMut::new();
        StorageCodec::encode(&record, &mut buf).expect("encode to pass");

        Self {
            keys,
            record: buf.freeze(),
        }
    }
}

impl HasRecordKeys for IngestRecord {
    fn record_keys(&self) -> Keys {
        self.keys.clone()
    }
}

impl StorageEncode for IngestRecord {
    fn default_codec(&self) -> StorageCodecKind {
        self.record.default_codec()
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
        self.record.encode(buf)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct IngestRequest {
    pub records: Arc<[IngestRecord]>,
}

impl From<Arc<[IngestRecord]>> for IngestRequest {
    fn from(records: Arc<[IngestRecord]>) -> Self {
        Self { records }
    }
}

// todo(azmy): Use bilrost (depends on the payload)
default_wire_codec!(IngestRequest);

#[derive(Debug, bilrost::Oneof, bilrost::Message)]
pub enum IngestResponse {
    Unknown,
    #[bilrost(tag = 1, message)]
    Ack,
    #[bilrost(tag = 2, message)]
    Starting,
    #[bilrost(tag = 3, message)]
    Stopping,
    #[bilrost(tag = 4, message)]
    NotLeader {
        of: PartitionId,
    },
    #[bilrost(tag = 5, message)]
    Internal {
        msg: String,
    },
}

bilrost_wire_codec!(IngestResponse);

define_rpc! {
    @request=IngestRequest,
    @response=IngestResponse,
    @service=PartitionLeaderService,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ReceivedIngestRequest {
    pub records: Vec<IngestRecord>,
}

default_wire_codec!(ReceivedIngestRequest);

/// The [`ReceivedIngestRequest`] uses the same TYPE
/// as [`IngestRequest`] to be able to directly decode
/// received RPC message directly to this type.
impl RpcRequest for ReceivedIngestRequest {
    const TYPE: &str = stringify!(IngestRequest);
    type Response = IngestResponse;
    type Service = PartitionLeaderService;
}
