// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Opt-in distributed query task protocol. Installation must explicitly acknowledge
//! the exact version before output can be requested. Unknown versions never fall back.

use std::collections::BTreeMap;

use bytes::Bytes;

use restate_clock::UniqueTimestamp;
use restate_util_string::ReString;

use super::{ProtocolVersion, ServiceTag, bilrost_wire_codec, define_rpc, define_service};

pub const DISTRIBUTED_QUERY_PROTOCOL_VERSION: u32 = 1;

pub struct DistributedQueryService;
define_service! {
    @service = DistributedQueryService,
    @tag = ServiceTag::DistributedQueryService,
}

/// Restate execution identity and the runtime's independent scheduling identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, bilrost::Message)]
pub struct QueryTaskId {
    #[bilrost(1)]
    pub session_id: ReString,
    #[bilrost(2)]
    pub query_ts: UniqueTimestamp,
    #[bilrost(tag(3), encoding(plainbytes))]
    pub runtime_query_id: [u8; 16],
    #[bilrost(4)]
    pub stage: u64,
    #[bilrost(5)]
    pub task: u64,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct QueryTaskInstall {
    #[bilrost(1)]
    pub version: u32,
    #[bilrost(2)]
    pub id: QueryTaskId,
    #[bilrost(3)]
    pub plan: Bytes,
    #[bilrost(4)]
    pub options: BTreeMap<ReString, ReString>,
    #[bilrost(5)]
    pub runtime_headers: BTreeMap<ReString, ReString>,
    #[bilrost(6)]
    pub query_start_time_ns: u64,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct QueryTaskExecute {
    #[bilrost(1)]
    pub id: QueryTaskId,
    #[bilrost(2)]
    pub partition_start: u64,
    #[bilrost(3)]
    pub partition_end: u64,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct QueryTaskNext {
    #[bilrost(1)]
    pub id: QueryTaskId,
    #[bilrost(2)]
    pub partition: u64,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct QueryTaskClose {
    #[bilrost(1)]
    pub id: QueryTaskId,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct QueryTaskInstalled {
    #[bilrost(1)]
    pub version: u32,
    #[bilrost(2)]
    pub schema: Bytes,
}

#[derive(Debug, Clone, bilrost::Message, bilrost::Oneof)]
pub enum QueryTaskReply {
    Unknown,
    #[bilrost(1)]
    Installed(QueryTaskInstalled),
    #[bilrost(2)]
    Executing(()),
    #[bilrost(3)]
    Batch(Bytes),
    #[bilrost(4)]
    End(()),
    #[bilrost(5)]
    Closed(()),
    #[bilrost(6)]
    Failure(ReString),
}

#[derive(Debug, Clone, bilrost::Message, bilrost::Oneof)]
pub enum QueryTaskRequest {
    Unknown,
    #[bilrost(1)]
    Install(QueryTaskInstall),
    #[bilrost(2)]
    Execute(QueryTaskExecute),
    #[bilrost(3)]
    Next(QueryTaskNext),
    #[bilrost(4)]
    Close(QueryTaskClose),
}

define_rpc! { @request = QueryTaskRequest, @response = QueryTaskReply, @service = DistributedQueryService, }
bilrost_wire_codec!(QueryTaskRequest, ProtocolVersion::V5);
bilrost_wire_codec!(QueryTaskReply, ProtocolVersion::V5);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::codec::{EncodeError, WireDecode, WireEncode};

    #[test]
    fn task_messages_require_v5_and_round_trip() {
        let id = QueryTaskId {
            session_id: "session".into(),
            query_ts: UniqueTimestamp::MIN,
            runtime_query_id: [1; 16],
            stage: 1,
            task: 0,
        };
        let request = QueryTaskRequest::Next(QueryTaskNext {
            id: id.clone(),
            partition: 2,
        });
        let reply = QueryTaskReply::Installed(QueryTaskInstalled {
            version: DISTRIBUTED_QUERY_PROTOCOL_VERSION,
            schema: Bytes::from_static(b"schema"),
        });
        let request_bytes = request.encode_to_bytes(ProtocolVersion::V5).unwrap();
        let reply_bytes = reply.encode_to_bytes(ProtocolVersion::V5).unwrap();
        for version in [ProtocolVersion::V2, ProtocolVersion::V3, ProtocolVersion::V4] {
            assert!(matches!(
                request.encode_to_bytes(version),
                Err(EncodeError::IncompatibleVersion {
                    min_required: ProtocolVersion::V5,
                    ..
                })
            ));
            assert!(reply.encode_to_bytes(version).is_err());
            assert!(QueryTaskRequest::try_decode(request_bytes.clone(), version).is_err());
            assert!(QueryTaskReply::try_decode(reply_bytes.clone(), version).is_err());
        }
        let QueryTaskRequest::Next(decoded) =
            QueryTaskRequest::try_decode(request_bytes, ProtocolVersion::V5).unwrap()
        else {
            panic!("expected Next request")
        };
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.partition, 2);
        let QueryTaskReply::Installed(decoded) =
            QueryTaskReply::try_decode(reply_bytes, ProtocolVersion::V5).unwrap()
        else {
            panic!("expected Installed reply")
        };
        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.schema, Bytes::from_static(b"schema"));
    }
}
