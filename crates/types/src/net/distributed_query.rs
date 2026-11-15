// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Distributed query tasks: how a query coordinator runs a part of a query plan,
//! a task, on another node. They need network protocol V7: a connection whose
//! peers negotiated an older version can't carry them, so a task to such a node
//! fails before anything runs there.
//!
//! A task takes four requests ([`QueryTaskRequest`]), each answered by a
//! [`QueryTaskReply`]:
//! 1. **Install** sends the task's plan and the query's context. The worker
//!    answers `Installed`, with the schema of the task's output.
//! 2. **Execute** starts the task over a half-open range of its plan's output
//!    partitions (DataFusion partitions, not Restate partitions). Answer:
//!    `Executing`.
//! 3. **Next** asks for the next batch of one output partition. Answer: `Batch`,
//!    one Arrow IPC-encoded record batch, or `End` once that partition has no
//!    more.
//! 4. **Close** releases the task. Answer: `Closed`.
//!
//! Any request can be answered with `Failure` instead, a [`QueryTaskError`]:
//! the node that raised it, what kind of failure it is (storage, admission,
//! execution, …), the cause's own message and, for admission failures, typed
//! details, for example that the partition's leader moved and to which node.
//! The coordinator reports it once, for example as
//! `N2:1: storage: 'service_stats/1' is not ready on partition 12`.
//!
//! For example, a coordinator reading output partitions 0 and 1 from a worker
//! installs the task, executes it over `0..2`, sends `Next` for each partition
//! until it gets `End`, and closes it.

mod error;

pub use error::{PartitionReadError, QueryErrorCode, QueryFailureKind, QueryTaskError};

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;

use restate_clock::UniqueTimestamp;
use restate_util_string::ReString;

use super::{ProtocolVersion, ServiceTag, bilrost_wire_codec, define_rpc, define_service};

/// The fabric service that carries distributed query tasks.
pub struct DistributedQueryService;
define_service! {
    @service = DistributedQueryService,
    @tag = ServiceTag::DistributedQueryService,
}

/// Identifies a task: the Restate query it belongs to (session and query
/// timestamp), and its place in the distributed runtime's schedule (the
/// runtime's query ID, the stage and the task within it).
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

/// Installs a task: its encoded plan, and the query's context that the worker
/// needs to run it and to report it, such as its headers and redacted SQL.
#[derive(Debug, Clone, bilrost::Message)]
pub struct QueryTaskInstall {
    #[bilrost(1)]
    pub id: QueryTaskId,
    #[bilrost(2)]
    pub plan: Bytes,
    #[bilrost(3)]
    pub options: BTreeMap<ReString, ReString>,
    #[bilrost(4)]
    pub runtime_headers: BTreeMap<ReString, ReString>,
    #[bilrost(5)]
    pub query_start_time_ns: u64,
    #[bilrost(6)]
    pub query_headers: Vec<QueryTaskHeader>,
    #[bilrost(7)]
    pub redacted_sql: ReString,
    #[bilrost(8)]
    pub planning_duration: Duration,
}

/// A header of the original query. A list, so that repeated headers and raw
/// byte values survive.
#[derive(Debug, Clone, bilrost::Message)]
pub struct QueryTaskHeader {
    #[bilrost(1)]
    pub name: ReString,
    #[bilrost(2)]
    pub value: Bytes,
}

/// Starts an installed task over its plan's output partitions
/// `partition_start..partition_end`.
#[derive(Debug, Clone, bilrost::Message)]
pub struct QueryTaskExecute {
    #[bilrost(1)]
    pub id: QueryTaskId,
    #[bilrost(2)]
    pub partition_start: u64,
    #[bilrost(3)]
    pub partition_end: u64,
}

/// Asks for the next batch of output partition `partition`.
#[derive(Debug, Clone, bilrost::Message)]
pub struct QueryTaskNext {
    #[bilrost(1)]
    pub id: QueryTaskId,
    #[bilrost(2)]
    pub partition: u64,
}

/// Releases a task.
#[derive(Debug, Clone, bilrost::Message)]
pub struct QueryTaskClose {
    #[bilrost(1)]
    pub id: QueryTaskId,
}

/// The answer to an install: the encoded schema of the task's output.
#[derive(Debug, Clone, bilrost::Message)]
pub struct QueryTaskInstalled {
    #[bilrost(1)]
    pub schema: Bytes,
}

/// The answer to any task request.
#[derive(Debug, Clone, bilrost::Message)]
pub struct QueryTaskReply {
    #[bilrost(oneof(1, 2, 3, 4, 5, 6))]
    pub body: QueryTaskReplyBody,
}

/// What a task reply carries; see the module documentation.
#[derive(Debug, Clone, bilrost::Message, bilrost::Oneof)]
pub enum QueryTaskReplyBody {
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
    Failure(QueryTaskError),
}

impl From<QueryTaskReplyBody> for QueryTaskReply {
    fn from(body: QueryTaskReplyBody) -> Self {
        Self { body }
    }
}

impl QueryTaskReply {
    /// A `Failure` reply for `error`.
    pub fn failure(error: QueryTaskError) -> Self {
        QueryTaskReplyBody::Failure(error).into()
    }

    /// The reply's body, or its failure.
    pub fn into_result(self) -> Result<QueryTaskReplyBody, QueryTaskError> {
        match self.body {
            QueryTaskReplyBody::Failure(error) => Err(error),
            body => Ok(body),
        }
    }
}

/// A request about one task; see the module documentation.
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
bilrost_wire_codec!(QueryTaskRequest, ProtocolVersion::V7);
bilrost_wire_codec!(QueryTaskReply, ProtocolVersion::V7);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GenerationalNodeId;
    use crate::identifiers::PartitionId;
    use crate::net::codec::{EncodeError, WireDecode, WireEncode};

    #[test]
    fn task_messages_require_v7_and_round_trip() {
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
        let reply = QueryTaskReply::from(QueryTaskReplyBody::Installed(QueryTaskInstalled {
            schema: Bytes::from_static(b"schema"),
        }));
        let request_bytes = request.encode_to_bytes(ProtocolVersion::V7).unwrap();
        let reply_bytes = reply.encode_to_bytes(ProtocolVersion::V7).unwrap();
        for version in [
            ProtocolVersion::Unknown,
            ProtocolVersion::V3,
            ProtocolVersion::V4,
            ProtocolVersion::V6,
        ] {
            assert!(matches!(
                request.encode_to_bytes(version),
                Err(EncodeError::IncompatibleVersion {
                    min_required: ProtocolVersion::V7,
                    ..
                })
            ));
            assert!(reply.encode_to_bytes(version).is_err());
            assert!(QueryTaskRequest::try_decode(request_bytes.clone(), version).is_err());
            assert!(QueryTaskReply::try_decode(reply_bytes.clone(), version).is_err());
        }
        let QueryTaskRequest::Next(decoded) =
            QueryTaskRequest::try_decode(request_bytes, ProtocolVersion::V7).unwrap()
        else {
            panic!("expected Next request")
        };
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.partition, 2);
        let QueryTaskReplyBody::Installed(decoded) =
            QueryTaskReply::try_decode(reply_bytes, ProtocolVersion::V7)
                .unwrap()
                .body
        else {
            panic!("expected Installed reply")
        };
        assert_eq!(decoded.schema, Bytes::from_static(b"schema"));
    }

    #[test]
    fn failures_carry_their_origin_kind_message_and_details() {
        let mut failure = PartitionReadError::NotLeader {
            partition: PartitionId::from(7),
            leader: Some(GenerationalNodeId::new(2, 3)),
        }
        .to_task_error();
        failure.origin = Some(GenerationalNodeId::new(1, 2));
        let bytes = QueryTaskReply::failure(failure.clone())
            .encode_to_bytes(ProtocolVersion::V7)
            .unwrap();
        let decoded = QueryTaskReply::try_decode(bytes, ProtocolVersion::V7).unwrap();
        assert_eq!(decoded.into_result().unwrap_err(), failure);
        assert_eq!(
            failure.to_string(),
            "N1:2: admission: not leader for partition 7; last observed leader: N2:3"
        );
    }
}
