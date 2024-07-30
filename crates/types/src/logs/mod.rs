// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::flexbuffers_storage_encode_decode;
use crate::identifiers::PartitionId;
use crate::time::NanosSinceEpoch;

pub mod builder;
pub mod metadata;

#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
    Serialize,
    Deserialize,
)]
pub struct LogId(u64);

impl LogId {
    // This is allows the usage of the first 62 bits for log ids while keeping a space for
    // internal logs as needed. Partitions cannot be larger than 2^62.
    pub const MAX_PARTITION_LOG: LogId = LogId((1 << 62) - 1);
    pub const MIN: LogId = LogId(0);
}

impl LogId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

impl From<PartitionId> for LogId {
    fn from(value: PartitionId) -> Self {
        LogId(*value)
    }
}

/// The log sequence number.
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    derive_more::Into,
    derive_more::From,
    derive_more::Add,
    derive_more::Display,
    Serialize,
    Deserialize,
)]
pub struct Lsn(u64);

impl Lsn {
    pub const fn new(lsn: u64) -> Self {
        Lsn(lsn)
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<crate::protobuf::common::Lsn> for Lsn {
    fn from(lsn: crate::protobuf::common::Lsn) -> Self {
        Self::from(lsn.value)
    }
}

impl From<Lsn> for crate::protobuf::common::Lsn {
    fn from(lsn: Lsn) -> Self {
        let value: u64 = lsn.into();
        Self { value }
    }
}

impl SequenceNumber for Lsn {
    /// The maximum possible sequence number, this is useful when creating a read stream
    /// with an open ended tail.
    const MAX: Self = Lsn(u64::MAX);
    /// 0 is not a valid sequence number. This sequence number represents invalid position
    /// in the log, or that the log has been that has been trimmed.
    const INVALID: Self = Lsn(0);
    /// Guaranteed to be less than or equal to the oldest possible sequence
    /// number in a log. This is useful when seeking to the head of a log.
    const OLDEST: Self = Lsn(1);

    fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    fn prev(self) -> Self {
        Self(self.0.saturating_sub(1))
    }
}

pub trait SequenceNumber
where
    Self:
        Copy + std::fmt::Debug + Sized + Into<u64> + From<u64> + Eq + PartialEq + Ord + PartialOrd,
{
    /// The maximum possible sequence number, this is useful when creating a read stream
    const MAX: Self;
    /// Not a valid sequence number. This sequence number represents invalid position
    /// in the log, or that the log has been that has been trimmed.
    const INVALID: Self;

    /// Guaranteed to be less than or equal to the oldest possible sequence
    /// number in a log. This is useful when seeking to the head of a log.
    const OLDEST: Self;

    fn next(self) -> Self;
    fn prev(self) -> Self;
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Header {
    pub created_at: NanosSinceEpoch,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            created_at: NanosSinceEpoch::now(),
        }
    }
}

/// Owned payload that loglets accept and return as is. This payload is converted
/// into Payload by bifrost on read and write.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Envelope {
    header: Header,
    body: Bytes,
}

/// Owned payload.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Payload {
    header: Header,
    body: Bytes,
}

impl Payload {
    pub fn new(body: impl Into<Bytes>) -> Self {
        Self {
            header: Header::default(),
            body: body.into(),
        }
    }

    pub fn body(&self) -> &Bytes {
        &self.body
    }

    pub fn split(self) -> (Header, Bytes) {
        (self.header, self.body)
    }

    pub fn into_body(self) -> Bytes {
        self.body
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn into_header(self) -> Header {
        self.header
    }

    pub fn body_size(&self) -> usize {
        self.body.len()
    }
}

flexbuffers_storage_encode_decode!(Payload);
