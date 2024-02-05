// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: Remove after fleshing the code out.
#![allow(dead_code)]

use bytes::Bytes;

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
    serde::Serialize,
    serde::Deserialize,
)]
pub struct LogId(u64);

/// Index of an entry in the log
impl LogId {
    // This is allows the usage of the first 62 bits for log ids while keeping a space for
    // internal logs as needed. Partitions cannot be larger than 2^62.
    pub const MAX_PARTITION_LOG: LogId = LogId((1 << 62) - 1);
    pub const MIN: LogId = LogId(0);
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
    serde::Serialize,
    serde::Deserialize,
    derive_more::Into,
    derive_more::Add,
    derive_more::Display,
)]
pub struct Lsn(pub(crate) u64);

/// Index of an entry in the log
impl Lsn {
    /// The maximum possible sequence number, this is useful when creating a read stream
    /// with an open ended tail.
    pub const MAX: Lsn = Lsn(u64::MAX);
    /// 0 is not a valid sequence number. This sequence number represents invalid position
    /// in the log, or that the log has been that has been trimmed.
    pub const INVALID: Lsn = Lsn(0);
    /// Guaranteed to be less than or equal to the oldest possible sequence
    /// number in a log. This is useful when seeking to the head of a log.
    pub const OLDEST: Lsn = Lsn(1);

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub fn prev(self) -> Self {
        if self == Self::INVALID {
            Self::INVALID
        } else {
            Self(std::cmp::max(Self::OLDEST.0, self.0 - 1))
        }
    }
}

/// Log metadata version.
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
    serde::Serialize,
    serde::Deserialize,
)]
#[display(fmt = "v{}", _0)]
pub struct Version(u64);

impl Version {
    pub const INVALID: Version = Version(0);
}

/// Details about why a log was sealed
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum SealReason {
    /// Log was sealed to perform a repartitioning operation (split or unsplit).
    /// The reader/writer need to figure out where to read/write next.
    Resharding,
    Other(String),
}

/// A single entry in the log.
#[derive(Debug, Clone, Default)]
pub struct DataRecord {
    header: Header,
    payload: Payload,
}

#[derive(Debug, Clone)]
pub enum MaybeRecord {
    TrimGap { until: Lsn },
    Data(Lsn, DataRecord),
    Seal(SealReason),
}

#[derive(Debug, Clone, Default)]
pub struct Header {}

/// Owned payload.
#[derive(Debug, Clone, Default)]
pub struct Payload {
    inner: Bytes,
}

#[derive(Debug, Clone, Default)]
pub struct AppendAttributes {}

#[derive(Debug, Clone, Default)]
pub struct ReadAttributes {}
