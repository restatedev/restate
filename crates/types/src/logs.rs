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
    derive_more::Into,
    derive_more::From,
    derive_more::Add,
    derive_more::Display,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct Lsn(u64);

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
        Self(self.0 + 1)
    }

    fn prev(self) -> Self {
        if self == Self::INVALID {
            Self::INVALID
        } else {
            Self(std::cmp::max(Self::OLDEST.0, self.0.saturating_sub(1)))
        }
    }
}

pub trait SequenceNumber
where
    Self: Sized + Into<u64> + From<u64> + Eq + PartialEq + Ord + PartialOrd,
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

/// Owned payload.
#[derive(
    Debug,
    Clone,
    Default,
    Eq,
    PartialEq,
    derive_more::From,
    derive_more::Into,
    derive_more::Deref,
    derive_more::DerefMut,
)]
pub struct Payload(Bytes);

impl From<&str> for Payload {
    fn from(value: &str) -> Self {
        Payload(Bytes::copy_from_slice(value.as_bytes()))
    }
}

impl From<String> for Payload {
    fn from(value: String) -> Self {
        Payload(Bytes::from(value))
    }
}
