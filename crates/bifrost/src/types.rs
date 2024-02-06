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

use crate::loglet::LogletOffset;

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
    derive_more::From,
    derive_more::Add,
    derive_more::Display,
)]
pub struct Lsn(pub(crate) u64);

impl Lsn {
    /// Converts a loglet offset into the virtual address (LSN).
    pub(crate) fn offset_by<S: SequenceNumber>(self, offset: S) -> Self {
        // This assumes that this will not overflow. That's not guaranteed to always be the
        // case but it should be extremely rare that it'd be okay to just wrap in this case.
        //
        // We subtract from OLDEST because loglets start their offset from 1, not 0.
        // 1 is the oldest valid offset within a loglet, 0 is an invalid offset.
        debug_assert!(offset != S::INVALID);
        Self(self.0.wrapping_add(offset.into()) - S::OLDEST.into())
    }

    /// Convert an LSN back to a loglet offset given a base_lsn.
    pub(crate) fn into_offset(self, base_lsn: Lsn) -> LogletOffset {
        LogletOffset(self.0.saturating_sub(base_lsn.0))
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

/// Details about why a log was sealed
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum SealReason {
    /// Log was sealed to perform a repartitioning operation (split or unsplit).
    /// The reader/writer need to figure out where to read/write next.
    Resharding,
    Other(String),
}

/// A single entry in the log.
#[derive(Debug, Clone)]
pub struct LogRecord<S: SequenceNumber = Lsn> {
    pub offset: S,
    pub record: Record<S>,
}

impl<S: SequenceNumber> LogRecord<S> {
    pub(crate) fn new_data(offset: S, payload: Payload) -> Self {
        Self {
            offset,
            record: Record::Data(payload),
        }
    }

    pub(crate) fn new_trim_gap(offset: S, until: S) -> Self {
        LogRecord {
            offset,
            record: Record::TrimGap(TrimGap { until }),
        }
    }

    pub(crate) fn with_base_lsn(self, base_lsn: Lsn) -> LogRecord<Lsn> {
        let record = match self.record {
            Record::TrimGap(_) => todo!(),
            Record::Data(payload) => Record::Data(payload),
            Record::Seal(reason) => Record::Seal(reason),
        };

        LogRecord {
            offset: base_lsn.offset_by(self.offset),
            record,
        }
    }
}

#[derive(Debug, Clone, strum_macros::EnumIs)]
pub enum Record<S: SequenceNumber = Lsn> {
    TrimGap(TrimGap<S>),
    Data(Payload),
    Seal(SealReason),
}

impl<S: SequenceNumber> Record<S> {
    pub fn payload(&self) -> Option<&Payload> {
        match self {
            Record::Data(payload) => Some(payload),
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn into_payload_unchecked(self) -> Payload {
        match self {
            Record::Data(payload) => payload,
            _ => panic!("not a data record"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TrimGap<S: SequenceNumber> {
    until: S,
}

/// Owned payload.
#[derive(Debug, Clone, Default, derive_more::From, derive_more::Into)]
pub struct Payload(Bytes);

impl From<&str> for Payload {
    fn from(value: &str) -> Self {
        Payload(Bytes::copy_from_slice(value.as_bytes()))
    }
}

impl Payload {
    #[cfg(test)]
    pub fn into_string(&self) -> String {
        String::from_utf8(self.0.to_vec()).unwrap()
    }
}

#[derive(Debug, Clone, Default)]
pub struct FindTailAttributes {
    // Ensure that we are reading the most recent metadata. This should be used when
    // linearizable metadata reads are required.
    // TODO: consistent_read: bool,
}
