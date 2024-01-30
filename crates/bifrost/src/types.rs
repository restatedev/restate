// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd, derive_more::Display)]
pub struct LogId(u64);

/// Index of an entry in the log
impl LogId {
    // This is allows the usage of the first 62 bits for log ids while keeping a space for
    // internal logs as needed. Partitions cannot be larger than 2^62.
    pub const MAX_PARTITION_LOG: LogId = LogId((u64::MAX << 62) - 1);
    pub const MIN: LogId = LogId(0);
}

/// The log sequence number.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd, derive_more::Display)]
pub struct Lsn(u64);

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
}

/// Details about why a log was sealed
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum SealReason {
    /// Log was sealed to perform a repartitioning operation (split or unsplit).
    /// The reader/writer need to figure out where to read/write next.
    Resharding,
    Other(String),
}
