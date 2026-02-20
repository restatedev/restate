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
use std::str::FromStr;

use restate_encoding::{BilrostNewType, NetSerde};

use crate::logs::LogId;
use crate::logs::metadata::SegmentIndex;

/// LogletId is a helper type to generate reliably unique identifiers for individual loglets in a
/// single chain.
///
/// This is not an essential type and loglet providers may choose to use their own type. This type
/// stitches the log-id and a segment-index in a u64 number which can be displayed as
/// `<logid>_<segment_index>`
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Clone,
    Copy,
    derive_more::From,
    derive_more::Deref,
    derive_more::Into,
    BilrostNewType,
    NetSerde,
)]
#[serde(transparent)]
#[repr(transparent)]
pub struct LogletId(u64);

impl LogletId {
    /// Creates a new [`LogletId`] from a [`LogId`] and a [`SegmentIndex`]. The upper
    /// 32 bits are the log_id and the lower are the segment_index.
    pub fn new(log_id: LogId, segment_index: SegmentIndex) -> Self {
        let id = (u64::from(u32::from(log_id)) << 32) | u64::from(u32::from(segment_index));
        Self(id)
    }

    /// It's your responsibility that the value has the right meaning.
    pub const fn new_unchecked(v: u64) -> Self {
        Self(v)
    }

    /// Creates a new [`LogletId`] by incrementing the lower 32 bits (segment index part).
    pub fn next(&self) -> Self {
        assert!(
            self.0 & 0xFFFFFFFF < u64::from(u32::MAX),
            "Segment part must not overflow into the LogId part"
        );
        Self(self.0 + 1)
    }

    pub fn log_id(&self) -> LogId {
        LogId::new(u32::try_from(self.0 >> 32).expect("upper 32 bits should fit into u32"))
    }

    pub fn segment_index(&self) -> SegmentIndex {
        SegmentIndex::from(
            u32::try_from(self.0 & 0xFFFFFFFF).expect("lower 32 bits should fit into u32"),
        )
    }
}

impl Display for LogletId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.log_id(), self.segment_index())
    }
}

impl FromStr for LogletId {
    type Err = <u64 as FromStr>::Err;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains('_') {
            let parts: Vec<&str> = s.split('_').collect();
            let log_id: u32 = parts[0].parse()?;
            let segment_index: u32 = parts[1].parse()?;
            Ok(LogletId::new(
                LogId::from(log_id),
                SegmentIndex::from(segment_index),
            ))
        } else {
            // treat the string as raw replicated log-id
            let id: u64 = s.parse()?;
            Ok(LogletId(id))
        }
    }
}
