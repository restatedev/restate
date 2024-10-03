// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines messages between replicated loglet instances

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::TargetName;
use crate::logs::metadata::SegmentIndex;
use crate::logs::{LogId, LogletOffset, Record, SequenceNumber, TailState};
use crate::net::define_rpc;
use crate::replicated_loglet::ReplicatedLogletId;

// ----- ReplicatedLoglet Sequencer API -----
define_rpc! {
    @request = Append,
    @response = Appended,
    @request_target = TargetName::ReplicatedLogletAppend,
    @response_target = TargetName::ReplicatedLogletAppended,
}

/// Status of sequencer response.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SequencerStatus {
    /// Ok is returned when request is accepted and processes
    /// successfully. Hence response body is valid
    Ok,
    /// Sealed is returned when the sequencer cannot accept more
    /// [`Append`] requests because it's sealed
    Sealed,
    /// Malformed means request was not accepted duo to bad request
    /// body or invalid data.
    Malformed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommonRequestHeader {
    /// This is used only to locate the loglet params if this operation activates
    /// the remote loglet
    pub log_id: LogId,
    pub segment_index: SegmentIndex,
    /// The loglet_id id globally unique
    pub loglet_id: ReplicatedLogletId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommonResponseHeader {
    pub known_global_tail: Option<LogletOffset>,
    pub sealed: Option<bool>,
    pub status: SequencerStatus,
}

impl CommonResponseHeader {
    pub fn new(tail_state: Option<TailState<LogletOffset>>) -> Self {
        Self {
            known_global_tail: tail_state.map(|t| t.offset()),
            sealed: tail_state.map(|t| t.is_sealed()),
            status: SequencerStatus::Ok,
        }
    }

    pub fn empty() -> Self {
        Self {
            known_global_tail: None,
            sealed: None,
            status: SequencerStatus::Ok,
        }
    }
}

// ** APPEND
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Append {
    #[serde(flatten)]
    pub header: CommonRequestHeader,
    pub payloads: Arc<[Record]>,
}

impl Append {
    pub fn estimated_encode_size(&self) -> usize {
        self.payloads
            .iter()
            .map(|p| p.estimated_encode_size())
            .sum::<usize>()
            + size_of::<CommonRequestHeader>()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Appended {
    #[serde(flatten)]
    pub header: CommonResponseHeader,
    // INVALID if Status indicates that the append failed
    pub first_offset: LogletOffset,
}

impl Deref for Appended {
    type Target = CommonResponseHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl DerefMut for Appended {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl Appended {
    pub fn empty() -> Self {
        Self {
            header: CommonResponseHeader::empty(),
            first_offset: LogletOffset::INVALID,
        }
    }

    pub fn new(tail_state: TailState<LogletOffset>, first_offset: LogletOffset) -> Self {
        Self {
            header: CommonResponseHeader::new(Some(tail_state)),
            first_offset,
        }
    }

    pub fn with_status(mut self, status: SequencerStatus) -> Self {
        self.header.status = status;
        self
    }
}

define_rpc! {
    @request = GetSequencerInfo,
    @response = SequencerInfo,
    @request_target = TargetName::ReplicatedLogletGetInfo,
    @response_target = TargetName::ReplicatedLogletInfo,
}

// ** APPEND
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSequencerInfo {
    #[serde(flatten)]
    pub header: CommonRequestHeader,
}

impl GetSequencerInfo {
    pub fn estimated_encode_size(&self) -> usize {
        size_of::<CommonRequestHeader>()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequencerInfo {
    #[serde(flatten)]
    pub header: CommonResponseHeader,
}
