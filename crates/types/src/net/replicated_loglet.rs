// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use super::ServiceTag;
use crate::logs::metadata::SegmentIndex;
use crate::logs::{LogId, LogletId, LogletOffset, Record, SequenceNumber, TailState};
use crate::net::{default_wire_codec, define_rpc, define_service};

pub struct SequencerDataService;
define_service! {
    @service = SequencerDataService,
    @tag = ServiceTag::SequencerDataService,
}

pub struct SequencerMetaService;
define_service! {
    @service = SequencerMetaService,
    @tag = ServiceTag::SequencerMetaService,
}

// ----- ReplicatedLoglet Sequencer API -----
define_rpc! {
    @request = Append,
    @response = Appended,
    @service = SequencerDataService,
}
default_wire_codec!(Append);
default_wire_codec!(Appended);

define_rpc! {
    @request = GetSequencerState,
    @response = SequencerState,
    @service = SequencerMetaService,
}
default_wire_codec!(GetSequencerState);
default_wire_codec!(SequencerState);

/// Status of sequencer response.
#[derive(Debug, Clone, Serialize, Deserialize, derive_more::IsVariant)]
pub enum SequencerStatus {
    /// Ok is returned when request is accepted and processes
    /// successfully. Hence response body is valid
    Ok,
    /// Sealed is returned when the sequencer cannot accept more
    /// [`Append`] requests because it's sealed
    Sealed,
    /// Local sequencer is not available anymore, reconfiguration is needed
    Gone,
    /// LogletID does not match Segment
    LogletIdMismatch,
    /// Invalid LogId
    UnknownLogId,
    /// Invalid segment index
    UnknownSegmentIndex,
    /// Operation has been rejected, node is not a sequencer
    NotSequencer,
    /// Sequencer is shutting down
    Shutdown,
    /// Generic error message.
    Error { retryable: bool, message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommonRequestHeader {
    /// This is used only to locate the loglet params if this operation activates
    /// the remote loglet
    pub log_id: LogId,
    pub segment_index: SegmentIndex,
    /// The loglet_id id globally unique
    pub loglet_id: LogletId,
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
    pub header: CommonRequestHeader,
    pub payloads: Arc<[Record]>,
}

impl Append {
    pub fn estimated_encode_size(&self) -> usize {
        self.payloads
            .iter()
            .map(|p| p.estimated_encode_size())
            .sum()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Appended {
    pub header: CommonResponseHeader,
    // INVALID if Status indicates that the append failed
    pub last_offset: LogletOffset,
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
            last_offset: LogletOffset::INVALID,
        }
    }

    pub fn new(tail_state: TailState<LogletOffset>, last_offset: LogletOffset) -> Self {
        Self {
            header: CommonResponseHeader::new(Some(tail_state)),
            last_offset,
        }
    }

    pub fn with_status(mut self, status: SequencerStatus) -> Self {
        self.header.status = status;
        self
    }
}

// ** GET_TAIL_INFO
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSequencerState {
    pub header: CommonRequestHeader,
    pub force_seal_check: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequencerState {
    pub header: CommonResponseHeader,
}
