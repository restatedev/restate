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

use restate_encoding::BilrostAs;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

use super::ServiceTag;
use crate::logs::metadata::SegmentIndex;
use crate::logs::{LogId, LogletId, LogletOffset, SequenceNumber, TailState};
use crate::net::{bilrost_wire_codec_with_v1_fallback, define_rpc, define_service};

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
bilrost_wire_codec_with_v1_fallback!(Append);
bilrost_wire_codec_with_v1_fallback!(Appended);

define_rpc! {
    @request = GetSequencerState,
    @response = SequencerState,
    @service = SequencerMetaService,
}
bilrost_wire_codec_with_v1_fallback!(GetSequencerState);
bilrost_wire_codec_with_v1_fallback!(SequencerState);

/// Non-success status of sequencer response.
#[derive(Debug, Clone, derive_more::IsVariant, BilrostAs, Default)]
#[bilrost_as(dto::SequencerStatus)]
pub enum SequencerStatus {
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
    /// Future unknown error type
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct CommonRequestHeader {
    /// This is used only to locate the loglet params if this operation activates
    /// the remote loglet
    #[bilrost(1)]
    pub log_id: LogId,
    #[bilrost(2)]
    pub segment_index: SegmentIndex,
    /// The loglet_id id globally unique
    #[bilrost(3)]
    pub loglet_id: LogletId,
}

#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
// todo: drop serde(from, into) in version 1.5
#[serde(
    from = "dto::CommonResponseHeaderV1",
    into = "dto::CommonResponseHeaderV1"
)]
pub struct CommonResponseHeader {
    #[bilrost(1)]
    pub known_global_tail: Option<LogletOffset>,
    #[bilrost(2)]
    pub sealed: Option<bool>,
    #[bilrost(3)]
    pub status: Option<SequencerStatus>,
}

impl CommonResponseHeader {
    pub fn new(tail_state: Option<TailState<LogletOffset>>) -> Self {
        Self {
            known_global_tail: tail_state.map(|t| t.offset()),
            sealed: tail_state.map(|t| t.is_sealed()),
            status: None,
        }
    }

    pub fn empty() -> Self {
        Self {
            known_global_tail: None,
            sealed: None,
            status: None,
        }
    }
}

// ** APPEND
#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct Append {
    #[bilrost(1)]
    pub header: CommonRequestHeader,
    #[bilrost(2)]
    pub payloads: crate::net::log_server::Payloads,
}

impl Append {
    pub fn estimated_encode_size(&self) -> usize {
        self.payloads
            .iter()
            .map(|p| p.estimated_encode_size())
            .sum()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct Appended {
    #[bilrost(1)]
    pub header: CommonResponseHeader,
    // INVALID if Status indicates that the append failed
    #[bilrost(2)]
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

    pub fn with_status(mut self, status: Option<SequencerStatus>) -> Self {
        self.header.status = status;
        self
    }
}

// ** GET_TAIL_INFO
#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct GetSequencerState {
    #[bilrost(1)]
    pub header: CommonRequestHeader,
    #[bilrost(2)]
    pub force_seal_check: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct SequencerState {
    #[bilrost(1)]
    pub header: CommonResponseHeader,
}

mod dto {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, bilrost::Oneof, bilrost::Message)]
    pub enum SequencerStatus {
        #[bilrost(2)]
        Sealed(()),
        #[bilrost(3)]
        Gone(()),
        #[bilrost(4)]
        LogletIdMismatch(()),
        #[bilrost(5)]
        UnknownLogId(()),
        #[bilrost(6)]
        UnknownSegmentIndex(()),
        #[bilrost(7)]
        NotSequencer(()),
        #[bilrost(8)]
        Shutdown(()),
        #[bilrost(9)]
        Error((bool, String)),
        Unknown,
    }

    impl From<&super::SequencerStatus> for SequencerStatus {
        fn from(status: &super::SequencerStatus) -> Self {
            match status {
                &super::SequencerStatus::Sealed => SequencerStatus::Sealed(()),
                &super::SequencerStatus::Gone => SequencerStatus::Gone(()),
                &super::SequencerStatus::LogletIdMismatch => SequencerStatus::LogletIdMismatch(()),
                &super::SequencerStatus::UnknownLogId => SequencerStatus::UnknownLogId(()),
                &super::SequencerStatus::UnknownSegmentIndex => {
                    SequencerStatus::UnknownSegmentIndex(())
                }
                &super::SequencerStatus::NotSequencer => SequencerStatus::NotSequencer(()),
                &super::SequencerStatus::Shutdown => SequencerStatus::Shutdown(()),
                super::SequencerStatus::Error { retryable, message } => {
                    SequencerStatus::Error((*retryable, message.clone()))
                }
                &super::SequencerStatus::Unknown => SequencerStatus::Unknown,
            }
        }
    }

    impl From<SequencerStatus> for super::SequencerStatus {
        fn from(status: SequencerStatus) -> Self {
            match status {
                SequencerStatus::Sealed(()) => super::SequencerStatus::Sealed,
                SequencerStatus::Gone(()) => super::SequencerStatus::Gone,
                SequencerStatus::LogletIdMismatch(()) => super::SequencerStatus::LogletIdMismatch,
                SequencerStatus::UnknownLogId(()) => super::SequencerStatus::UnknownLogId,
                SequencerStatus::UnknownSegmentIndex(()) => {
                    super::SequencerStatus::UnknownSegmentIndex
                }
                SequencerStatus::NotSequencer(()) => super::SequencerStatus::NotSequencer,
                SequencerStatus::Shutdown(()) => super::SequencerStatus::Shutdown,
                SequencerStatus::Error((retryable, message)) => {
                    super::SequencerStatus::Error { retryable, message }
                }
                SequencerStatus::Unknown => super::SequencerStatus::Unknown,
            }
        }
    }

    // This is for backward compatibility with serde/flexbuffers
    // only needed during update from v1.3.2 to v1.4.
    // TODO: remove this in version 1.5
    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub enum SequencerStatusV1 {
        #[default]
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

    impl From<super::SequencerStatus> for SequencerStatusV1 {
        fn from(status: super::SequencerStatus) -> Self {
            match status {
                super::SequencerStatus::Sealed => SequencerStatusV1::Sealed,
                super::SequencerStatus::Gone => SequencerStatusV1::Gone,
                super::SequencerStatus::LogletIdMismatch => SequencerStatusV1::LogletIdMismatch,
                super::SequencerStatus::UnknownLogId => SequencerStatusV1::UnknownLogId,
                super::SequencerStatus::UnknownSegmentIndex => {
                    SequencerStatusV1::UnknownSegmentIndex
                }
                super::SequencerStatus::NotSequencer => SequencerStatusV1::NotSequencer,
                super::SequencerStatus::Shutdown => SequencerStatusV1::Shutdown,
                super::SequencerStatus::Error { retryable, message } => {
                    SequencerStatusV1::Error { retryable, message }
                }
                super::SequencerStatus::Unknown => SequencerStatusV1::Error {
                    retryable: false,
                    message: "Unknown error".to_string(),
                },
            }
        }
    }

    impl From<SequencerStatusV1> for super::SequencerStatus {
        fn from(value: SequencerStatusV1) -> Self {
            match value {
                SequencerStatusV1::Ok => unreachable!(),
                SequencerStatusV1::Sealed => Self::Sealed,
                SequencerStatusV1::Gone => Self::Gone,
                SequencerStatusV1::LogletIdMismatch => Self::LogletIdMismatch,
                SequencerStatusV1::UnknownLogId => Self::UnknownLogId,
                SequencerStatusV1::UnknownSegmentIndex => Self::UnknownSegmentIndex,
                SequencerStatusV1::NotSequencer => Self::NotSequencer,
                SequencerStatusV1::Shutdown => Self::Shutdown,
                SequencerStatusV1::Error { retryable, message } => {
                    Self::Error { retryable, message }
                }
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct CommonResponseHeaderV1 {
        pub known_global_tail: Option<LogletOffset>,
        pub sealed: Option<bool>,
        pub status: SequencerStatusV1,
    }

    impl From<super::CommonResponseHeader> for CommonResponseHeaderV1 {
        fn from(header: super::CommonResponseHeader) -> Self {
            Self {
                known_global_tail: header.known_global_tail,
                sealed: header.sealed,
                status: header
                    .status
                    .map(|s| s.into())
                    .unwrap_or(SequencerStatusV1::Ok),
            }
        }
    }

    impl From<CommonResponseHeaderV1> for super::CommonResponseHeader {
        fn from(header: CommonResponseHeaderV1) -> Self {
            Self {
                known_global_tail: header.known_global_tail,
                sealed: header.sealed,
                status: match header.status {
                    SequencerStatusV1::Ok => None,
                    status => Some(status.into()),
                },
            }
        }
    }
}
