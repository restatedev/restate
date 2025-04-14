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

use prost_dto::{FromProst, IntoProst};
use serde::{Deserialize, Serialize};

use super::TargetName;
use super::codec::V2Convertible;
use crate::errors::ConversionError;
use crate::logs::metadata::SegmentIndex;
use crate::logs::{LogId, LogletId, LogletOffset, Record, SequenceNumber, TailState};
use crate::net::define_rpc;
use crate::protobuf::net::replicated_loglet as proto;

// ----- ReplicatedLoglet Sequencer API -----
define_rpc! {
    @request = Append,
    @response = Appended,
    @request_target = TargetName::ReplicatedLogletAppend,
    @response_target = TargetName::ReplicatedLogletAppended,
}

define_rpc! {
    @request = GetSequencerState,
    @response = SequencerState,
    @request_target = TargetName::ReplicatedLogletGetSequencerState,
    @response_target = TargetName::ReplicatedLogletSequencerState,
}

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

impl From<SequencerStatus> for proto::SequencerStatus {
    fn from(value: SequencerStatus) -> Self {
        use proto::sequencer_status::Status;

        let (status, message, retryable) = match value {
            SequencerStatus::Ok => (Status::Ok, None, None),
            SequencerStatus::Sealed => (Status::Sealed, None, None),
            SequencerStatus::Gone => (Status::Gone, None, None),
            SequencerStatus::LogletIdMismatch => (Status::LogletIdMismatch, None, None),
            SequencerStatus::UnknownLogId => (Status::UnknownLogId, None, None),
            SequencerStatus::UnknownSegmentIndex => (Status::UnknownSegmentIndex, None, None),
            SequencerStatus::NotSequencer => (Status::NotSequencer, None, None),
            SequencerStatus::Shutdown => (Status::Shutdown, None, None),
            SequencerStatus::Error { retryable, message } => {
                (Status::Error, Some(message), Some(retryable))
            }
        };

        Self {
            status: status.into(),
            message,
            retryable,
        }
    }
}

impl TryFrom<proto::SequencerStatus> for SequencerStatus {
    type Error = ConversionError;

    fn try_from(
        value: proto::SequencerStatus,
    ) -> Result<Self, <SequencerStatus as TryFrom<proto::SequencerStatus>>::Error> {
        use proto::sequencer_status::Status;

        match value.status() {
            Status::Unknown => Err(ConversionError::invalid_data("status")),
            Status::Ok => Ok(Self::Ok),
            Status::Sealed => Ok(Self::Sealed),
            Status::Gone => Ok(Self::Gone),
            Status::LogletIdMismatch => Ok(Self::LogletIdMismatch),
            Status::UnknownLogId => Ok(Self::UnknownLogId),
            Status::UnknownSegmentIndex => Ok(Self::UnknownSegmentIndex),
            Status::NotSequencer => Ok(Self::NotSequencer),
            Status::Shutdown => Ok(Self::Shutdown),
            Status::Error => Ok(Self::Error {
                retryable: value.retryable(),
                message: value
                    .message
                    .unwrap_or_else(|| String::from("no error message")),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target=crate::protobuf::net::replicated_loglet::CommonRequestHeader)]
pub struct CommonRequestHeader {
    /// This is used only to locate the loglet params if this operation activates
    /// the remote loglet
    pub log_id: LogId,
    pub segment_index: SegmentIndex,
    /// The loglet_id id globally unique
    pub loglet_id: LogletId,
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoProst)]
#[prost(target=crate::protobuf::net::replicated_loglet::CommonResponseHeader)]
pub struct CommonResponseHeader {
    pub known_global_tail: Option<LogletOffset>,
    pub sealed: Option<bool>,
    #[prost(required)]
    pub status: SequencerStatus,
}

impl TryFrom<proto::CommonResponseHeader> for CommonResponseHeader {
    type Error = ConversionError;

    fn try_from(value: proto::CommonResponseHeader) -> Result<Self, Self::Error> {
        let proto::CommonResponseHeader {
            known_global_tail,
            sealed,
            status,
        } = value;

        Ok(Self {
            known_global_tail: known_global_tail.map(Into::into),
            sealed,
            status: status
                .ok_or_else(|| ConversionError::missing_field("status"))?
                .try_into()?,
        })
    }
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

impl From<Append> for proto::Append {
    fn from(value: Append) -> Self {
        let Append { header, payloads } = value;
        Self {
            header: Some(header.into()),
            payloads: payloads.iter().cloned().map(Into::into).collect(),
        }
    }
}

impl TryFrom<proto::Append> for Append {
    type Error = ConversionError;

    fn try_from(value: proto::Append) -> Result<Self, Self::Error> {
        let proto::Append { header, payloads } = value;

        Ok(Append {
            header: header
                .ok_or_else(|| ConversionError::missing_field("header"))?
                .into(),
            payloads: {
                let mut result = Vec::with_capacity(payloads.len());
                for record in payloads {
                    result.push(record.try_into()?);
                }
                Arc::from(result)
            },
        })
    }
}

impl V2Convertible for Append {
    type Target = proto::Append;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

impl Append {
    pub fn estimated_encode_size(&self) -> usize {
        self.payloads
            .iter()
            .map(|p| p.estimated_encode_size())
            .sum()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoProst)]
#[prost(target=crate::protobuf::net::replicated_loglet::Appended)]
pub struct Appended {
    #[prost(required)]
    pub header: CommonResponseHeader,
    // INVALID if Status indicates that the append failed
    pub last_offset: LogletOffset,
}

impl TryFrom<proto::Appended> for Appended {
    type Error = ConversionError;

    fn try_from(value: proto::Appended) -> Result<Self, Self::Error> {
        let proto::Appended {
            header,
            last_offset,
        } = value;

        Ok(Self {
            header: header
                .ok_or_else(|| ConversionError::missing_field("header"))?
                .try_into()?,
            last_offset: last_offset.into(),
        })
    }
}

impl V2Convertible for Appended {
    type Target = proto::Appended;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
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
#[derive(Debug, Clone, Serialize, Deserialize, FromProst, IntoProst)]
#[prost(target=crate::protobuf::net::replicated_loglet::GetSequencerState)]
pub struct GetSequencerState {
    #[prost(required)]
    pub header: CommonRequestHeader,
    pub force_seal_check: bool,
}

impl V2Convertible for GetSequencerState {
    type Target = proto::GetSequencerState;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        Ok(target.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoProst)]
#[prost(target=crate::protobuf::net::replicated_loglet::SequenceState)]
pub struct SequencerState {
    #[prost(required)]
    pub header: CommonResponseHeader,
}

impl TryFrom<proto::SequenceState> for SequencerState {
    type Error = ConversionError;

    fn try_from(value: proto::SequenceState) -> Result<Self, Self::Error> {
        let proto::SequenceState { header } = value;

        Ok(Self {
            header: header
                .ok_or_else(|| ConversionError::missing_field("header"))?
                .try_into()?,
        })
    }
}

impl V2Convertible for SequencerState {
    type Target = proto::SequenceState;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}
