// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use super::ServiceTag;
use crate::logs::metadata::SegmentIndex;
use crate::logs::{LogId, LogletId, LogletOffset, SequenceNumber, TailState};
use crate::net::codec::{WireDecode, WireEncode, encode_as_bilrost};
use crate::net::{ProtocolVersion, bilrost_wire_codec, define_rpc, define_service};

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
// Note: Append has custom WireEncode/WireDecode implementations for V2 compatibility
bilrost_wire_codec!(Appended);

define_rpc! {
    @request = GetSequencerState,
    @response = SequencerState,
    @service = SequencerMetaService,
}
bilrost_wire_codec!(GetSequencerState);
bilrost_wire_codec!(SequencerState);

/// Non-success status of sequencer response.
#[derive(Debug, Clone, derive_more::IsVariant, Default, bilrost::Oneof, bilrost::Message)]
pub enum SequencerStatus {
    /// Sealed is returned when the sequencer cannot accept more
    /// [`Append`] requests because it's sealed
    #[bilrost(tag(2), message)]
    Sealed,
    /// Local sequencer is not available anymore, reconfiguration is needed
    #[bilrost(tag(3), message)]
    Gone,
    /// LogletID does not match Segment
    #[bilrost(tag(4), message)]
    LogletIdMismatch,
    /// Invalid LogId
    #[bilrost(tag(5), message)]
    UnknownLogId,
    /// Invalid segment index
    #[bilrost(tag(6), message)]
    UnknownSegmentIndex,
    /// Operation has been rejected, node is not a sequencer
    #[bilrost(tag(7), message)]
    NotSequencer,
    /// Sequencer is shutting down
    #[bilrost(tag(8), message)]
    Shutdown,
    /// Generic error message.
    #[bilrost(tag(9), message)]
    Error {
        #[bilrost(0)]
        retryable: bool,
        #[bilrost(1)]
        message: String,
    },
    /// Future unknown error type
    #[default]
    #[bilrost(empty)]
    Unknown,
}

#[derive(Debug, Clone, bilrost::Message)]
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

#[derive(Debug, Clone, bilrost::Message)]
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
#[derive(Debug, Clone, bilrost::Message)]
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

impl WireEncode for Append {
    fn encode_to_bytes(
        &self,
        protocol_version: ProtocolVersion,
    ) -> Result<bytes::Bytes, crate::net::codec::EncodeError> {
        match protocol_version {
            ProtocolVersion::Unknown => Err(crate::net::codec::EncodeError::IncompatibleVersion {
                type_tag: stringify!(Append),
                min_required: ProtocolVersion::V2,
                actual: protocol_version,
            }),
            ProtocolVersion::V2 => {
                // note: cloning the append message is cheap, but not free.
                // hopefully this code path will be short lived until all
                // nodes has been upgraded to protocol-version: V3
                let msg: compat::Append = self.clone().into();
                Ok(encode_as_bilrost(&msg))
            }
            ProtocolVersion::V3 => Ok(encode_as_bilrost(self)),
        }
    }
}

impl WireDecode for Append {
    type Error = anyhow::Error;

    fn try_decode(
        buf: impl bytes::Buf,
        protocol_version: ProtocolVersion,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        match protocol_version {
            ProtocolVersion::Unknown => Err(anyhow::anyhow!(
                "Append requires at least protocol version V2, got {:?}",
                protocol_version
            )),
            ProtocolVersion::V2 => {
                let msg: compat::Append =
                    crate::net::codec::decode_as_bilrost(buf, protocol_version)?;
                Ok(msg.into())
            }
            ProtocolVersion::V3 => crate::net::codec::decode_as_bilrost(buf, protocol_version),
        }
    }
}

#[derive(Debug, Clone, bilrost::Message)]
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
#[derive(Debug, Clone, bilrost::Message)]
pub struct GetSequencerState {
    #[bilrost(1)]
    pub header: CommonRequestHeader,
    #[bilrost(2)]
    pub force_seal_check: bool,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct SequencerState {
    #[bilrost(1)]
    pub header: CommonResponseHeader,
}

mod compat {
    use crate::net::log_server::CompatibilityPayloads;

    use super::CommonRequestHeader;

    /// V2-compatible Append message that uses unpacked Payloads encoding
    #[derive(Debug, Clone, bilrost::Message)]
    pub struct Append {
        #[bilrost(1)]
        pub header: CommonRequestHeader,
        #[bilrost(2)]
        pub payloads: CompatibilityPayloads,
    }

    impl From<super::Append> for Append {
        fn from(value: super::Append) -> Self {
            Self {
                header: value.header,
                payloads: value.payloads.into(),
            }
        }
    }

    impl From<Append> for super::Append {
        fn from(value: Append) -> Self {
            Self {
                header: value.header,
                payloads: value.payloads.into(),
            }
        }
    }
}
