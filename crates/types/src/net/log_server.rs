// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use bitflags::bitflags;
use prost_dto::{FromProst, IntoProst};
use serde::{Deserialize, Serialize};

use super::codec::{V2Convertible, WireDecode, WireEncode};
use super::{RpcRequest, TargetName};
use crate::GenerationalNodeId;
use crate::errors::ConversionError;
use crate::logs::{KeyFilter, LogletId, LogletOffset, Record, SequenceNumber, TailState};
use crate::protobuf::net::log_server as proto;
use crate::time::MillisSinceEpoch;

pub trait LogServerRequest: RpcRequest + WireEncode + Sync + Send + 'static {
    fn header(&self) -> &LogServerRequestHeader;
    fn header_mut(&mut self) -> &mut LogServerRequestHeader;
    fn refresh_header(&mut self, known_global_tail: LogletOffset) {
        let loglet_id = self.header().loglet_id;
        *self.header_mut() = LogServerRequestHeader {
            loglet_id,
            known_global_tail,
        }
    }
}

pub trait LogServerResponse: WireDecode + Sync + Send {
    fn header(&self) -> &LogServerResponseHeader;
}

macro_rules! define_logserver_rpc {
    (
        @request = $request:ty,
        @response = $response:ty,
        @request_target = $request_target:expr,
        @response_target = $response_target:expr,
    ) => {
        crate::net::define_rpc! {
            @request = $request,
            @response = $response,
            @request_target = $request_target,
            @response_target = $response_target,
        }

        impl LogServerRequest for $request {
            fn header(&self) -> &LogServerRequestHeader {
                &self.header
            }

            fn header_mut(&mut self) -> &mut LogServerRequestHeader {
                &mut self.header
            }
        }

        impl LogServerResponse for $response {
            fn header(&self) -> &LogServerResponseHeader {
                &self.header
            }
        }
    };
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target = crate::protobuf::net::log_server::Status)]
#[repr(u8)]
pub enum Status {
    /// Operation was successful
    Ok = 1,
    /// The node's storage system is disabled and cannot accept operations at the moment.
    Disabled,
    /// If the operation expired or not completed due to load shedding. The operation can be
    /// retried by the client. It's guaranteed that this store has not been persisted by the node.
    Dropped,
    /// Operation rejected on a sealed loglet
    Sealed,
    /// Loglet is being sealed and operation cannot be accepted
    Sealing,
    /// Operation has been rejected. Operation requires that the sender is the authoritative
    /// sequencer.
    SequencerMismatch,
    /// This indicates that the operation cannot be accepted due to the offset being out of bounds.
    /// For instance, if a store is sent to a log-server that with a lagging local commit offset.
    OutOfBounds,
    /// The record is malformed, this could be because it has too many records or any other reason
    /// that leads the server to reject processing it.
    Malformed,
}

// ----- LogServer API -----
// Requests: Bifrost -> LogServer //
// Responses LogServer -> Bifrost //

// Store
define_logserver_rpc! {
    @request = Store,
    @response = Stored,
    @request_target = TargetName::LogServerStore,
    @response_target = TargetName::LogServerStored,
}

// Release
define_logserver_rpc! {
    @request = Release,
    @response = Released,
    @request_target = TargetName::LogServerRelease,
    @response_target = TargetName::LogServerReleased,
}

// Seal
define_logserver_rpc! {
    @request = Seal,
    @response = Sealed,
    @request_target = TargetName::LogServerSeal,
    @response_target = TargetName::LogServerSealed,
}

// GetLogletInfo
define_logserver_rpc! {
    @request = GetLogletInfo,
    @response = LogletInfo,
    @request_target = TargetName::LogServerGetLogletInfo,
    @response_target = TargetName::LogServerLogletInfo,
}

// Trim
define_logserver_rpc! {
    @request = Trim,
    @response = Trimmed,
    @request_target = TargetName::LogServerTrim,
    @response_target = TargetName::LogServerTrimmed,
}

// GetRecords
define_logserver_rpc! {
    @request = GetRecords,
    @response = Records,
    @request_target = TargetName::LogServerGetRecords,
    @response_target = TargetName::LogServerRecords,
}

// WaitForTail
define_logserver_rpc! {
    @request = WaitForTail,
    @response = TailUpdated,
    @request_target = TargetName::LogServerWaitForTail,
    @response_target = TargetName::LogServerTailUpdated,
}

// GetDigest
define_logserver_rpc! {
    @request = GetDigest,
    @response = Digest,
    @request_target = TargetName::LogServerGetDigest,
    @response_target = TargetName::LogServerDigest,
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target = crate::protobuf::net::log_server::RequestHeader)]
pub struct LogServerRequestHeader {
    pub loglet_id: LogletId,
    /// If the sender has now knowledge of this value, it can safely be set to
    /// `LogletOffset::INVALID`
    pub known_global_tail: LogletOffset,
}

impl LogServerRequestHeader {
    pub fn new(loglet_id: LogletId, known_global_tail: LogletOffset) -> Self {
        Self {
            loglet_id,
            known_global_tail,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target = crate::protobuf::net::log_server::ResponseHeader)]
pub struct LogServerResponseHeader {
    /// The position after the last locally committed record on this node
    pub local_tail: LogletOffset,
    /// The node's view of the last global tail of the loglet. If unknown, it
    /// can be safely set to `LogletOffset::INVALID`.
    pub known_global_tail: LogletOffset,
    /// Whether this node has sealed or not (local to the log-server)
    pub sealed: bool,
    pub status: Status,
}

impl LogServerResponseHeader {
    pub fn new(local_tail_state: TailState<LogletOffset>, known_global_tail: LogletOffset) -> Self {
        Self {
            local_tail: local_tail_state.offset(),
            known_global_tail,
            sealed: local_tail_state.is_sealed(),
            status: Status::Ok,
        }
    }

    pub fn empty() -> Self {
        Self {
            local_tail: LogletOffset::INVALID,
            known_global_tail: LogletOffset::INVALID,
            sealed: false,
            status: Status::Disabled,
        }
    }
}

// ** STORE
#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From, derive_more::Into)]
pub struct StoreFlags(u32);
bitflags! {
    impl StoreFlags: u32 {
        const IgnoreSeal = 0b000_00001;
    }
}

/// Store one or more records on a log-server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Store {
    pub header: LogServerRequestHeader,
    // The receiver should skip handling this message if it hasn't started to act on it
    // before timeout expires.
    pub timeout_at: Option<MillisSinceEpoch>,
    pub flags: StoreFlags,
    /// Offset of the first record in the batch of payloads. Payloads in the batch get a gap-less
    /// range of offsets that starts with (includes) the value of `first_offset`.
    pub first_offset: LogletOffset,
    /// This is the sequencer identifier for this log. This should be set even for repair store messages.
    pub sequencer: GenerationalNodeId,
    /// Denotes the last record that has been safely uploaded to an archiving data store.
    pub known_archived: LogletOffset,
    // todo (asoli) serialize efficiently
    pub payloads: Arc<[Record]>,
}

impl From<Store> for proto::Store {
    fn from(value: Store) -> Self {
        let Store {
            header,
            timeout_at,
            flags,
            first_offset,
            sequencer,
            known_archived,
            payloads,
        } = value;

        Self {
            header: Some(header.into()),
            timeout_at: timeout_at.map(Into::into),
            first_offset: first_offset.into(),
            known_archived: known_archived.into(),
            flags: flags.0,
            sequencer: Some(sequencer.into()),
            payloads: payloads.iter().cloned().map(Into::into).collect(),
        }
    }
}

impl TryFrom<proto::Store> for Store {
    type Error = ConversionError;

    fn try_from(value: proto::Store) -> Result<Self, Self::Error> {
        let proto::Store {
            header,
            timeout_at,
            flags,
            first_offset,
            sequencer,
            known_archived,
            payloads,
        } = value;

        Ok(Self {
            header: header
                .ok_or_else(|| ConversionError::missing_field("header"))?
                .into(),
            timeout_at: timeout_at.map(Into::into),
            flags: StoreFlags::from(flags),
            first_offset: LogletOffset::from(first_offset),
            sequencer: sequencer
                .ok_or_else(|| ConversionError::missing_field("sequencer"))?
                .into(),
            known_archived: LogletOffset::from(known_archived),
            payloads: {
                let mut vec = Vec::with_capacity(payloads.len());
                for record in payloads {
                    vec.push(Record::try_from(record)?);
                }
                Arc::from(vec)
            },
        })
    }
}

impl V2Convertible for Store {
    type Target = proto::Store;

    fn into_v2(self) -> Self::Target {
        self.into()
    }

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }
}

impl Store {
    /// The message's timeout has passed, we should discard if possible.
    pub fn expired(&self) -> bool {
        self.timeout_at
            .is_some_and(|timeout_at| MillisSinceEpoch::now() >= timeout_at)
    }

    // returns None on overflow
    pub fn last_offset(&self) -> Option<LogletOffset> {
        let len: u32 = self.payloads.len().try_into().ok()?;
        self.first_offset.checked_add(len - 1).map(Into::into)
    }

    pub fn estimated_encode_size(&self) -> usize {
        self.payloads
            .iter()
            .map(|p| p.estimated_encode_size())
            .sum()
    }
}

/// Response to a `Store` request
#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target = crate::protobuf::net::log_server::Stored)]
pub struct Stored {
    #[prost(required)]
    pub header: LogServerResponseHeader,
}

impl V2Convertible for Stored {
    type Target = proto::Stored;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        Ok(target.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

impl Deref for Stored {
    type Target = LogServerResponseHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl DerefMut for Stored {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl Stored {
    pub fn empty() -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
        }
    }

    pub fn new(tail_state: TailState<LogletOffset>, known_global_tail: LogletOffset) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

// ** RELEASE
#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target=crate::protobuf::net::log_server::Release)]
pub struct Release {
    #[prost(required)]
    pub header: LogServerRequestHeader,
}

impl V2Convertible for Release {
    type Target = proto::Release;
    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        Ok(target.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target=crate::protobuf::net::log_server::Released)]
pub struct Released {
    #[prost(required)]
    pub header: LogServerResponseHeader,
}

impl V2Convertible for Released {
    type Target = proto::Released;
    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        Ok(target.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

impl Released {
    pub fn empty() -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
        }
    }

    pub fn new(tail_state: TailState<LogletOffset>, known_global_tail: LogletOffset) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
        }
    }

    pub fn status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

// ** SEAL
/// Seals the loglet so no further stores can be accepted
#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target=crate::protobuf::net::log_server::Seal)]
pub struct Seal {
    #[prost(required)]
    pub header: LogServerRequestHeader,
    /// This is the sequencer identifier for this log. This should be set even for repair store messages.
    #[prost(required)]
    pub sequencer: GenerationalNodeId,
}

impl V2Convertible for Seal {
    type Target = proto::Seal;
    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        Ok(target.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

/// Response to a `Seal` request
#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target=crate::protobuf::net::log_server::Sealed)]
pub struct Sealed {
    #[prost(required)]
    pub header: LogServerResponseHeader,
}

impl V2Convertible for Sealed {
    type Target = proto::Sealed;
    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        Ok(target.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

impl Deref for Sealed {
    type Target = LogServerResponseHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl DerefMut for Sealed {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl Sealed {
    pub fn empty() -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
        }
    }

    pub fn new(tail_state: TailState<LogletOffset>, known_global_tail: LogletOffset) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

// ** GET_LOGLET_INFO
#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target=crate::protobuf::net::log_server::GetLogletInfo)]
pub struct GetLogletInfo {
    #[prost(required)]
    pub header: LogServerRequestHeader,
}

impl V2Convertible for GetLogletInfo {
    type Target = proto::GetLogletInfo;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        Ok(target.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target = crate::protobuf::net::log_server::LogletInfo)]
pub struct LogletInfo {
    #[prost(required)]
    pub header: LogServerResponseHeader,
    pub trim_point: LogletOffset,
}

impl V2Convertible for LogletInfo {
    type Target = crate::protobuf::net::log_server::LogletInfo;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        Ok(target.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

impl Deref for LogletInfo {
    type Target = LogServerResponseHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl DerefMut for LogletInfo {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl LogletInfo {
    pub fn empty() -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
            trim_point: LogletOffset::INVALID,
        }
    }

    pub fn new(
        tail_state: TailState<LogletOffset>,
        trim_point: LogletOffset,
        known_global_tail: LogletOffset,
    ) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
            trim_point,
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target=crate::protobuf::net::log_server::Gap)]
pub struct Gap {
    /// to is inclusive
    pub to: LogletOffset,
}

#[derive(Debug, Clone, derive_more::IsVariant, derive_more::TryUnwrap, Serialize, Deserialize)]
pub enum MaybeRecord {
    TrimGap(Gap),
    ArchivalGap(Gap),
    // Record(s) existed but got filtered out
    FilteredGap(Gap),
    Data(Record),
}

impl From<MaybeRecord> for proto::MaybeRecord {
    fn from(value: MaybeRecord) -> Self {
        use proto::{maybe_record::GapKind, maybe_record::Payload};
        match value {
            MaybeRecord::ArchivalGap(gap) => Self {
                payload: Payload::Gap(gap.into()).into(),
                gap_kind: GapKind::ArchivalGap.into(),
            },
            MaybeRecord::FilteredGap(gap) => Self {
                payload: Payload::Gap(gap.into()).into(),
                gap_kind: GapKind::FilteredGap.into(),
            },
            MaybeRecord::TrimGap(gap) => Self {
                payload: Payload::Gap(gap.into()).into(),
                gap_kind: GapKind::TrimGap.into(),
            },
            MaybeRecord::Data(record) => Self {
                payload: Payload::Data(record.into()).into(),
                gap_kind: GapKind::Unknown.into(),
            },
        }
    }
}

impl TryFrom<proto::MaybeRecord> for MaybeRecord {
    type Error = ConversionError;
    fn try_from(value: proto::MaybeRecord) -> Result<Self, Self::Error> {
        use proto::maybe_record::{GapKind, Payload};
        let gap_kind = value.gap_kind();
        let result = match value
            .payload
            .ok_or_else(|| ConversionError::missing_field("payload"))?
        {
            Payload::Data(record) => Self::Data(record.try_into()?),
            Payload::Gap(gap) => {
                let gap = Gap { to: gap.to.into() };
                match gap_kind {
                    GapKind::Unknown => return Err(ConversionError::InvalidData("gap_kind")),
                    GapKind::ArchivalGap => Self::ArchivalGap(gap),
                    GapKind::FilteredGap => Self::FilteredGap(gap),
                    GapKind::TrimGap => Self::TrimGap(gap),
                }
            }
        };

        Ok(result)
    }
}
// ** GET_RECORDS

/// Returns a batch that includes **all** records that the node has between
/// `from_offset` and `to_offset` that match the filter. This might return different results if
/// more records were replicated behind the known_global_commit/local_tail after the original
/// request.
///
/// That said, it must not return "fewer" records unless there was a trim or archival of old
/// records.
///
/// If `to_offset` is higher than `local_tail`, then we return all records up-to the `local_tail`
/// and the value of `local_tail` in the response header will indicate what is the snapshot of the
/// local tail that was used during the read process and `next_offset` will be set accordingly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRecords {
    pub header: LogServerRequestHeader,
    /// if set, the server will stop reading when the next record will tip of the total number of
    /// bytes allocated. The returned `next_offset` can be used by the reader to move the cursor
    /// for subsequent reads.
    ///
    /// Note the limit is not strict, but it's a practical mechanism to limit the client memory
    /// buffer when reading from multiple servers. Additionally. It'll always try to get a single
    /// record even if that record exceeds the stated budget.
    pub total_limit_in_bytes: Option<usize>,
    /// Only read records that match the filter.
    pub filter: KeyFilter,
    /// inclusive
    pub from_offset: LogletOffset,
    /// inclusive (will be clipped to local_tail-1), actual value of local tail is set on the
    /// response header.
    pub to_offset: LogletOffset,
}

impl From<GetRecords> for proto::GetRecords {
    fn from(value: GetRecords) -> Self {
        let GetRecords {
            header,
            total_limit_in_bytes,
            filter,
            from_offset,
            to_offset,
        } = value;

        Self {
            header: Some(header.into()),
            total_limit_in_bytes: total_limit_in_bytes.map(|v| v as u64),
            filter: Some(filter.into()),
            from_offset: from_offset.into(),
            to_offset: to_offset.into(),
        }
    }
}

impl TryFrom<proto::GetRecords> for GetRecords {
    type Error = ConversionError;
    fn try_from(value: proto::GetRecords) -> Result<Self, Self::Error> {
        let proto::GetRecords {
            header,
            total_limit_in_bytes,
            filter,
            from_offset,
            to_offset,
        } = value;

        Ok(Self {
            header: header
                .ok_or_else(|| ConversionError::missing_field("header"))?
                .into(),
            total_limit_in_bytes: total_limit_in_bytes.map(|v| v as usize),
            filter: filter
                .ok_or_else(|| ConversionError::missing_field("filter"))?
                .try_into()?,
            from_offset: from_offset.into(),
            to_offset: to_offset.into(),
        })
    }
}

impl V2Convertible for GetRecords {
    type Target = proto::GetRecords;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Records {
    pub header: LogServerResponseHeader,
    /// Indicates the next offset to read from after this response. This is useful when
    /// the response is partial due to hitting budgeting limits (memory, buffer, etc.)
    ///
    /// If the returned set of records include all records originally requested, the
    /// value of next_offset will be set to `to_offset + 1`. On errors this will be set to the
    /// value of `from_offset`.
    pub next_offset: LogletOffset,
    /// Sorted by offset
    pub records: Vec<(LogletOffset, MaybeRecord)>,
}

impl From<Records> for proto::Records {
    fn from(value: Records) -> Self {
        let Records {
            header,
            next_offset,
            records,
        } = value;

        Self {
            header: Some(header.into()),
            next_offset: next_offset.into(),
            records: {
                let mut results = Vec::with_capacity(records.len());
                for (offset, maybe_record) in records {
                    results.push(proto::MaybeRecordWithOffset {
                        offset: offset.into(),
                        record: Some(maybe_record.into()),
                    });
                }
                results
            },
        }
    }
}

impl TryFrom<proto::Records> for Records {
    type Error = ConversionError;
    fn try_from(value: proto::Records) -> Result<Self, Self::Error> {
        let proto::Records {
            header,
            next_offset,
            records,
        } = value;

        Ok(Self {
            header: header
                .ok_or_else(|| ConversionError::missing_field("header"))?
                .into(),
            next_offset: next_offset.into(),
            records: {
                let mut results = Vec::with_capacity(records.len());
                for maybe_record in records {
                    results.push((
                        maybe_record.offset.into(),
                        maybe_record
                            .record
                            .ok_or_else(|| ConversionError::missing_field("record"))?
                            .try_into()?,
                    ));
                }
                results
            },
        })
    }
}

impl Deref for Records {
    type Target = LogServerResponseHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl DerefMut for Records {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl Records {
    pub fn empty(next_offset: LogletOffset) -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
            records: Vec::default(),
            next_offset,
        }
    }

    pub fn new(
        tail_state: TailState<LogletOffset>,
        known_global_tail: LogletOffset,
        next_offset: LogletOffset,
    ) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
            records: Vec::default(),
            next_offset,
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

impl V2Convertible for Records {
    type Target = proto::Records;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

// ** TRIM
#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target=crate::protobuf::net::log_server::Trim)]
pub struct Trim {
    #[prost(required)]
    pub header: LogServerRequestHeader,
    /// The trim_point is inclusive (will be trimmed)
    pub trim_point: LogletOffset,
}

impl V2Convertible for Trim {
    type Target = proto::Trim;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        Ok(target.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

/// Response to a `Trim` request
#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target=crate::protobuf::net::log_server::Trimmed)]
pub struct Trimmed {
    #[prost(required)]
    pub header: LogServerResponseHeader,
}

impl V2Convertible for Trimmed {
    type Target = proto::Trimmed;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        Ok(target.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

impl Deref for Trimmed {
    type Target = LogServerResponseHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl DerefMut for Trimmed {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl Trimmed {
    pub fn empty() -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
        }
    }

    pub fn new(tail_state: TailState<LogletOffset>, known_global_tail: LogletOffset) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

// ** WAIT_FOR_TAIL

/// Defines the tail we are interested in waiting for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TailUpdateQuery {
    /// The node's local tail must be at or higher than this value
    LocalTail(LogletOffset),
    /// The node must observe the global tail at or higher than this value
    GlobalTail(LogletOffset),
    /// Either the local tail or the global tail arriving at this value will resolve this request.
    LocalOrGlobal(LogletOffset),
}

impl From<TailUpdateQuery> for proto::TailUpdateQuery {
    fn from(value: TailUpdateQuery) -> Self {
        use proto::tail_update_query::TailQueryKind;

        match value {
            TailUpdateQuery::LocalTail(offset) => Self {
                offset: offset.into(),
                query_kind: TailQueryKind::LocalTail.into(),
            },
            TailUpdateQuery::GlobalTail(offset) => Self {
                offset: offset.into(),
                query_kind: TailQueryKind::GlobalTail.into(),
            },
            TailUpdateQuery::LocalOrGlobal(offset) => Self {
                offset: offset.into(),
                query_kind: TailQueryKind::LocalOrGlobal.into(),
            },
        }
    }
}

impl TryFrom<proto::TailUpdateQuery> for TailUpdateQuery {
    type Error = ConversionError;
    fn try_from(value: proto::TailUpdateQuery) -> Result<Self, Self::Error> {
        use proto::tail_update_query::TailQueryKind;

        let result = match value.query_kind() {
            TailQueryKind::LocalTail => Self::LocalTail(value.offset.into()),
            TailQueryKind::GlobalTail => Self::GlobalTail(value.offset.into()),
            TailQueryKind::LocalOrGlobal => Self::LocalOrGlobal(value.offset.into()),
            _ => return Err(ConversionError::invalid_data("query_kind")),
        };

        Ok(result)
    }
}

/// Subscribes to a notification that will be sent when the log-server reaches a minimum local-tail
/// or global-tail value OR if the node is sealed.
#[derive(Debug, Clone, Serialize, Deserialize, IntoProst)]
#[prost(target=crate::protobuf::net::log_server::WaitForTail)]
pub struct WaitForTail {
    #[prost(required)]
    pub header: LogServerRequestHeader,
    /// If the caller is not interested in observing a specific tail value (i.e. only interested in
    /// the seal signal), this should be set to `TailUpdateQuery::GlobalTail(LogletOffset::MAX)`.
    #[prost(required)]
    pub query: TailUpdateQuery,
}

impl TryFrom<proto::WaitForTail> for WaitForTail {
    type Error = ConversionError;

    fn try_from(value: proto::WaitForTail) -> Result<Self, Self::Error> {
        let proto::WaitForTail { header, query } = value;
        Ok(Self {
            header: header
                .ok_or_else(|| ConversionError::missing_field("header"))?
                .into(),
            query: query
                .ok_or_else(|| ConversionError::missing_field("query"))?
                .try_into()?,
        })
    }
}

impl V2Convertible for WaitForTail {
    type Target = proto::WaitForTail;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

/// Response to a `WaitForTail` request
#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target=crate::protobuf::net::log_server::TailUpdated)]
pub struct TailUpdated {
    #[prost(required)]
    pub header: LogServerResponseHeader,
}

impl V2Convertible for TailUpdated {
    type Target = proto::TailUpdated;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        Ok(target.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

impl Deref for TailUpdated {
    type Target = LogServerResponseHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl DerefMut for TailUpdated {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl TailUpdated {
    pub fn empty() -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
        }
    }

    pub fn new(tail_state: TailState<LogletOffset>, known_global_tail: LogletOffset) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

// ** GET_DIGEST

/// Request a digest of the loglet between two offsets from this node
#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target=crate::protobuf::net::log_server::GetDigest)]
pub struct GetDigest {
    #[prost(required)]
    pub header: LogServerRequestHeader,
    // inclusive
    pub from_offset: LogletOffset,
    pub to_offset: LogletOffset,
}

impl V2Convertible for GetDigest {
    type Target = proto::GetDigest;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        Ok(target.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, derive_more::Display, Serialize, Deserialize, IntoProst, FromProst,
)]
#[prost(target = crate::protobuf::net::log_server::DigestEntry)]
#[display("[{from_offset}..{to_offset}] -> {status} ({})",  self.len())]
pub struct DigestEntry {
    // inclusive
    pub from_offset: LogletOffset,
    pub to_offset: LogletOffset,
    pub status: RecordStatus,
}

#[derive(
    Debug, Clone, Eq, PartialEq, derive_more::Display, Serialize, Deserialize, IntoProst, FromProst,
)]
#[repr(u8)]
#[prost(target = crate::protobuf::net::log_server::RecordStatus)]
pub enum RecordStatus {
    #[display("T")]
    Trimmed,
    #[display("A")]
    Archived,
    #[display("X")]
    Exists,
}

impl DigestEntry {
    // how many offsets are included in this entry
    pub fn len(&self) -> usize {
        if self.to_offset >= self.from_offset {
            return 0;
        }

        usize::try_from(self.to_offset.saturating_sub(*self.from_offset)).expect("no overflow") + 1
    }

    pub fn is_empty(&self) -> bool {
        self.from_offset > self.to_offset
    }
}

/// Response to a `GetDigest` request
#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target = crate::protobuf::net::log_server::Digest)]
pub struct Digest {
    #[prost(required)]
    pub header: LogServerResponseHeader,
    // If the node's local trim-point (or archival-point) overlaps with the digest range, an entry will be
    // added to include where the trim-gap ends. Otherwise, offsets for non-existing records
    // will not be included in the response.
    //
    // Entries are sorted by `from_offset`. The response header includes the node's local tail and
    // its known_global_tail as per usual.
    //
    // entries's contents must be ignored if `status` != `Status::Ok`.
    pub entries: Vec<DigestEntry>,
}

impl V2Convertible for Digest {
    type Target = crate::protobuf::net::log_server::Digest;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        Ok(target.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

impl Digest {
    pub fn empty() -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
            entries: Default::default(),
        }
    }

    pub fn new(
        tail_state: TailState<LogletOffset>,
        known_global_tail: LogletOffset,
        entries: Vec<DigestEntry>,
    ) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
            entries,
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}
