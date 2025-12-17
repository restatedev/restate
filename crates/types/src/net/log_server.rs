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

use restate_encoding::{ArcedSlice, BilrostNewType, NetSerde};

use super::{RpcResponse, ServiceTag};
use crate::GenerationalNodeId;
use crate::logs::{KeyFilter, LogletId, LogletOffset, Record, SequenceNumber, TailState};
use crate::net::define_service;
use crate::time::MillisSinceEpoch;

pub struct LogServerDataService;
define_service! {
    @service = LogServerDataService,
    @tag = ServiceTag::LogServerDataService,
}

pub struct LogServerMetaService;
define_service! {
    @service = LogServerMetaService,
    @tag = ServiceTag::LogServerMetaService,
}

pub trait LogServerMessage {
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

pub trait LogServerResponse: RpcResponse {
    fn header(&self) -> &LogServerResponseHeader;
}

macro_rules! define_logserver_rpc {
    (
        @request = $request:ty $(as $as_request:ty)?,
        @response = $response:ty $(as $as_response:ty)?,
        @service = $service:ty,
    ) => {
        crate::net::define_rpc! {
            @request = $request,
            @response = $response,
            @service = $service,
        }
        crate::net::bilrost_wire_codec!($request $(as $as_request)?);
        crate::net::bilrost_wire_codec!($response $(as $as_response)?);

        impl LogServerMessage for $request {
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

macro_rules! define_logserver_unary {
    (
        @message = $message:ty $(as $as_message:ty)?,
        @service = $service:ty,
    ) => {
        crate::net::define_unary_message! {
            @message = $message,
            @service = $service,
        }
        crate::net::bilrost_wire_codec!($message $(as $as_message)?);

        impl LogServerMessage for $message {
            fn header(&self) -> &LogServerRequestHeader {
                &self.header
            }

            fn header_mut(&mut self) -> &mut LogServerRequestHeader {
                &mut self.header
            }
        }
    };
}

#[derive(
    Debug, Clone, Copy, Eq, PartialEq, IntoProst, FromProst, bilrost::Enumeration, NetSerde,
)]
#[prost(target = "crate::protobuf::log_server_common::Status")]
#[repr(u8)]
pub enum Status {
    Unknown = 0,
    /// Operation was successful
    #[bilrost(1)]
    Ok,
    /// The node's storage system is disabled and cannot accept operations at the moment.
    #[bilrost(2)]
    Disabled,
    /// If the operation expired or not completed due to load shedding. The operation can be
    /// retried by the client. It's guaranteed that this store has not been persisted by the node.
    #[bilrost(3)]
    Dropped,
    /// Operation rejected on a sealed loglet
    #[bilrost(4)]
    Sealed,
    /// Loglet is being sealed and operation cannot be accepted
    #[bilrost(5)]
    Sealing,
    /// Operation has been rejected. Operation requires that the sender is the authoritative
    /// sequencer.
    #[bilrost(6)]
    SequencerMismatch,
    /// This indicates that the operation cannot be accepted due to the offset being out of bounds.
    /// For instance, if a store is sent to a log-server that with a lagging local commit offset.
    #[bilrost(7)]
    OutOfBounds,
    /// The record is malformed, this could be because it has too many records or any other reason
    /// that leads the server to reject processing it.
    #[bilrost(8)]
    Malformed,
}

// ----- LogServer API -----
// Requests: Bifrost -> LogServer //
// Responses LogServer -> Bifrost //

// -- Data Service

// Store
define_logserver_rpc! {
    @request = Store,
    @response = Stored,
    @service = LogServerDataService,
}

// GetRecords
define_logserver_rpc! {
    @request = GetRecords,
    @response = Records,
    @service = LogServerDataService,
}

// -- Meta Service

// Release -- Unary
define_logserver_unary! {
    @message = Release,
    @service = LogServerMetaService,
}

// Seal
define_logserver_rpc! {
    @request = Seal,
    @response = Sealed,
    @service = LogServerMetaService,
}

// GetLogletInfo
define_logserver_rpc! {
    @request = GetLogletInfo,
    @response = LogletInfo,
    @service = LogServerMetaService,
}

// Trim
define_logserver_rpc! {
    @request = Trim,
    @response = Trimmed,
    @service = LogServerMetaService,
}

// WaitForTail
define_logserver_rpc! {
    @request = WaitForTail,
    @response = TailUpdated,
    @service = LogServerMetaService,
}

// GetDigest
define_logserver_rpc! {
    @request = GetDigest,
    @response = Digest,
    @service = LogServerMetaService,
}

#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct LogServerRequestHeader {
    #[bilrost(tag(1))]
    pub loglet_id: LogletId,
    /// If the sender has now knowledge of this value, it can safely be set to
    /// `LogletOffset::INVALID`
    #[bilrost(tag(2))]
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

#[derive(Debug, Clone, IntoProst, FromProst, bilrost::Message, NetSerde)]
#[prost(target = "crate::protobuf::log_server_common::ResponseHeader")]
pub struct LogServerResponseHeader {
    /// The position after the last locally committed record on this node
    #[bilrost(1)]
    pub local_tail: LogletOffset,
    /// The node's view of the last global tail of the loglet. If unknown, it
    /// can be safely set to `LogletOffset::INVALID`.
    #[bilrost(2)]
    pub known_global_tail: LogletOffset,
    /// Whether this node has sealed or not (local to the log-server)
    #[bilrost(3)]
    pub sealed: bool,
    #[bilrost(4)]
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
#[derive(Debug, Clone, BilrostNewType, NetSerde)]
pub struct StoreFlags(u32);
bitflags! {
    impl StoreFlags: u32 {
        const IgnoreSeal = 0b000_00001;
    }
}

#[derive(
    bilrost::Message,
    derive_more::Into,
    derive_more::From,
    derive_more::Deref,
    Debug,
    Clone,
    Default,
    NetSerde,
)]
pub struct Payloads(#[bilrost(encoding(ArcedSlice<unpacked>))] Arc<[Record]>);

impl From<Vec<Record>> for Payloads {
    fn from(value: Vec<Record>) -> Self {
        Payloads(Arc::from(value))
    }
}

/// Store one or more records on a log-server
#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct Store {
    #[bilrost(1)]
    pub header: LogServerRequestHeader,
    // The receiver should skip handling this message if it hasn't started to act on it
    // before timeout expires.
    #[bilrost(2)]
    pub timeout_at: Option<MillisSinceEpoch>,
    #[bilrost(3)]
    pub flags: StoreFlags,
    /// Offset of the first record in the batch of payloads. Payloads in the batch get a gap-less
    /// range of offsets that starts with (includes) the value of `first_offset`.
    #[bilrost(4)]
    pub first_offset: LogletOffset,
    /// This is the sequencer identifier for this log. This should be set even for repair store messages.
    #[bilrost(5)]
    pub sequencer: GenerationalNodeId,
    /// Denotes the last record that has been safely uploaded to an archiving data store.
    #[bilrost(6)]
    pub known_archived: LogletOffset,
    // todo (asoli) serialize efficiently
    #[bilrost(7)]
    pub payloads: Payloads,
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
#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct Stored {
    #[bilrost(1)]
    pub header: LogServerResponseHeader,
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
#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct Release {
    #[bilrost(1)]
    pub header: LogServerRequestHeader,
}

// ** SEAL
/// Seals the loglet so no further stores can be accepted
#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct Seal {
    #[bilrost(1)]
    pub header: LogServerRequestHeader,
    /// This is the sequencer identifier for this log. This should be set even for repair store messages.
    #[bilrost(2)]
    pub sequencer: GenerationalNodeId,
}

/// Response to a `Seal` request
#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct Sealed {
    #[bilrost(1)]
    pub header: LogServerResponseHeader,
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
#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct GetLogletInfo {
    #[bilrost(1)]
    pub header: LogServerRequestHeader,
}

#[derive(Debug, Clone, IntoProst, bilrost::Message, NetSerde)]
#[prost(target = "crate::protobuf::log_server_common::LogletInfo")]
pub struct LogletInfo {
    #[prost(required)]
    #[bilrost(1)]
    pub header: LogServerResponseHeader,
    #[bilrost(2)]
    pub trim_point: LogletOffset,
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

#[derive(Debug, Clone, Copy, PartialEq, bilrost::Message, NetSerde)]
pub struct Gap {
    /// to is inclusive
    #[bilrost(1)]
    pub to: LogletOffset,
}

#[derive(
    Debug,
    Clone,
    derive_more::IsVariant,
    derive_more::TryUnwrap,
    NetSerde,
    Default,
    bilrost::Oneof,
    bilrost::Message,
)]
pub enum MaybeRecord {
    #[default]
    #[bilrost(empty)]
    Unknown,
    #[bilrost(2)]
    TrimGap(Gap),
    #[bilrost(3)]
    ArchivalGap(Gap),
    // Record(s) existed but got filtered out
    #[bilrost(4)]
    FilteredGap(Gap),
    #[bilrost(1)]
    Data(Record),
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
#[derive(Debug, Clone, bilrost::Message, NetSerde)]
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

#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct Records {
    #[bilrost(1)]
    pub header: LogServerResponseHeader,
    /// Indicates the next offset to read from after this response. This is useful when
    /// the response is partial due to hitting budgeting limits (memory, buffer, etc.)
    ///
    /// If the returned set of records include all records originally requested, the
    /// value of next_offset will be set to `to_offset + 1`. On errors this will be set to the
    /// value of `from_offset`.
    #[bilrost(2)]
    pub next_offset: LogletOffset,
    /// Sorted by offset
    #[bilrost(3)]
    pub records: Vec<(LogletOffset, MaybeRecord)>,
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

// ** TRIM
#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct Trim {
    pub header: LogServerRequestHeader,
    /// The trim_point is inclusive (will be trimmed)
    pub trim_point: LogletOffset,
}

/// Response to a `Trim` request
#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct Trimmed {
    #[bilrost(1)]
    pub header: LogServerResponseHeader,
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
#[derive(Debug, Clone, Default, bilrost::Oneof, NetSerde)]
pub enum TailUpdateQuery {
    #[default]
    #[doc(hidden)]
    /// Used only as the default state
    Unknown,
    /// The node's local tail must be at or higher than this value
    #[bilrost(2)]
    LocalTail(LogletOffset),
    /// The node must observe the global tail at or higher than this value
    #[bilrost(3)]
    GlobalTail(LogletOffset),
    /// Either the local tail or the global tail arriving at this value will resolve this request.
    #[bilrost(4)]
    LocalOrGlobal(LogletOffset),
}

/// Subscribes to a notification that will be sent when the log-server reaches a minimum local-tail
/// or global-tail value OR if the node is sealed.
#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct WaitForTail {
    #[bilrost(1)]
    pub header: LogServerRequestHeader,
    /// If the caller is not interested in observing a specific tail value (i.e. only interested in
    /// the seal signal), this should be set to `TailUpdateQuery::GlobalTail(LogletOffset::MAX)`.
    #[bilrost(oneof(2, 3, 4))]
    pub query: TailUpdateQuery,
}

/// Response to a `WaitForTail` request
#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct TailUpdated {
    pub header: LogServerResponseHeader,
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
#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct GetDigest {
    pub header: LogServerRequestHeader,
    // inclusive
    pub from_offset: LogletOffset,
    pub to_offset: LogletOffset,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    derive_more::Display,
    IntoProst,
    FromProst,
    bilrost::Message,
    NetSerde,
)]
#[prost(target = "crate::protobuf::log_server_common::DigestEntry")]
#[display("[{from_offset}..{to_offset}] -> {status} ({})",  self.len())]
pub struct DigestEntry {
    // inclusive
    pub from_offset: LogletOffset,
    pub to_offset: LogletOffset,
    pub status: RecordStatus,
}

#[derive(
    Debug,
    Clone,
    Eq,
    PartialEq,
    derive_more::Display,
    IntoProst,
    FromProst,
    bilrost::Enumeration,
    NetSerde,
)]
#[repr(u8)]
#[prost(target = crate::protobuf::log_server_common::RecordStatus)]
pub enum RecordStatus {
    #[display("-")]
    Unknown = 0,
    #[display("T")]
    Trimmed = 1,
    #[display("A")]
    Archived = 2,
    #[display("X")]
    Exists = 3,
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
#[derive(Debug, Clone, IntoProst, FromProst, bilrost::Message, NetSerde)]
#[prost(target = "crate::protobuf::log_server_common::Digest")]
pub struct Digest {
    #[prost(required)]
    #[bilrost(1)]
    pub header: LogServerResponseHeader,
    // If the node's local trim-point (or archival-point) overlaps with the digest range, an entry will be
    // added to include where the trim-gap ends. Otherwise, offsets for non-existing records
    // will not be included in the response.
    //
    // Entries are sorted by `from_offset`. The response header includes the node's local tail and
    // its known_global_tail as per usual.
    //
    // entries's contents must be ignored if `status` != `Status::Ok`.
    #[bilrost(2)]
    pub entries: Vec<DigestEntry>,
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
