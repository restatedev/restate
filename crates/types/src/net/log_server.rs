// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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
use serde::{Deserialize, Serialize};

use super::TargetName;
use crate::logs::{KeyFilter, LogletOffset, Record, SequenceNumber, TailState};
use crate::net::define_rpc;
use crate::replicated_loglet::ReplicatedLogletId;
use crate::time::MillisSinceEpoch;
use crate::GenerationalNodeId;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Status {
    /// Operation was successful
    Ok = 0,
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
define_rpc! {
    @request = Store,
    @response = Stored,
    @request_target = TargetName::LogServerStore,
    @response_target = TargetName::LogServerStored,
}

// Release
define_rpc! {
    @request = Release,
    @response = Released,
    @request_target = TargetName::LogServerRelease,
    @response_target = TargetName::LogServerReleased,
}

// Seal
define_rpc! {
    @request = Seal,
    @response = Sealed,
    @request_target = TargetName::LogServerSeal,
    @response_target = TargetName::LogServerSealed,
}

// GetLogletInfo
define_rpc! {
    @request = GetLogletInfo,
    @response = LogletInfo,
    @request_target = TargetName::LogServerGetLogletInfo,
    @response_target = TargetName::LogServerLogletInfo,
}

// Trim
define_rpc! {
    @request = Trim,
    @response = Trimmed,
    @request_target = TargetName::LogServerTrim,
    @response_target = TargetName::LogServerTrimmed,
}

// GetRecords
define_rpc! {
    @request = GetRecords,
    @response = Records,
    @request_target = TargetName::LogServerGetRecords,
    @response_target = TargetName::LogServerRecords,
}

// WaitForTail
define_rpc! {
    @request = WaitForTail,
    @response = TailUpdated,
    @request_target = TargetName::LogServerWaitForTail,
    @response_target = TargetName::LogServerTailUpdated,
}

// GetDigest
define_rpc! {
    @request = GetDigest,
    @response = Digest,
    @request_target = TargetName::LogServerGetDigest,
    @response_target = TargetName::LogServerDigest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogServerRequestHeader {
    pub loglet_id: ReplicatedLogletId,
    /// If the sender has now knowledge of this value, it can safely be set to
    /// `LogletOffset::INVALID`
    pub known_global_tail: LogletOffset,
}

impl LogServerRequestHeader {
    pub fn new(loglet_id: ReplicatedLogletId, known_global_tail: LogletOffset) -> Self {
        Self {
            loglet_id,
            known_global_tail,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreFlags(u32);
bitflags! {
    impl StoreFlags: u32 {
        const IgnoreSeal = 0b000_00001;
    }
}

/// Store one or more records on a log-server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Store {
    #[serde(flatten)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stored {
    #[serde(flatten)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Release {
    #[serde(flatten)]
    pub header: LogServerRequestHeader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Released {
    #[serde(flatten)]
    pub header: LogServerResponseHeader,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Seal {
    #[serde(flatten)]
    pub header: LogServerRequestHeader,
    /// This is the sequencer identifier for this log. This should be set even for repair store messages.
    pub sequencer: GenerationalNodeId,
}

/// Response to a `Seal` request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sealed {
    #[serde(flatten)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetLogletInfo {
    #[serde(flatten)]
    pub header: LogServerRequestHeader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogletInfo {
    #[serde(flatten)]
    pub header: LogServerResponseHeader,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    #[serde(flatten)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Records {
    #[serde(flatten)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trim {
    #[serde(flatten)]
    pub header: LogServerRequestHeader,
    /// The trim_point is inclusive (will be trimmed)
    pub trim_point: LogletOffset,
}

/// Response to a `Trim` request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trimmed {
    #[serde(flatten)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TailUpdateQuery {
    /// The node's local tail must be at or higher than this value
    LocalTail(LogletOffset),
    /// The node must observe the global tail at or higher than this value
    GlobalTail(LogletOffset),
    /// Either the local tail or the global tail arriving at this value will resolve this request.
    LocalOrGlobal(LogletOffset),
}

/// Subscribes to a notification that will be sent when the log-server reaches a minimum local-tail
/// or global-tail value OR if the node is sealed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitForTail {
    #[serde(flatten)]
    pub header: LogServerRequestHeader,
    /// If the caller is not interested in observing a specific tail value (i.e. only interested in
    /// the seal signal), this should be set to `TailUpdateQuery::GlobalTail(LogletOffset::MAX)`.
    pub query: TailUpdateQuery,
}

/// Response to a `WaitForTail` request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TailUpdated {
    #[serde(flatten)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetDigest {
    #[serde(flatten)]
    pub header: LogServerRequestHeader,
    // inclusive
    pub from_offset: LogletOffset,
    pub to_offset: LogletOffset,
}

#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display, Serialize, Deserialize)]
#[display("[{from_offset}..{to_offset}] -> {status} ({})",  self.len())]
pub struct DigestEntry {
    // inclusive
    pub from_offset: LogletOffset,
    pub to_offset: LogletOffset,
    pub status: RecordStatus,
}

#[derive(Debug, Clone, Eq, PartialEq, derive_more::Display, Serialize, Deserialize)]
#[repr(u8)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Digest {
    #[serde(flatten)]
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
