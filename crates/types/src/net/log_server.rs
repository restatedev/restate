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

use bitflags::bitflags;
use serde::{Deserialize, Serialize};

use super::TargetName;
use crate::logs::{LogletOffset, Record, SequenceNumber, TailState};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendFlags(u32);

// ------- Node to Bifrost ------ //
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Append {
    pub loglet_id: ReplicatedLogletId,
    pub flags: AppendFlags,
    /// The receiver should skip handling this message if it hasn't started to act on it
    /// before timeout expires. 0 means no timeout
    pub timeout_at: MillisSinceEpoch,
    pub payloads: Vec<Record>,
}

impl Append {
    /// The message's timeout has passed, we should discard if possible.
    pub fn expired(&self) -> bool {
        MillisSinceEpoch::now() >= self.timeout_at
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Appended {
    known_global_tail: LogletOffset,
    status: Status,
    // INVALID if Status indicates that the append failed
    first_offset: LogletOffset,
}

// ------- Bifrost to LogServer ------ //
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogServerResponseHeader {
    pub local_tail: LogletOffset,
    pub sealed: bool,
    pub status: Status,
}

impl LogServerResponseHeader {
    pub fn new(tail_state: &TailState<LogletOffset>) -> Self {
        Self {
            local_tail: tail_state.offset(),
            sealed: tail_state.is_sealed(),
            status: Status::Ok,
        }
    }

    pub fn empty() -> Self {
        Self {
            local_tail: LogletOffset::INVALID,
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
    // The receiver should skip handling this message if it hasn't started to act on it
    // before timeout expires.
    pub timeout_at: Option<MillisSinceEpoch>,
    pub known_global_tail: LogletOffset,
    pub flags: StoreFlags,
    pub loglet_id: ReplicatedLogletId,
    /// Offset of the first record in the batch of payloads. Payloads in the batch get a gap-less
    /// range of offsets that starts with (includes) the value of `first_offset`.
    pub first_offset: LogletOffset,
    /// This is the sequencer identifier for this log. This should be set even for repair store messages.
    pub sequencer: GenerationalNodeId,
    /// Denotes the last record that has been safely uploaded to an archiving data store.
    pub known_archived: LogletOffset,
    // todo (asoli) serialize efficiently
    pub payloads: Vec<Record>,
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

    pub fn new(tail_state: &TailState<LogletOffset>) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state),
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
    pub loglet_id: ReplicatedLogletId,
    pub known_global_tail: LogletOffset,
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

    pub fn new(tail_state: &TailState<LogletOffset>) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state),
        }
    }

    pub fn status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

// ** SEAL
/// Store one or more records on a log-server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Seal {
    pub known_global_tail: LogletOffset,
    pub loglet_id: ReplicatedLogletId,
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

    pub fn new(tail_state: &TailState<LogletOffset>) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state),
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}
