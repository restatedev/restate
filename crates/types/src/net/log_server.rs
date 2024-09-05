// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bitflags::bitflags;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::TargetName;
use crate::logs::{Keys, LogletOffset};
use crate::net::define_rpc;
use crate::replicated_loglet::ReplicatedLogletId;
use crate::time::{MillisSinceEpoch, NanosSinceEpoch};
use crate::GenerationalNodeId;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(u8)]
pub enum Status {
    /// Operation was successful
    Ok = 0,
    /// The node's storage system is disabled and cannot accept operations at the moment.
    Disabled,
    /// If the operation expired or not completed due to load shedding. The operation can be
    /// retried by the client. It's guaranteed that this store has not been persisted by the node.
    Dropped,
    /// Operation rejected due to an ongoing or completed seal
    Sealed,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordHeader {
    pub created_at: NanosSinceEpoch,
}

#[derive(derive_more::Debug, Clone, Serialize, Deserialize)]
pub struct RecordPayload {
    pub created_at: NanosSinceEpoch,
    #[debug("Bytes({} bytes)", body.len())]
    pub body: Bytes,
    pub keys: Keys,
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
    pub payloads: Vec<RecordPayload>,
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
    // before timeout expires. 0 means no timeout
    pub timeout_at: MillisSinceEpoch,
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
    pub payloads: Vec<RecordPayload>,
}

impl Store {
    /// The message's timeout has passed, we should discard if possible.
    pub fn expired(&self) -> bool {
        MillisSinceEpoch::now() >= self.timeout_at
    }

    // returns None on overflow
    pub fn last_offset(&self) -> Option<LogletOffset> {
        let len: u32 = self.payloads.len().try_into().ok()?;
        self.first_offset.checked_add(len - 1).map(Into::into)
    }
}

/// Response to a `Store` request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stored {
    pub local_tail: LogletOffset,
    pub status: Status,
}

impl Stored {
    pub fn new(local_tail: LogletOffset) -> Self {
        Self {
            local_tail,
            status: Status::Ok,
        }
    }

    pub fn status(mut self, status: Status) -> Self {
        self.status = status;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Release {
    pub loglet_id: ReplicatedLogletId,
    pub known_global_tail: LogletOffset,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Released {
    pub known_global_tail: LogletOffset,
}
