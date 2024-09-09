// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo: remove after scaffolding is complete
#![allow(unused)]

use restate_types::logs::{LogletOffset, SequenceNumber};
use restate_types::time::MillisSinceEpoch;
use restate_types::PlainNodeId;

/// A marker stored in log-server-level loglet storage
///
/// The marker is used to sanity-check if the loglet storage is correctly initialized and whether
/// we lost the database or not.
///
/// The marker is stored as Json to help with debugging.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogStoreMarker {
    my_node_id: PlainNodeId,
    created_at: MillisSinceEpoch,
}

impl LogStoreMarker {
    pub fn new(my_node_id: PlainNodeId) -> Self {
        Self {
            my_node_id,
            created_at: MillisSinceEpoch::now(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("infallible serde")
    }

    pub fn from_slice(data: impl AsRef<[u8]>) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data.as_ref())
    }

    pub fn node_id(&self) -> PlainNodeId {
        self.my_node_id
    }

    pub fn created_at(&self) -> MillisSinceEpoch {
        self.created_at
    }
}

/// Metadata stored for every loglet-id known to this node
#[derive(Debug, Clone)]
pub struct LogletState {
    pub(crate) last_locally_committed: LogletOffset,
    pub(crate) last_globally_committed: LogletOffset,
    pub(crate) trim_point: LogletOffset,
    pub(crate) sealed: bool,
}

impl Default for LogletState {
    fn default() -> Self {
        Self {
            last_locally_committed: LogletOffset::INVALID,
            last_globally_committed: LogletOffset::INVALID,
            trim_point: LogletOffset::INVALID,
            sealed: false,
        }
    }
}
