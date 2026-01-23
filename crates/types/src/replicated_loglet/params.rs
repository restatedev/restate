// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::GenerationalNodeId;
use crate::logs::LogletId;
use crate::replication::{NodeSet, ReplicationProperty};

/// Configuration parameters of a replicated loglet segment
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct ReplicatedLogletParams {
    /// Unique identifier for this loglet
    pub loglet_id: LogletId,
    /// The sequencer node
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    pub sequencer: GenerationalNodeId,
    /// Replication properties of this loglet
    pub replication: ReplicationProperty,
    pub nodeset: NodeSet,
}

impl ReplicatedLogletParams {
    pub fn deserialize_from(slice: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(slice)
    }

    pub fn serialize(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}
