// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::GenerationalNodeId;

///
/// The header of the message envelope already contains all information needed for the
/// leadership announcement. The leader_epoch
///
/// Reserved for future use.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AnnounceLeader {
    /// The "generational" node id of the processor that is being announced as leader.
    /// This is used because the header only contains the plain id.
    node_id: GenerationalNodeId,
}
