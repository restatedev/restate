// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod configuration;
pub mod state;

use crate::PlainNodeId;
use crate::nodes_config::{NodeConfig, Role, WorkerState};
pub use configuration::*;
// re-exports of partition-related types in preparation for moving them under this module
pub use super::epoch::*;
pub use super::partition_table::*;

/// Candidates are nodes with the worker role that have the active worker state.
pub fn worker_candidate_filter(_node_id: PlainNodeId, config: &NodeConfig) -> bool {
    config.has_role(Role::Worker) && config.worker_config.worker_state == WorkerState::Active
}
