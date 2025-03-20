// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::table_util::format_using;

use super::schema::NodeStateBuilder;
use restate_types::{PlainNodeId, cluster::cluster_state::NodeState};

#[inline]
pub(crate) fn append_node_row(
    builder: &mut NodeStateBuilder,
    output: &mut String,
    node_id: PlainNodeId,
    node_state: &NodeState,
) {
    let mut row = builder.row();
    row.plain_node_id(format_using(output, &node_id));
    row.status(format_using(output, &node_state));

    match node_state {
        NodeState::Alive(alive) => {
            row.uptime(alive.uptime.as_secs());
            row.last_seen_at(alive.last_heartbeat_at.as_u64() as i64);
            row.gen_node_id(format_using(output, &alive.generational_node_id));
        }
        NodeState::Dead(dead) => {
            if let Some(ts) = dead.last_seen_alive {
                row.last_seen_at(ts.as_u64() as i64);
            }
        }
        NodeState::Suspect(suspect) => {
            row.last_attempt_at(suspect.last_attempt.as_u64() as i64);
            row.gen_node_id(format_using(output, &suspect.generational_node_id));
        }
    }
}
