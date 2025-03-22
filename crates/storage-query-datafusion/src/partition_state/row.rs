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

use super::schema::PartitionStateBuilder;
use restate_types::{
    GenerationalNodeId, cluster::cluster_state::PartitionProcessorStatus, identifiers::PartitionId,
};

#[inline]
pub(crate) fn append_partition_row(
    builder: &mut PartitionStateBuilder,
    output: &mut String,
    node_id: GenerationalNodeId,
    partition_id: PartitionId,
    state: &PartitionProcessorStatus,
) {
    let mut row = builder.row();

    row.partition_id(partition_id.into());
    row.plain_node_id(format_using(output, &node_id.as_plain()));
    row.gen_node_id(format_using(output, &node_id));
    row.target_mode(format_using(output, &state.planned_mode));

    row.effective_mode(format_using(output, &state.effective_mode));

    row.updated_at(state.updated_at.as_u64() as i64);
    if let Some(epoch) = state.last_observed_leader_epoch {
        row.leader_epoch(epoch.into());
    }

    if let Some(leader) = &state.last_observed_leader_node {
        row.leader(format_using(output, leader));
    }

    if let Some(lsn) = state.last_applied_log_lsn {
        row.applied_log_lsn(lsn.into());
    }

    if let Some(ts) = state.last_record_applied_at {
        row.last_record_applied_at(ts.as_u64() as i64);
    }

    row.skipped_records(state.num_skipped_records);
    row.replay_status(format_using(output, &state.replay_status));
    if let Some(lsn) = state.last_persisted_log_lsn {
        row.persisted_log_lsn(lsn.into());
    }

    if let Some(lsn) = state.last_archived_log_lsn {
        row.archived_log_lsn(lsn.into());
    }

    if let Some(lsn) = state.target_tail_lsn {
        row.target_tail_lsn(lsn.into());
    }
}
