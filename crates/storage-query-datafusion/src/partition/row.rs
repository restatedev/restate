// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::partitions::state::MembershipState;
use restate_types::{Version, partition_table::Partition};

use super::schema::PartitionBuilder;

pub(crate) fn append_partition_row(
    builder: &mut PartitionBuilder,
    membership: MembershipState,
    ver: Version,
    partition: &Partition,
) {
    let mut row = builder.row();
    row.log_id(partition.log_id().into());
    row.partition_id(partition.partition_id.into());
    row.start_key(*partition.key_range.start());
    row.end_key(*partition.key_range.end());
    row.cf_name(partition.data_cf_name());
    row.db_name(partition.db_name());
    let leadership = membership.current_leader();
    if leadership.current_leader.is_valid() {
        row.fmt_leader_gen_node_id(leadership.current_leader);
        row.fmt_leader_plain_node_id(leadership.current_leader.as_plain());
    }
    row.v_current(membership.observed_current_membership.version.into());
    row.current_replica_set(itertools::join(
        membership
            .observed_current_membership
            .members
            .iter()
            .map(|m| m.node_id),
        ",",
    ));
    if let Some(next_membership) = membership.observed_next_membership {
        row.v_next(next_membership.version.into());
        row.next_replica_set(itertools::join(
            next_membership.members.iter().map(|m| m.node_id),
            ",",
        ));
    }

    row.fmt_leader_epoch(leadership.current_leader_epoch);
    row.partition_table_version(ver.into());
}
