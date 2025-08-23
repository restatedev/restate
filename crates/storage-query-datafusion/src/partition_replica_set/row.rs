// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::cluster_state::ClusterState;
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::partitions::state::{MemberState, MembershipState};
use restate_types::{Version, partition_table::Partition};

use super::schema::PartitionReplicaSetBuilder;

pub(crate) fn append_replica_set_row(
    builder: &mut PartitionReplicaSetBuilder,
    membership: MembershipState,
    cluster_state: &ClusterState,
    partition: &Partition,
) {
    let leadership = membership.current_leader();

    let mut build_row = |members: Vec<MemberState>, version: Version, membership: &str| {
        for member in members {
            let mut row = builder.row();
            row.membership(membership);
            row.membership_version(version.into());
            row.partition_id(partition.partition_id.into());
            if member.durable_lsn != Lsn::INVALID {
                row.durable_lsn(member.durable_lsn.into());
            }
            row.fmt_plain_node_id(member.node_id);

            if let Some((gen_node_id, node_state)) =
                cluster_state.get_node_state_and_generation(member.node_id)
            {
                if node_state.is_alive() {
                    row.fmt_alive_gen_node_id(gen_node_id);
                }
                if leadership.current_leader == gen_node_id {
                    row.is_leader(true);
                    row.fmt_leader_epoch(leadership.current_leader_epoch);
                }
            }
        }
    };

    build_row(
        membership.observed_current_membership.members,
        membership.observed_current_membership.version,
        "current",
    );
    if let Some(next_membership) = membership.observed_next_membership {
        build_row(next_membership.members, next_membership.version, "next");
    }
}
