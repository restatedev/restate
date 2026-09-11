// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Placement-aware allocation of Restate partitions to DataFusion lanes.
//!
//! `PartitionedTableProvider::scan` calls [`plan_partitions_by_location`] after
//! partition pruning. A *physical partition* is a Restate partition/range that
//! must be scanned. A [`LogicalPartition`] is one DataFusion execution lane
//! containing one or more sequential physical scans. The planner first groups
//! physical partitions by their resolved local or remote owner, then divides
//! the configured parallelism budget among those groups. Consequently, a
//! logical partition never crosses an execution location, which lets the table
//! provider represent every remote group with an explicit `RemoteNodeExec`.

use restate_types::identifiers::PartitionId;
use restate_types::partition_table::Partition;

use crate::remote_query_scanner_manager::PartitionLocation;

/// One DataFusion execution lane containing physical scans at a single location.
#[derive(Debug, Clone)]
pub(super) struct LogicalPartition {
    pub(super) physical_partitions: Vec<(PartitionId, Partition)>,
}

impl LogicalPartition {
    fn new(physical_partitions: Vec<(PartitionId, Partition)>) -> Self {
        Self {
            physical_partitions,
        }
    }
}

/// Resolves physical partition placement and allocates location-isolated
/// DataFusion execution lanes. No lane can cross a local/remote plan boundary.
pub(super) fn plan_partitions_by_location(
    physical_partitions: Vec<(PartitionId, Partition)>,
    target_partitions: usize,
    mut locate: impl FnMut(PartitionId) -> anyhow::Result<PartitionLocation>,
) -> anyhow::Result<Vec<(PartitionLocation, Vec<LogicalPartition>)>> {
    let mut groups: Vec<LocatedPartitions> = Vec::new();

    for physical_partition @ (partition_id, _) in physical_partitions {
        let location = locate(partition_id)?;
        if let Some(group) = groups.iter_mut().find(|group| group.location == location) {
            group.physical_partitions.push(physical_partition);
        } else {
            groups.push(LocatedPartitions {
                location,
                physical_partitions: vec![physical_partition],
            });
        }
    }

    Ok(allocate_logical_partitions(groups, target_partitions))
}

/// Physical partitions that can share one local or remote plan boundary.
#[derive(Debug)]
struct LocatedPartitions {
    location: PartitionLocation,
    physical_partitions: Vec<(PartitionId, Partition)>,
}

fn allocate_logical_partitions(
    groups: Vec<LocatedPartitions>,
    target_partitions: usize,
) -> Vec<(PartitionLocation, Vec<LogicalPartition>)> {
    if groups.is_empty() {
        return Vec::new();
    }

    // Every placement needs at least one execution lane. Distribute the
    // remaining session parallelism without exceeding the scans in a group.
    let desired_lanes = target_partitions.max(groups.len()).min(
        groups
            .iter()
            .map(|group| group.physical_partitions.len())
            .sum(),
    );
    let mut lane_counts = vec![1; groups.len()];
    let mut remaining = desired_lanes.saturating_sub(groups.len());
    while remaining > 0 {
        let mut allocated = false;
        for (group, lanes) in groups.iter().zip(&mut lane_counts) {
            if *lanes < group.physical_partitions.len() {
                *lanes += 1;
                remaining -= 1;
                allocated = true;
                if remaining == 0 {
                    break;
                }
            }
        }
        if !allocated {
            break;
        }
    }

    groups
        .into_iter()
        .zip(lane_counts)
        .map(|(group, lanes)| {
            (
                group.location,
                physical_partitions_to_logical(group.physical_partitions, lanes),
            )
        })
        .collect()
}

fn physical_partitions_to_logical(
    physical_partitions: Vec<(PartitionId, Partition)>,
    target_partitions: usize,
) -> Vec<LogicalPartition> {
    if physical_partitions.len() <= target_partitions {
        return physical_partitions
            .into_iter()
            .map(|partition| LogicalPartition::new(vec![partition]))
            .collect();
    }

    let mut logical_partitions = vec![LogicalPartition::new(Vec::new()); target_partitions];
    for (index, partition) in physical_partitions.into_iter().enumerate() {
        logical_partitions[index % target_partitions]
            .physical_partitions
            .push(partition);
    }
    logical_partitions
}

#[cfg(test)]
mod tests {
    use restate_types::GenerationalNodeId;
    use restate_types::sharding::KeyRange;

    use super::*;

    fn physical_partition(id: u16) -> (PartitionId, Partition) {
        let partition_id = PartitionId::new_unchecked(id);
        (partition_id, Partition::new(partition_id, KeyRange::FULL))
    }

    #[test]
    fn logical_partitions_never_cross_planned_locations() {
        let remote_one = PartitionLocation::Remote {
            node_id: GenerationalNodeId::new(2, 1).into(),
        };
        let remote_two = PartitionLocation::Remote {
            node_id: GenerationalNodeId::new(3, 1).into(),
        };
        let physical_partitions = (0..10).map(physical_partition).collect();

        let allocated = plan_partitions_by_location(physical_partitions, 4, |partition_id| {
            Ok(match u32::from(partition_id) {
                0..=5 => PartitionLocation::Local,
                6..=8 => remote_one,
                _ => remote_two,
            })
        })
        .expect("partition placement should resolve");

        assert_eq!(allocated.len(), 3);
        assert_eq!(
            allocated
                .iter()
                .map(|(_, partitions)| partitions.len())
                .sum::<usize>(),
            4
        );
        assert_eq!(allocated[0].1.len(), 2);
        assert_eq!(allocated[1].1.len(), 1);
        assert_eq!(allocated[2].1.len(), 1);

        let mut partition_ids = allocated
            .iter()
            .flat_map(|(_, logical)| logical)
            .flat_map(|logical| &logical.physical_partitions)
            .map(|(partition_id, _)| *partition_id)
            .collect::<Vec<_>>();
        partition_ids.sort_unstable();
        assert_eq!(
            partition_ids,
            (0..10).map(PartitionId::new_unchecked).collect::<Vec<_>>()
        );
    }
}
