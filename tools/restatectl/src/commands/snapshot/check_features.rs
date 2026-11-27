// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use anyhow::{Context, bail};
use cling::prelude::*;
use itertools::Itertools;
use serde::Deserialize;

use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{CliContext, c_println, c_success, c_warn};
use restate_core::protobuf::cluster_ctrl_svc::{QueryRequest, new_cluster_ctrl_client};
use restate_types::nodes_config::Role;
use restate_types::partitions::PartitionFeatureChange;

use crate::connection::ConnectionInfo;
use crate::observe::sql::read_rows;

const QUERY: &str = "SELECT partition_id, latest_snapshot_features FROM partition_state";

/// What a replica reports about its partition's latest snapshot (`partition_state` table).
#[derive(Deserialize)]
struct SnapshotFeaturesRow {
    partition_id: u32,
    /// NULL for a replica that didn't report.
    latest_snapshot_features: Option<Vec<String>>,
}

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "check_features")]
#[command(
    after_long_help = "Checks that the latest snapshot of every partition has the given partition \
    features, as the worker nodes running the partition report it. A release that requires a \
    feature restores a partition that lacks it from its latest snapshot with the feature, so \
    every partition needs such a snapshot before upgrading to that release. The release notes \
    name the features to check. Fails if a partition's latest snapshot lacks one of them, or if \
    no worker node reports the partition. Create snapshots with `restatectl snapshots create`."
)]
pub struct CheckFeaturesOpts {
    /// Names of the partition features to check for, e.g. "journal_v1_retired"
    #[arg(required = true, value_name = "FEATURE")]
    features: Vec<String>,
}

async fn check_features(
    connection: &ConnectionInfo,
    opts: &CheckFeaturesOpts,
) -> anyhow::Result<()> {
    let unknown = opts
        .features
        .iter()
        .filter(|name| PartitionFeatureChange::from_enabled_name(name).is_none())
        .join(", ");
    if !unknown.is_empty() {
        c_warn!(
            "This restatectl doesn't know the partition features: {unknown}. Check their names, \
            or use the restatectl of the release that the release notes name them in."
        );
    }

    let partition_table = connection.get_partition_table().await?;

    let response = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .query(QueryRequest {
                    query: QUERY.to_owned(),
                })
                .await
        })
        .await?
        .into_inner();
    let (rows, node_warnings) = read_rows(response)
        .await
        .context("Failed to read the partition_state table; it needs servers on v1.8.2 or newer")?;
    for warning in node_warnings {
        c_warn!(
            "Node {} didn't report its partitions: {}",
            warning.node_id,
            warning.message
        );
    }

    // What the replicas of each partition report about its latest snapshot.
    let mut reports: BTreeMap<u32, Vec<Vec<String>>> = partition_table
        .iter_ids()
        .map(|id| (u32::from(*id), Vec::new()))
        .collect();
    for row in rows {
        let row = SnapshotFeaturesRow::deserialize(row)?;
        if let Some(features) = row.latest_snapshot_features {
            reports.entry(row.partition_id).or_default().push(features);
        }
    }

    let mut table = Table::new_styled();
    table.set_styled_header(vec!["PARTITION", "LATEST SNAPSHOT LACKS"]);
    let mut failed = 0;
    for (partition_id, reports) in &reports {
        let lacking = match missing_features(&opts.features, reports) {
            Some(missing) if missing.is_empty() => continue,
            Some(missing) => Cell::new(missing.join(", ")).fg(Color::Red),
            None => Cell::new("unknown: no worker node reports the partition").fg(Color::Red),
        };
        failed += 1;
        table.add_row(vec![Cell::new(partition_id), lacking]);
    }

    let features = opts.features.join(", ");
    if failed == 0 {
        c_success!(
            "The latest snapshots of all {} partitions have: {features}",
            reports.len()
        );
        return Ok(());
    }
    c_println!("{table}");
    bail!(
        "The latest snapshots of {failed} of {} partitions lack some of: {features}",
        reports.len()
    );
}

/// The `features` that the partition's latest snapshot lacks, given what its replicas report
/// about it, or `None` if no replica reports it. A replica can report an older snapshot than
/// another one. Since nothing turns off a feature that can be required, the replica that reports
/// the most of `features` reports the newest snapshot.
fn missing_features<'a>(features: &'a [String], reports: &[Vec<String>]) -> Option<Vec<&'a str>> {
    reports
        .iter()
        .map(|reported| {
            features
                .iter()
                .filter(|feature| !reported.contains(feature))
                .map(String::as_str)
                .collect::<Vec<_>>()
        })
        .min_by_key(Vec::len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_features_takes_the_most_complete_report() {
        let features = vec!["journal_v1_retired".to_owned(), "vqueues".to_owned()];
        let report = |names: &[&str]| names.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>();

        assert_eq!(missing_features(&features, &[]), None);
        // A partition without a snapshot, or one that doesn't record features, lacks all.
        assert_eq!(
            missing_features(&features, &[report(&[])]),
            Some(vec!["journal_v1_retired", "vqueues"])
        );
        // A replica that hasn't seen the newest snapshot yet doesn't hide it.
        assert_eq!(
            missing_features(
                &features,
                &[
                    report(&["journal_v2", "vqueues"]),
                    report(&["journal_v2", "vqueues", "journal_v1_retired"]),
                ]
            ),
            Some(vec![])
        );
    }
}
