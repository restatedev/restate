// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;

use anyhow::bail;
use cling::prelude::*;
use itertools::Itertools;

use restate_cli_util::_comfy_table::Table;
use restate_cli_util::ui::console::{Styled, StyledTable};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{CliContext, c_println, c_success, c_title, c_warn};
use restate_core::protobuf::cluster_ctrl_svc::{
    CheckClusterSafetyRequest, FailureDomain, new_cluster_ctrl_client,
};
use restate_types::PlainNodeId;
use restate_types::nodes_config::Role;

use crate::connection::ConnectionInfo;

/// The maximum number of ids or failure domains to print per row.
const MAX_LISTED: usize = 10;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "check_safety")]
pub struct CheckSafetyOpts {
    /// Check whether the cluster stays available if these nodes go down, e.g. before stopping
    /// them. Specify multiple nodes as a comma-separated list or specify the option multiple
    /// times.
    #[arg(long, value_delimiter = ',')]
    assume_dead: Vec<PlainNodeId>,
}

async fn check_safety(connection: &ConnectionInfo, opts: &CheckSafetyOpts) -> anyhow::Result<()> {
    let request = CheckClusterSafetyRequest {
        assume_dead_nodes: opts.assume_dead.iter().map(|id| u32::from(*id)).collect(),
    };
    let report = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .check_cluster_safety(request.clone())
                .await
        })
        .await?
        .into_inner();

    let mut logs = Table::new_styled();
    logs.add_kv_row(
        "Write unavailable:",
        list(&report.write_unavailable_logs, Style::Danger),
    )
    .add_kv_row(
        "Read unavailable:",
        list(&report.read_unavailable_logs, Style::Danger),
    )
    .add_kv_row(
        "Write at risk:",
        list(&report.write_at_risk_logs, Style::Warn),
    )
    .add_kv_row(
        "Read at risk:",
        list(&report.read_at_risk_logs, Style::Warn),
    )
    .add_kv_row(
        "Write-critical domains:",
        list(&domains(&report.log_write_critical_domains), Style::Warn),
    )
    .add_kv_row(
        "Read-critical domains:",
        list(&domains(&report.log_read_critical_domains), Style::Warn),
    )
    .add_kv_row("Unchecked:", list(&report.unchecked_logs, Style::Warn));
    c_title!("📜", "Logs");
    c_println!("{logs}");
    c_println!();

    let mut partitions = Table::new_styled();
    partitions
        .add_kv_row(
            "Unavailable:",
            list(&report.unavailable_partitions, Style::Danger),
        )
        .add_kv_row("At risk:", list(&report.at_risk_partitions, Style::Warn))
        .add_kv_row(
            "Critical domains:",
            list(&domains(&report.partition_critical_domains), Style::Warn),
        )
        .add_kv_row(
            "Unchecked:",
            list(&report.unchecked_partitions, Style::Warn),
        );
    c_title!("🧩", "Partitions");
    c_println!("{partitions}");
    c_println!();

    let condition = if opts.assume_dead.is_empty() {
        String::new()
    } else {
        format!(" if nodes {} go down", opts.assume_dead.iter().join(", "))
    };
    if !report.write_unavailable_logs.is_empty()
        || !report.read_unavailable_logs.is_empty()
        || !report.unavailable_partitions.is_empty()
    {
        bail!("Some logs or partitions are unavailable{condition}");
    }
    if !report.write_at_risk_logs.is_empty()
        || !report.read_at_risk_logs.is_empty()
        || !report.at_risk_partitions.is_empty()
    {
        c_warn!(
            "All logs and partitions are available{condition}, but losing one more failure domain \
            would make some of them unavailable"
        );
    } else {
        c_success!(
            "All logs and partitions are available{condition} and can tolerate losing a failure \
            domain"
        );
    }
    if !report.unchecked_logs.is_empty() || !report.unchecked_partitions.is_empty() {
        c_warn!("Some logs or partitions could not be checked");
    }

    Ok(())
}

fn domains(domains: &[FailureDomain]) -> Vec<String> {
    domains
        .iter()
        .map(|domain| format!("{} ({})", domain.name, domain.scope))
        .collect()
}

/// Lists the items, `none` if there are none.
fn list<T: Display>(items: &[T], style: Style) -> String {
    if items.is_empty() {
        return Styled(Style::Success, "none").to_string();
    }
    let mut listed = items.iter().take(MAX_LISTED).join(", ");
    if items.len() > MAX_LISTED {
        listed.push_str(&format!(", … ({} more)", items.len() - MAX_LISTED));
    }
    Styled(style, listed).to_string()
}
