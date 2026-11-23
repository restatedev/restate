// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `restatectl nodes check-safety` without preview.

use std::fmt::Display;

use anyhow::bail;
use itertools::Itertools;

use restate_cli_util::_comfy_table::Table;
use restate_cli_util::ui::console::{Styled, StyledTable};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{c_println, c_success, c_title, c_warn};
use restate_core::protobuf::cluster_ctrl_svc::{
    CheckClusterSafetyResponse, DomainImpact, MetadataAvailability,
};
use restate_types::PlainNodeId;

/// The maximum number of ids or failure domains to print per row.
const MAX_LISTED: usize = 10;

pub fn print(
    report: &CheckClusterSafetyResponse,
    assume_dead: &[PlainNodeId],
) -> anyhow::Result<()> {
    // the failure domains whose loss makes something unavailable
    let critical = |impacted: fn(&DomainImpact) -> bool| -> Vec<String> {
        report
            .domain_impacts
            .iter()
            .filter(|impact| impacted(impact))
            .filter_map(|impact| impact.domain.as_ref())
            .map(|domain| format!("{} ({})", domain.name, domain.scope))
            .collect()
    };

    let metadata_availability = report
        .metadata_availability
        .and_then(|availability| MetadataAvailability::try_from(availability).ok())
        .filter(|availability| *availability != MetadataAvailability::Unknown);
    if let Some(availability) = metadata_availability {
        let quorum = match availability {
            MetadataAvailability::Unavailable => Styled(Style::Danger, "unavailable"),
            MetadataAvailability::AtRisk => Styled(Style::Warn, "at risk"),
            MetadataAvailability::Unchecked => Styled(Style::Warn, "unchecked, members unknown"),
            _ => Styled(Style::Success, "available"),
        };
        let mut metadata = Table::new_styled();
        metadata.add_kv_row("Quorum:", quorum).add_kv_row(
            "Critical domains:",
            list(&critical(|impact| impact.metadata_unavailable), Style::Warn),
        );
        c_title!("🗄️", "Metadata store");
        c_println!("{metadata}");
        c_println!();
    }

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
        list(
            &critical(|impact| !impact.write_unavailable_logs.is_empty()),
            Style::Warn,
        ),
    )
    .add_kv_row(
        "Read-critical domains:",
        list(
            &critical(|impact| !impact.read_unavailable_logs.is_empty()),
            Style::Warn,
        ),
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
            list(
                &critical(|impact| !impact.unavailable_partitions.is_empty()),
                Style::Warn,
            ),
        )
        .add_kv_row(
            "Unchecked:",
            list(&report.unchecked_partitions, Style::Warn),
        );
    c_title!("🧩", "Partitions");
    c_println!("{partitions}");
    c_println!();

    let condition = if assume_dead.is_empty() {
        String::new()
    } else {
        format!(" if nodes {} go down", assume_dead.iter().join(", "))
    };
    if metadata_availability == Some(MetadataAvailability::Unavailable) {
        bail!("The metadata store has no quorum{condition}");
    }
    if !report.write_unavailable_logs.is_empty()
        || !report.read_unavailable_logs.is_empty()
        || !report.unavailable_partitions.is_empty()
    {
        bail!("Some logs or partitions are unavailable{condition}");
    }
    let metadata_unchecked = metadata_availability == Some(MetadataAvailability::Unchecked);
    if metadata_unchecked && !assume_dead.is_empty() {
        bail!(
            "The metadata store could not be checked, so it's unknown whether it keeps its \
             quorum{condition}"
        );
    }
    let unchecked = !report.unchecked_logs.is_empty() || !report.unchecked_partitions.is_empty();
    if unchecked && !assume_dead.is_empty() {
        bail!(
            "Some logs or partitions could not be checked, so it's unknown whether they stay \
             available{condition}"
        );
    }
    if metadata_unchecked {
        c_warn!("The metadata store could not be checked, none of its members are known");
    }
    if metadata_availability == Some(MetadataAvailability::AtRisk) {
        c_warn!(
            "The metadata store has a quorum{condition}, but losing one more failure domain would \
            make it unavailable"
        );
    }
    if !report.write_at_risk_logs.is_empty()
        || !report.read_at_risk_logs.is_empty()
        || !report.at_risk_partitions.is_empty()
    {
        c_warn!(
            "All logs and partitions are available{condition}, but losing one more failure domain \
            would make some of them unavailable"
        );
        if unchecked {
            c_warn!("Some logs or partitions could not be checked");
        }
    } else if unchecked {
        c_warn!(
            "All checked logs and partitions are available and can tolerate losing a failure \
            domain, but some could not be checked"
        );
    } else {
        c_success!(
            "All logs and partitions are available{condition} and can tolerate losing a failure \
            domain"
        );
    }

    Ok(())
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
