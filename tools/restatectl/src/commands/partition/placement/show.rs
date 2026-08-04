// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cling::prelude::*;
use tracing::error;

use restate_cli_util::_comfy_table::{Attribute, Cell, Color, Table};
use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_types::Versioned;
use restate_types::identifiers::PartitionId;
use restate_types::partitions::PartitionConfiguration;

use crate::connection::ConnectionInfo;
use crate::util::RangeParam;

use super::super::epoch_metadata::read_epoch_metadata;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "show_placement")]
pub struct ShowOpts {
    /// Partition id or range, e.g. "0", "1-4". Defaults to all partitions.
    #[arg()]
    partition_id: Vec<RangeParam<u16>>,
}

async fn show_placement(connection: &ConnectionInfo, opts: &ShowOpts) -> anyhow::Result<()> {
    let partition_table = connection.get_partition_table().await?;
    let partition_ids: Vec<_> = if opts.partition_id.is_empty() {
        partition_table.iter_ids().copied().collect()
    } else {
        opts.partition_id
            .iter()
            .flatten()
            .map(PartitionId::new_unchecked)
            .collect()
    };

    let mut table = Table::new_styled();
    table.set_styled_header(vec!["PARTITION", "CURRENT", "NEXT", "AUTOMATIC PLACEMENT"]);

    for partition_id in partition_ids {
        if !partition_table.contains(&partition_id) {
            error!("Partition {partition_id} does not exist, skipping.");
            continue;
        }

        let epoch_metadata = match read_epoch_metadata(connection, partition_id).await {
            Ok(Some(epoch_metadata)) => epoch_metadata,
            Ok(None) => {
                table.add_row(vec![
                    Cell::new(partition_id),
                    Cell::new("MISSING").fg(Color::Yellow),
                    Cell::new("-"),
                    Cell::new("-"),
                ]);
                continue;
            }
            Err(err) => {
                error!("Failed to get epoch metadata for partition {partition_id}: {err}");
                continue;
            }
        };

        let current = if epoch_metadata.current().is_valid() {
            render_configuration(epoch_metadata.current())
        } else {
            "UNASSIGNED".to_owned()
        };
        let next = epoch_metadata
            .next()
            .map(render_configuration)
            .unwrap_or_else(|| "-".to_owned());
        let policy = match &epoch_metadata.placement_policy().freeze {
            Some(freeze) => Cell::new(format!("FROZEN: {}", freeze.reason))
                .fg(Color::Red)
                .add_attribute(Attribute::Bold),
            None => Cell::new("ENABLED"),
        };

        table.add_row(vec![
            Cell::new(partition_id),
            Cell::new(current),
            Cell::new(next),
            policy,
        ]);
    }

    c_println!("{table}");
    Ok(())
}

fn render_configuration(configuration: &PartitionConfiguration) -> String {
    format!(
        "{} ({})",
        configuration.replica_set(),
        configuration.version()
    )
}
