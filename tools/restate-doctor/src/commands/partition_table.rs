// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Partition table calculations (partition key -> partition id)

use anyhow::Context;
use cling::prelude::*;
use comfy_table::Table;

use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_types::Version;
use restate_types::partition_table::{FindPartition, PartitionTable};

/// Partition table commands
#[derive(Run, Subcommand, Clone)]
pub enum PartitionTableCommand {
    /// Find the partition that owns a partition key
    Find(Find),
}

/// Translate partition keys to partition ids for a given sharding.
///
/// Restate shards the u64 partition key space into equally sized partitions,
/// so the number of partitions fully determines the sharding.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_find")]
pub struct Find {
    /// Number of partitions of the cluster
    #[arg(long, short)]
    pub num_partitions: u16,

    /// Partition key(s) to translate (decimal or 0x-prefixed hex)
    #[arg(required = true, value_parser = parse_partition_key)]
    pub partition_keys: Vec<u64>,
}

fn parse_partition_key(s: &str) -> Result<u64, std::num::ParseIntError> {
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u64::from_str_radix(hex, 16)
    } else {
        s.parse()
    }
}

pub async fn run_find(cmd: &Find) -> anyhow::Result<()> {
    let partition_table =
        PartitionTable::with_equally_sized_partitions(Version::MIN, cmd.num_partitions);

    let mut table = Table::new_styled();
    table.set_styled_header(vec![
        "PARTITION KEY",
        "PARTITION ID",
        "KEY RANGE",
        "CF NAME",
    ]);

    for partition_key in &cmd.partition_keys {
        let partition_id = partition_table
            .find_partition_id(*partition_key)
            .with_context(|| format!("could not resolve partition key {partition_key}"))?;
        let partition = partition_table
            .get(&partition_id)
            .expect("resolved partition must exist");

        table.add_row(vec![
            partition_key.to_string(),
            partition_id.to_string(),
            partition.key_range.to_string(),
            partition.cf_name().to_string(),
        ]);
    }

    c_println!("{table}");
    Ok(())
}
