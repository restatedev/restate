// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;

use cling::prelude::*;

use restate_types::Version;
use restate_types::partition_table::PartitionTable;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "generate_partition_table")]
pub struct GeneratePartitionTableOpts {
    #[clap(long, default_value = "2")]
    version: NonZeroU32,
    /// The number of logs
    #[clap(long, short)]
    num_partition: u16,
    /// Pretty json?
    #[clap(long)]
    pretty: bool,
}

async fn generate_partition_table(opts: &GeneratePartitionTableOpts) -> anyhow::Result<()> {
    let table = PartitionTable::with_equally_sized_partitions(
        Version::from(u32::from(opts.version)),
        opts.num_partition,
    );

    let output = if opts.pretty {
        serde_json::to_string_pretty(&table)?
    } else {
        serde_json::to_string(&table)?
    };
    println!("{output}");
    Ok(())
}
