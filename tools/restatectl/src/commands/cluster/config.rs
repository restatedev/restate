// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod get;
mod set;

use std::fmt::{self, Display, Write};

use cling::prelude::*;

use restate_types::{
    logs::metadata::DefaultProvider, partition_table::ReplicationStrategy,
    protobuf::cluster::ClusterConfiguration,
};

#[derive(Run, Subcommand, Clone)]
pub enum Config {
    /// Print a brief overview of the cluster status (nodes, logs, partitions)
    Get(get::ConfigGetOpts),
    Set(set::ConfigSetOpts),
}

fn cluster_config_string(config: ClusterConfiguration) -> anyhow::Result<String> {
    let mut w = String::default();

    writeln!(w, "⚙️ Cluster Configuration")?;
    write_leaf(
        &mut w,
        1,
        false,
        "Number of partitions",
        config.num_partitions,
    )?;
    let strategy: ReplicationStrategy =
        config.replication_strategy.unwrap_or_default().try_into()?;

    write_leaf(&mut w, 1, false, "Bifrost replication strategy", strategy)?;

    let provider: DefaultProvider = config.default_provider.unwrap_or_default().try_into()?;
    write_default_provider(&mut w, 1, provider)?;

    Ok(w)
}

fn write_default_provider<W: fmt::Write>(
    w: &mut W,
    depth: usize,
    provider: DefaultProvider,
) -> Result<(), fmt::Error> {
    let title = "Bifrost Provider";
    match provider {
        #[cfg(any(test, feature = "memory-loglet"))]
        DefaultProvider::InMemory => {
            write_leaf(w, depth, true, title, "in-memory")?;
        }
        DefaultProvider::Local => {
            write_leaf(w, depth, true, title, "local")?;
        }
        #[cfg(feature = "replicated-loglet")]
        DefaultProvider::Replicated(config) => {
            write_leaf(w, depth, true, title, "replicated")?;
            let depth = depth + 1;
            write_leaf(
                w,
                depth,
                false,
                "Node set selection strategy",
                config.nodeset_selection_strategy,
            )?;
            write_leaf(
                w,
                depth,
                true,
                "Replication property",
                config.replication_property.to_string(),
            )?;
        }
    }
    Ok(())
}

fn write_leaf<W: fmt::Write>(
    w: &mut W,
    depth: usize,
    last: bool,
    title: impl Display,
    value: impl Display,
) -> Result<(), fmt::Error> {
    let depth = depth + 1;
    let chr = if last { '└' } else { '├' };
    writeln!(w, "{chr:>depth$} {title}: {value}")
}
