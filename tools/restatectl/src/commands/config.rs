// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use std::fmt::Write;

use cling::prelude::*;

use restate_types::{
    logs::metadata::ProviderConfiguration, protobuf::cluster::ClusterConfiguration,
};

use crate::util::{write_default_provider, write_leaf};

#[derive(Run, Subcommand, Clone)]
pub enum ConfigOpts {
    /// Print a brief overview of the cluster configuration (nodes, logs, partitions)
    Get(get::ConfigGetOpts),
    /// Set new values for the cluster configuration
    Set(set::ConfigSetOpts),
}

pub fn cluster_config_string(config: &ClusterConfiguration) -> anyhow::Result<String> {
    let mut w = String::default();

    writeln!(w, "⚙️ Cluster Configuration")?;
    write_leaf(
        &mut w,
        0,
        false,
        "Number of partitions",
        config.num_partitions,
    )?;
    let strategy: &str = config
        .partition_replication
        .as_ref()
        .map(|p| p.replication_property.as_str())
        .unwrap_or("*");

    write_leaf(&mut w, 0, false, "Partition replication", strategy)?;

    let provider: ProviderConfiguration = config
        .bifrost_provider
        .clone()
        .unwrap_or_default()
        .try_into()?;

    write_default_provider(&mut w, 0, &provider)?;

    Ok(w)
}
