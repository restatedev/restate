// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod config;
pub(crate) mod overview;

use cling::prelude::*;
use config::Config;

use crate::commands::cluster::overview::ClusterStatusOpts;

#[derive(Run, Subcommand, Clone)]
pub enum Cluster {
    /// Print a brief overview of the cluster status (nodes, logs, partitions)
    Status(ClusterStatusOpts),
    /// Manage cluster configuration
    #[clap(subcommand)]
    Config(Config),
}
