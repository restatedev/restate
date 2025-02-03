// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cling::prelude::*;

use restate_cli_util::CliContext;
use restate_cli_util::CommonOpts;

use crate::commands::cluster::overview::ClusterStatusOpts;
use crate::commands::cluster::Cluster;
use crate::commands::log::Logs;
use crate::commands::metadata::Metadata;
use crate::commands::node::Nodes;
use crate::commands::partition::Partitions;
use crate::commands::replicated_loglet::ReplicatedLoglet;
use crate::commands::snapshot::Snapshot;
use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Clone)]
#[command(author, version = crate::build_info::version(), about, infer_subcommands = true)]
#[cling(run = "init")]
pub struct CliApp {
    #[clap(flatten)]
    pub common_opts: CommonOpts,
    #[clap(flatten)]
    pub connection: ConnectionInfo,
    #[clap(subcommand)]
    pub cmd: Command,
}

#[derive(Run, Subcommand, Clone)]
pub enum Command {
    /// Cluster operations
    #[clap(subcommand)]
    Cluster(Cluster),
    /// Print cluster status overview (shortcut to `cluster status`)
    Status(ClusterStatusOpts),
    /// Log operations
    #[clap(subcommand)]
    Logs(Logs),
    /// Cluster node status
    #[clap(subcommand)]
    Nodes(Nodes),
    /// Manage partition table
    #[clap(subcommand)]
    Partitions(Partitions),
    /// Metadata store operations
    #[clap(subcommand)]
    Metadata(Metadata),
    /// Partition processor snapshots
    #[clap(subcommand)]
    Snapshots(Snapshot),
    /// Commands that operate on replicated loglets
    #[clap(subcommand)]
    ReplicatedLoglet(ReplicatedLoglet),
}

fn init(common_opts: &CommonOpts) {
    CliContext::new(common_opts.clone()).set_as_global();
}
