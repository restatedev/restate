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

use restate_cli_util::CliContext;
use restate_cli_util::CommonOpts;

use crate::commands::completions::Completions;
use crate::commands::config::ConfigOpts;
use crate::commands::log::Logs;
use crate::commands::metadata::Metadata;
use crate::commands::metadata_server::MetadataServer;
use crate::commands::node::Nodes;
use crate::commands::partition::Partitions;
use crate::commands::provision::ProvisionOpts;
use crate::commands::replicated_loglet::ReplicatedLoglet;
use crate::commands::snapshot::Snapshot;
use crate::commands::sql::SqlOpts;
use crate::commands::status::ClusterStatusOpts;
use crate::connection::ConnectionInfo;

/// Restate Cluster Administration Tool
///
/// A command-line tool for managing Restate clusters. Designed to be be used by administrators,
/// restatectl can be used to inspect cluster health and perform low-level maintenance operations
/// on live nodes. It requires access to restate's node-to-node communication addresses (default on
/// port 5122)
///
/// https://docs.restate.dev
#[derive(Run, Parser, Clone)]
#[command(author, version = crate::build_info::version(), about, infer_subcommands = true)]
#[cling(run = "init")]
pub struct CliApp {
    #[clap(flatten)]
    pub connection: ConnectionInfo,
    #[clap(flatten)]
    pub common_opts: CommonOpts,
    #[clap(subcommand)]
    pub cmd: Command,
}

#[derive(Run, Subcommand, Clone)]
pub enum Command {
    /// Provision a new cluster
    Provision(ProvisionOpts),
    /// Print cluster status overview
    Status(ClusterStatusOpts),
    /// Cluster node status
    #[clap(subcommand)]
    Nodes(Nodes),
    /// Manage partition table
    #[clap(subcommand)]
    Partitions(Partitions),
    /// Log operations
    #[clap(subcommand)]
    Logs(Logs),
    /// Partition processor snapshots
    #[clap(subcommand)]
    Snapshots(Snapshot),
    /// Cluster configuration operations
    #[clap(subcommand)]
    Config(ConfigOpts),
    /// Metadata store operations
    #[clap(subcommand)]
    Metadata(Metadata),
    /// [low-level] Commands that operate on replicated loglets
    #[clap(subcommand)]
    ReplicatedLoglet(ReplicatedLoglet),
    /// Command that operate on the replicated metadata servers
    #[clap(subcommand)]
    MetadataServer(MetadataServer),
    /// Query cluster status
    Sql(SqlOpts),
    /// Run a AWS Lambda server
    Lambda(restate_cli_util::lambda::LambdaOpts),
    /// Generate or install shell completions
    #[clap(subcommand)]
    Completions(Completions),
}

fn init(common_opts: &CommonOpts) {
    CliContext::new(common_opts.clone()).set_as_global();
}
