// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use cling::prelude::*;

use restate_cli_util::{CliContext, CommonOpts};

use crate::commands::completions::Completions;
use crate::commands::id;
use crate::commands::partition_store;

/// Restate Doctor - Diagnostic tools Restate storage
///
/// A command-line tool to inspect and analyze Restate's partition-store (RocksDB)
/// and log-store databases. Supports both offline analysis (server stopped) and
/// live analysis via RocksDB secondary mode.
///
/// https://docs.restate.dev
#[derive(Run, Parser, Clone)]
#[command(author, version = crate::build_info::version(), about, infer_subcommands = true)]
#[cling(run = "init")]
pub struct CliApp {
    #[clap(flatten)]
    pub common_opts: CommonOpts,
    #[clap(flatten)]
    pub global_opts: GlobalOpts,
    #[clap(subcommand)]
    pub cmd: Command,
}

#[derive(Args, Collect, Clone, Default)]
pub struct GlobalOpts {
    /// Restate data directory (auto-discovers db/ and log-server/)
    #[arg(long, short = 'd', global = true, env = "RESTATE_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// Limit max open files for RocksDB (use if you hit "too many open files" errors)
    #[arg(long, global = true)]
    pub limit_open_files: Option<i32>,
}

#[derive(Run, Subcommand, Clone)]
pub enum Command {
    /// Analyze partition store (RocksDB)
    #[clap(subcommand)]
    PartitionStore(partition_store::PartitionStoreCommand),
    /// Decode and analyze resource IDs
    #[clap(subcommand)]
    Id(id::IdCommand),
    /// Generate or install shell completions
    #[clap(subcommand)]
    Completions(Completions),
}

fn init(common_opts: &CommonOpts) {
    // Initialize CLI context (handles colors, logging, etc.)
    CliContext::new(common_opts.clone()).set_as_global();
}
