// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use crate::commands::id;
use crate::output::OutputFormat;

/// Restate Doctor - Diagnostic tools Restate storage
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
    /// Output format
    #[arg(long, global = true, default_value = "human")]
    pub output: OutputFormat,
}

#[derive(Run, Subcommand, Clone)]
pub enum Command {
    #[clap(subcommand)]
    Id(id::IdCommand),
}

fn init(common_opts: &CommonOpts) {
    // Initialize CLI context (handles colors, logging, etc.)
    CliContext::new(common_opts.clone()).set_as_global();
}
