// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Result;
use clap_verbosity_flag::LogLevel;
use cling::prelude::*;
use tracing::info;
use tracing_log::AsTrace;

use crate::cli_env::CliEnv;
use crate::commands::*;

#[derive(Run, Parser, Clone)]
#[command(author, version = crate::build_info::version(), about, infer_subcommands = true)]
#[cling(run = "init")]
pub struct CliApp {
    #[clap(flatten)]
    #[cling(collect)]
    pub verbose: clap_verbosity_flag::Verbosity<Quiet>,
    #[clap(flatten)]
    pub global_opts: GlobalOpts,
    #[clap(subcommand)]
    pub cmd: Command,
}

#[derive(Args, Clone, Default)]
pub struct UiConfig {
    /// Which table output style to use
    #[arg(long, default_value = "compact", global = true)]
    pub table_style: TableStyle,
}

#[derive(ValueEnum, Clone, Copy, Default, PartialEq, Eq)]
pub enum TableStyle {
    #[default]
    /// No borders, condensed layout
    Compact,
    /// UTF8 borders, good for multiline text
    Borders,
}

#[derive(Args, Collect, Clone, Default)]
pub struct GlobalOpts {
    /// Auto answer "yes" to confirmation prompts
    #[arg(long, short, global = true)]
    pub yes: bool,

    #[clap(flatten)]
    pub ui_config: UiConfig,
}

#[derive(Run, Subcommand, Clone)]
pub enum Command {
    /// Prints general information about the configured environment
    #[clap(name = "whoami")]
    WhoAmiI(whoami::WhoAmI),
    /// Manage Restate's service registry
    #[clap(subcommand)]
    Services(services::Services),
    /// Manages your service deployments
    #[clap(subcommand)]
    Deployments(deployments::Deployments),
    /// Runs SQL queries against the data fusion service
    #[clap(hide = true)]
    Sql(sql::Sql),
    /// Download one of Restate's examples in this directory.
    #[clap(name = "example", alias = "examples")]
    Examples(examples::Examples),
}

fn init(
    Collected(verbosity): Collected<clap_verbosity_flag::Verbosity<Quiet>>,
    global_opts: &GlobalOpts,
) -> Result<State<CliEnv>> {
    let env = CliEnv::load(global_opts)?;
    // Setup logging from env and from -v .. -vvvv
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_max_level(verbosity.log_level_filter().as_trace())
        .with_ansi(env.colorful)
        .init();

    // We only log after we've initialized the logger with the desired log
    // level.
    match &env.loaded_env_file {
        Some(path) => {
            info!("Loaded environment file from: {}", path.display())
        }
        None => info!("Didn't load '.env' file"),
    };

    Ok(State(env))
}

/// Silent (no) logging by default in CLI
#[derive(Clone)]
pub struct Quiet;
impl LogLevel for Quiet {
    fn default() -> Option<tracing_log::log::Level> {
        None
    }
}
