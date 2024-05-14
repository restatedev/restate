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
use figment::Profile;
use tracing::info;
use tracing_log::AsTrace;

use crate::cli_env::{CliEnv, EnvironmentSource};
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

const DEFAULT_CONNECT_TIMEOUT: u64 = 5_000;

#[derive(Args, Collect, Clone, Default)]
pub struct GlobalOpts {
    /// Auto answer "yes" to confirmation prompts
    #[arg(long, short, global = true)]
    pub yes: bool,

    /// Connection timeout for service interactions, in milliseconds.
    #[arg(long, default_value_t = DEFAULT_CONNECT_TIMEOUT, global = true)]
    pub connect_timeout: u64,

    /// Overall request timeout for service interactions, in milliseconds.
    #[arg(long, global = true)]
    pub request_timeout: Option<u64>,

    #[clap(flatten)]
    pub ui_config: UiConfig,

    /// Environment to select from the config file. Environment is read from these sources in order of precedence:
    ///
    /// 1. This command line argument
    /// 2. $RESTATE_ENVIRONMENT
    /// 3. The file $RESTATE_CLI_CONFIG_HOME/environment (default: $HOME/.config/restate/environment)
    #[arg(long, short, global = true, verbatim_doc_comment)]
    pub environment: Option<Profile>,
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
    /// Manage active invocations
    #[clap(subcommand)]
    Invocations(invocations::Invocations),
    /// Runs SQL queries against the data fusion service
    #[clap(hide = true)]
    Sql(sql::Sql),
    /// Download one of Restate's examples in this directory.
    #[clap(name = "example", alias = "examples")]
    Examples(examples::Examples),

    /// Manage service state
    #[clap(name = "state", alias = "kv")]
    #[clap(subcommand)]
    State(state::ServiceState),

    #[cfg(feature = "cloud")]
    #[clap(subcommand)]
    /// Manage Restate Cloud
    Cloud(cloud::Cloud),
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

    match &env.environment_source {
        EnvironmentSource::Argument => {
            info!("Using environment from --environment")
        }
        EnvironmentSource::Environment => {
            info!("Using environment from $RESTATE_ENVIRONMENT")
        }
        EnvironmentSource::File => {
            info!("Using environment from {}", env.environment_file.display())
        }
        EnvironmentSource::None => {
            info!("Didn't load an environment")
        }
    }

    // We only log after we've initialized the logger with the desired log
    // level.
    match &env.loaded_env_file {
        Some(path) => {
            info!("Loaded .env file from: {}", path.display())
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
