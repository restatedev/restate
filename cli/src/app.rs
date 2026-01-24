// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Result;
use cling::prelude::*;
use figment::Profile;
use tracing::info;

use restate_cli_util::{CliContext, CommonOpts};

use crate::cli_env::{CliEnv, EnvironmentSource};
use crate::commands::completions::Completions;
use crate::commands::*;

/// Restate Command Line Interface
///
/// A command-line tool to inspect restate services status, invocations, deployment, and much more.
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
    /// Environment to select from the config file. Environment is read from these sources in order of precedence:
    ///     1. This command line argument
    ///     2. $RESTATE_ENVIRONMENT
    ///     3. The file $RESTATE_CLI_CONFIG_HOME/environment (default: $HOME/.config/restate/environment)
    /// If none of these are provided, the 'local' environment is used, pointing to an instance running locally.
    #[arg(long, short, global = true, verbatim_doc_comment)]
    pub environment: Option<Profile>,
}

#[derive(Run, Subcommand, Clone)]
pub enum Command {
    #[cfg(feature = "dev-cmd")]
    #[clap(name = "dev", visible_alias = "up")]
    Dev(dev::Dev),

    /// Prints general information about the configured environment
    #[clap(name = "whoami")]
    WhoAmI(whoami::WhoAmI),
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
    Sql(sql::Sql),
    /// Download one of Restate's examples in this directory.
    #[clap(name = "example", alias = "examples")]
    Examples(examples::Examples),

    /// Manage service state
    #[clap(name = "state", alias = "kv")]
    #[clap(subcommand)]
    State(state::ServiceState),

    /// Generate shell completions
    #[clap(subcommand)]
    Completions(Completions),

    /// Manage CLI config
    #[clap(subcommand, alias = "conf")]
    Config(config::Config),

    #[cfg(feature = "cloud")]
    #[clap(subcommand)]
    /// Manage Restate Cloud
    Cloud(cloud::Cloud),

    /// Run as an AWS Lambda server
    Lambda(restate_cli_util::lambda::LambdaOpts),
}

fn init(common_opts: &CommonOpts, global_opts: &GlobalOpts) -> Result<State<CliEnv>> {
    CliContext::new(common_opts.clone()).set_as_global();
    let env = CliEnv::load(global_opts)?;

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

    Ok(State(env))
}
