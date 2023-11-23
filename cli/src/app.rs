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
#[command(author, version = crate::build_info::version(), about)]
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

#[derive(Args, Clone)]
pub struct GlobalOpts {}

#[derive(Run, Subcommand, Clone)]
pub enum Command {
    #[clap(name = "whoami")]
    WhoAmiI(whoami::WhoAmI),
}

fn init(
    Collected(verbosity): Collected<clap_verbosity_flag::Verbosity<Quiet>>,
) -> Result<State<CliEnv>> {
    let env = CliEnv::load()?;
    // Setup logging from env and from -v .. -vvvv
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_max_level(verbosity.log_level_filter().as_trace())
        .with_ansi(env.is_terminal())
        .init();

    // We only log after we've initialized the logger with the desired log
    // level.
    match env.env_file_path() {
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
