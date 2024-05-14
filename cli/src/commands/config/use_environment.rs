use crate::{c_success, cli_env::CliEnv};
use anyhow::Result;
use cling::prelude::*;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_use_environment")]
pub struct UseEnvironment {
    /// The name of the environment in the CLI config file to switch to
    #[clap(index = 1)]
    environment_name: String,
}

pub async fn run_use_environment(State(env): State<CliEnv>, opts: &UseEnvironment) -> Result<()> {
    std::fs::write(env.environment_file.as_path(), &opts.environment_name)?;
    c_success!(
        "Updated {} to {}",
        env.environment_file.display(),
        &opts.environment_name
    );

    Ok(())
}
