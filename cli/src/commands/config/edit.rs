use crate::{cli_env::CliEnv, console};
use anyhow::Result;
use cling::prelude::*;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_edit")]
pub struct Edit {}

pub async fn run_edit(State(env): State<CliEnv>, _opts: &Edit) -> Result<()> {
    console::_gecho!(@nl_with_prefix, ("📝"), stderr, "Editing {}", env.config_file.display());

    env.open_default_editor(&env.config_file)?;

    Ok(())
}
