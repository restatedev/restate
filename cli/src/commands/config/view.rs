use crate::{c_println, cli_env::CliEnv, console};
use anyhow::Result;
use cling::prelude::*;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_view")]
pub struct View {}

pub async fn run_view(State(env): State<CliEnv>, _opts: &View) -> Result<()> {
    console::_gecho!(@nl_with_prefix, ("📝"), stderr, "Dumping {}:\n", env.config_file.display());

    let config_data = if env.config_file.is_file() {
        std::fs::read_to_string(env.config_file.as_path())?
    } else {
        "".into()
    };

    c_println!("{}", config_data);

    Ok(())
}
