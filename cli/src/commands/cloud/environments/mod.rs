mod configure;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
#[clap(visible_alias = "env", alias = "environment")]
pub enum Environments {
    /// Set up the CLI to talk to this Environment
    Configure(configure::Configure),
}
