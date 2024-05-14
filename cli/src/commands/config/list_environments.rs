use crate::{c_println, cli_env::CliEnv, console::StyledTable};
use anyhow::{Context, Result};
use cling::prelude::*;
use comfy_table::{Cell, Table};
use toml_edit::DocumentMut;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_list_environments")]
pub struct ListEnvironments {}

pub async fn run_list_environments(
    State(env): State<CliEnv>,
    _opts: &ListEnvironments,
) -> Result<()> {
    let config_data = if env.config_file.is_file() {
        std::fs::read_to_string(env.config_file.as_path())?
    } else {
        "".into()
    };

    let doc = config_data
        .parse::<DocumentMut>()
        .context("Failed to parse config file as TOML")?;

    let mut table = Table::new_styled(&env.ui_config);
    let header = vec!["CURRENT", "NAME", "ADMIN_URL"];
    table.set_styled_header(header);

    for (environment_name, config) in doc.iter() {
        if environment_name == "global" {
            continue;
        }

        let admin_url = config
            .get("admin_base_url")
            .and_then(|u| u.as_str())
            .unwrap_or("N/A");

        let current = if environment_name == env.environment.as_str() {
            "*"
        } else {
            ""
        };

        let row = vec![
            Cell::new(current),
            Cell::new(environment_name),
            Cell::new(admin_url),
        ];

        table.add_row(row);
    }

    c_println!("{}", table);

    Ok(())
}
