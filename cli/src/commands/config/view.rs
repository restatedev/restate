// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Context, Result};
use cling::prelude::*;
use toml_edit::{Item, Table, value};

use restate_cli_util::CliContext;
use restate_cli_util::c_println;

use crate::ui::fmt::{Field, Formatter, OutputFormatter};
use crate::{cli_env::CliEnv, console};

/// Print the CLI config file, with its tokens redacted
///
/// Bearer tokens and Restate Cloud credentials are replaced by `<redacted>`: open the file
/// to see them. With --json, prints the file converted to JSON.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_view")]
#[command(after_help = after_help!(
    learn_more: "https://docs.restate.dev/references/cli-config",
))]
pub struct View {}

pub async fn run_view(State(env): State<CliEnv>, _opts: &View) -> Result<()> {
    let mut doc = env.load_config_document()?;
    redact(doc.as_table_mut());

    if CliContext::get().json_output() {
        // Convert the TOML config into JSON so `config view --json` is machine-readable.
        let value: serde_json::Value =
            toml::from_str(&doc.to_string()).context("Failed to convert the config to JSON")?;
        let mut f = Formatter::new();
        f.value("config", Field::new(value));
        return f.finish();
    }

    console::c_eprintln!("Dumping {} (tokens redacted):\n", env.config_file.display());
    c_println!("{}", doc);

    Ok(())
}

const REDACTED_KEYS: [&str; 2] = ["bearer_token", "access_token"];
const REDACTED: &str = "<redacted>";

/// Replace the value of every credential key, at any depth, keeping the file's layout.
fn redact(table: &mut Table) {
    for (key, item) in table.iter_mut() {
        if REDACTED_KEYS.contains(&key.get()) {
            *item = value(REDACTED);
        } else if let Item::Table(nested) = item {
            redact(nested);
        } else if let Some(inline) = item.as_inline_table_mut() {
            for (key, inline_value) in inline.iter_mut() {
                if REDACTED_KEYS.contains(&key.get()) {
                    *inline_value = REDACTED.into();
                }
            }
        }
    }
}

/// Parse `config`, redact it and return it as text.
#[cfg(test)]
fn redacted(config: &str) -> String {
    let mut doc = config.parse::<toml_edit::DocumentMut>().unwrap();
    redact(doc.as_table_mut());
    doc.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_tokens_at_any_depth_and_keeps_the_rest() {
        let config = "[global]\nenvironment = \"prod\"\n\n[global.cloud]\naccess_token = \"eyJ\"\n\n[prod]\nadmin_base_url = \"https://prod:9070\"\nbearer_token = \"secret\"\ncloud = { account_id = \"acc\", access_token = \"x\" }\n";
        let out = redacted(config);
        assert!(!out.contains("eyJ") && !out.contains("secret") && !out.contains("\"x\""));
        assert!(out.contains("environment = \"prod\""));
        assert!(out.contains("admin_base_url = \"https://prod:9070\""));
        assert!(out.contains("account_id = \"acc\""));
        assert_eq!(out.matches(REDACTED).count(), 3);
    }
}
