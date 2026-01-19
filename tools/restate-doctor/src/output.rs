// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::str::FromStr;

use serde::Serialize;

// Re-export cli-util macros and helpers
pub use restate_cli_util::ui::console::{Icon, Styled, StyledTable};
pub use restate_cli_util::ui::stylesheet::{ERR_ICON, SUCCESS_ICON, Style};
pub use restate_cli_util::{
    _comfy_table as comfy_table, c_indentln, c_println, c_success, c_title, c_warn,
};

/// Output format for command results
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OutputFormat {
    #[default]
    Human,
    Json,
}

impl FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "human" | "text" | "table" => Ok(OutputFormat::Human),
            "json" => Ok(OutputFormat::Json),
            _ => Err(format!(
                "Unknown output format: {s}. Use 'human' or 'json'."
            )),
        }
    }
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputFormat::Human => write!(f, "human"),
            OutputFormat::Json => write!(f, "json"),
        }
    }
}

/// Helper to output data in the requested format
pub fn output<T: Serialize + HumanOutput>(format: OutputFormat, data: &T) -> anyhow::Result<()> {
    match format {
        OutputFormat::Human => {
            data.print_human();
        }
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(data)?;
            c_println!("{json}");
        }
    }
    Ok(())
}

/// Trait for types that can be displayed in human-readable format
pub trait HumanOutput {
    fn print_human(&self);
}

/// Format bytes as human-readable size
pub fn format_size(bytes: u64) -> String {
    bytesize::ByteSize(bytes).to_string()
}

/// Format a percentage
pub fn format_percent(value: f64) -> String {
    format!("{:.1}%", value * 100.0)
}

/// Health check result indicator
pub fn check_icon(passed: bool) -> Icon<'static, 'static> {
    if passed { SUCCESS_ICON } else { ERR_ICON }
}
