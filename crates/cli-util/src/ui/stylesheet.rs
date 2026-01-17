// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Visual constants and table styling for CLI output.
//!
//! This module defines the visual language used across Restate CLI tools:
//! - **Icons**: Emoji with text fallbacks for non-color terminals
//! - **Styles**: Semantic color mappings (success=green, danger=red, etc.)
//! - **StyledTable**: Factory trait for creating consistently styled tables
//!
//! # Example
//!
//! ```ignore
//! use restate_cli_util::ui::stylesheet::{Style, SUCCESS_ICON};
//! use restate_cli_util::ui::console::Styled;
//!
//! // Use semantic styles instead of hardcoding colors
//! c_println!("{} {}", SUCCESS_ICON, Styled(Style::Success, "Operation completed"));
//! ```

use crate::context::CliContext;
use crate::opts::TableStyle;

use super::console::{Icon, StyledTable};

/// Success indicator icon. Displays as checkmark emoji or "[OK]:" in non-color mode.
pub const SUCCESS_ICON: Icon = Icon("✅", "[OK]:");

/// Error indicator icon. Displays as X emoji or "[ERR]:" in non-color mode.
pub const ERR_ICON: Icon = Icon("❌", "[ERR]:");

/// Warning indicator icon. Displays as warning emoji or "[WARNING]:" in non-color mode.
pub const WARN_ICON: Icon = Icon("⚠️", "[WARNING]:");

/// Tip/hint indicator icon. Displays as lightbulb emoji or "[TIP]:" in non-color mode.
pub const TIP_ICON: Icon = Icon("💡", "[TIP]:");

/// Home indicator icon. Displays as house emoji or "[HOME]:" in non-color mode.
pub const HOME_ICON: Icon = Icon("🏠", "[HOME]:");

/// Connection/handshake indicator icon. Displays as handshake emoji or "[HANDSHAKE]:" in non-color mode.
pub const HANDSHAKE_ICON: Icon = Icon("🤝", "[HANDSHAKE]:");

/// Public/external resource indicator. Displays as globe emoji or "[GLOBE]:" in non-color mode.
pub const GLOBE_ICON: Icon = Icon("🌎", "[GLOBE]:");

/// Private/protected resource indicator. Displays as lock emoji or "[LOCK]:" in non-color mode.
pub const LOCK_ICON: Icon = Icon("🔒", "[LOCK]:");

/// Semantic text styles for CLI output.
///
/// These styles map to specific colors and attributes, ensuring consistent
/// visual language across all CLI commands. Always prefer semantic styles
/// over hardcoding colors directly.
///
/// # Style Mappings
///
/// | Style | Appearance | Use Case |
/// |-------|------------|----------|
/// | `Danger` | Red + Bold | Errors, removals, critical issues |
/// | `Warn` | Magenta | Caution, updates, non-critical warnings |
/// | `Success` | Green | Positive outcomes, additions, running states |
/// | `Info` | Bright + Bold | Important values (IDs, keys), emphasis |
/// | `Notice` | Italic | Secondary information, notes |
/// | `Normal` | Default | Regular text |
#[derive(Copy, Clone)]
pub enum Style {
    /// Red + Bold. Use for errors, removals, and critical issues.
    Danger,
    /// Magenta. Use for caution, updates, and non-critical warnings.
    Warn,
    /// Green. Use for positive outcomes, additions, and running states.
    Success,
    /// Bright + Bold. Use for important values like IDs, keys, and emphasis.
    Info,
    /// Italic. Use for secondary information and notes.
    Notice,
    /// Default styling. Use for regular text.
    Normal,
}

impl From<Style> for dialoguer::console::Style {
    fn from(style: Style) -> Self {
        use dialoguer::console::Style as DStyle;

        // Mapping styles to actual colors
        match style {
            Style::Danger => DStyle::new().red().bold(),
            Style::Warn => DStyle::new().magenta(),
            Style::Success => DStyle::new().green(),
            Style::Info => DStyle::new().bright().bold(),
            Style::Notice => DStyle::new().italic(),
            Style::Normal => DStyle::new(),
        }
    }
}

/// Implementation of [`StyledTable`] for `comfy_table::Table`.
///
/// Creates tables that respect the user's `--table-style` preference:
/// - **Compact**: No borders, condensed layout (default)
/// - **Borders**: UTF-8 borders with rounded corners
///
/// Tables also respect color settings, disabling styling when colors are off.
impl StyledTable for comfy_table::Table {
    fn new_styled() -> Self {
        let ctx = CliContext::get();
        let mut table = comfy_table::Table::new();
        table.set_content_arrangement(comfy_table::ContentArrangement::Dynamic);
        match ctx.table_style() {
            TableStyle::Compact => {
                table.load_preset(comfy_table::presets::NOTHING);
            }
            TableStyle::Borders => {
                table.load_preset(comfy_table::presets::UTF8_FULL);
                table.apply_modifier(comfy_table::modifiers::UTF8_ROUND_CORNERS);
            }
        }
        if !ctx.colors_enabled() {
            table.force_no_tty();
        } else {
            table.enforce_styling();
        }
        table
    }

    fn set_styled_header<T: ToString>(&mut self, headers: Vec<T>) -> &mut Self {
        self.set_header(
            headers
                .into_iter()
                .map(|c| comfy_table::Cell::new(c).add_attribute(comfy_table::Attribute::Bold)),
        )
    }

    fn add_kv_row<V: Into<comfy_table::Cell>>(&mut self, key: &str, value: V) -> &mut Self {
        self.add_row(vec![
            comfy_table::Cell::new(key).add_attribute(comfy_table::Attribute::Bold),
            value.into(),
        ])
    }
}
