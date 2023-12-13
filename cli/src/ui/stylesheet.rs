// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::app::UiConfig;

use super::console::{Icon, StyledTable};

pub const SUCCESS_ICON: Icon = Icon("✅", "[OK]:");
pub const ERR_ICON: Icon = Icon("❌", "[ERR]:");
pub const WARN_ICON: Icon = Icon("⚠️", "[WARNING]:");

#[derive(Copy, Clone)]
pub enum Style {
    Danger,
    Warn,
    Success,
    Info,
    Notice,
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

/// Defines how compact/borders table style will actually look like
impl StyledTable for comfy_table::Table {
    fn new_styled(ui_config: &UiConfig) -> Self {
        let mut table = comfy_table::Table::new();
        table.set_content_arrangement(comfy_table::ContentArrangement::Dynamic);
        match ui_config.table_style {
            crate::app::TableStyle::Compact => {
                table.load_preset(comfy_table::presets::NOTHING);
            }
            crate::app::TableStyle::Borders => {
                table.load_preset(comfy_table::presets::UTF8_FULL);
                table.apply_modifier(comfy_table::modifiers::UTF8_ROUND_CORNERS);
            }
        }
        if !crate::console::colors_enabled() {
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
