// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::console::{Icon, StyledTable};
use crate::app::UiConfig;

use comfy_table::{Cell, Color, Table};
use restate_meta_rest_model::handlers::HandlerMetadata;
use restate_meta_rest_model::services::ServiceType;

pub fn create_service_handlers_table(ui_config: &UiConfig, handlers: &[HandlerMetadata]) -> Table {
    let mut table = Table::new_styled(ui_config);
    table.set_styled_header(vec!["HANDLER", "INPUT", "OUTPUT"]);

    for handler in handlers {
        table.add_row(vec![
            Cell::new(&handler.name),
            Cell::new(&handler.input_description),
            Cell::new(&handler.output_description),
        ]);
    }
    table
}

pub fn create_service_handlers_table_diff(
    ui_config: &UiConfig,
    old_service_handlers: &[HandlerMetadata],
    new_service_handlers: &[HandlerMetadata],
) -> Table {
    let mut old_service_handlers = old_service_handlers
        .iter()
        .map(|m| (m.name.clone(), m))
        .collect::<std::collections::HashMap<_, _>>();

    let mut table = Table::new_styled(ui_config);
    table.set_styled_header(vec!["", "HANDLER", "INPUT", "OUTPUT"]);

    // Additions and updates
    for handler in new_service_handlers {
        let mut row = vec![];
        if old_service_handlers.remove(&handler.name).is_some() {
            // possibly updated.
            row.push(Cell::new(""));
            row.push(Cell::new(&handler.name));
        } else {
            // new method
            row.push(Cell::new("++").fg(Color::Green));
            row.push(Cell::new(&handler.name).fg(Color::Green));
        }
        row.extend_from_slice(&[
            Cell::new(&handler.input_description),
            Cell::new(&handler.output_description),
        ]);
        table.add_row(row);
    }

    // Removals
    for handler in old_service_handlers.values() {
        let row = vec![
            Cell::new("--").fg(Color::Red),
            Cell::new(&handler.name).fg(Color::Red),
            Cell::new(&handler.input_description),
            Cell::new(&handler.output_description),
        ];

        table.add_row(row);
    }
    table
}

pub fn icon_for_service_type(svc_type: &ServiceType) -> Icon {
    match svc_type {
        ServiceType::Service => Icon("", ""),
        ServiceType::VirtualObject => Icon("⬅️ 🚶🚶🚶", "virtual object"),
        ServiceType::Workflow => Icon("📝", "workflow"),
    }
}

pub fn icon_for_is_public(public: bool) -> Icon<'static, 'static> {
    if public {
        Icon("🌎", "[public]")
    } else {
        Icon("🔒", "[private]")
    }
}
