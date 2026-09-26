// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use comfy_table::{Cell, Color, Table};

use restate_cli_util::CliContext;
use restate_cli_util::ui::console::StyledTable;
use restate_types::invocation::ServiceType;
use restate_types::schema::service::HandlerMetadata;

use crate::ui::fmt::{Field, Formatter, OutputFormatter};

pub fn create_service_handlers_table<'a>(
    handlers: impl Iterator<Item = &'a HandlerMetadata>,
) -> Table {
    let mut table = Table::new_styled();
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

/// Handler rows for a formatter `table` section (headers: handler / input / output).
/// Sorted by handler name for deterministic output.
/// The first line of a handler's SDK-provided documentation, if any.
pub fn handler_description(handler: &HandlerMetadata) -> Option<&str> {
    handler
        .documentation
        .as_deref()
        .and_then(|doc| doc.lines().map(str::trim).find(|line| !line.is_empty()))
}

/// Write the handlers of a service as a `handlers` table, sorted by name. The human
/// output adds a DESCRIPTION column only when some handler is documented; JSON always
/// carries `description`.
pub fn write_service_handlers<'a>(
    f: &mut Formatter,
    handlers: impl Iterator<Item = &'a HandlerMetadata>,
) {
    let mut handlers: Vec<&HandlerMetadata> = handlers.collect();
    handlers.sort_by(|a, b| a.name.cmp(&b.name));
    let with_description = CliContext::get().json_output()
        || handlers.iter().any(|h| handler_description(h).is_some());
    let rows = handlers.into_iter().map(|handler| {
        let mut row = vec![
            Field::new(handler.name.to_string()),
            Field::new(handler.input_description.clone()),
            Field::new(handler.output_description.clone()),
        ];
        if with_description {
            row.push(Field::new(handler_description(handler)));
        }
        row
    });
    let headers: &[&str] = if with_description {
        &["handler", "input", "output", "description"]
    } else {
        &["handler", "input", "output"]
    };
    f.table("handlers", headers, rows);
}

pub fn create_service_handlers_table_diff<'a>(
    old_service_handlers: impl Iterator<Item = &'a HandlerMetadata>,
    new_service_handlers: impl Iterator<Item = &'a HandlerMetadata>,
) -> Table {
    let mut old_service_handlers = old_service_handlers
        .map(|m| (m.name.clone(), m))
        .collect::<std::collections::HashMap<_, _>>();

    let mut table = Table::new_styled();
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

pub fn service_type_label(svc_type: &ServiceType) -> &'static str {
    match svc_type {
        ServiceType::Service => "service",
        ServiceType::VirtualObject => "virtual object",
        ServiceType::Workflow => "workflow",
    }
}

/// Stable machine value for a service type (`service` / `virtual_object` / `workflow`,
/// matching the SQL `target_service_ty` vocabulary).
pub fn service_type_machine(svc_type: &ServiceType) -> &'static str {
    match svc_type {
        ServiceType::Service => "service",
        ServiceType::VirtualObject => "virtual_object",
        ServiceType::Workflow => "workflow",
    }
}

/// Service-type as a formatter [`Field`]: the machine value with a human-friendly display.
pub fn service_type_field(svc_type: &ServiceType) -> Field {
    Field::with_display(service_type_machine(svc_type), service_type_label(svc_type))
}

pub fn visibility_label(public: bool) -> &'static str {
    if public { "public" } else { "private" }
}
