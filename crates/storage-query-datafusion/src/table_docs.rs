// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::fmt::Write as _;

use crate::{
    deployment, inbox, index, invocation_state, invocation_status, journal, journal_events,
    promise, rules, scheduler_status, service, state, stats, user_limits, vqueue_entry_status,
    vqueue_meta, vqueues,
};

/// List of available table docs. Whenever you add a new table, add its table docs to
/// this array. This will ensure that the table docs will be included in the automatic
/// table docs generation process.
pub const ALL_TABLE_DOCS: &[StaticTableDocs] = &[
    index::by_virtual_object::schema::TABLE_DOCS,
    index::entry_next_at_by_service::schema::TABLE_DOCS,
    index::entry_next_at_by_virtual_object::schema::TABLE_DOCS,
    index::busy_vqueue::schema::TABLE_DOCS,
    index::entry_by_stage::schema::TABLE_DOCS,
    index::entry_next_at_by_stage::schema::TABLE_DOCS,
    deployment::schema::TABLE_DOCS,
    stats::deployment_stats::schema::TABLE_DOCS,
    inbox::schema::TABLE_DOCS,
    journal::schema::TABLE_DOCS,
    journal_events::schema::TABLE_DOCS,
    promise::schema::TABLE_DOCS,
    rules::schema::TABLE_DOCS,
    scheduler_status::schema::TABLE_DOCS,
    service::schema::TABLE_DOCS,
    stats::service_stats::schema::TABLE_DOCS,
    state::schema::TABLE_DOCS,
    user_limits::schema::TABLE_DOCS,
    stats::virtual_object_stats::schema::TABLE_DOCS,
    vqueue_entry_status::schema::TABLE_DOCS,
    vqueue_meta::schema::TABLE_DOCS,
    vqueues::schema::TABLE_DOCS,
];

pub trait TableDocs {
    fn name(&self) -> &str;

    fn description(&self) -> &str;

    fn columns(&self) -> &[TableColumn];
}

#[derive(Debug, Copy, Clone)]
pub struct TableColumn {
    pub name: &'static str,
    pub column_type: &'static str,
    pub description: &'static str,
}

#[derive(Debug, Copy, Clone)]
pub struct StaticTableDocs {
    pub name: &'static str,
    pub description: &'static str,
    pub columns: &'static [TableColumn],
}

impl TableDocs for StaticTableDocs {
    fn name(&self) -> &str {
        self.name
    }

    fn description(&self) -> &str {
        self.description
    }

    fn columns(&self) -> &[TableColumn] {
        self.columns
    }
}

#[derive(Debug)]
pub struct OwnedTableDocs {
    pub name: Cow<'static, str>,
    pub description: Cow<'static, str>,
    pub columns: Vec<TableColumn>,
}

impl From<&StaticTableDocs> for OwnedTableDocs {
    fn from(value: &StaticTableDocs) -> Self {
        OwnedTableDocs {
            name: Cow::Borrowed(value.name),
            description: Cow::Borrowed(value.description),
            columns: value.columns.to_vec(),
        }
    }
}

/// Returns every table's docs as owned values: all entries of [`ALL_TABLE_DOCS`]
/// followed by the synthesized `sys_invocation` view. This is the single source
/// of truth consumed by the docs generator and the CLI's embedded reference.
pub fn all_table_docs() -> Vec<OwnedTableDocs> {
    ALL_TABLE_DOCS
        .iter()
        .map(OwnedTableDocs::from)
        .chain(std::iter::once(sys_invocation_table_docs()))
        .collect()
}

impl TableDocs for OwnedTableDocs {
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn columns(&self) -> &[TableColumn] {
        &self.columns
    }
}

pub fn sys_invocation_table_docs() -> OwnedTableDocs {
    // We need to compile this manually, due to the fact that it's a view.
    use std::collections::HashMap;
    let mut sys_invocation_state: HashMap<&'static str, TableColumn> =
        invocation_state::schema::TABLE_DOCS
            .columns
            .iter()
            .map(|column| (column.name, *column))
            .collect();
    let mut sys_invocation_status: HashMap<&'static str, TableColumn> =
        invocation_status::schema::TABLE_DOCS
            .columns
            .iter()
            .map(|column| (column.name, *column))
            .collect();

    let columns = vec![
        sys_invocation_status.remove("id").expect("id should exist"),
        sys_invocation_status
            .remove("vqueue_id")
            .expect("vqueue_id should exist"),
        sys_invocation_status
            .remove("target")
            .expect("target should exist"),
        sys_invocation_status
            .remove("target_service_name")
            .expect("target_service_name should exist"),
        sys_invocation_status
            .remove("target_service_key")
            .expect("target_service_key should exist"),
        sys_invocation_status
            .remove("target_handler_name")
            .expect("target_handler_name should exist"),
        sys_invocation_status
            .remove("target_service_ty")
            .expect("target_service_ty should exist"),
        sys_invocation_status
            .remove("scope")
            .expect("scope should exist"),
        sys_invocation_status
            .remove("limit_key")
            .expect("limit_key should exist"),
        sys_invocation_status
            .remove("idempotency_key")
            .expect("idempotency_key should exist"),
        sys_invocation_status
            .remove("invoked_by")
            .expect("invoked_by should exist"),
        sys_invocation_status
            .remove("invoked_by_service_name")
            .expect("invoked_by_service_name should exist"),
        sys_invocation_status
            .remove("invoked_by_id")
            .expect("invoked_by_id should exist"),
        sys_invocation_status
            .remove("invoked_by_subscription_id")
            .expect("invoked_by_subscription_id should exist"),
        sys_invocation_status
            .remove("invoked_by_target")
            .expect("invoked_by_target should exist"),
        sys_invocation_status
            .remove("restarted_from")
            .expect("restarted_from should exist"),
        sys_invocation_status
            .remove("pinned_deployment_id")
            .expect("pinned_deployment_id should exist"),
        sys_invocation_status
            .remove("pinned_service_protocol_version")
            .expect("pinned_service_protocol_version should exist"),
        sys_invocation_status
            .remove("trace_id")
            .expect("trace_id should exist"),
        sys_invocation_status
            .remove("journal_size")
            .expect("journal_size should exist"),
        sys_invocation_status
            .remove("journal_commands_size")
            .expect("journal_commands_size should exist"),
        sys_invocation_status
            .remove("created_at")
            .expect("created_at should exist"),
        sys_invocation_status
            .remove("created_using_restate_version")
            .expect("created_using_restate_version should exist"),
        sys_invocation_status
            .remove("modified_at")
            .expect("modified_at should exist"),
        sys_invocation_status
            .remove("inboxed_at")
            .expect("inboxed_at should exist"),
        sys_invocation_status
            .remove("scheduled_at")
            .expect("scheduled_at should exist"),
        sys_invocation_status
            .remove("scheduled_start_at")
            .expect("scheduled_start_at should exist"),
        sys_invocation_status
            .remove("running_at")
            .expect("running_at should exist"),
        sys_invocation_status
            .remove("completed_at")
            .expect("completed_at should exist"),
        sys_invocation_status
            .remove("completion_retention")
            .expect("completion_retention should exist"),
        sys_invocation_status
            .remove("journal_retention")
            .expect("journal_retention should exist"),
        sys_invocation_status
            .remove("suspended_waiting_for_completions")
            .expect("suspended_waiting_for_completions should exist"),
        sys_invocation_status
            .remove("suspended_waiting_for_signals")
            .expect("suspended_waiting_for_signals should exist"),
        sys_invocation_status
            .remove("suspended_waiting_future_json")
            .expect("suspended_waiting_future_json should exist"),
        sys_invocation_state
            .remove("retry_count")
            .expect("retry_count should exist"),
        sys_invocation_state
            .remove("last_start_at")
            .expect("last_start_at should exist"),
        sys_invocation_state
            .remove("next_retry_at")
            .expect("next_retry_at should exist"),
        sys_invocation_state
            .remove("last_attempt_deployment_id")
            .expect("last_attempt_deployment_id should exist"),
        sys_invocation_state
            .remove("last_attempt_server")
            .expect("last_attempt_server should exist"),
        sys_invocation_state
            .remove("last_failure")
            .expect("last_failure should exist"),
        sys_invocation_state
            .remove("last_failure_error_code")
            .expect("last_failure_error_code should exist"),
        sys_invocation_state
            .remove("last_failure_related_entry_index")
            .expect("last_failure_related_entry_index should exist"),
        sys_invocation_state
            .remove("last_failure_related_entry_name")
            .expect("last_failure_related_entry_name should exist"),
        sys_invocation_state
            .remove("last_failure_related_entry_type")
            .expect("last_failure_related_entry_type should exist"),
        sys_invocation_state
            .remove("last_failure_related_command_index")
            .expect("last_failure_related_command_index should exist"),
        sys_invocation_state
            .remove("last_failure_related_command_name")
            .expect("last_failure_related_command_name should exist"),
        sys_invocation_state
            .remove("last_failure_related_command_type")
            .expect("last_failure_related_command_type should exist"),
        sys_invocation_state
            .remove("last_awaiting_on_future_json")
            .expect("last_awaiting_on_future_json should exist"),
        TableColumn {
            name: "status",
            column_type: "Utf8",
            description: "Either `pending` or `scheduled` or `ready` or `running` or `paused` or `backing-off` or `suspended` or `completed`.",
        },
        sys_invocation_status
            .remove("completion_result")
            .expect("completion_result should exist"),
        sys_invocation_status
            .remove("completion_failure")
            .expect("completion_failure should exist"),
    ];

    OwnedTableDocs {
        name: Cow::Borrowed("sys_invocation"),
        description: Cow::Borrowed(""),
        columns,
    }
}

/// Renders the SQL introspection reference embedded in the CLI
/// (`cli/src/commands/sql_tables.rs`), regenerated with
/// `cargo xtask generate-cli-sql-tables > cli/src/commands/sql_tables.rs`.
pub fn render_cli_sql_tables() -> String {
    let tables = all_table_docs();

    let mut out = String::from(
        "// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.\n\
         // All rights reserved.\n\
         //\n\
         // Use of this software is governed by the Business Source License\n\
         // included in the LICENSE file.\n\
         //\n\
         // As of the Change Date specified in that file, in accordance with\n\
         // the Business Source License, use of this software will be governed\n\
         // by the Apache License, Version 2.0.\n\n\
         // @generated by `cargo xtask generate-cli-sql-tables`. Do not edit manually.\n\n\
         pub struct SqlColumnDoc { pub name: &'static str, pub ty: &'static str, pub description: &'static str }\n\
         pub struct SqlTableDoc { pub name: &'static str, pub description: &'static str, pub columns: &'static [SqlColumnDoc] }\n\n\
         pub static SQL_TABLES: &[SqlTableDoc] = &[\n",
    );
    for table in &tables {
        let _ = writeln!(
            out,
            "    SqlTableDoc {{ name: {:?}, description: {:?}, columns: &[",
            table.name.as_ref(),
            table.description.as_ref().trim(),
        );
        for column in &table.columns {
            let _ = writeln!(
                out,
                "        SqlColumnDoc {{ name: {:?}, ty: {:?}, description: {:?} }},",
                column.name,
                column.column_type,
                column.description.trim(),
            );
        }
        out.push_str("    ] },\n");
    }
    out.push_str("];\n\n");

    // Condensed table list shown in `restate sql --help`.
    let names = tables
        .iter()
        .map(|t| t.name.as_ref())
        .collect::<Vec<_>>()
        .join(", ");
    let help = format!(
        "Queryable introspection tables:\n  {names}\n\nRun `restate sql describe <table>` for a \
         table's columns."
    );
    let _ = writeln!(out, "pub static SQL_TABLES_HELP: &str = {help:?};");
    out
}
