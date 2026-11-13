// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The SQL introspection reference, generated from the logical schemas.
//!
//! Every documented table is a logical schema declared with `define_schema!`:
//! its doc comment is the table's description and each column's doc comment
//! that column's description, so a table is documented where it is declared.
//! Three tables are assembled here: `sys_invocation` from the invocation status
//! and state tables it joins, `sys_vqueues`, documented in this module until
//! the view's definition moves next to it, and `sys_inbox`, the legacy inbox
//! table, which has no logical schema.
//!
//! `cargo xtask generate-table-docs` renders [`all_table_docs`] as the Markdown
//! reference, and `cargo xtask generate-cli-sql-tables` as the copy the CLI
//! embeds ([`render_cli_sql_tables`]).

use std::fmt::Write as _;

use crate::LogicalSchema;
use crate::schema::rows::TableColumn;
use crate::tables::{
    deployment, invocation_state, invocation_status, journal, journal_events, locks, promise,
    rules, scheduler, service, state, user_limits, vqueue_entry_status, vqueue_meta,
};

/// The documentation of one table: its name, its description and its columns.
#[derive(Debug, Clone)]
pub struct TableDocs {
    pub name: &'static str,
    pub description: String,
    pub columns: Vec<ColumnDocs>,
}

/// The documentation of one column: its name, its type as the reference names
/// it, and its description.
#[derive(Debug, Clone)]
pub struct ColumnDocs {
    pub name: &'static str,
    pub column_type: &'static str,
    pub description: String,
}

impl From<&TableColumn> for ColumnDocs {
    fn from(column: &TableColumn) -> Self {
        Self {
            name: column.name,
            column_type: column.column_type,
            description: single_line(column.description),
        }
    }
}

/// A doc comment as one line: the macro joins the comment's lines with
/// newlines, and each line keeps the space that follows its `///`.
fn single_line(doc: &str) -> String {
    doc.replace('\n', "")
}

/// The docs of a logical table.
macro_rules! docs {
    ($table:ty) => {
        TableDocs {
            name: <$table as LogicalSchema>::NAME,
            description: single_line(<$table>::DESCRIPTION),
            columns: <$table>::COLUMN_DOCS.iter().map(ColumnDocs::from).collect(),
        }
    };
}

/// Every documented table, in the order of the reference.
pub fn all_table_docs() -> Vec<TableDocs> {
    vec![
        docs!(deployment::Deployment),
        sys_inbox_table_docs(),
        docs!(journal::Journal),
        docs!(journal_events::JournalEvents),
        docs!(locks::Locks),
        docs!(promise::Promise),
        docs!(rules::Rules),
        docs!(scheduler::Scheduler),
        docs!(service::Service),
        docs!(state::State),
        docs!(user_limits::UserLimits),
        docs!(vqueue_entry_status::VqueueEntryStatus),
        docs!(vqueue_meta::VqueueMeta),
        sys_vqueues_table_docs(),
        sys_invocation_table_docs(),
    ]
}

/// `sys_invocation` is a view joining `sys_invocation_status` and
/// `sys_invocation_state`: its columns are theirs, in the view's order, plus
/// the combined `status`.
pub fn sys_invocation_table_docs() -> TableDocs {
    let column = |columns: &'static [TableColumn], name: &str| -> ColumnDocs {
        columns
            .iter()
            .find(|column| column.name == name)
            .map(ColumnDocs::from)
            .unwrap_or_else(|| panic!("the view's column `{name}` is not in its source table"))
    };
    let status = |name| column(invocation_status::InvocationStatus::COLUMN_DOCS, name);
    let state = |name| column(invocation_state::InvocationState::COLUMN_DOCS, name);
    let columns = vec![
        status("id"),
        status("vqueue_id"),
        status("target"),
        status("target_service_name"),
        status("target_service_key"),
        status("target_handler_name"),
        status("target_service_ty"),
        status("scope"),
        status("limit_key"),
        status("idempotency_key"),
        status("invoked_by"),
        status("invoked_by_service_name"),
        status("invoked_by_id"),
        status("invoked_by_subscription_id"),
        status("invoked_by_target"),
        status("restarted_from"),
        status("pinned_deployment_id"),
        status("pinned_service_protocol_version"),
        status("trace_id"),
        status("journal_size"),
        status("journal_commands_size"),
        status("created_at"),
        status("created_using_restate_version"),
        status("modified_at"),
        status("inboxed_at"),
        status("scheduled_at"),
        status("scheduled_start_at"),
        status("running_at"),
        status("completed_at"),
        status("completion_retention"),
        status("journal_retention"),
        status("suspended_waiting_for_completions"),
        status("suspended_waiting_for_signals"),
        status("suspended_waiting_future_json"),
        state("retry_count"),
        state("last_start_at"),
        state("next_retry_at"),
        state("last_attempt_deployment_id"),
        state("last_attempt_server"),
        state("last_failure"),
        state("last_failure_error_code"),
        state("last_failure_related_entry_index"),
        state("last_failure_related_entry_name"),
        state("last_failure_related_entry_type"),
        state("last_failure_related_command_index"),
        state("last_failure_related_command_name"),
        state("last_failure_related_command_type"),
        state("last_awaiting_on_future_json"),
        ColumnDocs {
            name: "status",
            column_type: "Utf8",
            description: "Either `pending` or `scheduled` or `ready` or `running` or `paused` or \
                          `backing-off` or `suspended` or `completed`."
                .to_owned(),
        },
        status("completion_result"),
        status("completion_failure"),
    ];
    TableDocs {
        name: "sys_invocation",
        description: "All invocations, whatever their status. One row per invocation, with its \
                      target and caller, lifecycle timestamps, and the retry and failure details \
                      of the current attempt."
            .to_owned(),
        columns,
    }
}

/// `sys_vqueues` is a view over the vqueue stage tables; its documentation lives
/// here until the view's definition moves next to it.
pub fn sys_vqueues_table_docs() -> TableDocs {
    TableDocs {
        name: "sys_vqueues",
        description: " Entries of each vqueue, by stage. One row per entry in a vqueue stage (inbox, running, paused, suspended or finished).".to_owned(),
        columns: vec![
            ColumnDocs { name: "partition_key", column_type: "UInt64", description: " Internal column that is used for partitioning. Can be ignored.".to_owned() },
            ColumnDocs { name: "id", column_type: "Utf8", description: " The VQueue Identifier (vq_...).".to_owned() },
            ColumnDocs { name: "stage", column_type: "Utf8", description: " The stage this entry currently belongs to. Choices are 'inbox', 'running', 'paused', 'suspended', and 'finished'.".to_owned() },
            ColumnDocs { name: "status", column_type: "Utf8", description: " The entry processing status. Examples are `new`, `scheduled`, `started`, `backing-off`, `yielded`, `killed`, `cancelled`, `failed`, and `succeeded`. Note that in stages some cases, the status will reflect the old status prior to transitioning into the current stage. We preserve the last known status because it will be useful to know when the entry transitions out of the current stage.".to_owned() },
            ColumnDocs { name: "has_lock", column_type: "Boolean", description: " Whether this entry currently holds a lock.".to_owned() },
            ColumnDocs { name: "run_at", column_type: "TimestampMillisecond", description: " The entry will be eligible to run after this timestamp. Only present for entries that are in the stage=inbox.".to_owned() },
            ColumnDocs { name: "sequence_number", column_type: "UInt64", description: " Sequence number encoded in the queue ordering key.".to_owned() },
            ColumnDocs { name: "entry_id", column_type: "Utf8", description: " Resource identifier of the entry: an invocation ID or a state mutation ID, without its sequence number. Use `canonical_id` to distinguish incarnations of the same resource. Due to quirks in DataFusion, this should remain `LargeUtf8` to match `id` in `sys_invocation_status` for dynamic filter pushdown.".to_owned() },
            ColumnDocs { name: "entry_kind", column_type: "Utf8", description: " Entry kind (`invocation` or `state-mutation`).".to_owned() },
            ColumnDocs { name: "created_at", column_type: "TimestampMillisecond", description: " Creation timestamp of the entry.".to_owned() },
            ColumnDocs { name: "transitioned_at", column_type: "TimestampMillisecond", description: " Timestamp of the latest stage transition.".to_owned() },
            ColumnDocs { name: "num_attempts", column_type: "UInt32", description: " Number of times this entry has been moved to the run queue.".to_owned() },
            ColumnDocs { name: "num_errors", column_type: "UInt32", description: " Number of times this entry has yielded execution due to transient errors.".to_owned() },
            ColumnDocs { name: "num_pauses", column_type: "UInt32", description: " Number of times this entry has been moved to the paused stage.".to_owned() },
            ColumnDocs { name: "num_suspensions", column_type: "UInt32", description: " Number of times this entry has been moved to the suspended stage.".to_owned() },
            ColumnDocs { name: "num_yields", column_type: "UInt32", description: " Number of times this entry has yielded execution.".to_owned() },
            ColumnDocs { name: "first_attempt_at", column_type: "TimestampMillisecond", description: " Timestamp of the first attempt to run this entry.".to_owned() },
            ColumnDocs { name: "latest_attempt_at", column_type: "TimestampMillisecond", description: " Timestamp of the latest attempt to run this entry.".to_owned() },
            ColumnDocs { name: "first_runnable_at", column_type: "TimestampMillisecond", description: " The realistic earliest time at which this entry can run its first attempt.".to_owned() },
            ColumnDocs { name: "deployment", column_type: "Utf8", description: " If set, the entry's pinned deployment identifier. Due to quirks in DataFusion, this should remain `LargeUtf8` to match `id` in `sys_deployment` for dynamic filter pushdown.".to_owned() },
            ColumnDocs { name: "canonical_id", column_type: "Utf8", description: " Canonical identifier of the entry: its resource ID followed by `_` and its sequence number.".to_owned() },
        ],
    }
}

/// `sys_inbox` is the legacy inbox table, still queryable but without a logical
/// schema; it is documented here until it gets one or is removed.
pub fn sys_inbox_table_docs() -> TableDocs {
    let column = |name, column_type, description: &str| ColumnDocs {
        name,
        column_type,
        description: description.to_owned(),
    };
    TableDocs {
        name: "sys_inbox",
        description:
            "Invocations queued for a Virtual Object or Workflow key. Only one invocation \
                      runs at a time per key; the others wait here, ordered by `sequence_number`."
                .to_owned(),
        columns: vec![
            column(
                "partition_key",
                "UInt64",
                "Internal column that is used for partitioning the services invocations. Can be \
                 ignored.",
            ),
            column(
                "service_name",
                "Utf8",
                "The name of the invoked virtual object/workflow.",
            ),
            column(
                "service_key",
                "Utf8",
                "The key of the virtual object/workflow.",
            ),
            column(
                "id",
                "Utf8",
                "[Invocation ID](/services/invocation/managing-invocations#invocation-id).",
            ),
            column("sequence_number", "UInt64", "Sequence number in the inbox."),
            column(
                "created_at",
                "TimestampMillisecond",
                "Timestamp indicating the start of this invocation. DEPRECATED: you should not \
                 use this field anymore, but join with the sys_invocation table",
            ),
        ],
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
            table.name,
            table.description.trim(),
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
        .map(|table| table.name)
        .collect::<Vec<_>>()
        .join(", ");
    let help = format!(
        "Queryable introspection tables:\n  {names}\n\nRun `restate sql describe <table>` for a \
         table's columns."
    );
    let _ = writeln!(out, "pub static SQL_TABLES_HELP: &str = {help:?};");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_table_is_documented_once_with_single_line_docs() {
        let tables = all_table_docs();
        let mut names: Vec<_> = tables.iter().map(|table| table.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), tables.len(), "table names are unique");
        for table in &tables {
            assert!(
                !table.description.trim().is_empty(),
                "{} has a description",
                table.name
            );
            assert!(!table.description.contains('\n'));
            assert!(!table.columns.is_empty(), "{} has columns", table.name);
            let mut columns: Vec<_> = table.columns.iter().map(|column| column.name).collect();
            columns.sort_unstable();
            columns.dedup();
            assert_eq!(
                columns.len(),
                table.columns.len(),
                "{} column names are unique",
                table.name
            );
            for column in &table.columns {
                assert!(
                    !column.description.contains('\n'),
                    "{}.{}",
                    table.name,
                    column.name
                );
                assert!(
                    !column.column_type.contains("Large"),
                    "{}.{}: reference type names",
                    table.name,
                    column.name
                );
            }
        }
        // The CLI copy names every table in its help text.
        let rendered = render_cli_sql_tables();
        let help = rendered
            .lines()
            .find(|line| line.starts_with("pub static SQL_TABLES_HELP"))
            .expect("help line");
        for table in &tables {
            assert!(help.contains(table.name), "{} is in the help", table.name);
        }
    }
}
