// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Interactive SQL terminal and result rendering for the snapshot debugger.

use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use comfy_table::{Cell, Table};
use futures::TryStreamExt;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use restate_cli_util::ui::console::{Styled, StyledTable};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{c_eprintln, c_error, c_println};
use restate_storage_query_datafusion::context::QueryContext;

/// Runs a single query, renders the result as a table, and reports timing.
pub(crate) async fn run_query(ctx: &QueryContext, query: &str) -> Result<()> {
    let start = Instant::now();
    let result = ctx.execute(query).await?;
    let batches: Vec<_> = result.stream.try_collect().await?;

    let mut table = Table::new_styled();
    if let Some(first) = batches.first() {
        let headers: Vec<String> = first
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_uppercase())
            .collect();
        table.set_styled_header(headers);
    }

    let format_options = FormatOptions::default().with_display_error(true);
    for batch in &batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &format_options))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        for row in 0..batch.num_rows() {
            let cells: Vec<Cell> = formatters.iter().map(|f| Cell::new(f.value(row))).collect();
            table.add_row(cells);
        }
    }

    let row_count = table.row_count();
    if row_count > 0 {
        c_println!("{table}");
    }
    c_eprintln!(
        "{} row(s). Query took {:?}",
        row_count,
        Styled(Style::Notice, start.elapsed())
    );
    Ok(())
}

/// Starts the interactive SQL terminal, persisting history under the data directory.
pub(crate) async fn run_repl(ctx: &QueryContext, data_dir: &Path) -> Result<()> {
    let history_path = data_dir.join(".snapshot-debugger-history");
    let mut editor = DefaultEditor::new()?;
    let _ = editor.load_history(&history_path);

    c_println!("Connected. Enter SQL queries terminated by ';'. Type \\q or Ctrl-D to exit.");
    c_println!("Tip: `SELECT table_name FROM information_schema.tables` lists available tables.");

    let mut buffer = String::new();
    loop {
        let prompt = if buffer.is_empty() { "sql> " } else { "  -> " };
        match editor.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if buffer.is_empty() && matches!(trimmed, "\\q" | "quit" | "exit") {
                    break;
                }
                buffer.push_str(&line);
                buffer.push('\n');

                // Execute once we have a complete, ';'-terminated statement.
                if buffer.trim_end().ends_with(';') {
                    let statement = buffer.trim().trim_end_matches(';').trim().to_owned();
                    buffer.clear();
                    if statement.is_empty() {
                        continue;
                    }
                    let _ = editor.add_history_entry(&statement);
                    if let Err(err) = run_query(ctx, &statement).await {
                        c_error!("{err}");
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C clears the in-progress statement rather than exiting.
                buffer.clear();
            }
            Err(ReadlineError::Eof) => break,
            Err(err) => {
                c_error!("{err}");
                break;
            }
        }
    }

    let _ = editor.save_history(&history_path);
    Ok(())
}
