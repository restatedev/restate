// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//

use anyhow::Result;
use arrow::error::ArrowError;
use arrow::util::display::ArrayFormatter;
use arrow::util::display::FormatOptions;
use cling::prelude::*;
use comfy_table::Cell;
use comfy_table::Table;
use serde::Deserialize;
use serde::Serialize;
use std::io;
use std::time::Instant;

use crate::c_eprintln;
use crate::c_println;
use crate::cli_env::CliEnv;
use crate::ui::console::Styled;
use crate::ui::console::StyledTable;
use crate::ui::stylesheet::Style;
use crate::ui::watcher::Watch;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_sql")]
pub struct Sql {
    /// The SQL query to run.
    query: String,

    #[clap(flatten)]
    watch: Watch,

    /// Print result as line delimited json instead of using the tabular format
    #[arg(long, alias = "ldjson")]
    pub jsonl: bool,

    /// Print result as json array instead of using the tabular format
    #[arg(long)]
    pub json: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SqlQueryRequest {
    pub query: String,
}

pub async fn run_sql(State(env): State<CliEnv>, opts: &Sql) -> Result<()> {
    opts.watch.run(|| run_query(&env, opts)).await
}

async fn run_query(env: &CliEnv, sql_opts: &Sql) -> Result<()> {
    let client = crate::clients::DataFusionHttpClient::new(env).await?;
    let start_time = Instant::now();
    let resp = client.run_query(sql_opts.query.clone()).await?;

    let mut table = Table::new_styled(&env.ui_config);
    // add headers.
    let mut headers = vec![];
    for col in resp.schema.fields() {
        headers.push(col.name().clone().to_uppercase());
    }
    table.set_styled_header(headers);

    if sql_opts.json {
        let mut writer = arrow::json::ArrayWriter::new(io::stdout());
        for batch in resp.batches {
            writer.write_batches(&[&batch])?;
        }
        writer.finish()?;
    } else if sql_opts.jsonl {
        let mut writer = arrow::json::LineDelimitedWriter::new(io::stdout());
        for batch in resp.batches {
            writer.write_batches(&[&batch])?;
        }
        writer.finish()?;
    } else {
        let format_options = FormatOptions::default().with_display_error(true);
        for batch in resp.batches {
            let formatters = batch
                .columns()
                .iter()
                .map(|c| ArrayFormatter::try_new(c.as_ref(), &format_options))
                .collect::<Result<Vec<_>, ArrowError>>()?;

            for row in 0..batch.num_rows() {
                let mut cells = Vec::new();
                for formatter in &formatters {
                    cells.push(Cell::new(formatter.value(row)));
                }
                table.add_row(cells);
            }
        }

        // Only print if there are actual results.
        if table.row_count() > 0 {
            c_println!("{}", table);
            c_println!();
        }
    }

    c_eprintln!(
        "{} rows. Query took {:?}",
        table.row_count(),
        Styled(Style::Notice, start_time.elapsed())
    );
    Ok(())
}
