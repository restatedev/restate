// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt::Write as _;
use std::path::Path;
use std::{env, fs};

use vergen_gitcl::{Build, Cargo, Emitter, Gitcl};

use restate_storage_query_datafusion::table_docs::all_table_docs;

/// Generates `$OUT_DIR/sql_tables.rs`, an embedded reference of the SQL
/// introspection tables consumed by the `restate sql` command. The reference is
/// built from the same source of truth that produces the online SQL docs.
fn generate_sql_tables_reference() -> Result<(), Box<dyn Error>> {
    let out_dir = env::var("OUT_DIR")?;
    let dest = Path::new(&out_dir).join("sql_tables.rs");

    let tables = all_table_docs();

    let mut out = String::new();
    out.push_str(
        "pub struct SqlColumnDoc { pub name: &'static str, pub ty: &'static str, pub description: &'static str }\n\
         pub struct SqlTableDoc { pub name: &'static str, pub description: &'static str, pub columns: &'static [SqlColumnDoc] }\n\n",
    );

    out.push_str("pub static SQL_TABLES: &[SqlTableDoc] = &[\n");
    for table in &tables {
        writeln!(
            out,
            "    SqlTableDoc {{ name: {:?}, description: {:?}, columns: &[",
            table.name.as_ref(),
            table.description.as_ref().trim(),
        )?;
        for column in &table.columns {
            writeln!(
                out,
                "        SqlColumnDoc {{ name: {:?}, ty: {:?}, description: {:?} }},",
                column.name,
                column.column_type,
                column.description.trim(),
            )?;
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
        "Queryable introspection tables:\n  {names}\n\nRun `restate sql tables` for the full column \
         reference, or `restate sql describe <table>` for a single table."
    );
    writeln!(out, "pub static SQL_TABLES_HELP: &str = {help:?};")?;

    fs::write(&dest, out)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let cargo = Cargo::builder()
        .features(true)
        .opt_level(true)
        .target_triple(true)
        .debug(true)
        .build();
    let git = Gitcl::builder()
        .branch(true)
        .commit_date(true)
        .commit_timestamp(true)
        .sha(true)
        .build();
    Emitter::default()
        .add_instructions(&Build::all_build())?
        .add_instructions(&cargo)?
        .add_instructions(&git)?
        .emit()?;

    generate_sql_tables_reference()?;

    Ok(())
}
