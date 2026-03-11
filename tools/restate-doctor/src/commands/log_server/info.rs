// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Database and column family information display for log-server store.

use std::path::Path;

use anyhow::Result;
use cling::prelude::*;
use comfy_table::{Cell, Color, Table};

use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{c_println, c_title};
use restate_log_server::rocksdb_logstore::{DATA_CF, METADATA_CF};
use restate_serde_util::ByteCount;

use crate::app::GlobalOpts;
use crate::util::rocksdb::{DEFAULT_CF, resolve_log_store_path};

use super::{LogServerOpts, open_log_store_db};

/// Display log-server database and column family information
///
/// Shows database size on disk, LSM tree structure including level sizes,
/// file counts, and column family details.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_info")]
pub struct Info {
    #[clap(flatten)]
    pub opts: LogServerOpts,

    /// Show per-file information for each level
    #[arg(long)]
    pub extra: bool,
}

pub async fn run_info(global_opts: &GlobalOpts, cmd: &Info) -> Result<()> {
    let path = resolve_log_store_path(global_opts.data_dir.as_deref(), cmd.opts.path.as_deref())?;
    let db_info = open_log_store_db(&path, cmd.opts.open_mode(), global_opts.limit_open_files)?;

    let db_size_on_disk = calculate_directory_size(&path)?;

    // The log-server has two CFs: "data" and "metadata"
    let cfs_to_inspect: Vec<&str> = db_info
        .column_families
        .iter()
        .filter_map(|name| {
            let s = name.as_str();
            if s == DEFAULT_CF { None } else { Some(s) }
        })
        .collect();

    let mut grand_total_size: u64 = 0;
    let mut grand_total_files: usize = 0;

    for cf_name in &cfs_to_inspect {
        let cf_handle = match db_info.db.cf_handle(cf_name) {
            Some(h) => h,
            None => continue,
        };

        let cf_meta = db_info.db.get_column_family_metadata_cf(&cf_handle);

        let cf_label = match *cf_name {
            DATA_CF => "Data CF (log records)",
            METADATA_CF => "Metadata CF (sequencer, trim points, seals)",
            other => other,
        };

        c_title!("", cf_label);

        let mut summary = Table::new_styled();
        summary.add_kv_row("Column Family:", *cf_name);
        summary.add_kv_row("Total Size:", ByteCount::from(cf_meta.size()).to_string());
        summary.add_kv_row("Total Files:", cf_meta.file_count());
        summary.add_kv_row("Levels:", cf_meta.level_count());
        c_println!("{summary}");

        grand_total_size += cf_meta.size();
        grand_total_files += cf_meta.file_count();

        // Level breakdown
        if cf_meta.level_count() > 0 {
            c_println!();
            let mut level_table = Table::new_styled();
            level_table.set_styled_header(vec![
                "LEVEL",
                "SIZE",
                "FILES",
                "AVG FILE SIZE",
                "% OF CF",
            ]);

            for level_ref in cf_meta.levels() {
                let percentage = if cf_meta.size() > 0 {
                    (level_ref.size() as f64 / cf_meta.size() as f64) * 100.0
                } else {
                    0.0
                };

                let avg_file_size = if level_ref.file_count() > 0 {
                    ByteCount::from(level_ref.size() / level_ref.file_count() as u64).to_string()
                } else {
                    "-".to_string()
                };

                let level_cell = if level_ref.size() > 0 {
                    Cell::new(format!("L{}", level_ref.level())).fg(Color::Green)
                } else {
                    Cell::new(format!("L{}", level_ref.level())).fg(Color::DarkGrey)
                };

                level_table.add_row(vec![
                    level_cell,
                    Cell::new(ByteCount::from(level_ref.size()).to_string()),
                    Cell::new(level_ref.file_count()),
                    Cell::new(avg_file_size),
                    Cell::new(format!("{:.1}%", percentage)),
                ]);

                // Per-file listing
                if cmd.extra {
                    for file_ref in level_ref.files() {
                        let filename = file_ref.relative_filename();
                        let live_info = db_info.get_live_file_info(&filename);
                        let entries = live_info
                            .map(|i| format_count(i.num_entries))
                            .unwrap_or_else(|| "-".to_string());
                        let deletions = live_info
                            .map(|i| format_count(i.num_deletions))
                            .unwrap_or_else(|| "-".to_string());
                        c_println!(
                            "    {} {} entries={} tombstones={}",
                            filename,
                            ByteCount::from(file_ref.size()),
                            entries,
                            deletions,
                        );
                    }
                }
            }
            c_println!("{level_table}");
        }

        c_println!();
    }

    // Summary
    c_title!("", "Summary");
    let mut summary = Table::new_styled();
    summary.add_kv_row("Path:", path.display().to_string());
    summary.add_kv_row(
        "Directory Size:",
        ByteCount::from(db_size_on_disk).to_string(),
    );
    summary.add_kv_row(
        "SST Data Size:",
        ByteCount::from(grand_total_size).to_string(),
    );
    summary.add_kv_row("Column Families:", cfs_to_inspect.len());
    summary.add_kv_row("Total SST Files:", grand_total_files);
    c_println!("{summary}");

    Ok(())
}

fn calculate_directory_size(path: &Path) -> Result<u64> {
    let mut total_size: u64 = 0;

    if path.is_file() {
        return Ok(path.metadata().map(|m| m.len()).unwrap_or(0));
    }

    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();

        if entry_path.is_file() {
            total_size += entry.metadata().map(|m| m.len()).unwrap_or(0);
        } else if entry_path.is_dir() {
            total_size += calculate_directory_size(&entry_path)?;
        }
    }

    Ok(total_size)
}

fn format_count(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}
