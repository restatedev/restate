// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Database and column family information display for partition store

use std::path::Path;

use anyhow::Result;
use cling::prelude::*;
use comfy_table::{Cell, Color, Table};

use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{c_println, c_title};
use restate_serde_util::ByteCount;

use crate::app::GlobalOpts;
use crate::util::rocksdb::{
    DEFAULT_CF, extract_file_number, open_db, resolve_partition_store_path,
};

use super::PartitionStoreOpts;

/// Display database and column family information
///
/// Shows database size on disk, LSM tree structure including level sizes,
/// file counts, and SST file details. Useful for understanding storage
/// distribution and diagnosing compaction issues.
///
/// Use `partition-store sst` for detailed SST file inspection with decoded keys.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_info")]
pub struct Info {
    #[clap(flatten)]
    pub opts: PartitionStoreOpts,

    /// Filter by column family name (exact match).
    ///
    /// Can be specified multiple times or comma-separated.
    /// Example: --cf=data-0 --cf=data-1 or --cf=data-0,data-1
    #[arg(long, short = 'c', value_delimiter = ',', num_args = 0..)]
    pub cf: Option<Vec<String>>,

    /// Show per-file information for each level
    #[arg(long)]
    pub extra: bool,
}

/// Aggregated statistics for display
struct CfStats {
    name: String,
    total_size: u64,
    file_count: usize,
    level_count: usize,
    levels: Vec<LevelStats>,
}

struct LevelStats {
    level: i32,
    size: u64,
    file_count: usize,
    files: Vec<FileStats>,
}

pub(super) struct FileStats {
    pub filename: String,
    pub size: u64,
    pub smallest_key: Option<Vec<u8>>,
    pub largest_key: Option<Vec<u8>>,
    /// Number of live entries (from live_files())
    pub num_entries: Option<u64>,
    /// Number of deletion tombstones (from live_files())
    pub num_deletions: Option<u64>,
}

pub async fn run_info(global_opts: &GlobalOpts, cmd: &Info) -> Result<()> {
    let path =
        resolve_partition_store_path(global_opts.data_dir.as_deref(), cmd.opts.path.as_deref())?;
    let db_info = open_db(&path, cmd.opts.open_mode(), global_opts.limit_open_files)?;

    // Calculate total database size on disk
    let db_size_on_disk = calculate_directory_size(&path)?;

    // Determine which CFs to inspect
    let cfs_to_inspect: Vec<&String> = if let Some(cf_filters) = &cmd.cf {
        db_info
            .column_families
            .iter()
            .filter(|name| cf_filters.iter().any(|f| f == *name))
            .collect()
    } else {
        // Include all CFs except "default" which is typically empty
        db_info
            .column_families
            .iter()
            .filter(|name| name.as_str() != DEFAULT_CF)
            .collect()
    };

    if cfs_to_inspect.is_empty() {
        c_println!("No column families found matching the specified filters.");
        return Ok(());
    }

    // Collect metadata for all requested column families
    let mut all_stats: Vec<CfStats> = Vec::new();
    let mut grand_total_size: u64 = 0;
    let mut grand_total_files: usize = 0;

    for cf_name in &cfs_to_inspect {
        let cf_handle = match db_info.db.cf_handle(cf_name) {
            Some(h) => h,
            None => continue,
        };

        let cf_meta = db_info.db.get_column_family_metadata_cf(&cf_handle);

        let mut levels = Vec::new();
        for level_ref in cf_meta.levels() {
            let mut files = Vec::new();
            if cmd.extra {
                for file_ref in level_ref.files() {
                    let filename = file_ref.relative_filename();
                    // Look up live file info for tombstone/entry counts
                    let live_info = db_info.get_live_file_info(&filename);
                    files.push(FileStats {
                        filename,
                        size: file_ref.size(),
                        smallest_key: file_ref.smallest_key(),
                        largest_key: file_ref.largest_key(),
                        num_entries: live_info.map(|i| i.num_entries),
                        num_deletions: live_info.map(|i| i.num_deletions),
                    });
                }
            }
            levels.push(LevelStats {
                level: level_ref.level(),
                size: level_ref.size(),
                file_count: level_ref.file_count(),
                files,
            });
        }

        let total_size = cf_meta.size();
        let file_count = cf_meta.file_count();
        grand_total_size += total_size;
        grand_total_files += file_count;

        all_stats.push(CfStats {
            name: cf_meta.name(),
            total_size,
            file_count,
            level_count: cf_meta.level_count(),
            levels,
        });
    }

    // Print column family details
    print_cf_details(&all_stats, cmd.extra);

    // Print summary at the bottom
    c_println!();
    c_title!("📊", "Summary");
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
    summary.add_kv_row("Column Families:", all_stats.len());
    summary.add_kv_row("Total SST Files:", grand_total_files);
    c_println!("{summary}");

    // Print size distribution chart
    if all_stats.len() > 1 {
        c_println!();
        print_size_distribution(&all_stats, grand_total_size);
    }

    Ok(())
}

/// Calculate the total size of a directory recursively
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

fn print_cf_details(stats: &[CfStats], show_extra: bool) {
    for (i, cf) in stats.iter().enumerate() {
        if i > 0 {
            c_println!();
        }
        c_title!("🗂️", &cf.name);

        // Summary table for this CF
        let mut summary = Table::new_styled();
        summary.add_kv_row("Total Size:", ByteCount::from(cf.total_size).to_string());
        summary.add_kv_row("Total Files:", cf.file_count);
        summary.add_kv_row("Levels:", cf.level_count);
        c_println!("{summary}");

        // Level breakdown table
        if !cf.levels.is_empty() {
            c_println!();
            let mut level_table = Table::new_styled();
            level_table.set_styled_header(vec![
                "LEVEL",
                "SIZE",
                "FILES",
                "AVG FILE SIZE",
                "% OF CF",
            ]);

            for level in &cf.levels {
                let percentage = if cf.total_size > 0 {
                    (level.size as f64 / cf.total_size as f64) * 100.0
                } else {
                    0.0
                };

                let avg_file_size = if level.file_count > 0 {
                    ByteCount::from(level.size / level.file_count as u64).to_string()
                } else {
                    "-".to_string()
                };

                // Color-code levels based on whether they have data
                let level_cell = if level.size > 0 {
                    Cell::new(format!("L{}", level.level)).fg(Color::Green)
                } else {
                    Cell::new(format!("L{}", level.level)).fg(Color::DarkGrey)
                };

                level_table.add_row(vec![
                    level_cell,
                    Cell::new(ByteCount::from(level.size).to_string()),
                    Cell::new(level.file_count),
                    Cell::new(avg_file_size),
                    Cell::new(format!("{:.1}%", percentage)),
                ]);
            }
            c_println!("{level_table}");

            // Show compact per-file listing
            if show_extra {
                for level in &cf.levels {
                    if level.files.is_empty() {
                        continue;
                    }

                    c_println!();
                    c_println!("  Level {} Files:", level.level);

                    let mut file_table = Table::new_styled();
                    file_table.set_styled_header(vec![
                        "FILE#",
                        "SIZE",
                        "ENTRIES",
                        "TOMBSTONES",
                        "TYPE",
                        "KEY RANGE",
                    ]);

                    // Sort files by file number
                    let mut sorted_files: Vec<_> = level.files.iter().collect();
                    sorted_files.sort_by_key(|f| extract_file_number(&f.filename));

                    for file in sorted_files {
                        let file_num = extract_file_number(&file.filename)
                            .map(|n| format!("{:06}", n))
                            .unwrap_or_else(|| file.filename.clone());

                        let (table_type, key_range) =
                            format_compact_key_range(&file.smallest_key, &file.largest_key);

                        let (entries_cell, tombstones_cell) =
                            format_entries_tombstones(file.num_entries, file.num_deletions);

                        file_table.add_row(vec![
                            Cell::new(file_num),
                            Cell::new(ByteCount::from(file.size).to_string()),
                            entries_cell,
                            tombstones_cell,
                            Cell::new(table_type),
                            Cell::new(key_range),
                        ]);
                    }

                    // Indent the file table
                    for line in file_table.to_string().lines() {
                        c_println!("    {line}");
                    }
                }

                c_println!();
                c_println!(
                    "  Tip: Use `partition-store sst --cf {} <file#>` for detailed file inspection",
                    cf.name
                );
            }
        }
    }
}

/// Tombstone ratio threshold for warning color (percentage)
const TOMBSTONE_WARNING_THRESHOLD: f64 = 20.0;

/// Format entries and tombstones with color-coded tombstone ratio
fn format_entries_tombstones(num_entries: Option<u64>, num_deletions: Option<u64>) -> (Cell, Cell) {
    let (entries, deletions) = match (num_entries, num_deletions) {
        (Some(e), Some(d)) => (e, d),
        _ => return (Cell::new("-"), Cell::new("-")),
    };

    let entries_cell = Cell::new(format_count(entries));

    let total = entries + deletions;
    let tombstone_ratio = if total > 0 {
        (deletions as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    let tombstone_text = if deletions > 0 {
        format!("{} ({:.0}%)", format_count(deletions), tombstone_ratio)
    } else {
        "0".to_string()
    };

    let tombstones_cell = if tombstone_ratio >= TOMBSTONE_WARNING_THRESHOLD {
        Cell::new(tombstone_text).fg(Color::Red)
    } else if tombstone_ratio >= TOMBSTONE_WARNING_THRESHOLD / 2.0 {
        Cell::new(tombstone_text).fg(Color::Yellow)
    } else {
        Cell::new(tombstone_text)
    };

    (entries_cell, tombstones_cell)
}

/// Format a count with K/M suffix for readability
fn format_count(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

/// Format a compact key range showing table type and truncated hex keys
fn format_compact_key_range(
    smallest: &Option<Vec<u8>>,
    largest: &Option<Vec<u8>>,
) -> (String, String) {
    let (small_type, small_hex) = format_key_compact(smallest.as_deref());
    let (large_type, large_hex) = format_key_compact(largest.as_deref());

    // Determine table type (use smallest, or largest if smallest unavailable)
    let table_type = if !small_type.is_empty() {
        if small_type == large_type {
            small_type
        } else {
            format!("{}..{}", small_type, large_type)
        }
    } else {
        large_type
    };

    let key_range = format!("{} .. {}", small_hex, large_hex);

    (table_type, key_range)
}

/// Format a key compactly: returns (table_type_abbrev, truncated_hex)
fn format_key_compact(key: Option<&[u8]>) -> (String, String) {
    let Some(key) = key else {
        return (String::new(), "?".to_string());
    };

    if key.len() < 2 {
        return ("?".to_string(), format_hex_truncated(key, 8));
    }

    use restate_partition_store::keys::KeyKind;

    let table_type = KeyKind::from_bytes(key[..2].try_into().unwrap())
        .map(key_kind_abbrev)
        .unwrap_or("??");

    // Show partition (bytes 2-5) + trailing key bytes for context
    let hex = if key.len() > 10 {
        // Show: first 2 (kind) + next 4 (partial partition) + ".." + last 4 bytes
        let last_start = key.len().saturating_sub(4);
        format!(
            "{}..{}",
            format_hex(&key[2..6.min(key.len())]),
            format_hex(&key[last_start..])
        )
    } else {
        format_hex_truncated(key, 10)
    };

    (table_type.to_string(), hex)
}

/// Get a short abbreviation for a KeyKind
fn key_kind_abbrev(kind: restate_partition_store::keys::KeyKind) -> &'static str {
    use restate_partition_store::keys::KeyKind;
    match kind {
        KeyKind::Deduplication => "Dedup",
        KeyKind::Fsm => "FSM",
        KeyKind::Idempotency => "Idemp",
        KeyKind::Inbox => "Inbox",
        #[allow(deprecated)]
        KeyKind::InvocationStatusV1 => "InvS1",
        KeyKind::InvocationStatus => "InvSt",
        KeyKind::Journal => "Jrnl",
        KeyKind::JournalV2 => "JrnV2",
        KeyKind::JournalV2NotificationIdToNotificationIndex => "JrNot",
        KeyKind::JournalV2CompletionIdToCommandIndex => "JrCmp",
        KeyKind::JournalEvent => "JrEvt",
        KeyKind::Outbox => "Outbx",
        KeyKind::ServiceStatus => "SvcSt",
        KeyKind::State => "State",
        KeyKind::Timers => "Timer",
        KeyKind::Promise => "Proms",
        KeyKind::VQueueActive => "VQAct",
        KeyKind::VQueueInbox => "VQInb",
        KeyKind::VQueueMeta => "VQMet",
        KeyKind::VQueueEntryState => "VQEnt",
        KeyKind::VQueueItems => "VQItm",
    }
}

fn format_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn format_hex_truncated(bytes: &[u8], max_bytes: usize) -> String {
    if bytes.len() <= max_bytes {
        format_hex(bytes)
    } else {
        format!("{}..", format_hex(&bytes[..max_bytes]))
    }
}

/// Print a visual bar chart showing the size distribution of column families
fn print_size_distribution(stats: &[CfStats], total_size: u64) {
    const BAR_WIDTH: usize = 40;

    // Sort by size descending for the chart
    let mut sorted: Vec<_> = stats.iter().collect();
    sorted.sort_by(|a, b| b.total_size.cmp(&a.total_size));

    // Find the largest CF size for scaling the bars
    let max_size = sorted.first().map(|cf| cf.total_size).unwrap_or(1);

    // Find the longest CF name for alignment
    let max_name_len = sorted.iter().map(|cf| cf.name.len()).max().unwrap_or(0);

    c_println!("Size Distribution:");
    for cf in &sorted {
        // Percentage of total (for display)
        let percentage = if total_size > 0 {
            (cf.total_size as f64 / total_size as f64) * 100.0
        } else {
            0.0
        };

        // Bar length relative to largest CF (for visual comparison)
        let bar_ratio = cf.total_size as f64 / max_size as f64;
        let filled = (bar_ratio * BAR_WIDTH as f64).round() as usize;
        let empty = BAR_WIDTH.saturating_sub(filled);
        let bar = format!("{}{}", "█".repeat(filled), "░".repeat(empty));

        c_println!(
            "  {:width$}  {} {:>5.1}%  {}",
            cf.name,
            bar,
            percentage,
            ByteCount::from(cf.total_size),
            width = max_name_len
        );
    }
}
