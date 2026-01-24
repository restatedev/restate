// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Detailed SST file inspection for partition store

use std::collections::HashMap;

use anyhow::{Result, bail};
use cling::prelude::*;
use comfy_table::{Color, Table};

use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{c_println, c_title};
use restate_partition_store::keys::KeyKind;
use restate_serde_util::ByteCount;

use crate::app::GlobalOpts;
use crate::util::colorize::{color_legend, colorize_key_hex};
use crate::util::rocksdb::{DbInfo, extract_file_number, open_db, resolve_partition_store_path};

use super::PartitionStoreOpts;

/// Inspect SST files in detail
///
/// Shows detailed information about specific SST files including decoded key
/// ranges with colorized hex display. Use this command after `partition-store info --extra`
/// to drill down into specific files.
///
/// If --cf is not specified, the column family is automatically detected from the
/// file number using live_files() metadata.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_sst")]
pub struct Sst {
    #[clap(flatten)]
    pub opts: PartitionStoreOpts,

    /// Column family name (optional, auto-detected if not specified)
    #[arg(long, short = 'c')]
    pub cf: Option<String>,

    /// SST file(s) to inspect.
    ///
    /// Accepts file numbers, filenames, or paths:
    /// - Numbers: 12345
    /// - Filenames: 012345.sst
    /// - Paths: /path/to/012345.sst
    #[arg(required = true)]
    pub files: Vec<String>,
}

pub async fn run_sst(global_opts: &GlobalOpts, cmd: &Sst) -> Result<()> {
    let path =
        resolve_partition_store_path(global_opts.data_dir.as_deref(), cmd.opts.path.as_deref())?;
    let db_info = open_db(&path, cmd.opts.open_mode(), global_opts.limit_open_files)?;

    // Parse file identifiers into file numbers
    let file_nums: Vec<u64> = cmd
        .files
        .iter()
        .filter_map(|f| parse_file_identifier(f))
        .collect();

    if file_nums.is_empty() {
        bail!(
            "No valid file numbers found. Accepts: numbers (12345), filenames (012345.sst), or paths."
        );
    }

    // If CF is specified, use the old approach (faster for many files in same CF)
    // Otherwise, use live_files() to auto-detect CF per file
    if let Some(cf_name) = &cmd.cf {
        run_with_explicit_cf(&db_info, cf_name, &file_nums)
    } else {
        run_with_auto_cf(&db_info, &file_nums)
    }
}

/// Parse a file identifier into a file number.
///
/// Accepts:
/// - Plain numbers: "12345" -> 12345
/// - Filenames: "012345.sst" -> 12345
/// - Paths: "/path/to/012345.sst" -> 12345
fn parse_file_identifier(input: &str) -> Option<u64> {
    // Try parsing as a plain number first
    if let Ok(num) = input.parse::<u64>() {
        return Some(num);
    }

    // Extract filename from path (if it's a path)
    let filename = std::path::Path::new(input)
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or(input);

    // Try extracting number from .sst filename
    extract_file_number(filename)
}

/// Run with explicit column family - builds file map from CF metadata
fn run_with_explicit_cf(db_info: &DbInfo, cf_name: &str, files: &[u64]) -> Result<()> {
    let cf_handle = db_info
        .db
        .cf_handle(cf_name)
        .ok_or_else(|| anyhow::anyhow!("Column family '{}' not found", cf_name))?;

    let cf_meta = db_info.db.get_column_family_metadata_cf(&cf_handle);

    // Build a map of file number -> file metadata
    let mut file_map: HashMap<u64, FileInfo> = HashMap::new();

    for level_ref in cf_meta.levels() {
        let level = level_ref.level();
        for file_ref in level_ref.files() {
            let filename = file_ref.relative_filename();
            if let Some(file_num) = extract_file_number(&filename) {
                let live_info = db_info.get_live_file_info(&filename);
                file_map.insert(
                    file_num,
                    FileInfo {
                        filename,
                        column_family: cf_name.to_string(),
                        level,
                        size: file_ref.size(),
                        smallest_key: file_ref.smallest_key(),
                        largest_key: file_ref.largest_key(),
                        num_entries: live_info.map(|i| i.num_entries),
                        num_deletions: live_info.map(|i| i.num_deletions),
                    },
                );
            }
        }
    }

    // Process requested files
    let mut found_any = false;
    for &file_num in files {
        if let Some(file) = file_map.get(&file_num) {
            if found_any {
                c_println!();
            }
            print_file_details(file_num, file);
            found_any = true;
        } else {
            c_println!("File {} not found in column family '{}'", file_num, cf_name);
        }
    }

    if !found_any {
        bail!(
            "None of the specified files were found. Use `partition-store info --cf {} --extra` to list available files.",
            cf_name
        );
    }

    Ok(())
}

/// Run with auto-detected column family using live_files()
fn run_with_auto_cf(db_info: &DbInfo, files: &[u64]) -> Result<()> {
    let mut found_any = false;
    let mut not_found: Vec<u64> = Vec::new();

    for &file_num in files {
        if let Some((filename, live_info)) = db_info.find_file_by_number(file_num) {
            if found_any {
                c_println!();
            }

            // Get detailed file info from column family metadata
            let file_info = get_file_info_from_cf(db_info, &live_info.column_family_name, filename)
                .unwrap_or_else(|| {
                    // Fallback to basic info from live_info
                    FileInfo {
                        filename: filename.to_string(),
                        column_family: live_info.column_family_name.clone(),
                        level: live_info.level,
                        size: 0, // Not available from live_info directly
                        smallest_key: None,
                        largest_key: None,
                        num_entries: Some(live_info.num_entries),
                        num_deletions: Some(live_info.num_deletions),
                    }
                });

            print_file_details(file_num, &file_info);
            found_any = true;
        } else {
            not_found.push(file_num);
        }
    }

    for file_num in &not_found {
        c_println!("File {} not found in any column family", file_num);
    }

    if !found_any {
        bail!(
            "None of the specified files were found. Use `partition-store info --extra` to list available files."
        );
    }

    Ok(())
}

/// Get detailed file info from column family metadata
fn get_file_info_from_cf(db_info: &DbInfo, cf_name: &str, filename: &str) -> Option<FileInfo> {
    let cf_handle = db_info.db.cf_handle(cf_name)?;
    let cf_meta = db_info.db.get_column_family_metadata_cf(&cf_handle);

    for level_ref in cf_meta.levels() {
        for file_ref in level_ref.files() {
            if file_ref.relative_filename() == filename {
                let live_info = db_info.get_live_file_info(filename);
                return Some(FileInfo {
                    filename: filename.to_string(),
                    column_family: cf_name.to_string(),
                    level: level_ref.level(),
                    size: file_ref.size(),
                    smallest_key: file_ref.smallest_key(),
                    largest_key: file_ref.largest_key(),
                    num_entries: live_info.map(|i| i.num_entries),
                    num_deletions: live_info.map(|i| i.num_deletions),
                });
            }
        }
    }
    None
}

struct FileInfo {
    filename: String,
    column_family: String,
    level: i32,
    size: u64,
    smallest_key: Option<Vec<u8>>,
    largest_key: Option<Vec<u8>>,
    // From live_files()
    num_entries: Option<u64>,
    num_deletions: Option<u64>,
}

/// Tombstone ratio threshold for warning (percentage)
const TOMBSTONE_WARNING_THRESHOLD: f64 = 20.0;

fn print_file_details(file_num: u64, file: &FileInfo) {
    c_title!("📄", &format!("SST File {}", file_num));

    let mut info = Table::new_styled();
    info.add_kv_row("Filename:", &file.filename);
    info.add_kv_row("Column Family:", &file.column_family);
    info.add_kv_row("Level:", file.level);
    info.add_kv_row("Size:", ByteCount::from(file.size).to_string());

    // Entry and tombstone counts
    if let (Some(entries), Some(deletions)) = (file.num_entries, file.num_deletions) {
        info.add_kv_row("Entries:", format_count(entries));

        let total = entries + deletions;
        let tombstone_ratio = if total > 0 {
            (deletions as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        let tombstone_text = if deletions > 0 {
            format!("{} ({:.1}%)", format_count(deletions), tombstone_ratio)
        } else {
            "0".to_string()
        };

        // Color-code tombstone ratio
        if tombstone_ratio >= TOMBSTONE_WARNING_THRESHOLD {
            info.add_kv_row(
                "Tombstones:",
                comfy_table::Cell::new(tombstone_text).fg(Color::Red),
            );
        } else if tombstone_ratio >= TOMBSTONE_WARNING_THRESHOLD / 2.0 {
            info.add_kv_row(
                "Tombstones:",
                comfy_table::Cell::new(tombstone_text).fg(Color::Yellow),
            );
        } else {
            info.add_kv_row("Tombstones:", tombstone_text);
        }
    }

    c_println!("{info}");

    c_println!();
    c_println!("Key colors: {}", color_legend());

    // Smallest key
    c_println!();
    c_title!("🔑", "Smallest Key");
    if let Some(ref key) = file.smallest_key {
        print_key_details(key);
    } else {
        c_println!("  (no key)");
    }

    // Largest key
    c_println!();
    c_title!("🔑", "Largest Key");
    if let Some(ref key) = file.largest_key {
        print_key_details(key);
    } else {
        c_println!("  (no key)");
    }
}

fn print_key_details(key: &[u8]) {
    let mut info = Table::new_styled();

    // Raw hex (colorized)
    info.add_kv_row("Hex:", colorize_key_hex(key));
    info.add_kv_row("Length:", format!("{} bytes", key.len()));

    // Decode table type
    if key.len() >= 2 {
        let kind = KeyKind::from_bytes(key[..2].try_into().unwrap());
        let kind_str = kind
            .map(|k| k.to_string())
            .unwrap_or_else(|| format!("Unknown({:02x}{:02x})", key[0], key[1]));
        info.add_kv_row("Table:", kind_str);

        // Decode partition key
        if key.len() >= 10 {
            let partition_bytes: [u8; 8] = key[2..10].try_into().unwrap();
            let partition_key = u64::from_be_bytes(partition_bytes);
            info.add_kv_row("Partition:", partition_key);
        }

        // Type-specific decoding
        if let Some(kind) = kind
            && let Some(details) = decode_key_details(kind, key)
        {
            info.add_kv_row("Details:", details);
        }
    }

    c_println!("{info}");
}

/// Decode type-specific key details
fn decode_key_details(kind: KeyKind, key: &[u8]) -> Option<String> {
    match kind {
        KeyKind::Fsm => {
            if key.len() >= 18 {
                let state_id_bytes: [u8; 8] = key[10..18].try_into().unwrap();
                let state_id = u64::from_be_bytes(state_id_bytes);
                return Some(format!("state_id={}", state_id));
            }
        }
        KeyKind::Outbox => {
            if key.len() >= 18 {
                let idx_bytes: [u8; 8] = key[10..18].try_into().unwrap();
                let idx = u64::from_be_bytes(idx_bytes);
                return Some(format!("message_index={}", idx));
            }
        }
        #[allow(deprecated)]
        KeyKind::InvocationStatus | KeyKind::InvocationStatusV1 => {
            if key.len() >= 26 {
                return Some(format!("invocation_uuid={}", format_uuid(&key[10..26])));
            }
        }
        KeyKind::Journal | KeyKind::JournalV2 => {
            if key.len() >= 30 {
                let idx_bytes: [u8; 4] = key[26..30].try_into().unwrap();
                let idx = u32::from_be_bytes(idx_bytes);
                return Some(format!(
                    "invocation_uuid={}, journal_index={}",
                    format_uuid(&key[10..26]),
                    idx
                ));
            } else if key.len() >= 26 {
                return Some(format!("invocation_uuid={}", format_uuid(&key[10..26])));
            }
        }
        KeyKind::Timers => {
            if key.len() >= 18 {
                let ts_bytes: [u8; 8] = key[10..18].try_into().unwrap();
                let ts = u64::from_be_bytes(ts_bytes);
                return Some(format!("timestamp={}", ts));
            }
        }
        KeyKind::State | KeyKind::ServiceStatus | KeyKind::Promise | KeyKind::Idempotency => {
            if key.len() > 10 {
                let mut details = Vec::new();
                let mut pos = 10;

                // Try to decode variable-length fields
                let field_names: &[&str] = match kind {
                    KeyKind::State => &["service_name", "service_key", "state_key"],
                    KeyKind::ServiceStatus => &["service_name", "service_key"],
                    KeyKind::Promise => &["service_name", "service_key", "promise_key"],
                    KeyKind::Idempotency => {
                        &["service_name", "service_key", "handler", "idempotency_key"]
                    }
                    _ => &[],
                };

                for field_name in field_names {
                    if pos >= key.len() {
                        break;
                    }
                    if let Some((value, consumed)) = decode_varint_string(&key[pos..]) {
                        details.push(format!("{}={}", field_name, value));
                        pos += consumed;
                    } else {
                        break;
                    }
                }

                if !details.is_empty() {
                    return Some(details.join(", "));
                }
            }
        }
        KeyKind::Inbox => {
            if key.len() > 10 {
                let mut details = Vec::new();
                let mut pos = 10;

                // service_name, service_key (variable), then sequence_number (8 bytes)
                for field_name in &["service_name", "service_key"] {
                    if pos >= key.len() {
                        break;
                    }
                    if let Some((value, consumed)) = decode_varint_string(&key[pos..]) {
                        details.push(format!("{}={}", field_name, value));
                        pos += consumed;
                    } else {
                        break;
                    }
                }

                if pos + 8 <= key.len() {
                    let seq_bytes: [u8; 8] = key[pos..pos + 8].try_into().unwrap();
                    let seq = u64::from_be_bytes(seq_bytes);
                    details.push(format!("sequence_number={}", seq));
                }

                if !details.is_empty() {
                    return Some(details.join(", "));
                }
            }
        }
        _ => {}
    }
    None
}

/// Format a UUID as hex string
fn format_uuid(uuid_bytes: &[u8]) -> String {
    if uuid_bytes.len() >= 16 {
        format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            uuid_bytes[0],
            uuid_bytes[1],
            uuid_bytes[2],
            uuid_bytes[3],
            uuid_bytes[4],
            uuid_bytes[5],
            uuid_bytes[6],
            uuid_bytes[7],
            uuid_bytes[8],
            uuid_bytes[9],
            uuid_bytes[10],
            uuid_bytes[11],
            uuid_bytes[12],
            uuid_bytes[13],
            uuid_bytes[14],
            uuid_bytes[15]
        )
    } else {
        uuid_bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

/// Decode a varint-prefixed string, returns (string, total_bytes_consumed)
fn decode_varint_string(data: &[u8]) -> Option<(String, usize)> {
    if data.is_empty() {
        return None;
    }

    // Decode varint length
    let mut len: usize = 0;
    let mut shift = 0;
    let mut varint_len = 0;

    for &byte in data.iter().take(10) {
        len |= ((byte & 0x7f) as usize) << shift;
        varint_len += 1;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }

    if varint_len == 0 || varint_len + len > data.len() {
        return None;
    }

    let s = String::from_utf8(data[varint_len..varint_len + len].to_vec()).ok()?;
    Some((s, varint_len + len))
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
