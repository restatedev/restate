// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Entry scanning for partition store

use anyhow::{Result, bail};
use cling::prelude::*;
use rocksdb::{IteratorMode, ReadOptions};
use strum::VariantArray;

use restate_partition_store::deduplication_table::DeduplicationKey;
use restate_partition_store::fsm_table::PartitionStateMachineKey;
use restate_partition_store::idempotency_table::IdempotencyKey;
use restate_partition_store::inbox_table::InboxKey;
use restate_partition_store::invocation_status_table::InvocationStatusKey;
use restate_partition_store::journal_events::JournalEventKey;
use restate_partition_store::journal_table::JournalKey as JournalKeyV1;
use restate_partition_store::journal_table_v2::{
    JournalCompletionIdToCommandIndexKey, JournalKey, JournalNotificationIdToNotificationIndexKey,
};
use restate_partition_store::keys::{KeyKind, TableKey};
use restate_partition_store::outbox_table::OutboxKey;
use restate_partition_store::promise_table::PromiseKey;
use restate_partition_store::service_status_table::ServiceStatusKey;
use restate_partition_store::state_table::StateKey;
use restate_partition_store::timer_table::TimersKey;
use restate_partition_store::vqueue_table::{
    ActiveKey, EntryStateKey, InboxKey as VQueueInboxKey, ItemsKey, MetaKey,
};

use comfy_table::Table;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{c_println, c_title};
use restate_serde_util::ByteCount;

use crate::app::GlobalOpts;
use crate::util::colorize::{color_legend, colorize_key_hex};
use crate::util::decode_value::decode_value;
use crate::util::hex_encode;
use crate::util::rocksdb::{open_db, resolve_partition_store_path};

use super::PartitionStoreOpts;

/// Build the help text showing all valid table type values
fn table_type_help() -> String {
    let mut help = String::from(
        "Filter by table type. Accepts either the 2-character prefix or the full name (case-insensitive).\n\n\
         Valid values:\n",
    );
    for kind in KeyKind::VARIANTS {
        let prefix = std::str::from_utf8(kind.as_bytes()).unwrap_or("??");
        help.push_str(&format!("  {prefix:4} {kind}\n"));
    }
    help
}

/// Scan entries in the partition store
///
/// Iterates through the RocksDB partition store and displays entry details
/// including decoded keys and values. Supports filtering by table type and
/// column family, with pagination via --skip and --limit.
///
/// Values under 100KB are automatically decoded and displayed. Larger values
/// show a truncated hex preview. Use the `get` command to retrieve full values.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_scan")]
pub struct Scan {
    #[clap(flatten)]
    pub opts: PartitionStoreOpts,

    /// Filter entries by table type (e.g., "State", "InvocationStatus", or 2-char prefix like "st")
    #[arg(long, short = 't', long_help = table_type_help())]
    pub table: Option<String>,

    /// Filter by column family name (exact match).
    ///
    /// Can be specified multiple times or comma-separated.
    /// Example: --cf=data-0 --cf=data-1 or --cf=data-0,data-1
    #[arg(long, short = 'c', value_delimiter = ',', num_args = 0..)]
    pub cf: Option<Vec<String>>,

    /// Maximum number of entries to display
    #[arg(long, short = 'n', default_value = "10")]
    pub limit: usize,

    /// Number of entries to skip (for pagination)
    #[arg(long, default_value = "0")]
    pub skip: usize,

    /// Show values as hex instead of decoded format
    #[arg(long)]
    pub hex: bool,
}

struct ScanOutput {
    path: String,
    entries: Vec<EntryInfo>,
    displayed: usize,
    has_large_values: bool,
}

struct EntryInfo {
    column_family: String,
    key_kind: String,
    key_size: usize,
    key_decoded: Option<String>,
    value_size: usize,
    /// Decoded value (for values < 100KB)
    value_decoded: Option<String>,
    /// Hex preview (truncated, for values >= 100KB or when --hex is used)
    value_hex_preview: Option<String>,
    /// Raw key bytes for colorized display
    key_bytes: Vec<u8>,
}

impl ScanOutput {
    fn print(&self) {
        // Summary section
        c_title!("🔍", "Partition Store Entries");
        let mut summary = Table::new_styled();
        summary.add_kv_row("Path:", &self.path);
        summary.add_kv_row("Entries:", format!("{} displayed", self.displayed));
        summary.add_kv_row("Key colors:", color_legend());
        c_println!("{summary}");

        if self.entries.is_empty() {
            c_println!("\nNo entries found matching the specified filters.");
            return;
        }

        // Display each entry in a table
        for (i, entry) in self.entries.iter().enumerate() {
            c_println!();
            c_title!("📄", &format!("Entry {}", i + 1));

            let mut table = Table::new_styled();
            table.add_kv_row("Column Family:", &entry.column_family);
            table.add_kv_row("Key Kind:", &entry.key_kind);
            if let Some(decoded) = &entry.key_decoded {
                table.add_kv_row("Key Decoded:", decoded);
            }
            table.add_kv_row("Key Hex:", colorize_key_hex(&entry.key_bytes));
            table.add_kv_row(
                "Size:",
                format!(
                    "key={}, value={}",
                    ByteCount::from(entry.key_size as u64),
                    ByteCount::from(entry.value_size as u64)
                ),
            );

            // Add value to table
            if let Some(decoded) = &entry.value_decoded {
                table.add_kv_row("Value:", decoded);
            } else if let Some(hex_preview) = &entry.value_hex_preview {
                table.add_kv_row("Value:", format!("{hex_preview}... (truncated)"));
            }

            c_println!("{table}");
        }

        // Hint about large/truncated values
        if self.has_large_values {
            c_println!();
            c_println!("Tip: Use `partition-store get <key>` to retrieve full values.");
        }
    }
}

/// Parse a table filter string into a KeyKind.
/// Accepts either the 2-character prefix (case-sensitive, e.g., "st", "iS")
/// or the full name (case-insensitive, e.g., "State", "invocationstatus").
fn parse_table_filter(filter: &str) -> Result<KeyKind> {
    // Try parsing as a 2-character prefix first (case-sensitive!)
    if filter.len() == 2 {
        if let Some(kind) = KeyKind::from_bytes(filter.as_bytes().try_into().unwrap()) {
            return Ok(kind);
        }
        // 2-char input that doesn't match any prefix - don't fall through to name matching
        let valid_prefixes: Vec<_> = KeyKind::VARIANTS
            .iter()
            .map(|k| {
                let prefix = std::str::from_utf8(k.as_bytes()).unwrap_or("??");
                format!("{prefix} ({k})")
            })
            .collect();
        bail!(
            "Unknown table prefix '{}' (prefixes are case-sensitive). Valid prefixes: {}",
            filter,
            valid_prefixes.join(", ")
        );
    }

    // Try matching by name (case-insensitive)
    let filter_lower = filter.to_lowercase();
    for kind in KeyKind::VARIANTS {
        if kind.to_string().to_lowercase() == filter_lower {
            return Ok(*kind);
        }
    }

    // Build error message with valid options
    let valid_prefixes: Vec<_> = KeyKind::VARIANTS
        .iter()
        .map(|k| {
            let prefix = std::str::from_utf8(k.as_bytes()).unwrap_or("??");
            format!("{prefix} ({k})")
        })
        .collect();

    bail!(
        "Unknown table type '{}'. Valid values: {}",
        filter,
        valid_prefixes.join(", ")
    );
}

/// Maximum value size to decode (100KB)
const MAX_DECODE_SIZE: usize = 100 * 1024;
/// Hex preview size for large values
const HEX_PREVIEW_SIZE: usize = 64;

pub async fn run_scan(
    global_opts: &GlobalOpts,
    Scan {
        opts,
        table,
        cf,
        limit,
        skip,
        hex,
    }: &Scan,
) -> Result<()> {
    let path = resolve_partition_store_path(global_opts.data_dir.as_deref(), opts.path.as_deref())?;
    let db_info = open_db(&path, opts.open_mode(), global_opts.limit_open_files)?;

    // Parse table filter if provided
    let table_filter = table.as_ref().map(|t| parse_table_filter(t)).transpose()?;

    let mut entries = Vec::new();
    let mut skipped = 0;
    let mut has_large_values = false;

    // Determine which CFs to scan (exclude "default", apply filter if specified)
    let cfs_to_scan: Vec<&String> = if let Some(cf_filters) = cf {
        db_info
            .data_cfs()
            .filter(|name| cf_filters.iter().any(|f| f == *name))
            .collect()
    } else {
        db_info.data_cfs().collect()
    };

    'outer: for cf_name in cfs_to_scan {
        let cf_handle = match db_info.db.cf_handle(cf_name) {
            Some(h) => h,
            None => continue,
        };

        let read_opts = ReadOptions::default();
        let iter = db_info
            .db
            .iterator_cf_opt(&cf_handle, read_opts, IteratorMode::Start);

        for item in iter {
            let (key, value) = item?;

            // Filter by table type if specified
            if let Some(filter_kind) = table_filter {
                if key.len() < 2 {
                    continue;
                }
                let entry_kind = KeyKind::from_bytes(key[..2].try_into().unwrap());
                if entry_kind != Some(filter_kind) {
                    continue;
                }
            }

            // Skip entries for pagination
            if skipped < *skip {
                skipped += 1;
                continue;
            }

            // Check limit
            if entries.len() >= *limit {
                break 'outer;
            }

            // Decode key using partition-store types
            let (key_kind_name, key_decoded, key_kind_enum) = decode_key(&key);

            let value_size = value.len();

            // Determine how to display the value
            let (value_decoded, value_hex_preview) = if *hex {
                // User requested hex output - show truncated hex preview
                let preview_len = HEX_PREVIEW_SIZE.min(value.len());
                (None, Some(hex_encode(&value[..preview_len])))
            } else if value_size <= MAX_DECODE_SIZE {
                // Small enough to decode - show decoded value
                let decoded =
                    key_kind_enum.map(|kind| decode_value(kind, &key, &value).to_string());
                (decoded, None)
            } else {
                // Too large - show hex preview and mark as having large values
                has_large_values = true;
                let preview_len = HEX_PREVIEW_SIZE.min(value.len());
                (None, Some(hex_encode(&value[..preview_len])))
            };

            entries.push(EntryInfo {
                column_family: cf_name.clone(),
                key_kind: key_kind_name,
                key_size: key.len(),
                key_decoded,
                value_size,
                value_decoded,
                value_hex_preview,
                key_bytes: key.to_vec(),
            });
        }
    }

    let displayed = entries.len();
    let output = ScanOutput {
        path: path.display().to_string(),
        entries,
        displayed,
        has_large_values,
    };

    output.print();
    Ok(())
}

/// Decode a key using the partition-store key types.
/// Returns (key_kind_name, decoded_key_string, key_kind_enum).
fn decode_key(key: &[u8]) -> (String, Option<String>, Option<KeyKind>) {
    if key.len() < 2 {
        return ("Unknown".to_string(), None, None);
    }

    // Parse the KeyKind from the first 2 bytes
    let key_kind = match KeyKind::from_bytes(key[..2].try_into().unwrap()) {
        Some(k) => k,
        None => {
            return (format!("Unknown({:02x}{:02x})", key[0], key[1]), None, None);
        }
    };

    let kind_name = key_kind.to_string();

    // Try to decode the full key using the appropriate key type
    let mut cursor = key;
    let decoded = match key_kind {
        KeyKind::Deduplication => DeduplicationKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::Fsm => PartitionStateMachineKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::Idempotency => IdempotencyKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::Inbox => InboxKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        #[allow(deprecated)]
        KeyKind::InvocationStatusV1 => {
            // Legacy format - try same as current
            InvocationStatusKey::deserialize_from(&mut cursor)
                .ok()
                .map(|k| format!("{k:?}"))
        }
        KeyKind::InvocationStatus => InvocationStatusKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::Journal => JournalKeyV1::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::JournalV2 => JournalKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::JournalV2NotificationIdToNotificationIndex => {
            JournalNotificationIdToNotificationIndexKey::deserialize_from(&mut cursor)
                .ok()
                .map(|k| format!("{k:?}"))
        }
        KeyKind::JournalV2CompletionIdToCommandIndex => {
            JournalCompletionIdToCommandIndexKey::deserialize_from(&mut cursor)
                .ok()
                .map(|k| format!("{k:?}"))
        }
        KeyKind::JournalEvent => JournalEventKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::Outbox => OutboxKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::ServiceStatus => ServiceStatusKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::State => StateKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::Timers => TimersKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::Promise => PromiseKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::VQueueActive => ActiveKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::VQueueInbox => VQueueInboxKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::VQueueMeta => MetaKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::VQueueEntryState => EntryStateKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
        KeyKind::VQueueItems => ItemsKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| format!("{k:?}")),
    };

    (kind_name, decoded, Some(key_kind))
}
