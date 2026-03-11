// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Scan log records from the log-server data column family.

use anyhow::{Context, Result};
use bytes::BytesMut;
use cling::prelude::*;
use comfy_table::Table;

use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{c_println, c_title};
use restate_log_server::rocksdb_logstore::DATA_CF;
use restate_log_server::rocksdb_logstore::keys::{DataRecordKey, KeyPrefixKind};
use restate_log_server::rocksdb_logstore::record_format::DataRecordDecoder;
use restate_serde_util::ByteCount;
use restate_types::logs::{LogId, LogletId, LogletOffset, Record, SequenceNumber};
use restate_types::storage::StorageCodec;
use restate_wal_protocol::Envelope;

use crate::app::GlobalOpts;
use crate::util::hex_encode;
use crate::util::rocksdb::resolve_log_store_path;

use super::{LogServerOpts, open_log_store_db};

/// Scan log records in the log-server store
///
/// Iterates through data records for a given loglet or log. By default, shows
/// a summary table with offset, timestamp, keys, command type, and size.
///
/// Use --decode to fully decode and display the WAL envelope (header + command)
/// as JSON. Use --hex to show the raw record body as hex bytes.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_scan")]
pub struct Scan {
    #[clap(flatten)]
    pub opts: LogServerOpts,

    /// Filter by loglet ID (e.g., "1_0" for log_id=1, segment=0, or raw u64)
    #[arg(long, group = "filter")]
    pub loglet_id: Option<LogletId>,

    /// Filter by log ID (shows all segments for this log)
    #[arg(long, group = "filter")]
    pub log_id: Option<u32>,

    /// Start reading from this offset (inclusive, applies per-loglet)
    #[arg(long, default_value = "0")]
    pub from_offset: u32,

    /// Maximum number of records to display
    #[arg(long, short = 'n', default_value = "20")]
    pub limit: usize,

    /// Number of records to skip (for pagination)
    #[arg(long, default_value = "0")]
    pub skip: usize,

    /// Fully decode and display WAL envelopes as JSON
    #[arg(long)]
    pub decode: bool,

    /// Show record body as hex (first N bytes)
    #[arg(long)]
    pub hex: bool,

    /// Maximum body bytes to show in hex mode
    #[arg(long, default_value = "64")]
    pub hex_limit: usize,
}

pub async fn run_scan(global_opts: &GlobalOpts, cmd: &Scan) -> Result<()> {
    let path = resolve_log_store_path(global_opts.data_dir.as_deref(), cmd.opts.path.as_deref())?;
    let db_info = open_log_store_db(&path, cmd.opts.open_mode(), global_opts.limit_open_files)?;

    let data_cf = db_info
        .db
        .cf_handle(DATA_CF)
        .context("Data column family not found")?;

    let loglet_filter: LogletFilter = match (cmd.loglet_id, cmd.log_id) {
        (Some(loglet_id), _) => LogletFilter::Single(loglet_id),
        (_, Some(log_id)) => LogletFilter::ByLogId(LogId::from(log_id)),
        _ => LogletFilter::All,
    };

    let from_offset = LogletOffset::new(cmd.from_offset);
    let mut records = Vec::new();
    let mut skipped = 0;

    // Single total-order scan across the entire data CF. We avoid prefix-based
    // iteration because the doctor tool opens the DB without the prefix extractor
    // that was configured at creation time. With total_order_seek the iterator
    // walks all keys in byte order regardless of prefix boundaries.
    let mut readopts = rocksdb::ReadOptions::default();
    readopts.set_total_order_seek(true);
    readopts.fill_cache(false);

    // Seek start: if filtering to a specific loglet, jump directly to it.
    let seek_key: [u8; DataRecordKey::size()] = match loglet_filter {
        LogletFilter::Single(id) => DataRecordKey::new(id, from_offset).to_binary_array(),
        _ => {
            // Start at the first data record key (prefix byte 'd')
            DataRecordKey::new(LogletId::from(0u64), LogletOffset::INVALID).to_binary_array()
        }
    };

    let mut iter = db_info.db.raw_iterator_cf_opt(&data_cf, readopts);
    iter.seek(seek_key);

    while iter.valid() {
        let Some(key_bytes) = iter.key() else {
            break;
        };
        let Some(value_bytes) = iter.value() else {
            break;
        };

        // Stop once we leave the data-record key space ('d' = 0x64)
        if key_bytes.is_empty() || key_bytes[0] != KeyPrefixKind::DataRecord as u8 {
            break;
        }
        // Safety: all data record keys are exactly DataRecordKey::size() bytes
        if key_bytes.len() != DataRecordKey::size() {
            iter.next();
            continue;
        }

        let decoded_key = DataRecordKey::from_slice(key_bytes);
        let loglet_id = decoded_key.loglet_id();
        let offset = decoded_key.offset();

        // Apply filters
        match loglet_filter {
            LogletFilter::Single(id) => {
                if loglet_id != id {
                    break; // past our target loglet, done
                }
                if offset < from_offset {
                    iter.next();
                    continue;
                }
            }
            LogletFilter::ByLogId(log_id) => {
                if loglet_id.log_id() != log_id {
                    // Skip this loglet entirely -- jump to the next one
                    iter.next();
                    continue;
                }
                if offset < from_offset {
                    iter.next();
                    continue;
                }
            }
            LogletFilter::All => {
                if offset < from_offset {
                    iter.next();
                    continue;
                }
            }
        }

        // Pagination: skip
        if skipped < cmd.skip {
            skipped += 1;
            iter.next();
            continue;
        }

        // Pagination: limit
        if records.len() >= cmd.limit {
            break;
        }

        let record_info = decode_record_info(loglet_id, offset, value_bytes, cmd);
        records.push(record_info);
        iter.next();
    }

    let truncated = records.len() >= cmd.limit;

    if let Err(e) = iter.status() {
        return Err(anyhow::anyhow!("Iterator error: {e}"));
    }

    // Print output
    c_title!("", "Log Server Records");
    let mut summary = Table::new_styled();
    summary.add_kv_row("Path:", path.display().to_string());
    summary.add_kv_row(
        "Filter:",
        match loglet_filter {
            LogletFilter::Single(id) => format!("loglet_id={id}"),
            LogletFilter::ByLogId(id) => format!("log_id={id}"),
            LogletFilter::All => "all loglets".to_string(),
        },
    );
    if truncated {
        summary.add_kv_row(
            "Records:",
            format!(
                "{} displayed (limit reached, use -n/--limit to show more)",
                records.len()
            ),
        );
    } else {
        summary.add_kv_row("Records:", format!("{} displayed", records.len()));
    }
    c_println!("{summary}");

    if records.is_empty() {
        c_println!("\nNo records found matching the specified filters.");
        return Ok(());
    }

    if cmd.decode {
        print_decoded_records(&records);
    } else {
        print_summary_table(&records);
    }

    Ok(())
}

fn print_summary_table(records: &[RecordInfo]) {
    c_println!();
    let mut table = Table::new_styled();
    table.set_styled_header(vec![
        "LOGLET",
        "OFFSET",
        "TIMESTAMP",
        "KEYS",
        "COMMAND",
        "BODY",
    ]);

    let mut total_body: u64 = 0;
    for rec in records {
        total_body += rec.body_size as u64;

        let body_col = if let Some(ref preview) = rec.body_hex {
            preview.clone()
        } else if let Some(ref err) = rec.decode_error {
            format!("ERROR: {err}")
        } else {
            ByteCount::from(rec.body_size as u64).to_string()
        };

        table.add_row(vec![
            &rec.loglet_id.to_string(),
            &rec.offset.to_string(),
            &rec.timestamp,
            &rec.keys,
            &rec.command_name,
            &body_col,
        ]);
    }

    c_println!("{table}");
    c_println!(
        "Total body size: {} ({} records)",
        ByteCount::from(total_body),
        records.len()
    );
}

fn print_decoded_records(records: &[RecordInfo]) {
    for (i, rec) in records.iter().enumerate() {
        c_println!();
        c_title!(
            "",
            &format!(
                "Record {} (loglet={}, offset={})",
                i + 1,
                rec.loglet_id,
                rec.offset
            )
        );

        let mut table = Table::new_styled();
        table.add_kv_row("Loglet ID:", rec.loglet_id.to_string());
        table.add_kv_row("Log ID:", rec.loglet_id.log_id().to_string());
        table.add_kv_row("Segment:", rec.loglet_id.segment_index().to_string());
        table.add_kv_row("Offset:", rec.offset.to_string());
        table.add_kv_row("Timestamp:", &rec.timestamp);
        table.add_kv_row("Keys:", &rec.keys);
        table.add_kv_row("Command:", &rec.command_name);
        table.add_kv_row(
            "Body Size:",
            ByteCount::from(rec.body_size as u64).to_string(),
        );

        if let Some(ref envelope_json) = rec.envelope_json {
            table.add_kv_row("Envelope:", envelope_json);
        }
        if let Some(ref err) = rec.decode_error {
            table.add_kv_row("Decode Error:", err);
        }
        if let Some(ref hex) = rec.body_hex {
            table.add_kv_row("Body Hex:", hex);
        }

        c_println!("{table}");
    }
}

fn decode_record_info(
    loglet_id: LogletId,
    offset: LogletOffset,
    value_bytes: &[u8],
    cmd: &Scan,
) -> RecordInfo {
    match DataRecordDecoder::new(value_bytes) {
        Ok(decoder) => match decoder.decode() {
            Ok(record) => build_record_info(loglet_id, offset, record, cmd),
            Err(e) => RecordInfo {
                loglet_id,
                offset,
                timestamp: "?".to_string(),
                keys: "?".to_string(),
                command_name: "?".to_string(),
                body_size: 0,
                body_hex: None,
                envelope_json: None,
                decode_error: Some(format!("record decode: {e}")),
            },
        },
        Err(e) => RecordInfo {
            loglet_id,
            offset,
            timestamp: "?".to_string(),
            keys: "?".to_string(),
            command_name: "?".to_string(),
            body_size: 0,
            body_hex: None,
            envelope_json: None,
            decode_error: Some(format!("format: {e}")),
        },
    }
}

fn build_record_info(
    loglet_id: LogletId,
    offset: LogletOffset,
    record: Record,
    cmd: &Scan,
) -> RecordInfo {
    let nanos = record.created_at().as_u64();
    let timestamp = jiff::Timestamp::from_nanosecond(nanos as i128)
        .map(|ts| ts.strftime("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|_| format!("{nanos}ns"));
    let keys_display = format!("{:?}", record.keys());

    // Extract body bytes for size calculation and optional hex/decode
    let body_bytes = record.body().encode_to_bytes(&mut BytesMut::new()).ok();
    let body_size = body_bytes.as_ref().map(|b| b.len()).unwrap_or(0);

    // Try to decode the WAL Envelope from the record body
    let (command_name, envelope_json, envelope_error) = match &body_bytes {
        Some(body) => {
            let mut cursor = std::io::Cursor::new(body.as_ref());
            match StorageCodec::decode::<Envelope, _>(&mut cursor) {
                Ok(envelope) => {
                    let name = envelope.command.name().to_string();
                    let json = if cmd.decode {
                        serde_json::to_string_pretty(&envelope).ok()
                    } else {
                        None
                    };
                    (name, json, None)
                }
                Err(e) => ("?".to_string(), None, Some(format!("envelope: {e}"))),
            }
        }
        None => ("?".to_string(), None, Some("no body".to_string())),
    };

    let body_hex = if cmd.hex {
        body_bytes.as_ref().map(|b| {
            let preview_len = cmd.hex_limit.min(b.len());
            let hex = hex_encode(&b[..preview_len]);
            if b.len() > cmd.hex_limit {
                format!("{hex}...")
            } else {
                hex
            }
        })
    } else {
        None
    };

    RecordInfo {
        loglet_id,
        offset,
        timestamp,
        keys: keys_display,
        command_name,
        body_size,
        body_hex,
        envelope_json,
        decode_error: envelope_error,
    }
}

struct RecordInfo {
    loglet_id: LogletId,
    offset: LogletOffset,
    timestamp: String,
    keys: String,
    command_name: String,
    body_size: usize,
    body_hex: Option<String>,
    envelope_json: Option<String>,
    decode_error: Option<String>,
}

#[derive(Clone, Copy)]
enum LogletFilter {
    Single(LogletId),
    ByLogId(LogId),
    All,
}
