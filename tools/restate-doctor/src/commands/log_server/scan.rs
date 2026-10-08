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
use bilrost::OwnedMessage;
use bytes::BytesMut;
use cling::prelude::*;
use comfy_table::Table;
use strum::VariantNames;

use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{c_println, c_title};
use restate_log_server::rocksdb_logstore::DATA_CF;
use restate_log_server::rocksdb_logstore::keys::{DataRecordKey, KeyPrefixKind};
use restate_log_server::rocksdb_logstore::record_format::DataRecordDecoder;
use restate_types::logs::{LogId, LogletId, LogletOffset, Record, SequenceNumber};
use restate_types::storage::{StorageCodecKind, StorageDecode, StorageDecodeError};
use restate_util_bytecount::ByteCount;
use restate_wal_protocol::{Envelope, v2};

use crate::app::GlobalOpts;
use crate::util::hex_encode;
use crate::util::rocksdb::resolve_log_store_path;

use super::{LogServerOpts, open_log_store_db};

/// Scan log records in the log-server store
///
/// Iterates through data records for a given loglet or log. By default, shows
/// a summary table with offset, timestamp, keys, command type, and size.
///
/// Use --decode to display v1 WAL envelopes (header + command) as JSON.
/// For v2 envelopes, only the command kind is decoded. Use --hex to show the
/// raw record body as hex bytes.
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

    /// Only show records created at or after this RFC3339 timestamp
    /// (e.g. 2026-06-10T12:00:00Z). Applies while scanning, so combine with
    /// --loglet-id/--log-id to avoid full-store scans.
    #[arg(long)]
    pub from_time: Option<jiff::Timestamp>,

    /// Only show records with these WAL command types (e.g. Invoke,InvokerEffect).
    /// Case-insensitive; can be specified multiple times or comma-separated.
    /// By default all command types are shown.
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    pub command: Vec<String>,

    /// Maximum number of records to display
    #[arg(long, short = 'n', default_value = "20")]
    pub limit: usize,

    /// Number of records to skip (for pagination)
    #[arg(long, default_value = "0")]
    pub skip: usize,

    /// Display WAL envelopes as JSON (v1 only; v2 shows the command kind)
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
    let from_time_nanos: Option<i128> = cmd.from_time.map(|ts| ts.as_nanosecond());

    // Resolve --command values to canonical command names upfront
    let command_filter: Option<Vec<&'static str>> = if cmd.command.is_empty() {
        None
    } else {
        Some(
            cmd.command
                .iter()
                .map(|c| resolve_command_name(c))
                .collect::<Result<Vec<_>>>()?,
        )
    };

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

        // Time filter: skip records created before --from-time. Records whose
        // header fails to decode are kept so the decode error is surfaced.
        if let Some(threshold) = from_time_nanos
            && let Ok(decoder) = DataRecordDecoder::new(value_bytes)
            && let Ok(created_at) = decoder.created_at()
            && (created_at.as_u64() as i128) < threshold
        {
            iter.next();
            continue;
        }

        // Command filter: requires decoding the record body. Records whose
        // envelope fails to decode have command_name "?" and never match.
        let mut decoded = None;
        if let Some(filter) = &command_filter {
            let info = decode_record_info(loglet_id, offset, value_bytes, cmd);
            if !filter.contains(&info.command_name.as_str()) {
                iter.next();
                continue;
            }
            decoded = Some(info);
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

        let record_info =
            decoded.unwrap_or_else(|| decode_record_info(loglet_id, offset, value_bytes, cmd));
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
    if let Some(from_time) = cmd.from_time {
        summary.add_kv_row("From time:", from_time.to_string());
    }
    if let Some(filter) = &command_filter {
        summary.add_kv_row("Commands:", filter.join(", "));
    }
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

fn resolve_command_name(command: &str) -> Result<&'static str> {
    v2::CommandKind::VARIANTS
        .iter()
        .find(|v| v.eq_ignore_ascii_case(command))
        .copied()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "unknown command type '{command}', valid command types: {}",
                v2::CommandKind::VARIANTS.join(", ")
            )
        })
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
        } else if rec.decode_error.is_none() {
            table.add_kv_row(
                "Envelope:",
                "Full JSON decoding is not supported for v2 envelopes; use --hex to inspect the body",
            );
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

    let (command_name, envelope_json, envelope_error) = match &body_bytes {
        Some(body) => match decode_envelope(body, cmd.decode) {
            Ok((name, json)) => (name, json, None),
            Err(e) => ("?".to_string(), None, Some(format!("envelope: {e}"))),
        },
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

fn decode_envelope(body: &[u8], decode: bool) -> Result<(String, Option<String>)> {
    let (&codec, body) = body.split_first().context("missing envelope codec")?;
    let codec = StorageCodecKind::try_from(codec)?;
    match codec {
        StorageCodecKind::FlexbuffersSerde | StorageCodecKind::Json => {
            // Decode v1 directly: converting to v2 adds validation and allocations that
            // aren't needed for inspection, and would discard the original header.
            let envelope = Envelope::decode(body, codec)?;
            let json = decode
                .then(|| serde_json::to_string_pretty(&envelope))
                .transpose()?;
            Ok((envelope.command.name().to_string(), json))
        }
        StorageCodecKind::Custom => {
            // Only read the v2 header; leave the potentially large payload untouched.
            let header = v2::Header::decode_length_delimited(body)?;
            Ok((header.kind().to_string(), None))
        }
        codec => Err(StorageDecodeError::UnsupportedCodecKind(codec).into()),
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use restate_storage_api::deduplication_table::{
        DedupInformation, DedupSequenceNumber, EpochSequenceNumber, ProducerId,
    };
    use restate_types::logs::Keys;
    use restate_types::storage::{PolyBytes, StorageCodec};
    use restate_wal_protocol::v1;
    use restate_wal_protocol::vqueues::VQueuesPauseCommand;

    use super::*;

    fn scan_body(body: Bytes, decode: bool) -> RecordInfo {
        let mut cmd = Scan::parse_from(["scan", "--hex"]);
        cmd.decode = decode;
        build_record_info(
            LogletId::from(0u64),
            LogletOffset::new(1),
            Record::from_parts(Default::default(), Keys::None, PolyBytes::Bytes(body)),
            &cmd,
        )
    }

    #[test]
    fn scan_v1_preserves_original_envelope() {
        // Even semantically invalid dedup metadata must remain inspectable: the
        // scanner should not require a successful conversion to a v2 envelope.
        for dedup in [
            None,
            Some(DedupInformation {
                producer_id: ProducerId::Partition(1.into()),
                sequence_number: DedupSequenceNumber::Esn(EpochSequenceNumber::new(1.into())),
            }),
        ] {
            let envelope = Envelope::new(
                v1::Header {
                    source: v1::Source::Ingress {},
                    dest: v1::Destination::Processor {
                        partition_key: 42,
                        dedup,
                    },
                },
                v1::Command::TruncateOutbox(123),
            );
            let mut buf = BytesMut::new();
            StorageCodec::encode(&envelope, &mut buf).unwrap();
            let body = buf.freeze();
            for decode in [false, true] {
                let info = scan_body(body.clone(), decode);
                assert_eq!(info.command_name, "TruncateOutbox");
                assert_eq!(info.body_size, body.len());
                assert!(info.decode_error.is_none());
                assert_eq!(
                    info.envelope_json,
                    decode.then(|| serde_json::to_string_pretty(&envelope).unwrap())
                );
            }

            // The v1 storage decoder also accepts JSON-encoded envelopes.
            let mut json_body = vec![u8::from(StorageCodecKind::Json)];
            json_body.extend(serde_json::to_vec(&envelope).unwrap());
            let info = scan_body(json_body.into(), true);
            assert_eq!(info.command_name, "TruncateOutbox");
            assert!(info.envelope_json.is_some());
            assert!(info.decode_error.is_none());
        }
    }

    #[test]
    fn scan_v2_command_and_filter() {
        let envelope = v2::Envelope::new(v2::Dedup::None, VQueuesPauseCommand { vqueues: vec![] });
        let mut buf = BytesMut::new();
        StorageCodec::encode(&envelope, &mut buf).unwrap();
        let body = buf.freeze();
        for decode in [false, true] {
            let info = scan_body(body.clone(), decode);
            assert_eq!(info.command_name, "VQueuesPause");
            assert_eq!(info.body_size, body.len());
            assert!(info.envelope_json.is_none());
            assert!(info.decode_error.is_none());
            assert_eq!(
                resolve_command_name("vqueuespause").unwrap(),
                info.command_name
            );
        }
        // All legacy filter names must still be accepted.
        for name in v1::Command::VARIANTS {
            assert_eq!(resolve_command_name(name).unwrap(), *name);
        }
        let error = resolve_command_name("not-a-command").unwrap_err();
        assert!(error.to_string().contains("VQueuesPause"));
        assert!(resolve_command_name("purgevqueuemeta").is_err());
    }

    #[test]
    fn scan_invalid_envelopes_reports_errors() {
        for body in [
            &[][..],
            &[255],
            &[u8::from(StorageCodecKind::Protobuf)],
            &[u8::from(StorageCodecKind::LengthPrefixedRawBytes)],
            &[u8::from(StorageCodecKind::Bilrost)],
            &[u8::from(StorageCodecKind::ZstdBilrostDefault)],
            &[u8::from(StorageCodecKind::FlexbuffersSerde)],
            &[u8::from(StorageCodecKind::Json)],
            &[u8::from(StorageCodecKind::Custom)],
            &[u8::from(StorageCodecKind::Custom), 10, 0],
        ] {
            let info = scan_body(Bytes::copy_from_slice(body), true);
            assert_eq!(info.command_name, "?", "{body:?}");
            assert!(info.envelope_json.is_none(), "{body:?}");
            assert!(info.decode_error.is_some(), "{body:?}");
            assert_eq!(info.body_hex.as_deref(), Some(hex_encode(body).as_str()));
        }
    }
}
