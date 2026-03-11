// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Query loglet metadata from the log-server metadata column family.

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use cling::prelude::*;
use comfy_table::Table;
use rocksdb::BoundColumnFamily;

use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{c_println, c_title};
use restate_log_server::rocksdb_logstore::keys::{
    DataRecordKey, KeyPrefixKind, MARKER_KEY, MetadataKey,
};
use restate_log_server::rocksdb_logstore::{DATA_CF, METADATA_CF};
use restate_types::logs::{LogId, LogletId, LogletOffset, SequenceNumber};
use restate_types::storage::StorageMarker;
use restate_types::{GenerationalNodeId, PlainNodeId};

use crate::app::GlobalOpts;
use crate::util::rocksdb::resolve_log_store_path;

use super::{LogServerOpts, open_log_store_db};

/// Query loglet metadata from the log-server store
///
/// Displays per-loglet metadata including sequencer identity, trim point,
/// seal status, and the locally observed tail offset. Can query a specific
/// loglet, all loglets for a log, or enumerate all known loglets.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_metadata")]
pub struct Metadata {
    #[clap(flatten)]
    pub opts: LogServerOpts,

    /// Query a specific loglet ID (e.g., "1_0" for log_id=1, segment=0, or raw u64)
    #[arg(long, group = "filter")]
    pub loglet_id: Option<LogletId>,

    /// Query all loglets for a given log ID
    #[arg(long, group = "filter")]
    pub log_id: Option<u32>,

    /// Also show the storage marker (node identity)
    #[arg(long)]
    pub marker: bool,
}

pub async fn run_metadata(global_opts: &GlobalOpts, cmd: &Metadata) -> Result<()> {
    let path = resolve_log_store_path(global_opts.data_dir.as_deref(), cmd.opts.path.as_deref())?;
    let db_info = open_log_store_db(&path, cmd.opts.open_mode(), global_opts.limit_open_files)?;

    let metadata_cf = db_info
        .db
        .cf_handle(METADATA_CF)
        .context("Metadata column family not found")?;
    let data_cf = db_info
        .db
        .cf_handle(DATA_CF)
        .context("Data column family not found")?;

    // Show storage marker if requested
    if cmd.marker {
        c_title!("", "Storage Marker");
        match db_info.db.get_pinned_cf(&metadata_cf, MARKER_KEY)? {
            Some(value) => {
                let marker: StorageMarker<PlainNodeId> =
                    StorageMarker::from_slice(&value).context("Failed to decode storage marker")?;
                let mut table = Table::new_styled();
                table.add_kv_row("Node ID:", marker.id().to_string());
                table.add_kv_row("Created At:", marker.created_at().to_string());
                c_println!("{table}");
            }
            None => {
                c_println!("  No storage marker found (store not yet initialized).");
            }
        }
        c_println!();
    }

    // Discover loglet IDs from metadata CF
    let all_loglet_ids = discover_loglet_ids_from_metadata(&db_info.db, &metadata_cf)?;

    let loglet_ids: Vec<LogletId> = match (cmd.loglet_id, cmd.log_id) {
        (Some(id), _) => vec![id],
        (_, Some(log_id)) => {
            let log_id = LogId::from(log_id);
            all_loglet_ids
                .into_iter()
                .filter(|id| id.log_id() == log_id)
                .collect()
        }
        _ => all_loglet_ids,
    };

    if loglet_ids.is_empty() {
        c_println!("No loglets found matching the specified filters.");
        return Ok(());
    }

    c_title!("", "Loglet Metadata");
    let mut summary = Table::new_styled();
    summary.add_kv_row("Path:", path.display().to_string());
    summary.add_kv_row("Loglets:", loglet_ids.len());
    c_println!("{summary}");
    c_println!();

    let mut table = Table::new_styled();
    table.set_styled_header(vec![
        "LOGLET ID",
        "LOG ID",
        "SEGMENT",
        "SEQUENCER",
        "TRIM POINT",
        "SEALED",
        "LOCAL TAIL",
        "KNOWN GLOBAL TAIL",
    ]);

    for loglet_id in &loglet_ids {
        let sequencer = load_sequencer(&db_info.db, &metadata_cf, *loglet_id)?;
        let trim_point = load_trim_point(&db_info.db, &metadata_cf, *loglet_id)?;
        let is_sealed = load_seal_status(&db_info.db, &metadata_cf, *loglet_id)?;
        let stored_local_tail = load_stored_local_tail(&db_info.db, &metadata_cf, *loglet_id)?;

        // Replicate the fallback logic from load_loglet_state in store.rs:
        // If the LocalTail metadata key is not set, fall back to scanning the
        // data CF for the last record. Then clamp against the trim point.
        let mut local_tail = match stored_local_tail {
            Some(tail) => tail,
            None => find_local_tail(&db_info.db, &data_cf, *loglet_id)?,
        };
        if trim_point >= local_tail {
            local_tail = trim_point.next();
        }

        let mut known_global_tail = load_known_global_tail(&db_info.db, &metadata_cf, *loglet_id)?;
        // The trim point is always globally committed, so use it as a lower bound.
        known_global_tail = known_global_tail.max(trim_point.next());

        table.add_row(vec![
            &loglet_id.to_string(),
            &loglet_id.log_id().to_string(),
            &loglet_id.segment_index().to_string(),
            &sequencer
                .map(|s| s.to_string())
                .unwrap_or_else(|| "-".to_string()),
            &format_offset(trim_point),
            &if is_sealed { "YES" } else { "no" }.to_string(),
            &local_tail.to_string(),
            &known_global_tail.to_string(),
        ]);
    }

    c_println!("{table}");

    Ok(())
}

fn format_offset(offset: LogletOffset) -> String {
    if offset == LogletOffset::INVALID {
        "-".to_string()
    } else {
        offset.to_string()
    }
}

fn load_sequencer(
    db: &rocksdb::DB,
    metadata_cf: &Arc<BoundColumnFamily<'_>>,
    loglet_id: LogletId,
) -> Result<Option<GenerationalNodeId>> {
    let key = MetadataKey::new(KeyPrefixKind::Sequencer, loglet_id).to_binary_array();
    let value = db.get_pinned_cf(metadata_cf, key)?;
    Ok(value.map(|v| GenerationalNodeId::decode(v.as_ref())))
}

fn load_trim_point(
    db: &rocksdb::DB,
    metadata_cf: &Arc<BoundColumnFamily<'_>>,
    loglet_id: LogletId,
) -> Result<LogletOffset> {
    let key = MetadataKey::new(KeyPrefixKind::TrimPoint, loglet_id).to_binary_array();
    let value = db.get_pinned_cf(metadata_cf, key)?;
    Ok(value
        .map(|v| LogletOffset::decode(v.as_ref()))
        .unwrap_or(LogletOffset::INVALID))
}

fn load_seal_status(
    db: &rocksdb::DB,
    metadata_cf: &Arc<BoundColumnFamily<'_>>,
    loglet_id: LogletId,
) -> Result<bool> {
    let key = MetadataKey::new(KeyPrefixKind::Seal, loglet_id).to_binary_array();
    let value = db.get_pinned_cf(metadata_cf, key)?;
    Ok(value.is_some())
}

fn load_stored_local_tail(
    db: &rocksdb::DB,
    metadata_cf: &Arc<BoundColumnFamily<'_>>,
    loglet_id: LogletId,
) -> Result<Option<LogletOffset>> {
    let key = MetadataKey::new(KeyPrefixKind::LocalTail, loglet_id).to_binary_array();
    let value = db.get_pinned_cf(metadata_cf, key)?;
    Ok(value.map(|v| LogletOffset::decode(v.as_ref())))
}

fn load_known_global_tail(
    db: &rocksdb::DB,
    metadata_cf: &Arc<BoundColumnFamily<'_>>,
    loglet_id: LogletId,
) -> Result<LogletOffset> {
    let key = MetadataKey::new(KeyPrefixKind::LastKnownGlobalTail, loglet_id).to_binary_array();
    let value = db.get_pinned_cf(metadata_cf, key)?;
    Ok(value
        .map(|v| LogletOffset::decode(v.as_ref()))
        .unwrap_or(LogletOffset::OLDEST))
}

/// Find the local tail by seeking to the last record for this loglet.
///
/// Uses total-order seek with explicit bounds since the doctor tool opens
/// the DB without the prefix extractor configured at creation time.
fn find_local_tail(
    db: &rocksdb::DB,
    data_cf: &Arc<BoundColumnFamily<'_>>,
    loglet_id: LogletId,
) -> Result<LogletOffset> {
    let oldest_key = DataRecordKey::new(loglet_id, LogletOffset::INVALID);
    let max_key = DataRecordKey::new(loglet_id, LogletOffset::MAX);
    let upper_bound = DataRecordKey::exclusive_upper_bound(loglet_id);

    let mut readopts = rocksdb::ReadOptions::default();
    readopts.fill_cache(false);
    readopts.set_total_order_seek(true);
    readopts.set_iterate_lower_bound(oldest_key.to_binary_array());
    readopts.set_iterate_upper_bound(upper_bound);

    let mut iter = db.raw_iterator_cf_opt(data_cf, readopts);
    iter.seek_for_prev(max_key.to_binary_array());

    if iter.valid()
        && let Some(key_bytes) = iter.key()
    {
        let decoded_key = DataRecordKey::from_slice(key_bytes);
        return Ok(decoded_key.offset().next());
    }

    Ok(LogletOffset::OLDEST)
}

/// Discover all unique loglet IDs from the metadata column family.
///
/// Scans all metadata keys (sequencer, trim_point, seal) and collects unique
/// loglet IDs.
fn discover_loglet_ids_from_metadata(
    db: &rocksdb::DB,
    metadata_cf: &Arc<BoundColumnFamily<'_>>,
) -> Result<Vec<LogletId>> {
    let mut loglet_ids = BTreeSet::new();

    let mut readopts = rocksdb::ReadOptions::default();
    readopts.set_total_order_seek(true);
    readopts.fill_cache(false);

    let mut iter = db.raw_iterator_cf_opt(metadata_cf, readopts);
    iter.seek_to_first();

    while iter.valid() {
        let Some(key_bytes) = iter.key() else {
            break;
        };

        // Skip the storage marker key (variable-length ASCII key)
        if key_bytes == MARKER_KEY {
            iter.next();
            continue;
        }

        // Metadata keys are exactly 9 bytes: [1 byte kind] [8 bytes loglet_id BE]
        if key_bytes.len() == MetadataKey::size() {
            let metadata_key = MetadataKey::from_slice(&mut &key_bytes[..]);
            loglet_ids.insert(metadata_key.loglet_id());
        }

        iter.next();
    }

    if let Err(e) = iter.status() {
        return Err(anyhow::anyhow!(
            "Iterator error during metadata discovery: {e}"
        ));
    }

    Ok(loglet_ids.into_iter().collect())
}
