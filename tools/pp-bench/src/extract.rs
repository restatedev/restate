// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Extract a snapshot bundle from a stopped Restate server's data directory.
//!
//! Produces a bundle directory containing:
//! - `partition-store/` — RocksDB checkpoint of the partition store (hardlinked SSTs)
//! - `commands.bin` — Extracted log records in PPBENCH format with real LSNs
//! - `metadata.json` — Bundle metadata (applied_lsn, partition_id, counts, etc.)

use std::path::Path;

use anyhow::{Context, bail};
use bytes::BytesMut;
use rocksdb::{ColumnFamilyDescriptor, DB, Options, SliceTransform};

use restate_cli_util::{c_println, c_success, c_warn};
use restate_log_server::rocksdb_logstore::keys::{DataRecordKey, KeyPrefix, KeyPrefixKind};
use restate_log_server::rocksdb_logstore::metadata_merge::{
    metadata_full_merge, metadata_partial_merge,
};
use restate_log_server::rocksdb_logstore::record_format::DataRecordDecoder;
use restate_log_server::rocksdb_logstore::{DATA_CF, METADATA_CF};
use restate_serde_util::ByteCount;
use restate_types::identifiers::PartitionId;
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::{LogId, LogletId, LogletOffset, Lsn, SequenceNumber};
use restate_types::storage::StorageCodec;
use restate_wal_protocol::Envelope;

use crate::ExtractOpts;
use crate::command_gen::ExtractedFileWriter;

/// Extract a snapshot bundle from a stopped Restate server's data directory.
/// This is an offline command — no TaskCenter needed.
pub fn extract_snapshot_bundle(opts: &ExtractOpts) -> anyhow::Result<()> {
    let data_dir = &opts.data_dir;
    let partition_store_path = data_dir.join("db");
    let log_store_path = data_dir.join("log-store");

    anyhow::ensure!(
        partition_store_path.exists(),
        "Partition store not found at {}",
        partition_store_path.display()
    );
    anyhow::ensure!(
        log_store_path.exists(),
        "Log store not found at {}",
        log_store_path.display()
    );

    // Create output directory
    std::fs::create_dir_all(&opts.output)?;

    let checkpoint_path = opts.output.join("partition-store");
    let commands_path = opts.output.join("commands.bin");
    let metadata_path = opts.output.join("metadata.json");

    // -----------------------------------------------------------------------
    // Step 1: Checkpoint the partition store
    // -----------------------------------------------------------------------
    c_println!("Checkpointing partition store...");

    let applied_lsn = checkpoint_partition_store(&partition_store_path, &checkpoint_path)?;
    c_success!(
        "Partition store checkpoint created (applied_lsn={})",
        u64::from(applied_lsn)
    );

    // -----------------------------------------------------------------------
    // Step 2: Determine LSN range for extraction
    // -----------------------------------------------------------------------
    let partition_id = PartitionId::from(opts.partition_id);
    let from_lsn = opts
        .from_lsn
        .map(Lsn::from)
        .unwrap_or_else(|| applied_lsn.next());

    c_println!(
        "Extracting log records from LSN {} (partition_id={})",
        u64::from(from_lsn),
        opts.partition_id
    );

    // -----------------------------------------------------------------------
    // Step 3: Extract log records
    // -----------------------------------------------------------------------
    let stats = extract_log_records(
        &log_store_path,
        &commands_path,
        partition_id,
        from_lsn,
        opts.to_lsn.map(Lsn::from),
        opts.limit,
    )?;

    c_success!(
        "Extracted {} commands (LSN {} .. {})",
        stats.num_commands,
        stats.first_lsn.map(u64::from).unwrap_or(0),
        stats.last_lsn.map(u64::from).unwrap_or(0),
    );

    // -----------------------------------------------------------------------
    // Step 4: Write metadata.json
    // -----------------------------------------------------------------------
    let metadata = serde_json::json!({
        "applied_lsn": u64::from(applied_lsn),
        "partition_id": u32::from(opts.partition_id),
        "start_lsn": stats.first_lsn.map(u64::from).unwrap_or(0),
        "end_lsn": stats.last_lsn.map(u64::from).unwrap_or(0),
        "num_commands": stats.num_commands,
        "source_data_dir": data_dir.display().to_string(),
    });
    std::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?)?;

    // -----------------------------------------------------------------------
    // Summary
    // -----------------------------------------------------------------------
    c_println!();
    c_println!("Snapshot bundle created at: {}", opts.output.display());
    c_println!(
        "  partition-store/  {}",
        ByteCount::<true>::new(dir_size(&checkpoint_path))
    );
    c_println!(
        "  commands.bin     {}",
        ByteCount::<true>::new(file_size(&commands_path))
    );
    c_println!(
        "  metadata.json    {}",
        ByteCount::<true>::new(file_size(&metadata_path))
    );

    if stats.num_commands == 0 {
        c_warn!("No commands extracted. The log may be empty or fully trimmed past applied_lsn.");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Partition store checkpoint + applied_lsn extraction
// ---------------------------------------------------------------------------

fn checkpoint_partition_store(db_path: &Path, checkpoint_path: &Path) -> anyhow::Result<Lsn> {
    // Open the partition store DB in read-only mode
    let cf_names = DB::list_cf(&Options::default(), db_path)
        .context("Failed to list partition store column families")?;

    let mut opts = Options::default();
    opts.set_disable_auto_compactions(true);

    // The partition store uses a merge operator on all CFs
    let descriptors: Vec<ColumnFamilyDescriptor> = cf_names
        .iter()
        .map(|name| {
            let mut cf_opts = Options::default();
            cf_opts.set_merge_operator(
                "PartitionMerge",
                restate_partition_store::keys::KeyKind::full_merge,
                restate_partition_store::keys::KeyKind::partial_merge,
            );
            ColumnFamilyDescriptor::new(name, cf_opts)
        })
        .collect();

    let db = DB::open_cf_descriptors_read_only(&opts, db_path, descriptors, false)
        .context("Failed to open partition store. Is the Restate server stopped?")?;

    // Read applied_lsn from the FSM table.
    // Key format: KeyKind::Fsm (2 bytes, big-endian) + PaddedPartitionId (8 bytes) + state_id (8 bytes, LE)
    // applied_lsn state_id = 2
    let applied_lsn = read_applied_lsn_raw(&db, &cf_names)?;

    // Create checkpoint using hardlinks
    let checkpoint =
        rocksdb::checkpoint::Checkpoint::new(&db).context("Failed to create checkpoint object")?;
    checkpoint
        .create_checkpoint(checkpoint_path)
        .context("Failed to create partition store checkpoint")?;

    Ok(applied_lsn)
}

/// Read the applied_lsn directly from the partition store's FSM table using raw key access.
/// This avoids needing the full PartitionStoreManager + TaskCenter machinery.
fn read_applied_lsn_raw(db: &DB, cf_names: &[String]) -> anyhow::Result<Lsn> {
    // Find the data CF (named "data-0" for partition 0, or similar)
    let data_cf_name = cf_names
        .iter()
        .find(|name| name.starts_with("data-"))
        .context("No data column family found in partition store")?;

    let cf = db
        .cf_handle(data_cf_name)
        .context("Data column family handle not found")?;

    // The FSM key for applied_lsn is constructed as:
    // KeyKind::Fsm = 0x0006 (big-endian 2 bytes)
    // PaddedPartitionId = partition_id as u64 big-endian (8 bytes)
    // state_id = 2_u64 (little-endian 8 bytes)
    //
    // We scan for FSM keys with state_id=2 (APPLIED_LSN).
    // For simplicity, try partition_id=0 first (the common case).
    let mut key = Vec::with_capacity(18);
    key.extend_from_slice(&0x0006_u16.to_be_bytes()); // KeyKind::Fsm
    key.extend_from_slice(&0_u64.to_be_bytes()); // PaddedPartitionId (partition 0)
    key.extend_from_slice(&2_u64.to_le_bytes()); // state_id = APPLIED_LSN

    if let Some(value) = db.get_cf(&cf, &key)?
        && value.len() >= 8
    {
        let seq = u64::from_le_bytes(value[..8].try_into().unwrap());
        return Ok(Lsn::from(seq));
    }

    // Fallback: if no applied_lsn is stored, the partition is fresh
    c_warn!("No applied_lsn found in partition store — assuming fresh partition (LSN=0)");
    Ok(Lsn::INVALID)
}

// ---------------------------------------------------------------------------
// Log record extraction
// ---------------------------------------------------------------------------

struct ExtractionStats {
    num_commands: u64,
    first_lsn: Option<Lsn>,
    last_lsn: Option<Lsn>,
}

fn extract_log_records(
    log_store_path: &Path,
    output_path: &Path,
    partition_id: PartitionId,
    from_lsn: Lsn,
    to_lsn: Option<Lsn>,
    limit: Option<u64>,
) -> anyhow::Result<ExtractionStats> {
    // Open log-store DB (same pattern as restate-doctor)
    let db = open_log_store_db(log_store_path)?;
    let data_cf = db
        .cf_handle(DATA_CF)
        .context("Data column family not found in log store")?;

    // Derive loglet_id from partition_id (single-segment assumption)
    let log_id = LogId::default_for_partition(partition_id);
    let loglet_id = LogletId::new(log_id, SegmentIndex::from(0));

    // For single-segment chains, LSN = loglet_offset
    let from_offset = LogletOffset::new(u32::try_from(u64::from(from_lsn)).unwrap_or(u32::MAX));

    // Set up iterator
    let seek_key = DataRecordKey::new(loglet_id, from_offset).to_binary_array();
    let upper_bound = DataRecordKey::exclusive_upper_bound(loglet_id);

    let mut readopts = rocksdb::ReadOptions::default();
    readopts.set_total_order_seek(true);
    readopts.fill_cache(false);
    readopts.set_iterate_upper_bound(upper_bound);

    let mut iter = db.raw_iterator_cf_opt(&data_cf, readopts);
    iter.seek(seek_key);

    let mut writer = ExtractedFileWriter::create(output_path, *partition_id)?;
    let mut scratch = BytesMut::with_capacity(4096);
    let mut stats = ExtractionStats {
        num_commands: 0,
        first_lsn: None,
        last_lsn: None,
    };

    while iter.valid() {
        let Some(key_bytes) = iter.key() else {
            break;
        };
        let Some(value_bytes) = iter.value() else {
            break;
        };

        // Stop once we leave the data-record key space
        if key_bytes.is_empty() || key_bytes[0] != KeyPrefixKind::DataRecord as u8 {
            break;
        }
        if key_bytes.len() != DataRecordKey::size() {
            iter.next();
            continue;
        }

        let decoded_key = DataRecordKey::from_slice(key_bytes);
        if decoded_key.loglet_id() != loglet_id {
            break;
        }

        let offset = decoded_key.offset();
        // For single-segment chains, LSN = offset value
        let lsn = Lsn::from(u64::from(offset));

        // Apply to_lsn filter
        if let Some(to) = to_lsn
            && lsn > to
        {
            break;
        }

        // Apply limit
        if let Some(max) = limit
            && stats.num_commands >= max
        {
            break;
        }

        // Decode the record
        let decoder = match DataRecordDecoder::new(value_bytes) {
            Ok(d) => d,
            Err(e) => {
                c_warn!("Skipping record at offset {offset}: decode error: {e}");
                iter.next();
                continue;
            }
        };
        let record = match decoder.decode() {
            Ok(r) => r,
            Err(e) => {
                c_warn!("Skipping record at offset {offset}: record error: {e}");
                iter.next();
                continue;
            }
        };

        // Decode the Envelope from the record body
        let body_bytes = record
            .body()
            .encode_to_bytes(&mut scratch)
            .map_err(|e| anyhow::anyhow!("Failed to encode record body at {offset}: {e}"))?;

        let mut cursor = std::io::Cursor::new(body_bytes.as_ref());
        let envelope: Envelope = match StorageCodec::decode(&mut cursor) {
            Ok(e) => e,
            Err(e) => {
                c_warn!("Skipping record at offset {offset}: envelope decode error: {e}");
                iter.next();
                continue;
            }
        };

        // Write to output
        writer.write_envelope(lsn, &envelope)?;

        if stats.first_lsn.is_none() {
            stats.first_lsn = Some(lsn);
        }
        stats.last_lsn = Some(lsn);
        stats.num_commands += 1;

        if stats.num_commands.is_multiple_of(100_000) {
            c_println!("  ... {} commands extracted", stats.num_commands);
        }

        iter.next();
    }

    if let Err(e) = iter.status() {
        bail!("Iterator error: {e}");
    }

    writer.finish()?;
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Log store DB opening (adapted from restate-doctor)
// ---------------------------------------------------------------------------

fn open_log_store_db(path: &Path) -> anyhow::Result<DB> {
    let cf_names = DB::list_cf(&Options::default(), path)
        .context("Failed to list log store column families")?;

    let mut opts = Options::default();
    opts.set_disable_auto_compactions(true);

    let descriptors: Vec<ColumnFamilyDescriptor> = cf_names
        .iter()
        .map(|name| {
            let mut cf_opts = Options::default();
            match name.as_str() {
                DATA_CF => {
                    cf_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(
                        KeyPrefix::size(),
                    ));
                }
                METADATA_CF => {
                    cf_opts.set_merge_operator(
                        "MetadataMerge",
                        metadata_full_merge,
                        metadata_partial_merge,
                    );
                }
                _ => {}
            }
            ColumnFamilyDescriptor::new(name, cf_opts)
        })
        .collect();

    DB::open_cf_descriptors_read_only(&opts, path, descriptors, false).context(
        "Failed to open log store. Is the Restate server stopped? \
         The log store requires exclusive access in read-only mode.",
    )
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn dir_size(path: &Path) -> u64 {
    if !path.exists() {
        return 0;
    }
    std::fs::read_dir(path)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
                .sum()
        })
        .unwrap_or(0)
}
