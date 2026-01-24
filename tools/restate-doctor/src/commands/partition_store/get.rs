// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Raw value retrieval from partition store

use std::io::{self, Write};

use anyhow::{Context, Result, bail};
use cling::prelude::*;

use restate_partition_store::keys::KeyKind;

use crate::app::GlobalOpts;
use crate::util::decode_value::decode_value;
use crate::util::rocksdb::{open_db, resolve_partition_store_path};

use super::PartitionStoreOpts;

/// Get a single value from the partition store by its key
///
/// Retrieves and decodes the value for a specific key. By default, displays
/// the value in human-readable format. Use --raw to output raw binary bytes
/// (suitable for piping to a file).
///
/// The key must be specified as a hex string (e.g., from scan output).
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_get")]
pub struct Get {
    #[clap(flatten)]
    pub opts: PartitionStoreOpts,

    /// The key to retrieve, as a hex string (e.g., "73740001...")
    pub key: String,

    /// Column family to search in (required if multiple data CFs exist)
    #[arg(long, short = 'c')]
    pub cf: Option<String>,

    /// Output raw binary bytes instead of decoded value
    #[arg(long)]
    pub raw: bool,
}

pub async fn run_get(global_opts: &GlobalOpts, cmd: &Get) -> Result<()> {
    let path =
        resolve_partition_store_path(global_opts.data_dir.as_deref(), cmd.opts.path.as_deref())?;
    let db_info = open_db(&path, cmd.opts.open_mode(), global_opts.limit_open_files)?;

    // Parse the hex key
    let key_bytes = parse_hex_key(&cmd.key)?;

    // Determine which CF to use
    let cf_name = resolve_column_family(&db_info, cmd.cf.as_deref(), &key_bytes)?;

    let cf_handle = db_info
        .db
        .cf_handle(&cf_name)
        .with_context(|| format!("Column family '{}' not found", cf_name))?;

    // Look up the key
    let value = db_info
        .db
        .get_cf(&cf_handle, &key_bytes)?
        .with_context(|| {
            format!(
                "Key not found in column family '{}'. Use 'scan' to find valid keys.",
                cf_name
            )
        })?;

    if cmd.raw {
        // Output raw binary to stdout
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        handle.write_all(&value)?;
        handle.flush()?;
    } else {
        // Decode and print human-readable output (default)
        let decoded = decode_value_for_key(&key_bytes, &value);
        println!("{}", decoded);
    }

    Ok(())
}

/// Parse a hex string into bytes
fn parse_hex_key(hex: &str) -> Result<Vec<u8>> {
    // Remove any whitespace and common prefixes
    let hex = hex.trim().trim_start_matches("0x").trim_start_matches("0X");

    if hex.is_empty() {
        bail!("Key cannot be empty");
    }

    if !hex.len().is_multiple_of(2) {
        bail!(
            "Invalid hex string: odd number of characters. \
             Hex strings must have an even length."
        );
    }

    let bytes: Result<Vec<u8>, _> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect();

    bytes.with_context(|| {
        format!(
            "Invalid hex string '{}'. Use hex output from 'inspect' command.",
            hex
        )
    })
}

/// Resolve which column family to use for the lookup
fn resolve_column_family(
    db_info: &crate::util::rocksdb::DbInfo,
    explicit_cf: Option<&str>,
    key_bytes: &[u8],
) -> Result<String> {
    if let Some(cf) = explicit_cf {
        return Ok(cf.to_string());
    }

    // Collect data column families
    let data_cfs: Vec<_> = db_info.data_cfs().collect();

    match data_cfs.len() {
        0 => bail!("No data column families found in database"),
        1 => Ok(data_cfs[0].clone()),
        _ => {
            // Multiple CFs - try to find the key in any of them
            for cf_name in &data_cfs {
                if let Some(cf_handle) = db_info.db.cf_handle(cf_name)
                    && db_info.db.get_cf(&cf_handle, key_bytes)?.is_some()
                {
                    return Ok((*cf_name).clone());
                }
            }
            // Key not found in any CF, require explicit specification
            bail!(
                "Multiple column families exist ({}). \
                 Specify which one to use with --cf",
                data_cfs
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
}

/// Decode a value based on the key bytes
fn decode_value_for_key(key: &[u8], value: &[u8]) -> String {
    if key.len() < 2 {
        return format!("<{} bytes, unknown key type>", value.len());
    }

    let key_kind = match KeyKind::from_bytes(key[..2].try_into().unwrap()) {
        Some(k) => k,
        None => return format!("<{} bytes, unknown key type>", value.len()),
    };

    decode_value(key_kind, key, value).to_string()
}
