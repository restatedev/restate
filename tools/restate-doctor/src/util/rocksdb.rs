// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for opening RocksDB databases in read-only mode for analysis.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rocksdb::{DB, LiveFile, Options};

/// Mode for opening the database
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OpenMode {
    /// Open in read-only mode (requires exclusive access)
    #[default]
    ReadOnly,
    /// Open as secondary instance (can run while primary is active)
    Secondary,
}

/// The default column family name in RocksDB (not used for partition data)
pub const DEFAULT_CF: &str = "default";

/// Extended information from `live_files()` that isn't available in column family metadata
#[derive(Debug, Clone)]
pub struct LiveFileInfo {
    /// Column family this file belongs to (used by sst command)
    #[allow(dead_code)]
    pub column_family_name: String,
    /// Level in the LSM tree (used by sst command)
    #[allow(dead_code)]
    pub level: i32,
    /// Number of live entries in the file
    pub num_entries: u64,
    /// Number of deletion tombstones in the file
    pub num_deletions: u64,
    /// Smallest sequence number in the file (for future use)
    #[allow(dead_code)]
    pub smallest_seqno: u64,
    /// Largest sequence number in the file (for future use)
    #[allow(dead_code)]
    pub largest_seqno: u64,
}

impl LiveFileInfo {
    /// Calculate tombstone ratio as a percentage (0.0 - 100.0)
    #[allow(dead_code)] // Used by sst command
    pub fn tombstone_ratio(&self) -> f64 {
        let total = self.num_entries + self.num_deletions;
        if total == 0 {
            0.0
        } else {
            (self.num_deletions as f64 / total as f64) * 100.0
        }
    }
}

impl From<&LiveFile> for LiveFileInfo {
    fn from(lf: &LiveFile) -> Self {
        Self {
            column_family_name: lf.column_family_name.clone(),
            level: lf.level,
            num_entries: lf.num_entries,
            num_deletions: lf.num_deletions,
            smallest_seqno: lf.smallest_seqno,
            largest_seqno: lf.largest_seqno,
        }
    }
}

/// Information about an opened RocksDB database
pub struct DbInfo {
    pub db: DB,
    pub column_families: Vec<String>,
    /// Live file info keyed by filename (e.g., "012345.sst")
    live_files: HashMap<String, LiveFileInfo>,
}

impl DbInfo {
    /// Returns an iterator over data column families (excludes "default")
    pub fn data_cfs(&self) -> impl Iterator<Item = &String> {
        self.column_families
            .iter()
            .filter(|name| name.as_str() != DEFAULT_CF)
    }

    /// Get live file info by filename (e.g., "012345.sst")
    pub fn get_live_file_info(&self, filename: &str) -> Option<&LiveFileInfo> {
        self.live_files.get(filename)
    }

    /// Find a file by its number (e.g., 12345), returns (filename, info) if found
    #[allow(dead_code)] // Used by sst command
    pub fn find_file_by_number(&self, file_num: u64) -> Option<(&str, &LiveFileInfo)> {
        // Search all filenames for a matching file number
        self.live_files.iter().find_map(|(name, info)| {
            extract_file_number(name)
                .filter(|&num| num == file_num)
                .map(|_| (name.as_str(), info))
        })
    }
}

/// Extract the file number from an SST filename (e.g., "012345.sst" -> 12345)
pub fn extract_file_number(filename: &str) -> Option<u64> {
    filename
        .strip_suffix(".sst")
        .and_then(|s| s.parse::<u64>().ok())
}

/// Open a RocksDB database for read-only analysis
///
/// If `max_open_files` is `Some`, limits the number of open file handles RocksDB will use.
/// This is useful in environments where the file descriptor limit cannot be increased.
pub fn open_db(
    path: impl AsRef<Path>,
    mode: OpenMode,
    max_open_files: Option<i32>,
) -> Result<DbInfo> {
    let path = path.as_ref().to_path_buf();

    if !path.exists() {
        bail!("Database path does not exist: {}", path.display());
    }

    // Discover column families
    let cf_names = DB::list_cf(&Options::default(), &path)
        .context("Failed to list column families. Is this a valid RocksDB database?")?;

    tracing::info!(
        "Opening database at {} with {} column families",
        path.display(),
        cf_names.len()
    );

    let mut opts = Options::default();
    if let Some(limit) = max_open_files {
        opts.set_max_open_files(limit);
    }

    let db = match mode {
        OpenMode::ReadOnly => DB::open_cf_for_read_only(&opts, &path, &cf_names, false).context(
            "Failed to open database in read-only mode. \
             This requires exclusive access - is the Restate server running? \
             Try using --secondary to analyze while the server is active.",
        )?,
        OpenMode::Secondary => {
            // Secondary mode requires a secondary path for WAL replay
            let secondary_path = std::env::temp_dir()
                .join(format!("restate-doctor-secondary-{}", std::process::id()));
            std::fs::create_dir_all(&secondary_path)?;

            DB::open_cf_as_secondary(&opts, &path, &secondary_path, &cf_names)
                .context("Failed to open database as secondary instance.")?
        }
    };

    // Collect live file info for tombstone analysis and file lookup
    // Note: LiveFile.name includes a leading '/' (e.g., "/012345.sst"), but
    // column family metadata returns relative filenames without the slash.
    // We normalize by stripping the leading slash.
    let live_files = db
        .live_files()
        .map(|files| {
            files
                .iter()
                .map(|lf| {
                    let normalized_name = lf.name.strip_prefix('/').unwrap_or(&lf.name);
                    (normalized_name.to_string(), LiveFileInfo::from(lf))
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(DbInfo {
        db,
        column_families: cf_names,
        live_files,
    })
}

/// Resolve the partition store path from data directory or explicit path
pub fn resolve_partition_store_path(
    data_dir: Option<&Path>,
    explicit_path: Option<&Path>,
) -> Result<PathBuf> {
    if let Some(path) = explicit_path {
        return Ok(path.to_path_buf());
    }

    if let Some(data_dir) = data_dir {
        let db_path = data_dir.join("db");
        if db_path.exists() {
            return Ok(db_path);
        }
        bail!(
            "Could not find partition store at {}/db. Specify --path explicitly.",
            data_dir.display()
        );
    }

    bail!("Either --data-dir or --path must be specified");
}
