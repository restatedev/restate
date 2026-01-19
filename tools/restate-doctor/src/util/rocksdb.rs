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

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rocksdb::{DB, Options};

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

/// Information about an opened RocksDB database
pub struct DbInfo {
    pub db: DB,
    pub column_families: Vec<String>,
}

impl DbInfo {
    /// Returns an iterator over data column families (excludes "default")
    pub fn data_cfs(&self) -> impl Iterator<Item = &String> {
        self.column_families
            .iter()
            .filter(|name| name.as_str() != DEFAULT_CF)
    }
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

    Ok(DbInfo {
        db,
        column_families: cf_names,
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
