// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod info;
mod metadata;
mod scan;

use std::path::{Path, PathBuf};

use anyhow::Result;
use cling::prelude::*;
use rocksdb::{Options, SliceTransform};

use restate_log_server::rocksdb_logstore::keys::KeyPrefix;
use restate_log_server::rocksdb_logstore::metadata_merge::{
    metadata_full_merge, metadata_partial_merge,
};
use restate_log_server::rocksdb_logstore::{DATA_CF, METADATA_CF};

use crate::util::rocksdb::{DbInfo, OpenMode, open_db_with_cf_options};

/// Log server analysis commands
#[derive(Run, Subcommand, Clone)]
pub enum LogServerCommand {
    /// Scan log records with filtering and pagination
    Scan(scan::Scan),
    /// Query loglet metadata (sequencer, trim point, seal status)
    Metadata(metadata::Metadata),
    /// Display database and column family information
    Info(info::Info),
}

/// Common options for log server commands
#[derive(Args, Clone)]
pub struct LogServerOpts {
    /// Path to log server DB (default: <data-dir>/log-store)
    #[arg(long, short)]
    pub path: Option<PathBuf>,

    /// Open database as a secondary instance.
    ///
    /// This allows analysis while the Restate server is running.
    /// Without this flag, the database is opened in read-only mode
    /// which requires exclusive access (server must be stopped).
    #[arg(long)]
    pub secondary: bool,
}

impl LogServerOpts {
    pub fn open_mode(&self) -> OpenMode {
        if self.secondary {
            OpenMode::Secondary
        } else {
            OpenMode::ReadOnly
        }
    }
}

/// Open the log-server RocksDB database with the correct per-CF options.
///
/// The data CF requires a prefix extractor (9 bytes = 1 byte kind + 8 bytes
/// loglet_id) to match how the log-server originally created the database.
/// The metadata CF requires a merge operator for trim-point updates.
fn open_log_store_db(
    path: impl AsRef<Path>,
    mode: OpenMode,
    max_open_files: Option<i32>,
) -> Result<DbInfo> {
    open_db_with_cf_options(path, mode, max_open_files, |cf_name| {
        let mut opts = Options::default();
        match cf_name {
            DATA_CF => {
                opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(KeyPrefix::size()));
            }
            METADATA_CF => {
                opts.set_merge_operator(
                    "MetadataMerge",
                    metadata_full_merge,
                    metadata_partial_merge,
                );
            }
            _ => {}
        }
        opts
    })
}
