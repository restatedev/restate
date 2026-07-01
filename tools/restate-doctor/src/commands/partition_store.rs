// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod get;
pub(crate) mod info;
mod scan;
mod sst;

use std::path::PathBuf;

use cling::prelude::*;

pub use get::Get;
pub use info::Info;
pub use scan::Scan;
pub use sst::Sst;

use crate::util::rocksdb::OpenMode;

/// Partition store analysis commands
#[derive(Run, Subcommand, Clone)]
pub enum PartitionStoreCommand {
    /// Scan entries with filtering and pagination
    Scan(Scan),
    /// Get a single value by its key (raw binary or decoded)
    Get(Get),
    /// Display database and column family information (size, LSM tree structure, levels, files)
    Info(Info),
    /// Inspect SST files in detail (decoded keys, colorized hex)
    Sst(Sst),
}

/// Common options for partition store commands
#[derive(Args, Clone)]
pub struct PartitionStoreOpts {
    /// Path to partition store DB (default: <data-dir>/db)
    #[arg(long, short)]
    pub path: Option<PathBuf>,

    /// DANGER: open the database in RocksDB read-only mode instead of as a
    /// secondary instance.
    ///
    /// Read-only mode requires exclusive access and WILL corrupt the database
    /// if a Restate server currently has it open. Only use it when the server
    /// is stopped. The default (secondary) mode is always safe.
    #[arg(long)]
    pub read_only: bool,
}

impl PartitionStoreOpts {
    pub fn open_mode(&self) -> OpenMode {
        if self.read_only {
            OpenMode::ReadOnly
        } else {
            OpenMode::Secondary
        }
    }
}
