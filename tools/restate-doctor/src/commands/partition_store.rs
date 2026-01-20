// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod get;
mod info;
mod scan;

use std::path::PathBuf;

use cling::prelude::*;

pub use get::Get;
pub use info::Info;
pub use scan::Scan;

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
}

/// Common options for partition store commands
#[derive(Args, Clone)]
pub struct PartitionStoreOpts {
    /// Path to partition store DB (default: <data-dir>/db)
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

impl PartitionStoreOpts {
    pub fn open_mode(&self) -> OpenMode {
        if self.secondary {
            OpenMode::Secondary
        } else {
            OpenMode::ReadOnly
        }
    }
}
