// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod server;

use crate::local::storage::RocksDbStorage;
use std::path::PathBuf;
pub use {server::BuildError, server::LocalMetadataServer, server::migrate_nodes_configuration};

pub mod storage;
#[cfg(test)]
mod tests;

const DATA_DIR: &str = "local-metadata-store";
const DB_NAME: &str = "local-metadata-store";
const KV_PAIRS: &str = "kv_pairs";
const SEALED_KEY: &str = "##restate_internal_sealed##";

/// Data directory of the local metadata server
pub fn data_dir() -> PathBuf {
    RocksDbStorage::data_dir()
}
