// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod repository;
mod snapshot_task;

const SNAPSHOT_STAGING_DIR: &str = "pp-snapshots";
const SNAPSHOT_DIR: &str = "db-snapshots";

fn snapshot_staging_dir() -> PathBuf {
    data_dir(SNAPSHOT_STAGING_DIR)
}

fn snapshots_base_dir() -> PathBuf {
    data_dir(SNAPSHOT_DIR)
}

pub use repository::SnapshotRepository;
use restate_core::config::data_dir;
pub use snapshot_task::*;
use std::path::PathBuf;
