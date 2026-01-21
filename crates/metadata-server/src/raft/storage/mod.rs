// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod keys;
mod rocksdb;
mod rocksdb_builder;

pub use rocksdb::{BuildError, Error, RocksDbStorage};

const DATA_DIR: &str = "replicated-metadata-server";
const DB_NAME: &str = "replicated-metadata-server";
const DATA_CF: &str = "data";
const METADATA_CF: &str = "metadata";
