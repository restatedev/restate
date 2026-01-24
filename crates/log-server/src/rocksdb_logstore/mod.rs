// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod builder;
mod error;
mod keys;
mod metadata_merge;
mod record_format;
mod store;
mod writer;

pub use self::builder::RocksDbLogStoreBuilder;
pub use self::store::RocksDbLogStore;
pub(crate) use error::*;

// matches the default directory name
const DB_NAME: &str = "log-server";
const DATA_CF: &str = "data";
const METADATA_CF: &str = "metadata";
