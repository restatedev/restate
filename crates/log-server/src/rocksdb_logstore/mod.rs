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
#[cfg(feature = "expose-internals")]
pub mod keys;
#[cfg(not(feature = "expose-internals"))]
mod keys;
#[cfg(feature = "expose-internals")]
pub mod metadata_merge;
#[cfg(not(feature = "expose-internals"))]
mod metadata_merge;
#[cfg(feature = "expose-internals")]
pub mod record_format;
#[cfg(not(feature = "expose-internals"))]
mod record_format;
mod store;
mod writer;

pub use self::builder::RocksDbLogStoreBuilder;
pub use self::store::RocksDbLogStore;
pub(crate) use error::*;

pub const DATA_CF: &str = "data";
pub const METADATA_CF: &str = "metadata";
