// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod cluster_controller;
mod error;
#[cfg(feature = "metadata-api")]
mod metadata_api;
mod metric_definitions;
mod query_utils;
mod rest_api;
mod schema_registry;
pub mod service;
mod state;
mod storage_accounting;
#[cfg(feature = "storage-query")]
mod storage_query;
#[cfg(feature = "serve-web-ui")]
mod web_ui;

pub use crate::storage_accounting::StorageAccountingTask;

pub use error::Error;
