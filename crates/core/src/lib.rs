// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![deny(clippy::perf)]
#![warn(
    clippy::large_futures,
    clippy::large_types_passed_by_value,
    clippy::use_debug,
    clippy::mutex_atomic
)]

mod error;
mod identification;
mod metadata;
pub mod metric_definitions;
pub mod network;
pub mod partitions;
pub mod protobuf;
pub mod task_center;
pub mod worker_api;
pub use error::*;

/// Run tests within task-center
///
///
/// You can configure the underlying runtime(s) as you would do with tokio
/// ```ignore
/// #[restate_core::test(_args_of_tokio_test)]
/// async fn test_name() {
///    TaskCenter::current();
/// }
/// ```
///
/// A generalised example is
/// ```no_run
/// #[restate_core::test(start_paused = true)]
/// async fn test_name() {
///    TaskCenter::current();
/// }
/// ```
///
#[cfg(any(test, feature = "test-util"))]
pub use restate_core_derive::test;

pub use identification::Identification;

pub use metadata::{
    Metadata, MetadataBuilder, MetadataKind, MetadataManager, MetadataWriter, SyncError,
    TargetVersion, spawn_metadata_manager,
};
pub use task_center::{
    AsyncRuntime, MetadataFutureExt, RuntimeError, RuntimeTaskHandle, TaskCenter,
    TaskCenterBuildError, TaskCenterBuilder, TaskCenterFutureExt, TaskContext, TaskHandle, TaskId,
    TaskKind, cancellation_token, cancellation_watcher, is_cancellation_requested, my_node_id,
};

#[cfg(any(test, feature = "test-util"))]
pub mod test_env;

#[cfg(any(test, feature = "test-util"))]
pub use test_env::{TestCoreEnv, TestCoreEnvBuilder};
