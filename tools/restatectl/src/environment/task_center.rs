// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::path::PathBuf;

use tracing::warn;

use restate_core::{TaskCenter, TaskCenterBuilder};
use restate_types::config::Configuration;
use restate_types::config_loader::ConfigLoaderBuilder;
use restate_types::live::Pinned;

/// Loads configuration, creates a task center, executes the supplied function body in scope of TC, and shuts down.
pub async fn run_in_task_center<F, O>(config_file: Option<&PathBuf>, fn_body: F) -> O::Output
where
    F: FnOnce(Pinned<Configuration>, TaskCenter) -> O,
    O: Future,
{
    let config_path = config_file
        .as_ref()
        .map(|p| std::fs::canonicalize(p).expect("config-file path is valid"));

    let config_loader = ConfigLoaderBuilder::default()
        .load_env(true)
        .path(config_path.clone())
        .build()
        .unwrap();

    let config = match config_loader.load_once() {
        Ok(c) => c,
        Err(e) => {
            // We cannot use tracing here as it's not configured yet
            eprintln!("{:?}", e);
            std::process::exit(1);
        }
    };

    restate_types::config::set_current_config(config);
    if rlimit::increase_nofile_limit(u64::MAX).is_err() {
        warn!("Failed to increase the number of open file descriptors limit.");
    }

    let config = Configuration::pinned();

    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .ingress_runtime_handle(tokio::runtime::Handle::current())
        .options(config.common.clone())
        .build()
        .expect("task_center builds");

    let result = task_center
        .run_in_scope("main", None, fn_body(config, task_center.clone()))
        .await;

    task_center.shutdown_node("finished", 0).await;
    result
}
