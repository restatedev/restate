// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};

use restate_types::config::CommonOptions;

use super::{OwnedHandle, TaskCenterInner};

static WORKER_ID: AtomicUsize = const { AtomicUsize::new(0) };

#[derive(Debug, thiserror::Error)]
pub enum TaskCenterBuildError {
    #[error(transparent)]
    Tokio(#[from] tokio::io::Error),
}

/// Used to create a new task center. In practice, there should be a single task center for the
/// entire process but we might need to create more than one in integration test scenarios.
#[derive(Default)]
pub struct TaskCenterBuilder {
    default_runtime_handle: Option<tokio::runtime::Handle>,
    default_runtime: Option<tokio::runtime::Runtime>,
    options: Option<CommonOptions>,
    #[cfg(any(test, feature = "test-util"))]
    pause_time: bool,
}

impl TaskCenterBuilder {
    pub fn default_runtime_handle(mut self, handle: tokio::runtime::Handle) -> Self {
        self.default_runtime_handle = Some(handle);
        self.default_runtime = None;
        self
    }

    pub fn options(mut self, options: CommonOptions) -> Self {
        self.options = Some(options);
        self
    }

    pub fn default_runtime(mut self, runtime: tokio::runtime::Runtime) -> Self {
        self.default_runtime_handle = Some(runtime.handle().clone());
        self.default_runtime = Some(runtime);
        self
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn pause_time(mut self, pause_time: bool) -> Self {
        self.pause_time = pause_time;
        self
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn default_for_tests() -> Self {
        Self::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .pause_time(true)
    }

    pub fn build(mut self) -> Result<OwnedHandle, TaskCenterBuildError> {
        let options = self.options.unwrap_or_default();
        if self.default_runtime_handle.is_none() {
            let mut default_runtime_builder = tokio_builder("worker", &options);
            let default_runtime = default_runtime_builder.build()?;
            self.default_runtime_handle = Some(default_runtime.handle().clone());
            self.default_runtime = Some(default_runtime);
        }

        if cfg!(any(test, feature = "test-util")) {
            eprintln!("!!!! Running with test-util enabled !!!!");
        }
        Ok(OwnedHandle::new(TaskCenterInner::new(
            self.default_runtime_handle.unwrap(),
            self.default_runtime,
            #[cfg(any(test, feature = "test-util"))]
            self.pause_time,
        )))
    }
}

fn tokio_builder(prefix: &'static str, common_opts: &CommonOptions) -> tokio::runtime::Builder {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_name_fn(move || {
        let id = WORKER_ID.fetch_add(1, Ordering::Relaxed);
        format!("rs:{prefix}-{id}")
    });

    builder.worker_threads(common_opts.default_thread_pool_size());

    builder
}
