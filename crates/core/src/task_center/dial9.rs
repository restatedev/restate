// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optional Dial9 instrumentation for Task Center runtimes.

#[cfg(feature = "dial9")]
mod inner {
    use std::time::Duration;

    use dial9::{
        Dial9Handle, Dial9HandleTokioExt, Dial9TokioHandle, DiskBuffer, Recorder,
        TokioAttachOptions,
    };

    #[cfg(target_os = "linux")]
    use dial9::RecorderPerfExt;
    #[cfg(target_os = "linux")]
    use dial9::cpu::{CpuProfilingConfig, SchedEventConfig};

    use restate_types::config::Dial9Options;

    /// Owns the process-wide recorder. It must be dropped after all attached runtimes.
    pub struct Dial9State {
        recorder: Option<Recorder>,
        handle: Dial9Handle,
    }

    impl Dial9State {
        pub fn new(options: &Dial9Options) -> Self {
            let writer = DiskBuffer::builder()
                .base_path(options.trace_dir())
                .max_file_size(options.max_file_size.as_u64())
                .max_total_size(options.max_total_size.as_u64())
                .build();

            let recorder = match writer {
                Ok(writer) => {
                    let builder = dial9::recorder(writer);
                    #[cfg(target_os = "linux")]
                    let builder = builder
                        .with_cpu_profiling(CpuProfilingConfig::default().include_kernel(true))
                        .with_sched_events(SchedEventConfig::default().include_kernel(true));
                    Some(builder.build())
                }
                Err(error) => {
                    tracing::warn!(%error, "Failed to initialize Dial9 telemetry");
                    None
                }
            };
            let handle = recorder
                .as_ref()
                .map_or_else(Dial9Handle::disabled, |recorder| recorder.handle().clone());

            Self { recorder, handle }
        }

        pub fn disabled() -> Self {
            Self {
                recorder: None,
                handle: Dial9Handle::disabled(),
            }
        }

        pub fn handle(&self) -> Dial9Handle {
            self.handle.clone()
        }
    }

    impl Drop for Dial9State {
        fn drop(&mut self) {
            if let Some(recorder) = self.recorder.take() {
                recorder.graceful_shutdown(Duration::from_secs(2));
            }
        }
    }

    pub fn build_runtime(
        handle: &Dial9Handle,
        runtime_name: &str,
        builder: tokio::runtime::Builder,
        fallback_builder: impl FnOnce() -> tokio::runtime::Builder,
    ) -> std::io::Result<tokio::runtime::Runtime> {
        let options = TokioAttachOptions::builder()
            .runtime_name(runtime_name.to_owned())
            .task_tracking_enabled(true)
            .build();

        match handle.attach_tokio_runtime(builder, options) {
            Ok(runtime) => Ok(runtime),
            Err(error) => {
                tracing::warn!(%error, %runtime_name, "Failed to instrument Tokio runtime with Dial9");
                fallback_builder().build()
            }
        }
    }

    pub fn spawn<F, T>(
        task_builder: tokio::task::Builder<'_>,
        future: F,
        runtime: &tokio::runtime::Handle,
    ) -> tokio::task::JoinHandle<T>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        Dial9TokioHandle::current().spawn_with(future, |future| {
            task_builder
                .spawn_on(future, runtime)
                .expect("runtime can spawn tasks")
        })
    }
}

#[cfg(not(feature = "dial9"))]
mod inner {
    use restate_types::config::Dial9Options;

    pub struct Dial9State;

    impl Dial9State {
        pub fn new(_options: &Dial9Options) -> Self {
            Self
        }

        pub fn disabled() -> Self {
            Self
        }

        pub fn handle(&self) -> Dial9Handle {
            Dial9Handle
        }
    }

    #[derive(Clone)]
    pub struct Dial9Handle;

    pub fn build_runtime(
        _handle: &Dial9Handle,
        _runtime_name: &str,
        mut builder: tokio::runtime::Builder,
        _fallback_builder: impl FnOnce() -> tokio::runtime::Builder,
    ) -> std::io::Result<tokio::runtime::Runtime> {
        builder.build()
    }

    pub fn spawn<F, T>(
        task_builder: tokio::task::Builder<'_>,
        future: F,
        runtime: &tokio::runtime::Handle,
    ) -> tokio::task::JoinHandle<T>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        task_builder
            .spawn_on(future, runtime)
            .expect("runtime can spawn tasks")
    }
}

pub(super) use inner::*;
