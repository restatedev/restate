// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dial9 tokio runtime telemetry support.
//!
//! When the `dial9` feature is enabled, this module provides wake-tracked
//! spawning, per-runtime trace files, and CPU profiling (on Linux). When the
//! feature is disabled, every type is a zero-sized no-op so callers never need
//! `#[cfg]` annotations.

#[cfg(feature = "dial9")]
mod inner {
    use std::collections::HashMap;

    use parking_lot::Mutex;

    use restate_types::SharedString;
    use restate_types::config::{Configuration, Dial9Options};

    use super::super::TaskCenterBuildError;

    // Per-thread telemetry handle for partition runtimes. Each partition
    // runtime thread sets this when it starts so that `spawn_on_runtime()`
    // can use the correct handle for wake-tracked spawning.
    thread_local! {
        static DIAL9_HANDLE: std::cell::RefCell<Option<dial9_tokio_telemetry::telemetry::TelemetryHandle>> = const { std::cell::RefCell::new(None) };
    }

    /// Holds the default-runtime telemetry guard and per-runtime guards map.
    /// Must be declared before `default_runtime` in [`TaskCenterInner`] so that
    /// the guard is flushed before the runtime is dropped.
    pub struct Dial9State {
        guard: Option<dial9_tokio_telemetry::telemetry::TelemetryGuard>,
        runtime_guards:
            Mutex<HashMap<SharedString, dial9_tokio_telemetry::telemetry::TelemetryGuard>>,
    }

    impl Dial9State {
        /// State with an active default-runtime guard.
        fn new(guard: dial9_tokio_telemetry::telemetry::TelemetryGuard) -> Self {
            Self {
                guard: Some(guard),
                runtime_guards: Mutex::new(HashMap::new()),
            }
        }

        /// State without an active guard (e.g. when the runtime handle was
        /// provided externally or dial9 initialisation failed).
        pub fn empty() -> Self {
            Self {
                guard: None,
                runtime_guards: Mutex::new(HashMap::new()),
            }
        }

        /// Whether dial9 telemetry is active for the default runtime.
        pub fn is_active(&self) -> bool {
            self.guard.is_some()
        }

        /// Extract a cloneable handle from the default-runtime guard.
        pub fn default_handle(&self) -> Dial9Handle {
            Dial9Handle(self.guard.as_ref().map(|g| g.handle()))
        }
    }

    /// A cloneable handle for wake-tracked spawning. Wraps an optional
    /// `TelemetryHandle` — `None` means we fall back to plain tokio spawning.
    #[derive(Clone)]
    pub struct Dial9Handle(Option<dial9_tokio_telemetry::telemetry::TelemetryHandle>);

    impl Dial9Handle {
        /// Return `self` if it has a handle, otherwise `other`.
        pub fn or(self, other: Self) -> Self {
            if self.0.is_some() { self } else { other }
        }
    }

    /// Build the default runtime with dial9 tracing. On failure, logs a warning
    /// and falls back to an uninstrumented runtime.
    pub fn build_default_runtime(
        builder: tokio::runtime::Builder,
        fallback_builder: impl FnOnce() -> tokio::runtime::Builder,
    ) -> Result<(tokio::runtime::Runtime, Dial9State), TaskCenterBuildError> {
        match create_traced_runtime("default", builder) {
            Ok((rt, guard)) => Ok((rt, Dial9State::new(guard))),
            Err(e) => {
                tracing::warn!(
                    "Failed to initialize dial9 telemetry: {e}, \
                     running without instrumentation"
                );
                let rt = fallback_builder().build()?;
                Ok((rt, Dial9State::empty()))
            }
        }
    }

    /// Build a child (partition) runtime with dial9 tracing, storing the guard
    /// in `state` so it stays alive while the runtime runs.
    ///
    /// Guards are dropped with `TaskCenterInner`, not in `drop_runtime()`,
    /// because `TelemetryGuard::drop()` accesses a dial9-internal thread-local
    /// that may already be destroyed on the runtime thread.
    ///
    /// If `state.is_active()` is false, this just builds a plain runtime.
    pub fn build_child_runtime(
        runtime_name: &SharedString,
        mut builder: tokio::runtime::Builder,
        state: &Dial9State,
        fallback_builder: impl FnOnce() -> tokio::runtime::Builder,
    ) -> (tokio::runtime::Runtime, Dial9Handle) {
        if !state.is_active() {
            let rt = builder.build().expect("runtime builder succeeds");
            return (rt, Dial9Handle(None));
        }

        match create_traced_runtime(runtime_name, builder) {
            Ok((rt, guard)) => {
                let handle = Dial9Handle(Some(guard.handle()));
                state
                    .runtime_guards
                    .lock()
                    .insert(runtime_name.clone(), guard);
                (rt, handle)
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to instrument runtime {runtime_name} with dial9: {e}, \
                     falling back to uninstrumented runtime"
                );
                let rt = fallback_builder()
                    .build()
                    .expect("runtime builder succeeds");
                (rt, Dial9Handle(None))
            }
        }
    }

    /// Set the per-thread dial9 handle so that [`spawn_or_fallback`] can use it
    /// for wake-tracked spawning on inherited runtimes.
    pub fn set_thread_local(handle: Dial9Handle) {
        if let Some(h) = handle.0 {
            DIAL9_HANDLE.with_borrow_mut(|slot| *slot = Some(h));
        }
    }

    /// Get the thread-local dial9 handle (set by [`set_thread_local`]).
    pub fn thread_local_handle() -> Dial9Handle {
        Dial9Handle(DIAL9_HANDLE.with_borrow(|h| h.clone()))
    }

    /// Spawn a future with wake-tracked telemetry if a handle is available,
    /// otherwise fall back to plain tokio spawning.
    ///
    /// Always spawns on the explicit `runtime` handle via `spawn_on` so that
    /// tasks targeting `AsyncRuntime::Default` land on the default runtime even
    /// when the caller is on a partition-processor runtime.
    pub fn spawn_or_fallback<F, T>(
        handle: Dial9Handle,
        task_builder: tokio::task::Builder<'_>,
        fut: F,
        runtime: &tokio::runtime::Handle,
    ) -> tokio::task::JoinHandle<T>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        if let Some(dh) = handle.0 {
            // Wrap in dial9 tracing but spawn on the correct target runtime.
            // `TelemetryHandle::spawn()` uses `tokio::spawn()` which would
            // place the task on the *current* runtime instead of `runtime`.
            let traced_handle = dh.traced_handle();
            task_builder
                .spawn_on(
                    async move {
                        let task_id = tokio::task::try_id()
                            .map(dial9_tokio_telemetry::telemetry::TaskId::from)
                            .unwrap_or_default();
                        dial9_tokio_telemetry::traced::Traced::new(fut, traced_handle, task_id)
                            .await
                    },
                    runtime,
                )
                .expect("runtime can spawn tasks")
        } else {
            task_builder
                .spawn_on(fut, runtime)
                .expect("runtime can spawn tasks")
        }
    }

    /// Create a `TracedRuntime` wrapping the given builder, with its own trace
    /// files under `{trace_dir}/{runtime_name}/`.
    fn create_traced_runtime(
        runtime_name: &str,
        builder: tokio::runtime::Builder,
    ) -> std::io::Result<(
        tokio::runtime::Runtime,
        dial9_tokio_telemetry::telemetry::TelemetryGuard,
    )> {
        let opts = Configuration::pinned().common.dial9.clone();
        let trace_path = opts.trace_dir().join(runtime_name).join("trace");
        let writer = create_dial9_writer(&trace_path, &opts)?;
        let traced = dial9_tokio_telemetry::telemetry::TracedRuntime::builder()
            .with_task_tracking(true)
            // The trace path is needed for the background symbolization worker to
            // find sealed segments, resolve addresses to symbols, and write back
            // SymbolTableEntry events into the trace files.
            .with_trace_path(&trace_path);

        // cpu-profiling feature is automatically enabled on Linux via Cargo.toml
        #[cfg(target_os = "linux")]
        let traced = traced
            .with_cpu_profiling(dial9_tokio_telemetry::telemetry::CpuProfilingConfig::default())
            .with_sched_events(dial9_tokio_telemetry::telemetry::SchedEventConfig {
                include_kernel: true,
            });

        traced.build_and_start(builder, writer)
    }

    fn create_dial9_writer(
        trace_path: &std::path::Path,
        opts: &Dial9Options,
    ) -> std::io::Result<dial9_tokio_telemetry::telemetry::RotatingWriter> {
        dial9_tokio_telemetry::telemetry::RotatingWriter::builder()
            .base_path(trace_path)
            .max_file_size(opts.max_file_size.as_u64())
            .max_total_size(opts.max_total_size.as_u64())
            .build()
    }
}

#[cfg(not(feature = "dial9"))]
mod inner {
    use super::super::TaskCenterBuildError;

    /// Zero-sized no-op replacement for [`Dial9State`] when the feature is off.
    pub struct Dial9State;

    impl Dial9State {
        pub fn empty() -> Self {
            Self
        }

        pub fn default_handle(&self) -> Dial9Handle {
            Dial9Handle
        }
    }

    /// Zero-sized no-op replacement for [`Dial9Handle`] when the feature is off.
    #[derive(Clone)]
    pub struct Dial9Handle;

    impl Dial9Handle {
        pub fn or(self, _other: Self) -> Self {
            self
        }
    }

    pub fn build_default_runtime(
        mut builder: tokio::runtime::Builder,
        _fallback_builder: impl FnOnce() -> tokio::runtime::Builder,
    ) -> Result<(tokio::runtime::Runtime, Dial9State), TaskCenterBuildError> {
        Ok((builder.build()?, Dial9State))
    }

    pub fn build_child_runtime(
        _runtime_name: &restate_types::SharedString,
        mut builder: tokio::runtime::Builder,
        _state: &Dial9State,
        _fallback_builder: impl FnOnce() -> tokio::runtime::Builder,
    ) -> (tokio::runtime::Runtime, Dial9Handle) {
        (
            builder.build().expect("runtime builder succeeds"),
            Dial9Handle,
        )
    }

    pub fn set_thread_local(_handle: Dial9Handle) {}

    pub fn thread_local_handle() -> Dial9Handle {
        Dial9Handle
    }

    pub fn spawn_or_fallback<F, T>(
        _handle: Dial9Handle,
        task_builder: tokio::task::Builder<'_>,
        fut: F,
        runtime: &tokio::runtime::Handle,
    ) -> tokio::task::JoinHandle<T>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        task_builder
            .spawn_on(fut, runtime)
            .expect("runtime can spawn tasks")
    }
}

pub(super) use inner::*;
