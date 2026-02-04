// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(not(target_env = "msvc"))]
mod pprof {
    use std::ffi::CStr;
    use std::ptr;

    use axum::Json;
    use http::StatusCode;
    use serde::Serialize;
    use tikv_jemalloc_ctl::{arenas, epoch};

    pub use crate::network_server::jemalloc::JemallocStats;

    pub async fn heap() -> Result<impl axum::response::IntoResponse, (StatusCode, String)> {
        match jemalloc_pprof::PROF_CTL.as_ref() {
            Some(prof_ctl) => {
                let mut prof_ctl = prof_ctl.lock().await;
                if prof_ctl.activated() {
                    let pprof = prof_ctl
                        .dump_pprof()
                        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
                    Ok(pprof)
                } else {
                    Err((
                        axum::http::StatusCode::PRECONDITION_FAILED,
                        "Heap profiling not activated: first curl -XPUT :5122/debug/pprof/heap/activate\n"
                            .into(),
                    ))
                }
            }
            None => Err((
                axum::http::StatusCode::PRECONDITION_FAILED,
                "Heap profiling not enabled: run with MALLOC_CONF=\"prof:true\"\n".into(),
            )),
        }
    }

    pub async fn activate_heap() -> Result<(), (StatusCode, String)> {
        match jemalloc_pprof::PROF_CTL.as_ref() {
            Some(prof_ctl) => {
                let mut prof_ctl = prof_ctl.lock().await;
                prof_ctl
                    .activate()
                    .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
                Ok(())
            }
            None => Err((
                axum::http::StatusCode::PRECONDITION_FAILED,
                "Heap profiling not enabled: run with MALLOC_CONF=\"prof:true\"\n".into(),
            )),
        }
    }

    pub async fn deactivate_heap() -> Result<(), (StatusCode, String)> {
        match jemalloc_pprof::PROF_CTL.as_ref() {
            Some(prof_ctl) => {
                let mut prof_ctl = prof_ctl.lock().await;
                prof_ctl
                    .deactivate()
                    .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
                Ok(())
            }
            None => Err((
                axum::http::StatusCode::PRECONDITION_FAILED,
                "Heap profiling not enabled: run with MALLOC_CONF=\"prof:true\"\n".into(),
            )),
        }
    }

    /// Generates a heap flamegraph SVG.
    ///
    /// This endpoint is only available when the `heap-flamegraph` feature is enabled.
    #[cfg(feature = "heap-flamegraph")]
    pub async fn heap_flamegraph() -> Result<impl axum::response::IntoResponse, (StatusCode, String)>
    {
        use axum::body::Body;
        use axum::http::header::CONTENT_TYPE;
        use axum::response::Response;

        match jemalloc_pprof::PROF_CTL.as_ref() {
            Some(prof_ctl) => {
                let mut prof_ctl = prof_ctl.lock().await;
                if prof_ctl.activated() {
                    let svg = prof_ctl
                        .dump_flamegraph()
                        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
                    Response::builder()
                        .header(CONTENT_TYPE, "image/svg+xml")
                        .body(Body::from(svg))
                        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
                } else {
                    Err((
                        axum::http::StatusCode::PRECONDITION_FAILED,
                        "Heap profiling not activated: first curl -XPUT :5122/debug/heap/activate\n"
                            .into(),
                    ))
                }
            }
            None => Err((
                axum::http::StatusCode::PRECONDITION_FAILED,
                "Heap profiling not enabled: run with MALLOC_CONF=\"prof:true\"\n".into(),
            )),
        }
    }

    /// Stub for heap flamegraph when the feature is not enabled.
    #[cfg(not(feature = "heap-flamegraph"))]
    pub async fn heap_flamegraph() -> (StatusCode, String) {
        (
            axum::http::StatusCode::NOT_FOUND,
            "Heap flamegraph is only available when the heap-flamegraph feature is enabled\n".into(),
        )
    }

    /// Result of a jemalloc purge operation
    #[derive(Debug, Serialize)]
    pub struct JemallocPurgeResult {
        /// Memory stats before purge
        pub before: JemallocStats,
        /// Memory stats after purge
        pub after: JemallocStats,
        /// Number of arenas purged
        pub arenas_purged: usize,
    }

    /// Purges unused dirty pages across all arenas, releasing memory back to the OS.
    ///
    /// This is useful for reclaiming memory that jemalloc is holding onto but not
    /// actively using.
    ///
    /// # Warning
    ///
    /// This is an **expensive operation** that can cause significant latency spikes.
    /// It may block allocations while the purge is in progress. Only use this for
    /// debugging or one-off memory reclamation. Do not call this regularly in production.
    pub async fn jemalloc_purge() -> Result<Json<JemallocPurgeResult>, (StatusCode, String)> {
        // Get stats before purge (this also advances the epoch)
        let before = JemallocStats::read().map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to read stats before purge: {err}"),
            )
        })?;

        // Get number of arenas
        let narenas: u32 = arenas::narenas::read().map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to read number of arenas: {err}"),
            )
        })?;

        // Purge all arenas using arena.<i>.purge
        //
        // From jemalloc documentation: "If <i> equals `MALLCTL_ARENAS_ALL`, all arenas are
        // purged." The MALLCTL_ARENAS_ALL constant is defined as 4096 in jemalloc's
        // jemalloc.h but is not exposed by the Rust bindings.
        //
        // Reference: http://jemalloc.net/jemalloc.3.html#arena.i.purge
        //
        // SAFETY: This is a void mallctl operation (no input, no output).
        // We must call mallctl directly with null pointers since the
        // tikv_jemalloc_ctl::raw API doesn't support void operations.
        const MALLCTL_ARENAS_ALL_PURGE: &CStr = c"arena.4096.purge";
        unsafe {
            let ret = tikv_jemalloc_sys::mallctl(
                MALLCTL_ARENAS_ALL_PURGE.as_ptr(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                0,
            );
            if ret != 0 {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to purge arenas: mallctl returned {ret}"),
                ));
            }
        }

        // Get stats after purge
        epoch::advance().map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to advance jemalloc epoch after purge: {err}"),
            )
        })?;

        let after = JemallocStats::read_without_epoch_advance().map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to read stats after purge: {err}"),
            )
        })?;

        Ok(Json(JemallocPurgeResult {
            before,
            after,
            arenas_purged: narenas as usize,
        }))
    }
}

#[cfg(target_env = "msvc")]
mod pprof {
    use http::StatusCode;

    pub async fn heap() -> (StatusCode, String) {
        (
            axum::http::StatusCode::PRECONDITION_FAILED,
            "Heap profiling is not available on MSVC targets\n".into(),
        )
    }

    pub async fn activate_heap() -> (StatusCode, String) {
        heap().await
    }

    pub async fn deactivate_heap() -> (StatusCode, String) {
        heap().await
    }

    pub async fn jemalloc_purge() -> (StatusCode, String) {
        (
            axum::http::StatusCode::PRECONDITION_FAILED,
            "jemalloc purge is not available on MSVC targets\n".into(),
        )
    }

    pub async fn heap_flamegraph() -> (StatusCode, String) {
        (
            axum::http::StatusCode::PRECONDITION_FAILED,
            "Heap flamegraph is not available on MSVC targets\n".into(),
        )
    }
}

pub use pprof::*;
