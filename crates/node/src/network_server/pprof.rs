// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(target_os = "linux")]
mod pprof {
    use http::StatusCode;

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
}

#[cfg(not(target_os = "linux"))]
mod pprof {
    use http::StatusCode;

    pub async fn heap() -> (StatusCode, String) {
        (
            axum::http::StatusCode::PRECONDITION_FAILED,
            "Heap profiling is only available on Linux\n".into(),
        )
    }

    pub async fn activate_heap() -> (StatusCode, String) {
        heap().await
    }

    pub async fn deactivate_heap() -> (StatusCode, String) {
        heap().await
    }
}

pub use pprof::*;
