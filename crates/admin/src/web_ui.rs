// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use http::{HeaderValue, StatusCode, Uri, header};
use http_body_util::Full;

async fn redirect_to_ui(uri: Uri) -> impl IntoResponse {
    let query = uri.query().map(|q| format!("?{}", q)).unwrap_or_default();
    Redirect::permanent(&format!("/ui/{}", query))
}

async fn serve_web_ui(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches("/ui").trim_start_matches('/');

    match restate_web_ui::ASSETS.get_file(path) {
        Some(file) => {
            let mime_type = mime_guess::from_path(path).first_or_text_plain();
            Response::builder()
                .status(StatusCode::OK)
                .header(
                    header::CONTENT_TYPE,
                    HeaderValue::from_str(mime_type.as_ref()).unwrap(),
                )
                .header("Service-Worker-Allowed", "/")
                .body(Full::from(file.contents()))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                HeaderValue::from_str(mime_guess::mime::TEXT_HTML_UTF_8.as_ref()).unwrap(),
            )
            .header("Service-Worker-Allowed", "/")
            .body(Full::from(
                restate_web_ui::ASSETS
                    .get_file("index.html")
                    .expect("index.html must be present!")
                    .contents(),
            ))
            .unwrap(),
    }
}

pub(crate) fn web_ui_router() -> axum::Router {
    axum::Router::new()
        .route("/", get(redirect_to_ui))
        .route("/ui", get(redirect_to_ui))
        .route("/ui/", get(serve_web_ui))
        .route("/ui/{*path}", get(serve_web_ui))
}
