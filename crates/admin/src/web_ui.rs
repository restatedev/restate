// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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
use http::{header, HeaderValue, StatusCode, Uri};
use http_body_util::Full;

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
                .body(Full::from(file.contents()))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                HeaderValue::from_str(mime_guess::mime::TEXT_HTML_UTF_8.as_ref()).unwrap(),
            )
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
        .route("/ui", get(|| async { Redirect::permanent("/ui/") }))
        .route("/ui/", get(serve_web_ui))
        .route("/ui/*path", get(serve_web_ui))
}
