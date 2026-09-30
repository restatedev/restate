// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arrow::error::ArrowError;
use serde::Deserialize;
use thiserror::Error;
use url::Url;

/// The error of the CLI's HTTP clients (admin API, SQL queries, Restate Cloud).
#[derive(Error, Debug)]
pub enum ClientError {
    /// The server answered with an error status.
    #[error(transparent)]
    Api(#[from] ApiError),
    /// The server's response couldn't be parsed. The serde error isn't the source: the
    /// message already includes it.
    #[error("Unexpected response from the server: {0}")]
    Serialization(serde_json::Error),
    #[error(transparent)]
    Network(#[from] reqwest::Error),
    #[error(
        "The Restate server '{0}' lacks JSON /query support. Please update the CLI to match the Restate server version '{1}'."
    )]
    JSONSupport(Url, String),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    UrlParse(#[from] url::ParseError),
}

impl From<serde_json::Error> for ClientError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization(err)
    }
}

/// The admin API's error body. Keep in sync with `ErrorDescriptionResponse` in
/// `crates/admin/src/rest_api/mod.rs`.
#[derive(Deserialize, Debug, Clone)]
pub struct ApiErrorBody {
    pub restate_code: Option<String>,
    pub message: String,
}

impl ApiErrorBody {
    /// The server's JSON error body, or its raw text when it isn't JSON (e.g. a plain
    /// `Cannot parse deployment …` 400).
    pub fn parse(body: String) -> Self {
        serde_json::from_str(&body).unwrap_or_else(|_| Self::from(body.trim().to_owned()))
    }

    /// The server's message, without the `[CODE] ` prefixes it may repeat (once per
    /// error layer): the code is reported, and linked to its docs, on its own.
    pub fn message(&self) -> &str {
        let mut message = self.message.as_str();
        if let Some(code) = &self.restate_code {
            let tag = format!("[{code}] ");
            while let Some(rest) = message.strip_prefix(&tag) {
                message = rest;
            }
        }
        message
    }
}

impl From<String> for ApiErrorBody {
    fn from(message: String) -> Self {
        Self {
            message,
            restate_code: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ApiError {
    pub http_status_code: reqwest::StatusCode,
    /// The request URL, only for display (a `String` rather than a `Url` keeps
    /// `ClientError` small).
    pub url: String,
    pub body: ApiErrorBody,
}

/// Where a Restate error code (e.g. `META0003`) is documented. The page's anchors are
/// lowercase.
pub fn error_docs_url(code: &str) -> String {
    format!(
        "https://docs.restate.dev/references/errors#{}",
        code.to_lowercase()
    )
}

/// The HTTP exchange that failed, e.g. `403 Forbidden at 'http://…'`. Plain text: it is
/// part of [`RestateCliError`](crate::error::RestateCliError)'s message, which JSON
/// output carries too.
impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at '{}'", self.http_status_code, self.url)
    }
}

impl std::error::Error for ApiError {}
