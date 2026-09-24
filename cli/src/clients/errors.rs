// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Deserialize;
use url::Url;

use restate_cli_util::ui::stylesheet::Style;

use crate::console::Styled;

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
    pub url: Url,
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

impl std::fmt::Display for ApiErrorBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.restate_code {
            Some(code) => {
                // The server's message may repeat the code as `[CODE] ` prefixes (once
                // per error layer); the docs link below already names it.
                let tag = format!("[{code}] ");
                let mut message = self.message.as_str();
                while let Some(rest) = message.strip_prefix(&tag) {
                    message = rest;
                }
                write!(
                    f,
                    "{message}\n  -> See {}",
                    Styled(Style::Info, error_docs_url(code))
                )
            }
            None => write!(f, "{}", self.message),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.body)?;
        write!(
            f,
            "  -> Http status code {} at '{}'",
            Styled(Style::Warn, &self.http_status_code),
            Styled(Style::Info, &self.url),
        )?;
        Ok(())
    }
}

impl std::error::Error for ApiError {}
