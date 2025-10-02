// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

impl std::fmt::Display for ApiErrorBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = self.restate_code.as_deref().unwrap_or("<UNKNOWN>");
        write!(f, "{} {}", Styled(Style::Warn, code), self.message)?;
        Ok(())
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
