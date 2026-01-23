// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use codederror::Code;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[cfg_attr(feature = "utoipa-schema", derive(utoipa::ToSchema))]
pub struct Info {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    code: Option<String>,
    message: String,
}

impl Info {
    pub fn new(message: impl ToString) -> Self {
        Self {
            code: None,
            message: message.to_string(),
        }
    }

    pub fn new_with_code(code: &'static Code, message: impl ToString) -> Self {
        Self {
            code: Some(code.code().to_string()),
            message: message.to_string(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}
