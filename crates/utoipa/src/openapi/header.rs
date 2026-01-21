// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implements [OpenAPI Header Object][header] types.
//!
//! [header]: https://spec.openapis.org/oas/latest.html#header-object

use super::{RefOr, Schema, builder, set_value};
use serde::{Deserialize, Serialize};
use serde_json::json;

builder! {
    HeaderBuilder;

    /// Implements [OpenAPI Header Object][header] for response headers.
    ///
    /// [header]: https://spec.openapis.org/oas/latest.html#header-object
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Clone, PartialEq)]
    #[cfg_attr(feature = "debug", derive(Debug))]
    pub struct Header {
        /// Schema of header type.
        pub schema: RefOr<Schema>,

        /// Additional description of the header value.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub description: Option<String>,
    }
}

impl Header {
    /// Create new header
    pub fn new<C: Into<RefOr<Schema>>>(component: C) -> Self {
        Self {
            schema: component.into(),
            ..Default::default()
        }
    }
}

impl Default for Header {
    fn default() -> Self {
        Self {
            description: Default::default(),
            schema: Schema::new(json!({"type": "string"})).into(),
        }
    }
}

impl HeaderBuilder {
    /// Add schema of header.
    pub fn schema<I: Into<RefOr<Schema>>>(mut self, component: I) -> Self {
        set_value!(self schema component.into())
    }

    /// Add additional description for header.
    pub fn description<S: Into<String>>(mut self, description: Option<S>) -> Self {
        set_value!(self description description.map(|description| description.into()))
    }
}
