// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// Export schema types to be used by other crates without exposing the fact
// that we are using proxying to restate-schema-api or restate-types
pub use restate_schema_api::service::{MethodMetadata, ServiceMetadataResolver};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ListServiceMethodsResponse {
    pub methods: Vec<MethodMetadata>,
}
