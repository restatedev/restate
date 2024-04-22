// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Export schema types to be used by other crates without exposing the fact
// that we are using proxying to restate-schema-api or restate-types
pub use restate_schema_api::service::{HandlerMetadata, ServiceMetadata};
pub use restate_types::identifiers::ServiceRevision;
pub use restate_types::invocation::ServiceType;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ListServicesResponse {
    pub services: Vec<ServiceMetadata>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ModifyServiceRequest {
    /// # Public
    ///
    /// If true, the service can be invoked through the ingress.
    /// If false, the service can be invoked only from another Restate service.
    #[serde(default)]
    pub public: Option<bool>,
    /// # Idempotency retention
    ///
    /// Modify the retention of idempotent requests for this service.
    #[serde(default, with = "serde_with::As::<Option<serde_with::DisplayFromStr>>")]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    pub idempotency_retention: Option<humantime::Duration>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ModifyServiceStateRequest {
    /// # Version
    ///
    /// If set, the latest version of the state is compared with this value and the operation will fail
    /// when the versions differ.
    pub version: Option<String>,

    /// # Service key
    ///
    /// To what virtual object key to apply this change
    pub object_key: String,

    /// # New State
    ///
    /// The new state to replace the previous state with
    pub new_state: HashMap<String, Bytes>,
}
