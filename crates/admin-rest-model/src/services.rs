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
use std::time::Duration;

use restate_types::schema::service::ServiceMetadata;

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
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format or the ISO8601.
    #[serde(
        default,
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>"
    )]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    pub idempotency_retention: Option<Duration>,

    /// # Workflow completion retention
    ///
    /// Modify the retention of the workflow completion. This can be modified only for workflow services!
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format or the ISO8601.
    #[serde(
        default,
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>"
    )]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    pub workflow_completion_retention: Option<Duration>,

    /// # Inactivity timeout
    ///
    /// This timer guards against stalled service/handler invocations. Once it expires,
    /// Restate triggers a graceful termination by asking the service invocation to
    /// suspend (which preserves intermediate progress).
    ///
    /// The 'abort timeout' is used to abort the invocation, in case it doesn't react to
    /// the request to suspend.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format or the ISO8601.
    ///
    /// This overrides the default inactivity timeout set in invoker options.
    #[serde(
        default,
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>"
    )]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    pub inactivity_timeout: Option<Duration>,

    /// # Abort timeout
    ///
    /// This timer guards against stalled service/handler invocations that are supposed to
    /// terminate. The abort timeout is started after the 'inactivity timeout' has expired
    /// and the service/handler invocation has been asked to gracefully terminate. Once the
    /// timer expires, it will abort the service/handler invocation.
    ///
    /// This timer potentially **interrupts** user code. If the user code needs longer to
    /// gracefully terminate, then this value needs to be set accordingly.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format or the ISO8601.
    ///
    /// This overrides the default abort timeout set in invoker options.
    #[serde(
        default,
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>"
    )]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    pub abort_timeout: Option<Duration>,
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
