// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::retries::RetryPolicy;

/// # Networking options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "WorkerOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct NetworkingOptions {
    /// # Retry policy
    ///
    /// Retry policy to use for internal node-to-node networking.
    pub retry_policy: RetryPolicy,
}

impl NetworkingOptions {}

const NETWORKING_DEFAULT_RETRY_INITIAL_INTERVAL: Duration = Duration::from_millis(10);
const NETWORKING_DEFAULT_RETRY_MAX_INTERVAL: Duration = Duration::from_millis(500);
const NETWORKING_DEFAULT_RETRY_MAX_ATTEMPTS: usize = 10;

impl Default for NetworkingOptions {
    fn default() -> Self {
        Self {
            retry_policy: RetryPolicy::exponential(
                NETWORKING_DEFAULT_RETRY_INITIAL_INTERVAL,
                2.0,
                Some(NETWORKING_DEFAULT_RETRY_MAX_ATTEMPTS),
                Some(NETWORKING_DEFAULT_RETRY_MAX_INTERVAL),
            ),
        }
    }
}
