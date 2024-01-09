// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde_with::serde_as;
use std::time::Duration;

/// Partition processor options
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "PartitionProcessorOptions", default)
)]
#[builder(default)]
pub struct Options {
    /// # Maximum batch duration
    ///
    /// The maximum duration the partition processor can spend on batching commands before
    /// checking for signals from its actuators. The larger the value, the fewer disk writes
    /// are being performed which can improve overall throughput at the cost of potentially
    /// longer invocation latencies.
    #[serde_as(as = "serde_with::NoneAsEmptyString")]
    #[cfg_attr(feature = "options_schema", schemars(with = "Option<String>"))]
    pub max_batch_duration: Option<humantime::Duration>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_batch_duration: Some(Duration::from_millis(50).into()),
        }
    }
}
