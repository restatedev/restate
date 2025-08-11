// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "InvocationOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
// NOTE: These are some common invocation options we want flattened in the main configuration, for nicer UX.
pub struct InvocationOptions {
    /// # Default journal retention
    ///
    /// Default journal retention for all invocations. A value of `0` or `None` means no retention by default.
    ///
    /// In production setups, it is advisable to disable default journal retention,
    /// and configure journal retention per service using the respective SDK APIs.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(
        feature = "schemars",
        schemars(with = "Option<restate_serde_util::DurationString>")
    )]
    pub default_journal_retention: Option<Duration>,

    /// # Maximum journal retention duration
    ///
    /// Maximum journal retention duration that can be configured.
    /// When discovering a service deployment, or when modifying the journal retention using the Admin API, the given value will be clamped.
    ///
    /// `None` means no limit.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(
        feature = "schemars",
        schemars(with = "Option<restate_serde_util::DurationString>")
    )]
    pub max_journal_retention: Option<Duration>,
}

impl Default for InvocationOptions {
    fn default() -> Self {
        Self {
            default_journal_retention: Some(Duration::from_secs(60 * 60 * 24)),
            max_journal_retention: None,
        }
    }
}
