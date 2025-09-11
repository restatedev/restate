// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use restate_time_util::{FriendlyDuration, NonZeroFriendlyDuration};

#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "InvocationOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
// NOTE: These are some common invocation options we want flattened in the main configuration, for nicer UX.
pub struct InvocationOptions {
    /// # Default journal retention
    ///
    /// Default journal retention for all invocations. A value of `0` means no retention by default.
    ///
    /// In production setups, it is advisable to disable default journal retention,
    /// and configure journal retention per service using the respective SDK APIs.
    #[serde(skip_serializing_if = "FriendlyDuration::is_zero", default)]
    pub default_journal_retention: FriendlyDuration,

    /// # Maximum journal retention duration
    ///
    /// Maximum journal retention duration that can be configured.
    /// When discovering a service deployment, or when modifying the journal retention using the Admin API, the given value will be clamped.
    ///
    /// Unset means no limit.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub max_journal_retention: Option<FriendlyDuration>,

    // TODO(slinkydeveloper) on 1.6 this option becomes mandatory, and serde should default to the values set below
    /// # Default retry policy
    ///
    /// The default retry policy to use for invocations.
    ///
    /// The retry policy can be customized on a service/handler basis, using the respective SDK APIs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_retry_policy: Option<InvocationRetryPolicyOptions>,

    /// # Max configurable value for retry policy max attempts
    ///
    /// Maximum max attempts configurable in an invocation retry policy.
    /// When discovering a service deployment with configured retry policies, or when modifying the invocation retry policy using the Admin API, the given value will be clamped.
    ///
    /// `None` means no limit, that is infinite retries is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retry_policy_max_attempts: Option<NonZeroUsize>,
}

impl Default for InvocationOptions {
    fn default() -> Self {
        Self {
            default_journal_retention: FriendlyDuration::from_secs(60 * 60 * 24),
            max_journal_retention: None,
            default_retry_policy: None,
            max_retry_policy_max_attempts: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(rename = "InvocationRetryPolicyOptions", default)
)]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct InvocationRetryPolicyOptions {
    /// # Initial Interval
    ///
    /// Initial interval for the first retry attempt.
    #[serde(default = "default_initial_interval")]
    pub(crate) initial_interval: NonZeroFriendlyDuration,

    /// # Factor
    ///
    /// The factor to use to compute the next retry attempt. Default: `2.0`.
    #[serde(default = "default_exponentiation_factor")]
    pub(crate) exponentiation_factor: f32,

    /// # Max attempts
    ///
    /// Number of maximum attempts (including the initial) before giving up. Infinite retries if unset. No retries if set to 1.
    #[serde(default = "default_max_attempts")]
    pub(crate) max_attempts: Option<NonZeroUsize>,

    /// # On max attempts
    ///
    /// Behavior when max attempts are reached.
    #[serde(default)]
    pub(crate) on_max_attempts: OnMaxAttempts,

    /// # Max interval
    ///
    /// Maximum interval between retries.
    #[serde(default)]
    pub(crate) max_interval: Option<NonZeroFriendlyDuration>,
}

impl Default for InvocationRetryPolicyOptions {
    fn default() -> Self {
        Self {
            initial_interval: default_initial_interval(),
            exponentiation_factor: default_exponentiation_factor(),
            max_attempts: default_max_attempts(),
            on_max_attempts: OnMaxAttempts::default(),
            max_interval: None,
        }
    }
}

fn default_initial_interval() -> NonZeroFriendlyDuration {
    NonZeroFriendlyDuration::from_millis_unchecked(500)
}

fn default_max_attempts() -> Option<NonZeroUsize> {
    Some(NonZeroUsize::new(20).unwrap())
}

fn default_exponentiation_factor() -> f32 {
    2.0
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "OnMaxAttempts"))]
#[serde(rename_all = "kebab-case")]
pub enum OnMaxAttempts {
    /// Pause the invocation when max attempts are reached.
    #[default]
    Pause,
    /// Kill the invocation when max attempts are reached.
    Kill,
}
