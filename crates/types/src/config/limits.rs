// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_time_util::NonZeroFriendlyDuration;

/// # Limits options
///
/// Cluster-wide settings that govern the limiter subsystem (rule book,
/// per-rule capacity caps, etc.). These knobs apply regardless of which
/// roles a node runs.
/// *Since v1.7.0*
#[serde_as]
#[derive(Debug, Default, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "LimitsOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct LimitsOptions {
    /// # Rule book
    ///
    /// Settings for the cluster-global rule book that drives the limiter.
    /// *Since v1.7.0*
    #[serde(default)]
    pub rule_book: RuleBookOptions,
}

/// # Rule book options
///
/// Tuning knobs for the cluster-global limiter rule book.
/// *Since v1.7.0*
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "RuleBookOptions", default))]
#[serde(rename_all = "kebab-case")]
pub struct RuleBookOptions {
    /// # Poll interval
    ///
    /// How often each node's `RuleBookCache` polls the metadata store
    /// for rule-book updates. The cache also receives push-style
    /// notifications when partition processors apply
    /// `Command::UpsertRuleBook` from Bifrost, so this poll interval
    /// is mainly a fallback for cross-node propagation when no
    /// partition leader has yet observed the change. Default: 30 s.
    /// *Since v1.7.0*
    #[serde(default = "RuleBookOptions::default_poll_interval")]
    pub poll_interval: NonZeroFriendlyDuration,
}

impl RuleBookOptions {
    fn default_poll_interval() -> NonZeroFriendlyDuration {
        // 30 minutes as most of the distribution work should happen through Bifrost.
        // Only if the partition replication is set to 1, we really need the poll
        // mechanism.
        NonZeroFriendlyDuration::from_secs_unchecked(30)
    }
}

impl Default for RuleBookOptions {
    fn default() -> Self {
        Self {
            poll_interval: Self::default_poll_interval(),
        }
    }
}
