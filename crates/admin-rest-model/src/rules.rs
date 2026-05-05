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

use restate_limiter::{LimitsPatch, PersistedRule, RuleId, RulePattern, UserLimits};
use restate_serde_util::UpdateField;
use restate_types::Version;
use restate_util_string::ReString;

/// Request body for `POST /rules`.
#[serde_as]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Deserialize)]
pub struct CreateRuleRequest {
    /// The pattern that selects which scope/limit-key combinations the
    /// rule applies to. Examples: `"*"`, `"scope1/*"`, `"scope1/foo/bar"`.
    #[cfg_attr(feature = "schema", schema(value_type = String))]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub pattern: RulePattern<ReString>,
    #[serde(default)]
    pub limits: UserLimits,
    /// Free-form description shown in the rule book; not consulted at
    /// runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Soft-tombstone toggle. `true` parks the rule (the runtime treats
    /// it as absent) without removing it.
    #[serde(default)]
    pub disabled: bool,
}

/// Request body for `PATCH /rules/{rule_id}`.
///
/// Uses RFC 7396 JSON Merge Patch semantics on each field: absent →
/// keep, JSON `null` → clear (only meaningful for fields where `None`
/// is a valid state), value → set.
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Default, Deserialize)]
pub struct PatchRuleRequest {
    #[serde(default)]
    pub limits: LimitsPatch,
    /// `null` to clear, string to set, omit to leave unchanged.
    #[cfg_attr(feature = "schema", schema(value_type = Option<String>, nullable))]
    #[serde(default)]
    pub reason: UpdateField<String>,
    /// `true` parks the rule, `false` reactivates; omit to leave unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disabled: Option<bool>,
}

#[serde_as]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize)]
pub struct RuleResponse {
    /// Server-derived id, rendered as `rul_…`.
    #[cfg_attr(feature = "schema", schema(value_type = String))]
    pub id: RuleId,
    #[cfg_attr(feature = "schema", schema(value_type = String))]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub pattern: RulePattern<ReString>,
    pub limits: UserLimits,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub disabled: bool,
    /// Per-rule version: bumped on runtime-relevant changes.
    #[cfg_attr(feature = "schema", schema(value_type = u32))]
    pub version: Version,
    /// Seconds since UNIX epoch.
    pub last_modified_seconds_since_epoch: u64,
}

impl From<(RuleId, PersistedRule)> for RuleResponse {
    fn from((id, rule): (RuleId, PersistedRule)) -> Self {
        RuleResponse {
            id,
            pattern: rule.pattern,
            limits: rule.limits,
            reason: rule.reason,
            disabled: rule.disabled,
            version: rule.version,
            last_modified_seconds_since_epoch: rule.last_modified.as_unix_seconds(),
        }
    }
}
