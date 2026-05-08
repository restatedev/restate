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

use restate_limiter::{PersistedRule, Precondition, RulePattern, UserLimits};
use restate_types::Version;
use restate_util_string::ReString;

/// One entry in the body of `PUT /limits/rules`.
///
/// Each entry carries a fully-specified rule body plus an optional
/// [`Precondition`]. Omitting the `precondition` field defaults to
/// `Precondition::None` (unconditional upsert).
#[serde_as]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Deserialize)]
pub struct UpsertRuleRequest {
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
    pub description: Option<String>,
    /// Soft-tombstone toggle. `true` parks the rule (the runtime treats
    /// it as absent) without removing it.
    #[serde(default)]
    pub disabled: bool,
    /// Optimistic-concurrency guard. `{ "type": "matches", "version": v }`
    /// requires the rule's current version to be `v`;
    /// `{ "type": "does_not_exist" }` requires the rule to be absent
    /// (strict insert); `{ "type": "none" }` (or omitted) is
    /// unconditional.
    #[serde(default)]
    pub precondition: Precondition,
}

/// One entry in the body of `POST /limits/rules/bulk-delete`.
#[serde_as]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Deserialize)]
pub struct DeleteRuleRequest {
    #[cfg_attr(feature = "schema", schema(value_type = String))]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub pattern: RulePattern<ReString>,
    /// Optimistic-concurrency match. Absent → unconditional delete
    /// (idempotent: deleting an already-absent rule succeeds as a
    /// no-op). Present → reject unless the rule's current version is
    /// the supplied value.
    #[cfg_attr(feature = "schema", schema(value_type = Option<u32>))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<Version>,
}

#[serde_as]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize)]
pub struct RuleResponse {
    #[cfg_attr(feature = "schema", schema(value_type = String))]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub pattern: RulePattern<ReString>,
    pub limits: UserLimits,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub disabled: bool,
    /// Per-rule version: bumped on runtime-relevant changes.
    #[cfg_attr(feature = "schema", schema(value_type = u32))]
    pub version: Version,
    /// Millis since UNIX epoch.
    pub last_modified_millis_since_epoch: u64,
}

impl From<(RulePattern<ReString>, &PersistedRule)> for RuleResponse {
    fn from((pattern, rule): (RulePattern<ReString>, &PersistedRule)) -> Self {
        RuleResponse {
            pattern,
            limits: rule.limits.clone(),
            description: rule.description.clone(),
            disabled: rule.disabled,
            version: rule.version,
            last_modified_millis_since_epoch: rule.last_modified.as_u64(),
        }
    }
}
