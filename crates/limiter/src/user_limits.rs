// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Runtime-side limits and rule-mutation channel types.
//!
//! [`UserLimits`] is the per-rule effective-limits shape used both as the
//! in-memory value the runtime consumes when checking capacity and (under
//! the `bilrost` feature) as the on-disk shape stored inside a
//! [`crate::PersistedRule`]. New limit kinds are added here once.
//!
//! [`RuleUpdate`] is the channel-level message the per-PP `UserLimiter`
//! consumes; it is produced by [`crate::RuleBook::diff`].

use std::num::NonZeroU32;

use restate_util_string::ReString;

use crate::RulePattern;

/// Per-rule effective limits.
///
/// `None` on a field means "unlimited" (no rule constrains this dimension).
/// Under the `bilrost` feature this type is also the wire shape persisted
/// inside [`crate::PersistedRule`]; under `serde` it's the JSON wire shape
/// for the admin REST model — adding a new limit kind here means allocating
/// a fresh `bilrost(tag(...))` next to the new field.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "bilrost", derive(bilrost::Message))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[non_exhaustive]
pub struct UserLimits {
    /// Maximum concurrent invocations. `None` means unlimited.
    #[cfg_attr(feature = "bilrost", bilrost(tag(1)))]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    #[cfg_attr(feature = "schema", schema(value_type = Option<u32>, minimum = 1))]
    pub action_concurrency: Option<NonZeroU32>,
}

impl UserLimits {
    pub fn new(action_concurrency: Option<NonZeroU32>) -> Self {
        Self { action_concurrency }
    }
}

/// A single mutation produced by [`crate::RuleBook::diff`].
///
/// Pattern-keyed (matches the `Rules` runtime store). Disabled or absent
/// rules in the projected view become [`RuleUpdate::Remove`]; rules whose
/// runtime-relevant fields change become [`RuleUpdate::Upsert`].
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum RuleUpdate {
    /// Insert a new rule or update an existing one with the same pattern.
    Upsert {
        pattern: RulePattern<ReString>,
        limit: UserLimits,
    },
    /// Remove a rule by its pattern.
    Remove { pattern: RulePattern<ReString> },
}
