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
//! [`UserLimits`] is the materialised, in-memory shape of a rule's effective
//! limits — what the runtime actually consumes when checking capacity. It is
//! produced from [`crate::PersistedUserLimits`] (the on-disk shape) at apply
//! time.
//!
//! [`RuleUpdate`] is the channel-level message the per-PP `UserLimiter`
//! consumes; it is produced by [`crate::RuleBook::diff`].

use std::num::NonZeroU64;

use restate_util_string::ReString;

#[cfg(feature = "rule-book")]
use crate::PersistedUserLimits;
use crate::RulePattern;

/// Materialised, runtime-side limits for a single rule.
///
/// `None` on a field means "unlimited" (no rule constrains this dimension).
/// New limit kinds are added here together with the corresponding field on
/// [`crate::PersistedUserLimits`].
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct UserLimits {
    /// Maximum concurrent invocations. `None` means unlimited.
    pub action_concurrency: Option<NonZeroU64>,
}

impl UserLimits {
    pub fn new(action_concurrency: Option<NonZeroU64>) -> Self {
        Self { action_concurrency }
    }
}

#[cfg(feature = "rule-book")]
impl From<&PersistedUserLimits> for UserLimits {
    fn from(persisted: &PersistedUserLimits) -> Self {
        Self {
            action_concurrency: persisted.action_concurrency,
        }
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
