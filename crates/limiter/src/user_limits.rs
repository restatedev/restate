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
    /// Maximum concurrent running invocations. `None` means unlimited.
    #[cfg_attr(feature = "bilrost", bilrost(tag(1)))]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    #[cfg_attr(feature = "schema", schema(value_type = Option<u32>, minimum = 1))]
    pub concurrency: Option<NonZeroU32>,

    /// Scheduling weight of this rule's scope in the scheduler's weighted
    /// round-robin: a scope with weight N receives N dispatch slots per cycle
    /// relative to weight-1 groups, regardless of how many queues it has.
    /// `None` means the default weight of 1. Only scope-level exact patterns
    /// are consulted. Requires the vqueues scheduler.
    ///
    /// Upserts replace the whole limits object: omitting this field resets
    /// the weight (the CLI preserves it by re-reading the current rule).
    #[cfg_attr(feature = "bilrost", bilrost(tag(2)))]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    #[cfg_attr(feature = "schema", schema(value_type = Option<u32>, minimum = 1))]
    pub scheduling_weight: Option<NonZeroU32>,
}

impl UserLimits {
    pub fn new(concurrency: Option<NonZeroU32>) -> Self {
        Self {
            concurrency,
            scheduling_weight: None,
        }
    }

    pub fn with_scheduling_weight(mut self, scheduling_weight: Option<NonZeroU32>) -> Self {
        self.scheduling_weight = scheduling_weight;
        self
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use bilrost::{Message, OwnedMessage};

    use super::*;

    /// Wire compatibility: pre-weight encodings (concurrency only) decode with
    /// `scheduling_weight = None`, and a default weight is omitted on the wire
    /// so old readers see the exact pre-weight byte layout.
    #[test]
    fn scheduling_weight_wire_compat() {
        // "old" shape: only tag(1) present
        let old = UserLimits::new(NonZeroU32::new(100));
        let old_bytes = old.encode_to_bytes();
        let decoded = <UserLimits as OwnedMessage>::decode(old_bytes.clone()).unwrap();
        assert_eq!(decoded.scheduling_weight, None);
        assert_eq!(decoded.concurrency, NonZeroU32::new(100));

        // new shape round-trips
        let new = UserLimits::new(NonZeroU32::new(100)).with_scheduling_weight(NonZeroU32::new(10));
        let new_bytes = new.encode_to_bytes();
        let decoded = <UserLimits as OwnedMessage>::decode(new_bytes).unwrap();
        assert_eq!(decoded.scheduling_weight, NonZeroU32::new(10));

        // None weight encodes identically to the old shape (old readers
        // decoding new writers see no unknown data)
        let new_default = UserLimits::new(NonZeroU32::new(100));
        assert_eq!(new_default.encode_to_bytes(), old_bytes);
    }
}
