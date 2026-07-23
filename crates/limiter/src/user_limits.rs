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

/// Parameters for the adaptive (Gradient2) concurrency controller.
///
/// All fields are optional; an empty object activates the controller with
/// defaults. The learned limit always stays within `[min, max]`; when a static
/// [`UserLimits::concurrency`] is also set it takes precedence.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "bilrost", derive(bilrost::Message))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct AdaptiveConcurrency {
    /// Lower bound for the learned limit. Default 4 per partition: below
    /// ~2-4 slots long-running handlers stop producing a usable latency
    /// gradient (and throughput dies), so the controller never goes there.
    #[cfg_attr(feature = "bilrost", bilrost(tag(1)))]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    #[cfg_attr(feature = "schema", schema(value_type = Option<u32>, minimum = 1))]
    pub min: Option<NonZeroU32>,

    /// Upper bound for the learned limit. Default 10000 (practically open —
    /// the node-level invoker permit pool is the real system ceiling).
    /// Recommended: set to the previous static cap so worst-case behavior is
    /// never worse than the static configuration.
    #[cfg_attr(feature = "bilrost", bilrost(tag(2)))]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    #[cfg_attr(feature = "schema", schema(value_type = Option<u32>, minimum = 1))]
    pub max: Option<NonZeroU32>,

    /// Latency tolerance in permille: how much slower than "usual" (the long
    /// EMA) the current sample may be before the limit shrinks. 1500 = 1.5x.
    /// Default 1500. (Integer permille keeps the wire shape `Eq`/exact.)
    #[cfg_attr(feature = "bilrost", bilrost(tag(3)))]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    #[cfg_attr(feature = "schema", schema(value_type = Option<u32>, minimum = 1))]
    pub tolerance_permille: Option<NonZeroU32>,

    /// Smoothing factor in permille for limit updates (200 = 0.2, bounding
    /// shrink to ~10%/sample). Default 200.
    #[cfg_attr(feature = "bilrost", bilrost(tag(4)))]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    #[cfg_attr(feature = "schema", schema(value_type = Option<u32>, minimum = 1))]
    pub smoothing_permille: Option<NonZeroU32>,
}

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
    /// round-robin: a scope with weight N receives N dispatch slots per
    /// cycle relative to weight-1 groups. `None` means the default weight
    /// of 1. Requires the vqueues scheduler.
    #[cfg_attr(feature = "bilrost", bilrost(tag(2)))]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    #[cfg_attr(feature = "schema", schema(value_type = Option<u32>, minimum = 1))]
    pub scheduling_weight: Option<NonZeroU32>,

    /// Adaptive (Gradient2) concurrency controller configuration. `None`
    /// means no controller. Inactive while [`Self::concurrency`] is set
    /// (static limit takes precedence — the rollback path).
    #[cfg_attr(feature = "bilrost", bilrost(tag(3)))]
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub adaptive_concurrency: Option<AdaptiveConcurrency>,
}

impl UserLimits {
    pub fn new(concurrency: Option<NonZeroU32>) -> Self {
        Self {
            concurrency,
            scheduling_weight: None,
            adaptive_concurrency: None,
        }
    }

    pub fn with_scheduling_weight(mut self, scheduling_weight: Option<NonZeroU32>) -> Self {
        self.scheduling_weight = scheduling_weight;
        self
    }

    pub fn with_adaptive_concurrency(mut self, adaptive: Option<AdaptiveConcurrency>) -> Self {
        self.adaptive_concurrency = adaptive;
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

    /// Wire compatibility for tag(3): pre-adaptive encodings decode with
    /// `adaptive_concurrency = None`; a None adaptive field encodes
    /// byte-identical to the pre-adaptive layout; set fields round-trip
    /// (including the all-defaults empty object).
    #[test]
    fn adaptive_concurrency_wire_compat() {
        let old = UserLimits::new(NonZeroU32::new(100)).with_scheduling_weight(NonZeroU32::new(10));
        let old_bytes = old.encode_to_bytes();
        let decoded = <UserLimits as OwnedMessage>::decode(old_bytes.clone()).unwrap();
        assert_eq!(decoded.adaptive_concurrency, None);

        // None adaptive encodes identically to the pre-adaptive shape
        let new_default =
            UserLimits::new(NonZeroU32::new(100)).with_scheduling_weight(NonZeroU32::new(10));
        assert_eq!(new_default.encode_to_bytes(), old_bytes);

        // empty adaptive object (all defaults) round-trips as Some(default)
        let empty_adaptive =
            UserLimits::new(None).with_adaptive_concurrency(Some(AdaptiveConcurrency::default()));
        let decoded =
            <UserLimits as OwnedMessage>::decode(empty_adaptive.encode_to_bytes()).unwrap();
        assert_eq!(
            decoded.adaptive_concurrency,
            Some(AdaptiveConcurrency::default())
        );

        // fully-populated round-trip
        let full = UserLimits::new(None).with_adaptive_concurrency(Some(AdaptiveConcurrency {
            min: NonZeroU32::new(8),
            max: NonZeroU32::new(400),
            tolerance_permille: NonZeroU32::new(1250),
            smoothing_permille: NonZeroU32::new(300),
        }));
        let decoded = <UserLimits as OwnedMessage>::decode(full.encode_to_bytes()).unwrap();
        assert_eq!(decoded, full);
    }
}
