// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistent, versioned rule book that backs the in-memory [`crate::Rules`] store.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

use restate_clock::rough_ts::RoughTimestamp;
use restate_encoding::BilrostNewType;
use restate_types::base62_util::base62_max_length_for_type;
use restate_types::errors::IdDecodeError;
use restate_types::id_util::{IdDecoder, IdEncoder};
use restate_types::identifiers::ResourceId;
use restate_types::{IdResourceType, Version, Versioned};
use restate_util_string::ReString;

use crate::{RulePattern, RuleUpdate, UserLimits};

/// Hard cap on the number of rules a single rule book may carry.
///
/// Prevents an admin from accidentally producing a book that exceeds the
/// metadata-store value-size limit (~32 MB). At the planned cap of ~100k
/// rules in a book the encoded size stays well under that ceiling. Exposed
/// here as a constant for now — Step 6 makes this configurable via
/// `worker.rule_book.max_rules`.
pub const MAX_RULES_PER_BOOK: usize = 10_000;

/// Deterministic identifier for a rule. Derived from the rule's
/// [`RulePattern`] via xxhash3-64 over the canonical [`fmt::Display`] form,
/// then encoded as `rul_…` using the standard [`IdEncoder`].
///
/// Because the ID is fully determined by the pattern, two rule books that
/// contain the same pattern always agree on its ID. This is what lets the
/// rule book diff key by `RuleId` without ever re-keying.
///
/// # Width: 64-bit hash (vs the 128-bit norm)
///
/// All other restate `ResourceId`s are 128-bit. We deliberately use a 64-bit
/// hash here for rendered-ID brevity (`rul_…` is ~11 base62 chars instead of
/// ~22). The collision probability with k rules is roughly k² / 2⁶⁵:
///
/// |   k    | P(any collision)        |
/// |--------|-------------------------|
/// |  10 k  | ~2.7 × 10⁻¹²            |
/// | 100 k  | ~2.7 × 10⁻¹⁰            |
/// |   1 M  | ~2.7 × 10⁻⁸ (≈ 1 in 37 M)|
///
/// Collisions are detectable at admin write time and surface as a 409 from
/// the create endpoint.
// todo(tillrohrmann) Revisit if 8-byte hash provides enough collision tolerance
#[derive(Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash, BilrostNewType)]
pub struct RuleId(u64);

impl RuleId {
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<&RulePattern<ReString>> for RuleId {
    fn from(pattern: &RulePattern<ReString>) -> Self {
        // The Display form is canonical: same pattern, same string, same hash.
        Self(xxhash_rust::xxh3::xxh3_64(pattern.to_string().as_bytes()))
    }
}

impl ResourceId for RuleId {
    const RAW_BYTES_LEN: usize = size_of::<u64>();
    const RESOURCE_TYPE: IdResourceType = IdResourceType::Rule;

    type StrEncodedLen = generic_array::ConstArrayLength<
        // prefix + separator + version + suffix
        { Self::RESOURCE_TYPE.as_str().len() + 2 + base62_max_length_for_type::<u64>() },
    >;

    fn push_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
        encoder.push_u64(self.0);
    }
}

impl fmt::Display for RuleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut encoder = IdEncoder::new();
        self.push_to_encoder(&mut encoder);
        f.write_str(encoder.as_str())
    }
}

impl fmt::Debug for RuleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for RuleId {
    type Err = IdDecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut decoder = IdDecoder::new(input)?;
        if decoder.resource_type != Self::RESOURCE_TYPE {
            return Err(IdDecodeError::TypeMismatch);
        }
        let raw: u64 = decoder.cursor.decode_next()?;
        if decoder.cursor.remaining() > 0 {
            return Err(IdDecodeError::Length);
        }
        Ok(Self(raw))
    }
}

impl serde::Serialize for RuleId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for RuleId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = <std::borrow::Cow<'de, str> as serde::Deserialize>::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// One persisted rule entry.
///
/// `version` advances on runtime-relevant changes only (`limits`, `disabled`).
/// Edits to `reason` bump only `last_modified` and the enclosing
/// [`RuleBook::version`]. See the file header for the full version-bump
/// contract.
///
/// `disabled` (rather than `enabled`) is the field name so the common case
/// — an active rule — corresponds to bilrost's empty state for `bool`
/// (`false`) and gets omitted from the wire.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct PersistedRule {
    #[bilrost(tag(1))]
    pub version: Version,
    #[bilrost(tag(2))]
    pub pattern: RulePattern<ReString>,
    #[bilrost(tag(3))]
    pub limits: UserLimits,
    /// Operators can leave a reason for why the rule was created/modified.
    #[bilrost(tag(4))]
    pub reason: Option<String>,
    /// Soft tombstone. `true` means the rule is parked and the runtime
    /// should treat it as absent. Default `false` means the rule is
    /// active. Can be used to temporarily disable a rule while keeping
    /// it stored.
    #[bilrost(tag(5))]
    pub disabled: bool,
    /// Wall-clock time of the last write that touched this entry.
    ///
    /// Informational only — runtime semantics depend on `version`, not on
    /// time. [`RoughTimestamp`] is 4 bytes with second precision (vs 8
    /// for [`restate_clock::time::MillisSinceEpoch`]); that is more than
    /// enough for an admin audit trail and `RoughTimestamp::now()` reads
    /// the cached `WallClock::recent_ms`, so minting one on every write
    /// is free.
    #[bilrost(tag(6))]
    pub last_modified: RoughTimestamp,
}

/// The cluster-wide rule book.
///
/// Lives under a single key in the metadata store. The
/// [`crate::rule_store::Rules`] runtime store is materialized from this.
///
/// # Why key by [`RuleId`] and not by [`RulePattern`]
///
/// `RuleId` is the public-facing handle for admin REST/CLI surfaces
/// (`DELETE /rules/{rul_…}`): a short, opaque, URL-safe base62 string that
/// stays stable even if the pattern grammar evolves.
///
/// It comes at the cost of storing an additional `std::mem::size_of::<RuleId>()`
/// (8) bytes per rule entry.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct RuleBook {
    #[bilrost(tag(1))]
    version: Version,
    #[bilrost(tag(2), encoding(map<general, general>))]
    rules: HashMap<RuleId, PersistedRule>,
}

impl RuleBook {
    /// Empty book at [`Version::INVALID`]. The first successful write bumps
    /// it to [`Version::MIN`].
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn rules(&self) -> impl Iterator<Item = (&RuleId, &PersistedRule)> {
        self.rules.iter()
    }

    pub fn get(&self, id: &RuleId) -> Option<&PersistedRule> {
        self.rules.get(id)
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Apply a CRUD-style change to the rule book in place.
    ///
    /// On success the book version is bumped if anything changed. Per-rule
    /// version is bumped only on runtime-relevant changes (`limits`,
    /// `disabled`); reason-only edits leave the per-rule version alone.
    /// Pure no-op patches don't move anything.
    pub fn apply_change(&mut self, id: RuleId, change: RuleChange) -> Result<(), RuleBookError> {
        let new_book_version = self.version.next();
        let now = RoughTimestamp::now();

        match change {
            RuleChange::Create(new_rule) => {
                if self.rules.contains_key(&id) {
                    return Err(RuleBookError::AlreadyExists(id));
                }
                if self.rules.len() >= MAX_RULES_PER_BOOK {
                    return Err(RuleBookError::CapExceeded {
                        cap: MAX_RULES_PER_BOOK,
                    });
                }
                let derived = RuleId::from(&new_rule.pattern);
                if derived != id {
                    return Err(RuleBookError::IdPatternMismatch);
                }
                self.rules.insert(
                    id,
                    PersistedRule {
                        version: new_book_version,
                        pattern: new_rule.pattern,
                        limits: new_rule.limits,
                        reason: new_rule.reason,
                        disabled: new_rule.disabled,
                        last_modified: now,
                    },
                );
                self.version = new_book_version;
                Ok(())
            }
            RuleChange::Patch(patch) => {
                let rule = self.rules.get_mut(&id).ok_or(RuleBookError::NotFound(id))?;

                let mut runtime_changed = false;
                let mut anything_changed = false;

                // limits.action_concurrency
                if let Some(new_val) = patch.limits.action_concurrency.into_target()
                    && rule.limits.action_concurrency != new_val
                {
                    rule.limits.action_concurrency = new_val;
                    runtime_changed = true;
                    anything_changed = true;
                }

                // disabled (toggle the soft tombstone)
                if let Some(new_val) = patch.disabled
                    && rule.disabled != new_val
                {
                    rule.disabled = new_val;
                    runtime_changed = true;
                    anything_changed = true;
                }

                // reason
                if let Some(new_reason) = patch.reason.into_target()
                    && rule.reason != new_reason
                {
                    rule.reason = new_reason;
                    anything_changed = true;
                }

                if runtime_changed {
                    rule.version = rule.version.next();
                }

                if anything_changed {
                    rule.last_modified = now;
                    self.version = new_book_version;
                }
                Ok(())
            }
            RuleChange::Delete => {
                if self.rules.remove(&id).is_none() {
                    return Err(RuleBookError::NotFound(id));
                }
                self.version = new_book_version;
                Ok(())
            }
        }
    }

    /// Compute the runtime-relevant diff against `previous`. Returns the
    /// list of [`RuleUpdate`]s that should be forwarded to the per-PP
    /// `UserLimiter`.
    ///
    /// Projection rules:
    /// - A rule is *visible* in the projection iff it is present and
    ///   `disabled == false`.
    /// - A rule transitioning from invisible → visible emits `Upsert`.
    /// - Visible → invisible (disabled or deleted) emits `Remove`.
    /// - Visible in both with a different per-rule `version` emits
    ///   `Upsert` (the per-rule version is the single source of truth for
    ///   "anything runtime-relevant changed").
    /// - Visible in both with equal per-rule `version` emits nothing.
    ///
    /// `last_modified` and `reason` never produce updates on their own.
    pub fn diff(&self, previous: &Self) -> Vec<RuleUpdate> {
        let mut updates = Vec::new();

        let mut removals: HashSet<_> = previous.rules.keys().collect();

        for (id, current) in &self.rules {
            // id hasn't been removed from current
            removals.remove(&id);

            let current_visible = !current.disabled;
            let prev_visible_version = previous
                .rules
                .get(id)
                .filter(|r| !r.disabled)
                .map(|r| r.version);

            match (prev_visible_version, current_visible) {
                // Was visible, still visible — only emit if version changed.
                (Some(prev_version), true) if prev_version != current.version => {
                    updates.push(RuleUpdate::Upsert {
                        pattern: current.pattern.clone(),
                        limit: current.limits.clone(),
                    });
                }
                (Some(_), true) => {
                    // No version bump → no runtime change.
                }
                // Was visible, now invisible.
                (Some(_), false) => {
                    updates.push(RuleUpdate::Remove {
                        pattern: current.pattern.clone(),
                    });
                }
                // Was invisible, now visible.
                (None, true) => {
                    updates.push(RuleUpdate::Upsert {
                        pattern: current.pattern.clone(),
                        limit: current.limits.clone(),
                    });
                }
                // Was invisible, still invisible.
                (None, false) => {}
            }
        }

        // Removals: ids that were visible in `previous` but absent from `self`.
        for id in removals {
            if let Some(rule) = previous.rules.get(id)
                && !rule.disabled
            {
                updates.push(RuleUpdate::Remove {
                    pattern: rule.pattern.clone(),
                })
            }
        }

        updates
    }

    /// Produce the diff as if `self` were applied on top of an empty book.
    /// Used by consumers that need to seed their runtime state on bootstrap.
    pub fn diff_from_empty(&self) -> Vec<RuleUpdate> {
        self.diff(&Self::empty())
    }

    /// Test/internal helper.
    #[cfg(test)]
    pub fn from_parts(version: Version, rules: HashMap<RuleId, PersistedRule>) -> Self {
        Self { version, rules }
    }
}

/// Patch payload for [`RuleChange::Patch`]. Each field carries the
/// admin's intent for that specific attribute, mirroring RFC 7396 JSON
/// Merge Patch semantics (absent → keep, null → delete, value → set).
///
/// `disabled` is non-optional in the persisted form, so we use
/// `Option<bool>` directly: `None` means keep, `Some(v)` means overwrite.
#[derive(Debug, Default, Clone)]
pub struct RulePatch {
    pub limits: LimitsPatch,
    pub reason: UpdateField<String>,
    pub disabled: Option<bool>,
}

#[derive(Debug, Default, Clone)]
pub struct LimitsPatch {
    pub action_concurrency: UpdateField<NonZeroU64>,
}

/// Body of [`RuleChange::Create`].
#[derive(Debug, Clone)]
pub struct NewRule {
    pub pattern: RulePattern<ReString>,
    pub limits: UserLimits,
    pub reason: Option<String>,
    /// Default `false` (rule is active). Set to `true` to create a parked
    /// rule that is invisible to the runtime until later toggled.
    pub disabled: bool,
}

/// CRUD-style change applied to a [`RuleBook`].
#[derive(Debug, Clone)]
pub enum RuleChange {
    Create(NewRule),
    Patch(RulePatch),
    Delete,
}

/// Sparse-update helper used in [`RulePatch`].
///
/// Maps to RFC 7396 JSON Merge Patch:
/// - `Keep` corresponds to an absent field — leave the existing value as-is.
/// - `Overwrite(v)` corresponds to a JSON value — set the field.
/// - `Delete` corresponds to a JSON `null` — clear the field (only
///   meaningful for fields where `None`/absence is a valid state).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum UpdateField<T> {
    #[default]
    Keep,
    Overwrite(T),
    Delete,
}

impl<T> UpdateField<T> {
    /// Resolve a sparse update into the *new* `Option<T>` value, or `None`
    /// to mean "leave the field unchanged".
    ///
    /// This collapses the three-valued `Keep`/`Overwrite`/`Delete` shape
    /// into the form the writer needs:
    /// - `Some(Some(v))` → set the field to `v`.
    /// - `Some(None)` → clear the field.
    /// - `None` → no change requested.
    pub fn into_target(self) -> Option<Option<T>> {
        match self {
            UpdateField::Keep => None,
            UpdateField::Overwrite(v) => Some(Some(v)),
            UpdateField::Delete => Some(None),
        }
    }
}

/// Errors returned from [`RuleBook::apply_change`].
#[derive(Debug, thiserror::Error)]
pub enum RuleBookError {
    /// Tried to create a rule whose `RuleId` already exists in the book.
    #[error("rule {0} already exists")]
    AlreadyExists(RuleId),
    /// Tried to patch or delete a rule that's not in the book.
    #[error("rule {0} not found")]
    NotFound(RuleId),
    /// Adding the rule would exceed [`MAX_RULES_PER_BOOK`].
    #[error("rule book is full ({cap} rules)")]
    CapExceeded { cap: usize },
    /// `RuleId` provided for a create does not hash from the rule's pattern.
    #[error("rule id does not match pattern hash")]
    IdPatternMismatch,
}

impl Default for RuleBook {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            rules: HashMap::new(),
        }
    }
}

impl Versioned for RuleBook {
    fn version(&self) -> Version {
        self.version
    }
}

// `RulePattern<ReString>` rides through bilrost via its `Display`/`FromStr`
// round-trip — same trick as `InvocationId`. This avoids spreading bilrost
// glue across `Pattern`/`RestrictedValue` and keeps the wire form
// human-readable.
restate_encoding::bilrost_as_display_from_str!(RulePattern<ReString>);

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use bilrost::{Message, OwnedMessage};

    use super::*;

    fn pat(s: &str) -> RulePattern<ReString> {
        s.parse().unwrap()
    }

    #[test]
    fn rule_id_is_deterministic_for_same_pattern() {
        let a = RuleId::from(&pat("scope1/*/tenant1"));
        let b = RuleId::from(&pat("scope1/*/tenant1"));
        assert_eq!(a, b);
    }

    #[test]
    fn rule_id_differs_for_different_patterns() {
        let a = RuleId::from(&pat("scope1/*/tenant1"));
        let b = RuleId::from(&pat("scope1/*/tenant2"));
        assert_ne!(a, b);
    }

    #[test]
    fn rule_id_display_roundtrip() {
        let original = RuleId::from(&pat("scope1/*/tenant1"));
        let s = original.to_string();
        assert!(s.starts_with("rul_"), "encoded id: {s}");
        let parsed: RuleId = s.parse().unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn rule_id_rejects_other_resource_types() {
        let err = "inv_1abc".parse::<RuleId>().unwrap_err();
        assert!(matches!(
            err,
            IdDecodeError::TypeMismatch | IdDecodeError::Length
        ));
    }

    #[test]
    fn empty_rule_book_is_at_invalid_version() {
        let book = RuleBook::empty();
        assert_eq!(book.version(), Version::INVALID);
        assert_eq!(book.rules().next(), None);
    }

    #[test]
    fn rule_book_bilrost_roundtrip() {
        let mut rules = HashMap::new();
        let p1 = pat("*");
        let p2 = pat("scope1/*/tenant1");
        rules.insert(
            RuleId::from(&p1),
            PersistedRule {
                pattern: p1,
                limits: UserLimits {
                    action_concurrency: NonZeroU64::new(1000),
                },
                reason: Some("global default".to_owned()),
                disabled: false,
                last_modified: RoughTimestamp::new(42),
                version: Version::from(1),
            },
        );
        rules.insert(
            RuleId::from(&p2),
            PersistedRule {
                pattern: p2,
                limits: UserLimits {
                    action_concurrency: NonZeroU64::new(10),
                },
                reason: None,
                disabled: true,
                last_modified: RoughTimestamp::new(43),
                version: Version::from(2),
            },
        );
        let book = RuleBook::from_parts(Version::from(2), rules);

        let bytes = book.encode_to_bytes();
        let decoded = RuleBook::decode(bytes).unwrap();
        assert_eq!(book, decoded);
    }

    fn new_rule(pattern: &str, concurrency: u64) -> NewRule {
        NewRule {
            pattern: pat(pattern),
            limits: UserLimits {
                action_concurrency: NonZeroU64::new(concurrency),
            },
            reason: None,
            disabled: false,
        }
    }

    fn id(pattern: &str) -> RuleId {
        RuleId::from(&pat(pattern))
    }

    /// Helper for diff assertions: extract pattern strings and tag.
    fn updates_summary(updates: &[RuleUpdate]) -> Vec<(String, &'static str)> {
        updates
            .iter()
            .map(|u| match u {
                RuleUpdate::Upsert { pattern, .. } => (pattern.to_string(), "upsert"),
                RuleUpdate::Remove { pattern } => (pattern.to_string(), "remove"),
            })
            .collect()
    }

    #[test]
    fn create_inserts_at_book_version() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        assert_eq!(book.version(), Version::from(1));
        let r = book.get(&id("*")).unwrap();
        assert_eq!(r.version, Version::from(1));
        assert!(!r.disabled);
        assert_eq!(r.limits.action_concurrency, NonZeroU64::new(1000));
    }

    #[test]
    fn create_rejects_duplicate_id() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        let err = book
            .apply_change(id("*"), RuleChange::Create(new_rule("*", 2000)))
            .unwrap_err();
        assert!(matches!(err, RuleBookError::AlreadyExists(_)));
    }

    #[test]
    fn create_rejects_id_pattern_mismatch() {
        let mut book = RuleBook::empty();
        let err = book
            .apply_change(id("scope1"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap_err();
        assert!(matches!(err, RuleBookError::IdPatternMismatch));
    }

    #[test]
    fn patch_runtime_change_bumps_per_rule_and_book_versions() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        let v_before_book = book.version();
        let v_before_rule = book.get(&id("*")).unwrap().version;

        book.apply_change(
            id("*"),
            RuleChange::Patch(RulePatch {
                limits: LimitsPatch {
                    action_concurrency: UpdateField::Overwrite(NonZeroU64::new(500).unwrap()),
                },
                ..Default::default()
            }),
        )
        .unwrap();
        let r = book.get(&id("*")).unwrap();
        assert_eq!(r.limits.action_concurrency, NonZeroU64::new(500));
        assert_eq!(r.version, v_before_rule.next());
        assert_eq!(book.version(), v_before_book.next());
    }

    #[test]
    fn patch_reason_only_bumps_book_but_not_rule_version() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        let v_before_book = book.version();
        let v_before_rule = book.get(&id("*")).unwrap().version;

        book.apply_change(
            id("*"),
            RuleChange::Patch(RulePatch {
                reason: UpdateField::Overwrite("vendor X paused".to_owned()),
                ..Default::default()
            }),
        )
        .unwrap();
        let r = book.get(&id("*")).unwrap();
        assert_eq!(r.reason.as_deref(), Some("vendor X paused"));
        assert_eq!(r.version, v_before_rule, "per-rule version must NOT bump");
        assert_eq!(
            book.version(),
            v_before_book.next(),
            "book version must bump"
        );
    }

    #[test]
    fn patch_full_noop_does_not_bump_anything() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        let v_before_book = book.version();
        let v_before_rule = book.get(&id("*")).unwrap().version;

        // All Keep, plus disabled=None means no-op.
        book.apply_change(id("*"), RuleChange::Patch(RulePatch::default()))
            .unwrap();
        let r = book.get(&id("*")).unwrap();
        assert_eq!(r.version, v_before_rule);
        assert_eq!(book.version(), v_before_book);

        // Re-asserting the same values is also a no-op.
        book.apply_change(
            id("*"),
            RuleChange::Patch(RulePatch {
                limits: LimitsPatch {
                    action_concurrency: UpdateField::Overwrite(NonZeroU64::new(1000).unwrap()),
                },
                disabled: Some(false),
                reason: UpdateField::Keep,
            }),
        )
        .unwrap();
        let r = book.get(&id("*")).unwrap();
        assert_eq!(r.version, v_before_rule);
        assert_eq!(book.version(), v_before_book);
    }

    #[test]
    fn patch_clears_optional_fields_via_delete() {
        let mut book = RuleBook::empty();
        book.apply_change(
            id("*"),
            RuleChange::Create(NewRule {
                reason: Some("initial".to_owned()),
                ..new_rule("*", 1000)
            }),
        )
        .unwrap();

        // Clear reason and limits.
        book.apply_change(
            id("*"),
            RuleChange::Patch(RulePatch {
                limits: LimitsPatch {
                    action_concurrency: UpdateField::Delete,
                },
                reason: UpdateField::Delete,
                disabled: None,
            }),
        )
        .unwrap();
        let r = book.get(&id("*")).unwrap();
        assert!(r.limits.action_concurrency.is_none());
        assert!(r.reason.is_none());
    }

    #[test]
    fn patch_disabled_toggle_is_runtime_relevant() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        let v_before_rule = book.get(&id("*")).unwrap().version;

        book.apply_change(
            id("*"),
            RuleChange::Patch(RulePatch {
                disabled: Some(true),
                ..Default::default()
            }),
        )
        .unwrap();
        let r = book.get(&id("*")).unwrap();
        assert!(r.disabled);
        assert_eq!(r.version, v_before_rule.next());
    }

    #[test]
    fn patch_unknown_rule_is_not_found() {
        let mut book = RuleBook::empty();
        let err = book
            .apply_change(id("*"), RuleChange::Patch(RulePatch::default()))
            .unwrap_err();
        assert!(matches!(err, RuleBookError::NotFound(_)));
    }

    #[test]
    fn delete_then_recreate_assigns_strictly_greater_version() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        // bump the rule a few times
        for i in 1u64..=3 {
            book.apply_change(
                id("*"),
                RuleChange::Patch(RulePatch {
                    limits: LimitsPatch {
                        action_concurrency: UpdateField::Overwrite(
                            NonZeroU64::new(1000 + i).unwrap(),
                        ),
                    },
                    ..Default::default()
                }),
            )
            .unwrap();
        }
        let v_before_delete = book.get(&id("*")).unwrap().version;

        book.apply_change(id("*"), RuleChange::Delete).unwrap();
        assert!(book.get(&id("*")).is_none());

        // Recreate.
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        let v_after_create = book.get(&id("*")).unwrap().version;
        assert!(
            v_after_create > v_before_delete,
            "recreated rule's version {v_after_create:?} must exceed pre-delete version {v_before_delete:?}",
        );
    }

    #[test]
    fn delete_unknown_rule_is_not_found() {
        let mut book = RuleBook::empty();
        let err = book.apply_change(id("*"), RuleChange::Delete).unwrap_err();
        assert!(matches!(err, RuleBookError::NotFound(_)));
    }

    // -- diff() tests -------------------------------------------------------

    #[test]
    fn diff_identical_books_is_empty() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        let snapshot = book.clone();
        assert!(book.diff(&snapshot).is_empty());
    }

    #[test]
    fn diff_addition_emits_upsert() {
        let prev = RuleBook::empty();
        let mut next = prev.clone();
        next.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        let updates = next.diff(&prev);
        assert_eq!(updates_summary(&updates), vec![("*".to_owned(), "upsert")]);
    }

    #[test]
    fn diff_modification_emits_upsert() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        let prev = book.clone();
        book.apply_change(
            id("*"),
            RuleChange::Patch(RulePatch {
                limits: LimitsPatch {
                    action_concurrency: UpdateField::Overwrite(NonZeroU64::new(500).unwrap()),
                },
                ..Default::default()
            }),
        )
        .unwrap();
        let updates = book.diff(&prev);
        assert_eq!(updates_summary(&updates), vec![("*".to_owned(), "upsert")]);
        if let RuleUpdate::Upsert { limit, .. } = &updates[0] {
            assert_eq!(limit.action_concurrency, NonZeroU64::new(500));
        }
    }

    #[test]
    fn diff_deletion_emits_remove() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        let prev = book.clone();
        book.apply_change(id("*"), RuleChange::Delete).unwrap();
        let updates = book.diff(&prev);
        assert_eq!(updates_summary(&updates), vec![("*".to_owned(), "remove")]);
    }

    #[test]
    fn diff_disable_emits_remove() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        let prev = book.clone();
        book.apply_change(
            id("*"),
            RuleChange::Patch(RulePatch {
                disabled: Some(true),
                ..Default::default()
            }),
        )
        .unwrap();
        let updates = book.diff(&prev);
        assert_eq!(updates_summary(&updates), vec![("*".to_owned(), "remove")]);
    }

    #[test]
    fn diff_reenable_emits_upsert() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        book.apply_change(
            id("*"),
            RuleChange::Patch(RulePatch {
                disabled: Some(true),
                ..Default::default()
            }),
        )
        .unwrap();
        let prev = book.clone();
        book.apply_change(
            id("*"),
            RuleChange::Patch(RulePatch {
                disabled: Some(false),
                ..Default::default()
            }),
        )
        .unwrap();
        let updates = book.diff(&prev);
        assert_eq!(updates_summary(&updates), vec![("*".to_owned(), "upsert")]);
    }

    #[test]
    fn diff_reason_only_change_emits_nothing() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        let prev = book.clone();
        book.apply_change(
            id("*"),
            RuleChange::Patch(RulePatch {
                reason: UpdateField::Overwrite("audit note".to_owned()),
                ..Default::default()
            }),
        )
        .unwrap();
        assert!(book.diff(&prev).is_empty());
    }

    #[test]
    fn diff_disabled_rule_in_both_books_emits_nothing() {
        let mut book = RuleBook::empty();
        book.apply_change(
            id("*"),
            RuleChange::Create(NewRule {
                disabled: true,
                ..new_rule("*", 1000)
            }),
        )
        .unwrap();
        let snapshot = book.clone();
        assert!(book.diff(&snapshot).is_empty());
    }

    #[test]
    fn diff_delete_and_recreate_with_same_content_emits_redundant_upsert() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        let prev = book.clone();
        book.apply_change(id("*"), RuleChange::Delete).unwrap();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        // Per the design notes: redundant Upsert is acceptable; the
        // per-rule version moved forward even though user-visible content
        // is unchanged.
        let updates = book.diff(&prev);
        assert_eq!(updates_summary(&updates), vec![("*".to_owned(), "upsert")]);
    }

    #[test]
    fn diff_from_empty_seeds_runtime_with_visible_rules_only() {
        let mut book = RuleBook::empty();
        book.apply_change(id("*"), RuleChange::Create(new_rule("*", 1000)))
            .unwrap();
        book.apply_change(
            id("scope1/*/tenant1"),
            RuleChange::Create(NewRule {
                disabled: true,
                ..new_rule("scope1/*/tenant1", 10)
            }),
        )
        .unwrap();
        let updates = book.diff_from_empty();
        // Only the visible (`disabled: false`) rule shows up.
        assert_eq!(updates_summary(&updates), vec![("*".to_owned(), "upsert")]);
    }
}
