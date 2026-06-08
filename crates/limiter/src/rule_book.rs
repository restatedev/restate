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
use std::sync::Arc;

use restate_clock::time::MillisSinceEpoch;
use restate_types::{Version, Versioned};
use restate_util_string::ReString;

use crate::{RulePattern, RuleUpdate, UserLimits};

/// Hard cap on the number of rules a single rule book may carry.
///
/// Prevents an admin from accidentally producing a book that exceeds the
/// metadata-store value-size limit (~32 MB). A configurable knob will
/// replace this constant once the user-facing API stabilizes.
pub const MAX_RULES_PER_BOOK: usize = 10_000;

/// One persisted rule entry. Keyed by its [`RulePattern`] in the
/// owning [`RuleBook`].
///
/// `version` advances on runtime-relevant changes only (`limits`,
/// `disabled`). Edits to `description` bump the enclosing [`RuleBook::version`].
/// See [`RuleBook::apply_change`] for the full version-bump contract.
///
/// `disabled` (rather than `enabled`) is the field name so the common case
/// — an active rule — corresponds to bilrost's empty state for `bool`
/// (`false`) and gets omitted from the wire.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct PersistedRule {
    /// Per-rule version. Advances on runtime-relevant changes
    /// (`limits`, `disabled`); reason-only edits leave it alone. See
    /// [`RuleBook::apply_changes`] for the full bump contract.
    #[bilrost(tag(1))]
    pub version: Version,
    #[bilrost(tag(3))]
    pub limits: UserLimits,
    /// Free-form operator-supplied description for the rule.
    #[bilrost(tag(4))]
    pub description: Option<String>,
    /// Soft tombstone. `true` parks the rule so the runtime treats it
    /// as absent without removing the persisted entry.
    #[bilrost(tag(5))]
    pub disabled: bool,
    /// Wall-clock time of the last write that touched this entry.
    /// Informational only — runtime semantics depend on `version`.
    #[bilrost(tag(6))]
    pub last_modified: MillisSinceEpoch,
}

/// Fire-and-forget callback that pushes a freshly written rule book
/// into a co-located worker's `RuleBookCache`. `None` on admin-only
/// nodes; the worker then learns about updates via its metadata-store
/// poll loop instead.
///
/// Takes the book by value: the cache only allocates an `Arc` for it
/// on the newer-version branch, so admin handlers don't have to
/// pre-wrap their result.
pub trait RuleBookObserver: Send + Sync {
    /// Notify a newly observed rule book.
    fn notify_observed(&self, rule_book: RuleBook);

    /// Obtain the last known rule book
    fn get(&self) -> Arc<RuleBook>;
}

/// The cluster-wide rule book.
///
/// Lives under a single key in the metadata store. The
/// [`crate::rule_store::Rules`] runtime store is materialized from this.
///
/// Rules are keyed by their [`RulePattern`] so the pattern is the single
/// source of truth for a rule's identity.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct RuleBook {
    #[bilrost(tag(1))]
    version: Version,
    #[bilrost(tag(2), encoding(map<general, general>))]
    rules: HashMap<RulePattern<ReString>, PersistedRule>,
}

impl RuleBook {
    /// Empty book at [`Version::INVALID`]. The first successful write bumps
    /// it to [`Version::MIN`].
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&RulePattern<ReString>, &PersistedRule)> {
        self.rules.iter()
    }

    pub fn get(&self, pattern: &RulePattern<ReString>) -> Option<&PersistedRule> {
        self.rules.get(pattern)
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Apply a batch of CRUD-style changes to the rule book atomically.
    ///
    /// Changes are processed in iteration order. The batch is
    /// all-or-nothing: every precondition and the [`MAX_RULES_PER_BOOK`]
    /// cap are checked against the *evolving* state of the batch (so a
    /// rule deleted earlier in the batch counts as absent for later
    /// steps). On error nothing is mutated; on success all changes are
    /// committed in one go.
    ///
    /// The book version is bumped by exactly one if the batch produces
    /// any state change, and is unchanged otherwise. Per-rule versions
    /// are bumped only on runtime-relevant changes (`limits`,
    /// `disabled`); reason-only edits leave the per-rule version alone.
    /// Pure no-op upserts and deletes of already-absent rules don't move
    /// anything.
    pub fn apply_changes<I>(&mut self, changes: I) -> Result<(), RuleBookError>
    where
        I: IntoIterator<Item = (RulePattern<ReString>, RuleChange)>,
    {
        // Per-touched-pattern post-batch state. Some(rule) = insert/replace
        // in self.rules; None = remove from self.rules. Patterns absent
        // from this map are read straight from self.rules during the walk
        // (so memory cost is O(touched), not O(book)).
        let mut staged: HashMap<RulePattern<ReString>, Option<PersistedRule>> = HashMap::new();
        let mut sim_count = self.rules.len();
        let new_book_version = self.version.next();
        let now = MillisSinceEpoch::now();

        for (pattern, change) in changes {
            let resolved: Option<&PersistedRule> = match staged.get(&pattern) {
                Some(slot) => slot.as_ref(),
                None => self.rules.get(&pattern),
            };
            let actual_version = resolved.map(|r| r.version);

            match change {
                RuleChange::Upsert(upsert) => {
                    if !check_precondition(upsert.precondition, actual_version) {
                        return Err(RuleBookError::PreconditionFailed {
                            pattern,
                            precondition: upsert.precondition,
                            actual: actual_version,
                        });
                    }
                    match resolved {
                        None => {
                            if sim_count >= MAX_RULES_PER_BOOK {
                                return Err(RuleBookError::CapExceeded {
                                    cap: MAX_RULES_PER_BOOK,
                                });
                            }
                            sim_count += 1;
                            staged.insert(
                                pattern,
                                Some(PersistedRule {
                                    version: new_book_version,
                                    limits: upsert.limits,
                                    description: upsert.description,
                                    disabled: upsert.disabled,
                                    last_modified: now,
                                }),
                            );
                        }
                        Some(existing) => {
                            let runtime_changed = existing.limits != upsert.limits
                                || existing.disabled != upsert.disabled;
                            let anything_changed =
                                runtime_changed || existing.description != upsert.description;
                            if anything_changed {
                                let (new_version, last_modified) = if runtime_changed {
                                    (existing.version.next(), now)
                                } else {
                                    (existing.version, existing.last_modified)
                                };
                                staged.insert(
                                    pattern,
                                    Some(PersistedRule {
                                        version: new_version,
                                        limits: upsert.limits,
                                        description: upsert.description,
                                        disabled: upsert.disabled,
                                        last_modified,
                                    }),
                                );
                            }
                        }
                    }
                }
                RuleChange::Delete { precondition } => {
                    if !check_precondition(precondition, actual_version) {
                        return Err(RuleBookError::PreconditionFailed {
                            pattern,
                            precondition,
                            actual: actual_version,
                        });
                    }
                    if resolved.is_some() {
                        sim_count -= 1;
                        staged.insert(pattern, None);
                    }
                }
            }
        }

        if staged.is_empty() {
            return Ok(());
        }

        for (pattern, rule) in staged {
            match rule {
                Some(rule) => {
                    self.rules.insert(pattern, rule);
                }
                None => {
                    self.rules.remove(&pattern);
                }
            }
        }
        self.version = new_book_version;

        Ok(())
    }

    /// Convenience wrapper around [`Self::apply_changes`] for a single
    /// change.
    pub fn apply_change(
        &mut self,
        pattern: RulePattern<ReString>,
        change: RuleChange,
    ) -> Result<(), RuleBookError> {
        self.apply_changes(std::iter::once((pattern, change)))
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
    /// `last_modified` and `description` never produce updates on their own.
    pub fn diff(&self, previous: &Self) -> Box<[RuleUpdate]> {
        let mut updates = Vec::new();

        let mut removals: HashSet<_> = previous.rules.keys().collect();

        for (pattern, current) in &self.rules {
            // pattern hasn't been removed from self
            removals.remove(&pattern);

            let current_visible = !current.disabled;
            let prev_visible_version = previous
                .rules
                .get(pattern)
                .filter(|r| !r.disabled)
                .map(|r| r.version);

            match (prev_visible_version, current_visible) {
                // Was visible, still visible — only emit if version changed.
                (Some(prev_version), true) if prev_version != current.version => {
                    updates.push(RuleUpdate::Upsert {
                        pattern: pattern.clone(),
                        limit: current.limits.clone(),
                    });
                }
                (Some(_), true) => {
                    // No version bump → no runtime change.
                }
                // Was visible, now invisible.
                (Some(_), false) => {
                    updates.push(RuleUpdate::Remove {
                        pattern: pattern.clone(),
                    });
                }
                // Was invisible, now visible.
                (None, true) => {
                    updates.push(RuleUpdate::Upsert {
                        pattern: pattern.clone(),
                        limit: current.limits.clone(),
                    });
                }
                // Was invisible, still invisible.
                (None, false) => {}
            }
        }

        // Removals: patterns that were visible in `previous` but absent from `self`.
        for pattern in removals {
            if let Some(rule) = previous.rules.get(pattern)
                && !rule.disabled
            {
                updates.push(RuleUpdate::Remove {
                    pattern: pattern.clone(),
                })
            }
        }

        updates.into_boxed_slice()
    }

    /// Produce the diff as if `self` were applied on top of an empty book.
    /// Used by consumers that need to seed their runtime state on bootstrap.
    pub fn diff_from_empty(&self) -> Box<[RuleUpdate]> {
        self.diff(&Self::empty())
    }

    /// Encode the rule book as bilrost-encoded bytes. Used by callers that
    /// want to embed the book in another wire format opaquely (e.g. the
    /// `Command::UpsertRuleBook` envelope) without taking a direct
    /// dependency on the `bilrost` crate.
    pub fn bilrost_encode_to_bytes(&self) -> bytes::Bytes {
        bilrost::Message::encode_to_bytes(self)
    }

    /// Decode a rule book from bilrost-encoded bytes. Inverse of
    /// [`Self::bilrost_encode_to_bytes`].
    pub fn bilrost_decode<B: bytes::Buf>(buf: B) -> Result<Self, bilrost::DecodeError> {
        <Self as bilrost::OwnedMessage>::decode(buf)
    }

    /// Test/internal helper.
    #[cfg(test)]
    pub fn from_parts(
        version: Version,
        rules: HashMap<RulePattern<ReString>, PersistedRule>,
    ) -> Self {
        Self { version, rules }
    }
}

/// Body of [`RuleChange::Upsert`]. Carries a fully-specified rule body
/// plus an optional optimistic-concurrency [`Precondition`].
#[derive(Debug, Clone)]
pub struct RuleUpsert {
    pub limits: UserLimits,
    pub description: Option<String>,
    /// Default `false` (rule is active). Set to `true` to write a parked
    /// rule that is invisible to the runtime until later toggled.
    pub disabled: bool,
    pub precondition: Precondition,
}

/// CRUD-style change applied to a [`RuleBook`].
#[derive(Debug, Clone)]
pub enum RuleChange {
    Upsert(RuleUpsert),
    Delete { precondition: Precondition },
}

/// Optimistic-concurrency guard for a [`RuleChange`].
///
/// - [`Precondition::None`] applies the change unconditionally.
/// - [`Precondition::Matches`] requires the rule to be present at the
///   given version; otherwise the change is rejected with
///   [`RuleBookError::PreconditionFailed`].
/// - [`Precondition::DoesNotExist`] requires the rule to be absent.
///   Combined with `Upsert` this is a pure insert.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[cfg_attr(
    feature = "serde",
    serde(tag = "type", content = "version", rename_all = "snake_case")
)]
pub enum Precondition {
    #[default]
    None,
    Matches(Version),
    DoesNotExist,
}

fn check_precondition(precondition: Precondition, actual: Option<Version>) -> bool {
    match precondition {
        Precondition::None => true,
        Precondition::Matches(v) => actual == Some(v),
        Precondition::DoesNotExist => actual.is_none(),
    }
}

/// Errors returned from [`RuleBook::apply_change`].
#[derive(Debug, thiserror::Error)]
pub enum RuleBookError {
    /// Inserting the rule would exceed [`MAX_RULES_PER_BOOK`].
    #[error("rule book is full ({cap} rules)")]
    CapExceeded { cap: usize },
    /// The supplied [`Precondition`] did not hold against the rule's
    /// actual state. `actual = None` means the rule was absent.
    #[error(
        "rule precondition {precondition:?} for pattern {pattern} failed (actual version: {actual:?})"
    )]
    PreconditionFailed {
        pattern: RulePattern<ReString>,
        precondition: Precondition,
        actual: Option<Version>,
    },
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

mod storage {
    use bytes::BytesMut;

    use restate_types::storage::{
        StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError,
        decode, encode,
    };

    use super::RuleBook;

    impl StorageEncode for RuleBook {
        fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
            encode::encode_bilrost(self, buf)
        }

        fn default_codec(&self) -> StorageCodecKind {
            StorageCodecKind::Bilrost
        }
    }

    impl StorageDecode for RuleBook {
        fn decode<B: bytes::Buf>(
            buf: &mut B,
            kind: StorageCodecKind,
        ) -> Result<Self, StorageDecodeError> {
            assert_eq!(kind, StorageCodecKind::Bilrost);
            decode::decode_bilrost(buf)
        }
    }
}

// `RulePattern<ReString>` rides through bilrost via its `Display`/`FromStr`
// round-trip — same trick as `InvocationId`. This avoids spreading bilrost
// glue across `Pattern`/`RestrictedValue` and keeps the wire form
// human-readable.
restate_encoding::bilrost_as_display_from_str!(RulePattern<ReString>);

#[cfg(test)]
mod tests {
    use bilrost::{Message, OwnedMessage};
    use std::num::NonZeroU32;

    use super::*;

    fn pat(s: &str) -> RulePattern<ReString> {
        s.parse().unwrap()
    }

    fn upsert(concurrency: u32) -> RuleUpsert {
        RuleUpsert {
            limits: UserLimits {
                action_concurrency: NonZeroU32::new(concurrency),
            },
            description: None,
            disabled: false,
            precondition: Precondition::None,
        }
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
    fn empty_rule_book_is_at_invalid_version() {
        let book = RuleBook::empty();
        assert_eq!(book.version(), Version::INVALID);
        assert_eq!(book.iter().next(), None);
    }

    #[test]
    fn rule_book_bilrost_roundtrip() {
        let mut rules = HashMap::new();
        rules.insert(
            pat("*"),
            PersistedRule {
                limits: UserLimits {
                    action_concurrency: NonZeroU32::new(1000),
                },
                description: Some("global default".to_owned()),
                disabled: false,
                last_modified: MillisSinceEpoch::new(42),
                version: Version::from(1),
            },
        );
        rules.insert(
            pat("scope1/*/tenant1"),
            PersistedRule {
                limits: UserLimits {
                    action_concurrency: NonZeroU32::new(10),
                },
                description: None,
                disabled: true,
                last_modified: MillisSinceEpoch::new(43),
                version: Version::from(2),
            },
        );
        let book = RuleBook::from_parts(Version::from(2), rules);

        let bytes = book.encode_to_bytes();
        let decoded = RuleBook::decode(bytes).unwrap();
        assert_eq!(book, decoded);
    }

    // -- apply_change(Upsert) -----------------------------------------------

    #[test]
    fn upsert_inserts_at_book_version() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        assert_eq!(book.version(), Version::from(1));
        let r = book.get(&pat("*")).unwrap();
        assert_eq!(r.version, Version::from(1));
        assert!(!r.disabled);
        assert_eq!(r.limits.action_concurrency, NonZeroU32::new(1000));
    }

    #[test]
    fn upsert_runtime_change_bumps_per_rule_and_book_versions() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let v_before_book = book.version();
        let v_before_rule = book.get(&pat("*")).unwrap().version;

        book.apply_change(pat("*"), RuleChange::Upsert(upsert(500)))
            .unwrap();
        let r = book.get(&pat("*")).unwrap();
        assert_eq!(r.limits.action_concurrency, NonZeroU32::new(500));
        assert_eq!(r.version, v_before_rule.next());
        assert_eq!(book.version(), v_before_book.next());
    }

    #[test]
    fn upsert_reason_only_change_bumps_book_but_not_rule_version() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let v_before_book = book.version();
        let v_before_rule = book.get(&pat("*")).unwrap().version;

        book.apply_change(
            pat("*"),
            RuleChange::Upsert(RuleUpsert {
                description: Some("vendor X paused".to_owned()),
                ..upsert(1000)
            }),
        )
        .unwrap();
        let r = book.get(&pat("*")).unwrap();
        assert_eq!(r.description.as_deref(), Some("vendor X paused"));
        assert_eq!(r.version, v_before_rule, "per-rule version must NOT bump");
        assert_eq!(
            book.version(),
            v_before_book.next(),
            "book version must bump"
        );
    }

    #[test]
    fn upsert_full_noop_does_not_bump_anything() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let v_before_book = book.version();
        let v_before_rule = book.get(&pat("*")).unwrap().version;

        // Re-asserting the same values is a no-op.
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let r = book.get(&pat("*")).unwrap();
        assert_eq!(r.version, v_before_rule);
        assert_eq!(book.version(), v_before_book);
    }

    #[test]
    fn upsert_disabled_toggle_is_runtime_relevant() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let v_before_rule = book.get(&pat("*")).unwrap().version;

        book.apply_change(
            pat("*"),
            RuleChange::Upsert(RuleUpsert {
                disabled: true,
                ..upsert(1000)
            }),
        )
        .unwrap();
        let r = book.get(&pat("*")).unwrap();
        assert!(r.disabled);
        assert_eq!(r.version, v_before_rule.next());
    }

    // -- apply_change(Upsert) with Precondition -----------------------------

    #[test]
    fn upsert_does_not_exist_inserts_when_absent() {
        let mut book = RuleBook::empty();
        book.apply_change(
            pat("*"),
            RuleChange::Upsert(RuleUpsert {
                precondition: Precondition::DoesNotExist,
                ..upsert(1000)
            }),
        )
        .unwrap();
        assert!(book.get(&pat("*")).is_some());
    }

    #[test]
    fn upsert_does_not_exist_rejects_when_present() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let actual = book.get(&pat("*")).unwrap().version;
        let err = book
            .apply_change(
                pat("*"),
                RuleChange::Upsert(RuleUpsert {
                    precondition: Precondition::DoesNotExist,
                    ..upsert(2000)
                }),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            RuleBookError::PreconditionFailed {
                pattern,
                precondition: Precondition::DoesNotExist,
                actual: Some(a),
            } if a == actual && pattern == pat("*")
        ));
    }

    #[test]
    fn upsert_matches_succeeds_when_version_matches() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let v = book.get(&pat("*")).unwrap().version;
        book.apply_change(
            pat("*"),
            RuleChange::Upsert(RuleUpsert {
                precondition: Precondition::Matches(v),
                ..upsert(500)
            }),
        )
        .unwrap();
        assert_eq!(
            book.get(&pat("*")).unwrap().limits.action_concurrency,
            NonZeroU32::new(500)
        );
    }

    #[test]
    fn upsert_matches_rejects_when_version_differs() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let actual = book.get(&pat("*")).unwrap().version;
        let stale = Version::from(u32::from(actual) + 7);
        let err = book
            .apply_change(
                pat("*"),
                RuleChange::Upsert(RuleUpsert {
                    precondition: Precondition::Matches(stale),
                    ..upsert(500)
                }),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            RuleBookError::PreconditionFailed {
                pattern,
                precondition: Precondition::Matches(e),
                actual: Some(a),
            } if e == stale && a == actual && pattern == pat("*")
        ));
    }

    #[test]
    fn upsert_matches_on_absent_rule_rejects() {
        let mut book = RuleBook::empty();
        let err = book
            .apply_change(
                pat("*"),
                RuleChange::Upsert(RuleUpsert {
                    precondition: Precondition::Matches(Version::from(1)),
                    ..upsert(1000)
                }),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            RuleBookError::PreconditionFailed {
                precondition: Precondition::Matches(_),
                actual: None,
                pattern: _pattern,
            }
        ));
    }

    // -- apply_change(Delete) ----------------------------------------------

    #[test]
    fn delete_then_recreate_assigns_strictly_greater_version() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        // bump the rule a few times
        for i in 1u32..=3 {
            book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000 + i)))
                .unwrap();
        }
        let v_before_delete = book.get(&pat("*")).unwrap().version;

        book.apply_change(
            pat("*"),
            RuleChange::Delete {
                precondition: Precondition::None,
            },
        )
        .unwrap();
        assert!(book.get(&pat("*")).is_none());

        // Recreate.
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let v_after_create = book.get(&pat("*")).unwrap().version;
        assert!(
            v_after_create > v_before_delete,
            "recreated rule's version {v_after_create:?} must exceed pre-delete version {v_before_delete:?}",
        );
    }

    #[test]
    fn delete_matches_succeeds_when_version_matches() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let v = book.get(&pat("*")).unwrap().version;
        book.apply_change(
            pat("*"),
            RuleChange::Delete {
                precondition: Precondition::Matches(v),
            },
        )
        .unwrap();
        assert!(book.get(&pat("*")).is_none());
    }

    #[test]
    fn delete_matches_rejects_when_version_differs() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let actual = book.get(&pat("*")).unwrap().version;
        let stale = Version::from(u32::from(actual) + 7);
        let err = book
            .apply_change(
                pat("*"),
                RuleChange::Delete {
                    precondition: Precondition::Matches(stale),
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            RuleBookError::PreconditionFailed {
                pattern,
                precondition: Precondition::Matches(e),
                actual: Some(a),
            } if e == stale && a == actual && pattern == pat("*")
        ));
        // Rule must still be present because the precondition guarded the delete.
        assert!(book.get(&pat("*")).is_some());
    }

    #[test]
    fn delete_matches_on_absent_rule_rejects() {
        let mut book = RuleBook::empty();
        let err = book
            .apply_change(
                pat("*"),
                RuleChange::Delete {
                    precondition: Precondition::Matches(Version::from(1)),
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            RuleBookError::PreconditionFailed {
                precondition: Precondition::Matches(_),
                actual: None,
                pattern: _pattern,
            }
        ));
    }

    #[test]
    fn delete_does_not_exist_rejects_when_present() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let actual = book.get(&pat("*")).unwrap().version;
        let err = book
            .apply_change(
                pat("*"),
                RuleChange::Delete {
                    precondition: Precondition::DoesNotExist,
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            RuleBookError::PreconditionFailed {
                pattern,
                precondition: Precondition::DoesNotExist,
                actual: Some(a),
            } if a == actual && pattern == pat("*")
        ));
        // Rule untouched.
        assert!(book.get(&pat("*")).is_some());
    }

    // -- apply_changes (batch) ----------------------------------------------

    #[test]
    fn batch_applies_independent_changes() {
        let mut book = RuleBook::empty();
        let v_before = book.version();
        book.apply_changes([
            (pat("a"), RuleChange::Upsert(upsert(1))),
            (pat("b"), RuleChange::Upsert(upsert(2))),
            (pat("c"), RuleChange::Upsert(upsert(3))),
        ])
        .unwrap();
        // Three inserts but only one book version bump.
        assert_eq!(book.version(), v_before.next());
        assert_eq!(book.len(), 3);
        // All inserts share the post-batch book version.
        let post = book.version();
        for p in ["a", "b", "c"] {
            assert_eq!(book.get(&pat(p)).unwrap().version, post);
        }
    }

    #[test]
    fn batch_aborts_atomically_on_precondition_failure() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("a"), RuleChange::Upsert(upsert(1)))
            .unwrap();
        let snapshot = book.clone();

        // Second change should fail (DoesNotExist on present rule).
        let err = book
            .apply_changes([
                (pat("b"), RuleChange::Upsert(upsert(2))),
                (
                    pat("a"),
                    RuleChange::Upsert(RuleUpsert {
                        precondition: Precondition::DoesNotExist,
                        ..upsert(99)
                    }),
                ),
            ])
            .unwrap_err();
        assert!(matches!(
            err,
            RuleBookError::PreconditionFailed {
                precondition: Precondition::DoesNotExist,
                ..
            }
        ));
        // Book is byte-identical to the snapshot — first upsert was rolled back.
        assert_eq!(book, snapshot);
    }

    // -- diff() tests -------------------------------------------------------

    #[test]
    fn diff_identical_books_is_empty() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let snapshot = book.clone();
        assert!(book.diff(&snapshot).is_empty());
    }

    #[test]
    fn diff_addition_emits_upsert() {
        let prev = RuleBook::empty();
        let mut next = prev.clone();
        next.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let updates = next.diff(&prev);
        assert_eq!(updates_summary(&updates), vec![("*".to_owned(), "upsert")]);
    }

    #[test]
    fn diff_modification_emits_upsert() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let prev = book.clone();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(500)))
            .unwrap();
        let updates = book.diff(&prev);
        assert_eq!(updates_summary(&updates), vec![("*".to_owned(), "upsert")]);
        if let RuleUpdate::Upsert { limit, .. } = &updates[0] {
            assert_eq!(limit.action_concurrency, NonZeroU32::new(500));
        }
    }

    #[test]
    fn diff_deletion_emits_remove() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let prev = book.clone();
        book.apply_change(
            pat("*"),
            RuleChange::Delete {
                precondition: Precondition::None,
            },
        )
        .unwrap();
        let updates = book.diff(&prev);
        assert_eq!(updates_summary(&updates), vec![("*".to_owned(), "remove")]);
    }

    #[test]
    fn diff_disable_emits_remove() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let prev = book.clone();
        book.apply_change(
            pat("*"),
            RuleChange::Upsert(RuleUpsert {
                disabled: true,
                ..upsert(1000)
            }),
        )
        .unwrap();
        let updates = book.diff(&prev);
        assert_eq!(updates_summary(&updates), vec![("*".to_owned(), "remove")]);
    }

    #[test]
    fn diff_reenable_emits_upsert() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        book.apply_change(
            pat("*"),
            RuleChange::Upsert(RuleUpsert {
                disabled: true,
                ..upsert(1000)
            }),
        )
        .unwrap();
        let prev = book.clone();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let updates = book.diff(&prev);
        assert_eq!(updates_summary(&updates), vec![("*".to_owned(), "upsert")]);
    }

    #[test]
    fn diff_description_only_change_emits_nothing() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        let prev = book.clone();
        book.apply_change(
            pat("*"),
            RuleChange::Upsert(RuleUpsert {
                description: Some("audit note".to_owned()),
                ..upsert(1000)
            }),
        )
        .unwrap();
        assert!(book.diff(&prev).is_empty());
    }

    #[test]
    fn diff_disabled_rule_in_both_books_emits_nothing() {
        let mut book = RuleBook::empty();
        book.apply_change(
            pat("*"),
            RuleChange::Upsert(RuleUpsert {
                disabled: true,
                ..upsert(1000)
            }),
        )
        .unwrap();
        let snapshot = book.clone();
        assert!(book.diff(&snapshot).is_empty());
    }

    #[test]
    fn diff_from_empty_seeds_runtime_with_visible_rules_only() {
        let mut book = RuleBook::empty();
        book.apply_change(pat("*"), RuleChange::Upsert(upsert(1000)))
            .unwrap();
        book.apply_change(
            pat("scope1/*/tenant1"),
            RuleChange::Upsert(RuleUpsert {
                disabled: true,
                ..upsert(10)
            }),
        )
        .unwrap();
        let updates = book.diff_from_empty();
        // Only the visible (`disabled: false`) rule shows up.
        assert_eq!(updates_summary(&updates), vec![("*".to_owned(), "upsert")]);
    }
}
