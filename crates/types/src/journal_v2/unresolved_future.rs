// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    collections::HashSet,
    fmt::{self, Debug},
};

use itertools::{Itertools, Position};
use serde::{Deserialize, Serialize};

use crate::journal_v2::{NotificationId, raw::RawNotificationResultVariant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CombinatorType {
    // Should be treated as FirstCompleted
    // Unknown combinator result can be flipped in the sdk
    // It also the default combinator for SuspendedV2 message.
    Unknown,
    /// Resolve as soon as any one child future completes with success, or with failure (same as JS Promise.race).
    FirstCompleted,
    /// Wait for every child to complete, regardless of success or failure (same as JS Promise.allSettled).
    AllCompleted,
    /// Resolve on the first success; fail only if all children fail (same as JS Promise.any).
    FirstSucceededOrAllFailed,
    /// Resolve when all children succeed; short-circuit on the first failure (same as JS Promise.all).
    AllSucceededOrFirstFailed,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum UnresolvedFuture {
    Single(NotificationId),
    FirstCompleted(Vec<UnresolvedFuture>),
    AllCompleted(Vec<UnresolvedFuture>),
    FirstSucceededOrAllFailed(Vec<UnresolvedFuture>),
    AllSucceededOrFirstFailed(Vec<UnresolvedFuture>),
    Unknown(Vec<UnresolvedFuture>),
}

impl UnresolvedFuture {
    #[cfg(any(test, feature = "test-util"))]
    pub fn empty() -> Self {
        Self::Unknown(Vec::default())
    }

    pub fn builder(combinator: CombinatorType) -> UnresolvedFutureBuilder {
        UnresolvedFutureBuilder::new(combinator)
    }

    pub fn unknown_from_iter<I, T>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<UnresolvedFuture>,
    {
        Self::Unknown(iter.into_iter().map(Into::into).collect())
    }

    pub fn is_empty(&self) -> bool {
        let inner = match self {
            Self::Single(_) => return false,
            Self::Unknown(inner)
            | Self::FirstCompleted(inner)
            | Self::AllCompleted(inner)
            | Self::FirstSucceededOrAllFailed(inner)
            | Self::AllSucceededOrFirstFailed(inner) => inner,
        };

        inner.iter().all(|f| f.is_empty())
    }

    pub fn split(
        self,
    ) -> (
        CombinatorType,
        HashSet<NotificationId>,
        Vec<UnresolvedFuture>,
    ) {
        let (combinator, mut inner) = match self {
            Self::Single(notification) => {
                return (
                    CombinatorType::FirstCompleted,
                    HashSet::from([notification]),
                    Vec::default(),
                );
            }
            Self::FirstCompleted(inner) => (CombinatorType::FirstCompleted, inner),
            Self::AllCompleted(inner) => (CombinatorType::AllCompleted, inner),
            Self::FirstSucceededOrAllFailed(inner) => {
                (CombinatorType::FirstSucceededOrAllFailed, inner)
            }
            Self::AllSucceededOrFirstFailed(inner) => {
                (CombinatorType::AllSucceededOrFirstFailed, inner)
            }
            Self::Unknown(inner) => {
                let mut notifications = HashSet::new();
                for nested in inner {
                    nested.flatten_inner(&mut notifications);
                }
                return (CombinatorType::Unknown, notifications, Vec::default());
            }
        };

        let mut notifications = HashSet::new();
        let mut i = 0;
        while i < inner.len() {
            if matches!(inner[i], Self::Single(_)) {
                let notification = inner.swap_remove(i);
                match notification {
                    Self::Single(notification) => notifications.insert(notification),
                    _ => unreachable!(),
                };
                continue;
            }

            i += 1;
        }
        (combinator, notifications, inner)
    }

    pub fn flatten(&self) -> HashSet<NotificationId> {
        let mut set = HashSet::default();
        self.flatten_inner(&mut set);
        set
    }

    fn flatten_inner(&self, set: &mut HashSet<NotificationId>) {
        match self {
            Self::Single(notification) => {
                set.insert(notification.clone());
            }
            Self::FirstCompleted(futures)
            | Self::AllCompleted(futures)
            | Self::AllSucceededOrFirstFailed(futures)
            | Self::FirstSucceededOrAllFailed(futures)
            | Self::Unknown(futures) => {
                for nested in futures {
                    nested.flatten_inner(set);
                }
            }
        }
    }

    fn resolve_inner(
        &mut self,
        notification_id: &NotificationId,
        result: RawNotificationResultVariant,
    ) -> ResolveResult {
        match self {
            Self::Single(inner) => {
                if inner == notification_id {
                    result.into()
                } else {
                    ResolveResult::Pending
                }
            }
            Self::Unknown(futures) | Self::FirstCompleted(futures) => {
                if futures
                    .iter_mut()
                    .any(|f| f.resolve_inner(notification_id, result).is_completed())
                {
                    ResolveResult::Success
                } else {
                    ResolveResult::Pending
                }
            }
            Self::AllCompleted(futures) => {
                futures.retain_mut(|f| f.resolve_inner(notification_id, result).is_pending());

                if futures.is_empty() {
                    ResolveResult::Success
                } else {
                    ResolveResult::Pending
                }
            }
            Self::FirstSucceededOrAllFailed(futures) => {
                let mut i = 0;
                while i < futures.len() {
                    match futures[i].resolve_inner(notification_id, result) {
                        ResolveResult::Success => {
                            return ResolveResult::Success;
                        }
                        ResolveResult::Failure => {
                            futures.swap_remove(i);
                        }
                        ResolveResult::Pending => i += 1,
                    }
                }

                // if all nested futures has been evicted
                // without success, then resolve as failure.
                // otherwise, pending.
                if futures.is_empty() {
                    ResolveResult::Failure
                } else {
                    ResolveResult::Pending
                }
            }
            Self::AllSucceededOrFirstFailed(futures) => {
                let mut i = 0;
                while i < futures.len() {
                    match futures[i].resolve_inner(notification_id, result) {
                        ResolveResult::Success => {
                            futures.swap_remove(i);
                        }
                        ResolveResult::Failure => return ResolveResult::Failure,
                        ResolveResult::Pending => i += 1,
                    }
                }

                // if all nested futures has been evicted
                // without failure, then resolve as success.
                // otherwise, pending.
                if futures.is_empty() {
                    ResolveResult::Success
                } else {
                    ResolveResult::Pending
                }
            }
        }
    }

    /// Applies a single notification to this UnresolvedFuture, advancing its state.
    ///
    /// Returns `true` once the future has reached a terminal state — either
    /// success or failure, as dictated by its [`CombinatorType`]. A `true`
    /// return signals that a suspended invocation waiting on this future can
    /// be resumed; the caller still needs to inspect the remaining state to
    /// determine the outcome.
    ///
    /// Returns `false` if more notifications are needed, or if
    /// `notification_id` did not match anything in this future (including
    /// its nested children).
    pub fn resolve(
        &mut self,
        notification_id: &NotificationId,
        result: RawNotificationResultVariant,
    ) -> bool {
        if result.is_unknown() {
            // short circuit if the result is unknown. No result variant is associated with
            // this notification. There is no way we can evaluate this future reliably.
            return true;
        }

        self.resolve_inner(notification_id, result).is_completed()
    }

    pub fn resolve_all<'a>(
        &mut self,
        notifications: impl Iterator<Item = (&'a NotificationId, RawNotificationResultVariant)>,
    ) -> bool {
        for (notification_id, result) in notifications {
            if self.resolve(notification_id, result) {
                return true;
            }
        }

        false
    }

    /// Rewrite this future tree so that no `Unknown` node appears as a
    /// descendant of `FirstSucceededOrAllFailed` or `AllSucceededOrFirstFailed`.
    ///
    /// # Why this matters
    ///
    /// `FirstSucceededOrAllFailed` and `AllSucceededOrFirstFailed` need to
    /// distinguish *success* from *failure* for every child.  An `Unknown`
    /// child can only report *completed/pending* — never success/failure —
    /// so leaving one inside these combinators would make the wake-up
    /// decision unreliable.
    ///
    /// `Unknown` is harmless inside `FirstCompleted`, `AllCompleted`, and
    /// other `Unknown` nodes because those only care about
    /// completed-vs-pending.
    ///
    /// # Algorithm
    ///
    /// 1. Recurse into the tree. Whenever a `FirstSucceededOrAllFailed` or
    ///    `AllSucceededOrFirstFailed` node (or any descendant of one) contains
    ///    an `Unknown` child, extract that child's contents into a separate
    ///    `unknown_nodes` list.  Nested `Unknown` nodes are flattened
    ///    recursively.
    /// 2. After the full traversal, if any nodes were extracted, wrap the
    ///    (now Unknown-free) original root together with the extracted nodes
    ///    in a new top-level `Unknown` combinator.
    /// 3. Degenerate combinators (≤ 1 child) are collapsed to their single
    ///    child (or an empty `Unknown` sentinel).
    pub fn normalize(&mut self) {
        let mut unknown_nodes = vec![];
        Self::normalize_inner(self, false, &mut unknown_nodes);

        if !unknown_nodes.is_empty() {
            let current_root = std::mem::replace(self, UnresolvedFuture::Unknown(vec![]));
            // Only include the root if it's not an empty sentinel
            if !current_root.is_empty() {
                unknown_nodes.insert(0, current_root);
            }
            *self = UnresolvedFuture::Unknown(unknown_nodes);
        }
    }

    /// Recurse into a node, extracting Unknowns when inside a fsaf/asff context.
    /// `extract` is true when we're inside a fsaf/asff subtree and must pull out any Unknown.
    fn normalize_inner(
        fut: &mut UnresolvedFuture,
        extract: bool,
        unknown_nodes: &mut Vec<UnresolvedFuture>,
    ) {
        match fut {
            UnresolvedFuture::Single(_) => return,
            UnresolvedFuture::Unknown(_) => {
                // If we're in extract mode, the caller handles extraction.
                // Otherwise, recurse into Unknown's children (they may contain fsaf/asff).
                if !extract && let UnresolvedFuture::Unknown(children) = fut {
                    for child in children.iter_mut() {
                        Self::normalize_inner(child, false, unknown_nodes);
                    }
                }
                return;
            }
            _ => {}
        }

        // Determine if THIS node triggers extraction for its children
        let needs_extraction = matches!(
            fut,
            UnresolvedFuture::FirstSucceededOrAllFailed(_)
                | UnresolvedFuture::AllSucceededOrFirstFailed(_)
        );
        // Children inherit extract mode from parent, or enter it if this node is fsaf/asff
        let child_extract = extract || needs_extraction;

        let children = match fut {
            UnresolvedFuture::FirstCompleted(c)
            | UnresolvedFuture::AllCompleted(c)
            | UnresolvedFuture::FirstSucceededOrAllFailed(c)
            | UnresolvedFuture::AllSucceededOrFirstFailed(c) => c,
            _ => unreachable!(),
        };

        // Recurse into non-Unknown children
        for child in children.iter_mut() {
            Self::normalize_inner(child, child_extract, unknown_nodes);
        }

        // Extract Unknown children if we're in extract mode
        if child_extract {
            let mut i = 0;
            while i < children.len() {
                if matches!(children[i], UnresolvedFuture::Unknown(_)) {
                    let unknown = children.swap_remove(i);
                    if let UnresolvedFuture::Unknown(extracted) = unknown {
                        for gc in extracted {
                            Self::collect_from_unknown_child(gc, unknown_nodes);
                        }
                    }
                } else {
                    i += 1;
                }
            }
        }

        // Simplify degenerate combinators
        let children = match fut {
            UnresolvedFuture::FirstCompleted(c)
            | UnresolvedFuture::AllCompleted(c)
            | UnresolvedFuture::FirstSucceededOrAllFailed(c)
            | UnresolvedFuture::AllSucceededOrFirstFailed(c) => c,
            _ => return,
        };
        if children.len() <= 1 {
            *fut = children.pop().unwrap_or(UnresolvedFuture::Unknown(vec![]));
        }
    }

    /// Process a child extracted from an Unknown node.
    /// If the child is itself Unknown, flatten recursively.
    /// Otherwise, normalize it and add to unknown_nodes.
    fn collect_from_unknown_child(
        fut: UnresolvedFuture,
        unknown_nodes: &mut Vec<UnresolvedFuture>,
    ) {
        match fut {
            UnresolvedFuture::Unknown(children) => {
                for child in children {
                    Self::collect_from_unknown_child(child, unknown_nodes);
                }
            }
            mut other => {
                Self::normalize_inner(&mut other, false, unknown_nodes);
                if !other.is_empty() {
                    unknown_nodes.push(other);
                }
            }
        }
    }
}

impl Debug for UnresolvedFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let nested = match self {
            Self::Single(notif) => return write!(f, "{notif}"),
            Self::Unknown(inner) => {
                write!(f, "unknown(")?;
                inner
            }
            Self::FirstCompleted(inner) => {
                write!(f, "first_completed(")?;
                inner
            }
            Self::AllCompleted(inner) => {
                write!(f, "all_completed(")?;
                inner
            }
            Self::FirstSucceededOrAllFailed(inner) => {
                write!(f, "first_succeeded_or_all_failed(")?;
                inner
            }
            Self::AllSucceededOrFirstFailed(inner) => {
                write!(f, "all_succeeded_or_first_failed(")?;
                inner
            }
        };

        for (pos, future) in nested.iter().with_position() {
            write!(f, "{future:?}")?;
            if matches!(pos, Position::First | Position::Middle) {
                write!(f, ", ")?;
            }
        }

        write!(f, ")")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResolveResult {
    Pending,
    Success,
    Failure,
}

impl From<RawNotificationResultVariant> for ResolveResult {
    fn from(value: RawNotificationResultVariant) -> Self {
        if value.is_success() {
            ResolveResult::Success
        } else {
            ResolveResult::Failure
        }
    }
}

impl ResolveResult {
    fn is_pending(&self) -> bool {
        self == &ResolveResult::Pending
    }

    fn is_completed(&self) -> bool {
        matches!(self, Self::Success | Self::Failure)
    }
}

impl From<NotificationId> for UnresolvedFuture {
    fn from(value: NotificationId) -> Self {
        Self::Single(value)
    }
}

pub struct UnresolvedFutureBuilder {
    inner: Vec<UnresolvedFuture>,
    combinator: CombinatorType,
}

impl UnresolvedFutureBuilder {
    pub fn new(combinator: CombinatorType) -> Self {
        Self {
            combinator,
            inner: Vec::default(),
        }
    }

    /// First succeeded or all failed.
    pub fn first_succeeded() -> Self {
        Self {
            inner: Vec::default(),
            combinator: CombinatorType::FirstSucceededOrAllFailed,
        }
    }

    /// All succeeded or first failed.
    pub fn all_succeeded() -> Self {
        Self {
            inner: Vec::default(),
            combinator: CombinatorType::AllSucceededOrFirstFailed,
        }
    }

    /// First completed
    pub fn first_completed() -> Self {
        Self {
            inner: Vec::default(),
            combinator: CombinatorType::FirstCompleted,
        }
    }

    /// All completed
    pub fn all_settled() -> Self {
        Self {
            inner: Vec::default(),
            combinator: CombinatorType::AllCompleted,
        }
    }

    pub fn future(mut self, fut: impl Into<UnresolvedFuture>) -> Self {
        self.inner.push(fut.into());
        self
    }

    pub fn futures<I, T>(mut self, futures: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<UnresolvedFuture>,
    {
        self.inner.extend(futures.into_iter().map(Into::into));
        self
    }

    pub fn build(self) -> anyhow::Result<UnresolvedFuture> {
        let result = match self.combinator {
            CombinatorType::Unknown => UnresolvedFuture::Unknown(self.inner),
            CombinatorType::FirstCompleted => UnresolvedFuture::FirstCompleted(self.inner),
            CombinatorType::AllCompleted => UnresolvedFuture::AllCompleted(self.inner),
            CombinatorType::FirstSucceededOrAllFailed => {
                UnresolvedFuture::FirstSucceededOrAllFailed(self.inner)
            }
            CombinatorType::AllSucceededOrFirstFailed => {
                UnresolvedFuture::AllSucceededOrFirstFailed(self.inner)
            }
        };

        if result.is_empty() {
            anyhow::bail!("UnresolvedFuture cannot be empty")
        }
        Ok(result)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn must_build(self) -> UnresolvedFuture {
        self.build().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use paste::paste;

    use super::*;

    // --- Helpers ---

    fn nid(id: u32) -> NotificationId {
        NotificationId::CompletionId(id)
    }

    const OK: RawNotificationResultVariant = RawNotificationResultVariant::Void;
    const ERR: RawNotificationResultVariant = RawNotificationResultVariant::Failure;

    fn single(n: u32) -> UnresolvedFuture {
        UnresolvedFuture::Single(nid(n))
    }

    fn unknown(children: impl Into<Vec<UnresolvedFuture>>) -> UnresolvedFuture {
        UnresolvedFuture::Unknown(children.into())
    }

    fn first_completed(children: impl Into<Vec<UnresolvedFuture>>) -> UnresolvedFuture {
        UnresolvedFuture::FirstCompleted(children.into())
    }

    fn all_completed(children: impl Into<Vec<UnresolvedFuture>>) -> UnresolvedFuture {
        UnresolvedFuture::AllCompleted(children.into())
    }

    fn first_succeeded_or_all_failed(
        children: impl Into<Vec<UnresolvedFuture>>,
    ) -> UnresolvedFuture {
        UnresolvedFuture::FirstSucceededOrAllFailed(children.into())
    }

    fn all_succeeded_or_first_failed(
        children: impl Into<Vec<UnresolvedFuture>>,
    ) -> UnresolvedFuture {
        UnresolvedFuture::AllSucceededOrFirstFailed(children.into())
    }

    fn success(id: u32) -> (NotificationId, RawNotificationResultVariant) {
        (nid(id), OK)
    }

    fn failure(id: u32) -> (NotificationId, RawNotificationResultVariant) {
        (nid(id), ERR)
    }

    // --- Macros ---

    macro_rules! test_normalization {
        ($test_name:ident: $input:expr => $expected:expr) => {
            paste! {
                #[test]
                fn [<normalize_ $test_name>]() {
                    let mut fut = $input;
                    fut.normalize();
                    assert_eq!(fut, $expected);
                }
            }
        };
    }

    macro_rules! test_try_future_resolve {
        ($test_name:ident: [$($notif:expr),* $(,)?], $input:expr => Resolved) => {
            paste! {
                #[test]
                fn [<try_resolve_ $test_name>]() {
                    let mut fut = $input;
                    fut.normalize();
                    let notifs = [$($notif),*];
                    assert!(
                        fut.resolve_all(notifs.iter().map(|(id, r)| (id, *r))),
                        "expected Resolved, got Unresolved({fut:?})"
                    );
                }
            }
        };
        ($test_name:ident: [$($notif:expr),* $(,)?], $input:expr => Unresolved($expected:expr)) => {
            paste! {
                #[test]
                fn [<try_resolve_ $test_name>]() {
                    let mut fut = $input;
                    fut.normalize();
                    let notifs = [$($notif),*];
                    assert!(
                        !fut.resolve_all(notifs.iter().map(|(id, r)| (id, *r))),
                        "expected Unresolved, got Resolved"
                    );
                    assert_eq!(fut, $expected, "remaining future mismatch");
                }
            }
        };
    }

    // ==================== Normalization: Single ====================

    test_normalization!(single_is_noop:
        single(1) => single(1));

    // ==================== Normalization: Unknown ====================

    test_normalization!(unknown_at_root_is_noop:
        unknown([single(1), single(2)]) => unknown([single(1), single(2)]));
    // Deep: unknown inside asff inside unknown inside all_completed
    test_normalization!(deep_nested_asff_with_unknown:
        all_completed([
            unknown([
                all_succeeded_or_first_failed([single(1), unknown([single(2)])])
            ])
        ])
        => unknown([unknown([single(1)]), single(2)]));

    // ==================== Normalization: FirstCompleted ====================

    test_normalization!(nested_combinators_no_unknowns_is_noop:
        first_completed([single(1), all_completed([single(2), single(3)])])
        => first_completed([single(1), all_completed([single(2), single(3)])]));
    // Unknown inside first_completed is fine — no extraction needed
    test_normalization!(first_completed_with_unknown_is_noop:
        first_completed([single(1), unknown([single(2)])])
        => first_completed([single(1), unknown([single(2)])]));
    // Unknown wrapping a combinator inside first_completed — no extraction
    test_normalization!(first_completed_with_unknown_wrapping_combinator:
    first_completed([
        unknown([all_completed([single(1), single(2)])]),
        single(3)
    ])
    => first_completed([
        unknown([all_completed([single(1), single(2)])]),
        single(3)
    ]));
    // first_completed with one Unknown child — collapses (1 child)
    test_normalization!(first_completed_unknown_with_nested_unknown_collapses:
        first_completed([unknown([all_completed([single(1), unknown([single(2)])])])])
        => unknown([all_completed([single(1), unknown([single(2)])])]));

    // ==================== Normalization: AllCompleted ====================

    test_normalization!(no_unknowns_is_noop:
        all_completed([single(1), single(2)]) => all_completed([single(1), single(2)]));
    // Unknown inside all_completed is fine — no extraction
    test_normalization!(all_completed_with_unknown_is_noop:
        all_completed([single(1), unknown([single(2)])])
        => all_completed([single(1), unknown([single(2)])]));
    test_normalization!(all_completed_multiple_unknowns_is_noop:
        all_completed([unknown([single(1)]), unknown([single(2)])])
        => all_completed([unknown([single(1)]), unknown([single(2)])]));
    test_normalization!(all_completed_nested_unknown_is_noop:
        all_completed([single(1), unknown([unknown([single(2)])])])
        => all_completed([single(1), unknown([unknown([single(2)])])]));

    // ==================== Normalization: FirstSucceededOrAllFailed ====================

    // fsaf extracts unknowns
    test_normalization!(fsaf_extracts_unknown:
        first_succeeded_or_all_failed([single(1), unknown([single(2)])])
        => unknown([single(1), single(2)]));
    test_normalization!(nested_unknown_inside_fsaf_asff:
        first_succeeded_or_all_failed([single(1), all_succeeded_or_first_failed([single(2), unknown([single(3)])])])
        => unknown([first_succeeded_or_all_failed([single(1), single(2)]), single(3)]));
    // Unknown deep inside fsaf subtree gets extracted
    test_normalization!(fsaf_with_all_completed_containing_unknown:
        first_succeeded_or_all_failed([single(1), all_completed([single(2), unknown([single(3)])])])
        => unknown([first_succeeded_or_all_failed([single(1), single(2)]), single(3)]));
    test_normalization!(fsaf_with_unknown_containing_asff:
        first_succeeded_or_all_failed([single(1), unknown([all_succeeded_or_first_failed([single(2), single(3)])])])
        => unknown([single(1), all_succeeded_or_first_failed([single(2), single(3)])]));
    // Cascading extraction
    test_normalization!(fsaf_with_unknown_containing_asff_with_unknown:
        first_succeeded_or_all_failed([single(1), unknown([single(2), all_succeeded_or_first_failed([single(3), unknown([single(4)])])])])
        => unknown([single(1), single(2), single(4), single(3)]));

    // ==================== Normalization: AllSucceededOrFirstFailed ====================

    // asff extracts unknowns
    test_normalization!(asff_extracts_unknown:
        all_succeeded_or_first_failed([single(1), unknown([single(2)])])
        => unknown([single(1), single(2)]));
    test_normalization!(asff_with_unknown_containing_fsaf:
        all_succeeded_or_first_failed([single(1), unknown([single(2), first_succeeded_or_all_failed([single(3), single(4)])])])
        => unknown([single(1), single(2), first_succeeded_or_all_failed([single(3), single(4)])]));
    // all_completed inside extracted unknown is kept as-is
    test_normalization!(asff_with_unknown_containing_all_completed_with_unknown:
        all_succeeded_or_first_failed([single(1), unknown([all_completed([single(2), unknown([single(3)])])])])
        => unknown([single(1), all_completed([single(2), unknown([single(3)])])]));

    // ==================== Resolution: Single ====================

    test_try_future_resolve!(single_succeeded:
        [success(1)], single(1) => Resolved);
    test_try_future_resolve!(single_failed:
        [failure(1)], single(1) => Resolved);
    test_try_future_resolve!(single_pending:
        [], single(1) => Unresolved(single(1)));

    // ==================== Resolution: FirstCompleted ====================

    test_try_future_resolve!(first_completed_none_ready:
        [], first_completed([single(1), single(2), single(3)])
        => Unresolved(first_completed([single(1), single(2), single(3)])));
    test_try_future_resolve!(first_completed_one_succeeded:
        [success(2)], first_completed([single(1), single(2), single(3)])
        => Resolved);
    // Resolves on any completion, even failure
    test_try_future_resolve!(first_completed_one_failed:
        [failure(1)], first_completed([single(1), single(2), single(3)])
        => Resolved);
    // first_completed(1, unknown(2)) — unknown(2) completes
    test_try_future_resolve!(first_completed_with_unknown_resolves:
        [success(2)], first_completed([single(1), unknown([single(2)])])
        => Resolved);
    // first_completed(unknown(all_completed(1, 2)), 3) — completing 3 resolves
    test_try_future_resolve!(first_completed_unknown_wrapping_combinator_resolves_on_leaf:
        [success(3)],
        first_completed([unknown([all_completed([single(1), single(2)])]), single(3)])
        => Resolved);
    // Completing 1 alone doesn't resolve: unknown(all_completed(1,2)) needs both
    test_try_future_resolve!(first_completed_unknown_wrapping_combinator_partial_inner:
        [success(1)],
        first_completed([unknown([all_completed([single(1), single(2)])]), single(3)])
        => Unresolved(first_completed([unknown([all_completed([single(2)])]), single(3)])));
    // Completing 1 and 2 resolves
    test_try_future_resolve!(first_completed_unknown_wrapping_combinator_inner_done:
        [success(1), success(2)],
        first_completed([unknown([all_completed([single(1), single(2)])]), single(3)])
        => Resolved);
    // Completing just 1 is NOT enough
    test_try_future_resolve!(deep_unknown_not_prematurely_resolved:
        [success(1)],
        first_completed([unknown([all_completed([single(1), unknown([single(2)])])])])
        => Unresolved(unknown([all_completed([unknown([single(2)])])])));
    test_try_future_resolve!(deep_unknown_resolves_when_all_done:
        [success(1), success(2)],
        first_completed([unknown([all_completed([single(1), unknown([single(2)])])])])
        => Resolved);

    // ==================== Resolution: AllCompleted ====================

    test_try_future_resolve!(all_completed_none_ready:
        [], all_completed([single(1), single(2), single(3)])
        => Unresolved(all_completed([single(1), single(2), single(3)])));
    test_try_future_resolve!(all_completed_partial:
        [success(1), failure(3)], all_completed([single(1), single(2), single(3)])
        => Unresolved(all_completed([single(2)])));
    test_try_future_resolve!(all_completed_all_done:
        [success(1), failure(2)], all_completed([single(1), single(2)])
        => Resolved);
    // Handle 1 completes but unknown(2) still pending
    test_try_future_resolve!(all_completed_with_unknown_partial:
        [success(1)], all_completed([single(1), unknown([single(2)])])
        => Unresolved(all_completed([unknown([single(2)])])));
    test_try_future_resolve!(all_completed_with_unknown_all_done:
        [success(1), success(2)], all_completed([single(1), unknown([single(2)])])
        => Resolved);

    // ==================== Resolution: FirstSucceededOrAllFailed ====================

    test_try_future_resolve!(first_succeeded_or_all_failed_none_ready:
        [], first_succeeded_or_all_failed([single(1), single(2), single(3)])
        => Unresolved(first_succeeded_or_all_failed([single(1), single(2), single(3)])));
    test_try_future_resolve!(first_succeeded_or_all_failed_one_succeeded:
        [success(2)], first_succeeded_or_all_failed([single(1), single(2), single(3)])
        => Resolved);
    // swap_remove changes order
    test_try_future_resolve!(first_succeeded_or_all_failed_some_failed_some_pending:
        [failure(1)], first_succeeded_or_all_failed([single(1), single(2), single(3)])
        => Unresolved(first_succeeded_or_all_failed([single(3), single(2)])));
    test_try_future_resolve!(first_succeeded_or_all_failed_all_failed:
        [failure(1), failure(2)], first_succeeded_or_all_failed([single(1), single(2)])
        => Resolved);

    // fsaf(1, asff(2, unknown(3))) -> unknown(fsaf(1, 2), 3)
    test_try_future_resolve!(normalization_fsaf_asff_unknown_success:
        [success(3)],
        first_succeeded_or_all_failed([single(1), all_succeeded_or_first_failed([single(2), unknown([single(3)])])])
        => Resolved);
    test_try_future_resolve!(normalization_fsaf_asff_unknown_failure:
        [failure(3)],
        first_succeeded_or_all_failed([single(1), all_succeeded_or_first_failed([single(2), unknown([single(3)])])])
        => Resolved);
    // 1 fails -> fsaf prunes it, fsaf(2) still pending. 3 still pending.
    test_try_future_resolve!(normalization_nested_fsaf_asff_partial:
        [failure(1)],
        first_succeeded_or_all_failed([single(1), all_succeeded_or_first_failed([single(2), unknown([single(3)])])])
        => Unresolved(unknown([first_succeeded_or_all_failed([single(2)]), single(3)])));
    // fsaf(1, all_completed(2, unknown(3))) — deep extraction
    test_try_future_resolve!(normalization_fsaf_with_nested_unknown_in_all_completed:
        [success(3)],
        first_succeeded_or_all_failed([single(1), all_completed([single(2), unknown([single(3)])])])
        => Resolved);
    test_try_future_resolve!(normalization_fsaf_with_nested_unknown_in_all_completed_pending:
        [],
        first_succeeded_or_all_failed([single(1), all_completed([single(2), unknown([single(3)])])])
        => Unresolved(unknown([first_succeeded_or_all_failed([single(1), single(2)]), single(3)])));
    // fsaf(1, unknown(asff(2, 3))) -> unknown(1, asff(2, 3))
    test_try_future_resolve!(fsaf_unknown_asff_resolves_when_inner_done:
        [success(2), success(3)],
        first_succeeded_or_all_failed([single(1), unknown([all_succeeded_or_first_failed([single(2), single(3)])])])
        => Resolved);
    test_try_future_resolve!(fsaf_unknown_asff_inner_failure_resolves:
        [failure(2)],
        first_succeeded_or_all_failed([single(1), unknown([all_succeeded_or_first_failed([single(2), single(3)])])])
        => Resolved);
    test_try_future_resolve!(fsaf_unknown_asff_inner_partial:
        [success(2)],
        first_succeeded_or_all_failed([single(1), unknown([all_succeeded_or_first_failed([single(2), single(3)])])])
        => Unresolved(unknown([single(1), all_succeeded_or_first_failed([single(3)])])));

    // ==================== Resolution: AllSucceededOrFirstFailed ====================

    test_try_future_resolve!(all_succeeded_or_first_failed_none_ready:
        [], all_succeeded_or_first_failed([single(1), single(2), single(3)])
        => Unresolved(all_succeeded_or_first_failed([single(1), single(2), single(3)])));
    test_try_future_resolve!(all_succeeded_or_first_failed_all_succeeded:
        [success(1), success(2)], all_succeeded_or_first_failed([single(1), single(2)])
        => Resolved);
    test_try_future_resolve!(all_succeeded_or_first_failed_one_failed:
        [failure(2)], all_succeeded_or_first_failed([single(1), single(2), single(3)])
        => Resolved);
    // swap_remove changes order
    test_try_future_resolve!(all_succeeded_or_first_failed_some_succeeded_some_pending:
        [success(1)], all_succeeded_or_first_failed([single(1), single(2), single(3)])
        => Unresolved(all_succeeded_or_first_failed([single(3), single(2)])));
    // Inner failure propagates up
    test_try_future_resolve!(promise_all_short_circuits_on_nested_failure:
        [failure(2)],
        all_succeeded_or_first_failed([all_succeeded_or_first_failed([single(1), single(2)]), single(3)])
        => Resolved);

    // asff(1, unknown(2)) -> unknown(1, 2). Failure of 2 wakes.
    test_try_future_resolve!(normalization_asff_extracts_unknown:
        [failure(2)],
        all_succeeded_or_first_failed([single(1), unknown([single(2)])])
        => Resolved);
    // asff(1, unknown(2, fsaf(3, 4))) -> unknown(1, 2, fsaf(3, 4))
    test_try_future_resolve!(asff_unknown_fsaf_resolves_on_leaf:
        [success(1)],
        all_succeeded_or_first_failed([single(1), unknown([single(2), first_succeeded_or_all_failed([single(3), single(4)])])])
        => Resolved);
    test_try_future_resolve!(asff_unknown_fsaf_resolves_on_inner_fsaf_success:
        [success(3)],
        all_succeeded_or_first_failed([single(1), unknown([single(2), first_succeeded_or_all_failed([single(3), single(4)])])])
        => Resolved);
    // 3 fails but 4 pending -> fsaf pending -> nothing completed
    test_try_future_resolve!(asff_unknown_fsaf_failure_doesnt_resolve:
        [failure(3)],
        all_succeeded_or_first_failed([single(1), unknown([single(2), first_succeeded_or_all_failed([single(3), single(4)])])])
        => Unresolved(unknown([single(1), single(2), first_succeeded_or_all_failed([single(4)])])));
    // Both 3 and 4 fail -> fsaf completed -> unknown wakes
    test_try_future_resolve!(asff_unknown_fsaf_all_inner_fail:
        [failure(3), failure(4)],
        all_succeeded_or_first_failed([single(1), unknown([single(2), first_succeeded_or_all_failed([single(3), single(4)])])])
        => Resolved);
    test_try_future_resolve!(asff_unknown_fsaf_pending:
        [],
        all_succeeded_or_first_failed([single(1), unknown([single(2), first_succeeded_or_all_failed([single(3), single(4)])])])
        => Unresolved(unknown([single(1), single(2), first_succeeded_or_all_failed([single(3), single(4)])])));
    // asff(1, unknown(all_completed(2, unknown(3)))) -> unknown(1, all_completed(2, unknown(3)))
    test_try_future_resolve!(asff_unknown_all_completed_with_unknown_resolves_on_leaf:
        [success(1)],
        all_succeeded_or_first_failed([single(1), unknown([all_completed([single(2), unknown([single(3)])])])])
        => Resolved);
    test_try_future_resolve!(asff_unknown_all_completed_with_unknown_partial:
        [success(2)],
        all_succeeded_or_first_failed([single(1), unknown([all_completed([single(2), unknown([single(3)])])])])
        => Unresolved(unknown([single(1), all_completed([unknown([single(3)])])])));
    test_try_future_resolve!(asff_unknown_all_completed_with_unknown_all_done:
        [success(2), success(3)],
        all_succeeded_or_first_failed([single(1), unknown([all_completed([single(2), unknown([single(3)])])])])
        => Resolved);

    // ==================== Resolution: Unknown ====================

    test_try_future_resolve!(unknown_none_ready:
        [], unknown([single(1), single(2)])
        => Unresolved(unknown([single(1), single(2)])));
    test_try_future_resolve!(unknown_one_ready:
        [success(2)], unknown([single(1), single(2)])
        => Resolved);

    // ==================== Resolution: Nested combinators ====================

    test_try_future_resolve!(nested_all_inside_first_completed:
        [success(3)], first_completed([all_completed([single(1), single(2)]), single(3)])
        => Resolved);
    test_try_future_resolve!(nested_first_completed_inside_all_partial:
        [success(1)],
        all_completed([first_completed([single(1), single(2)]), first_completed([single(3), single(4)])])
        => Unresolved(all_completed([first_completed([single(3), single(4)])])));
    test_try_future_resolve!(nested_first_completed_inside_all_complete:
        [success(1), success(4)],
        all_completed([first_completed([single(1), single(2)]), first_completed([single(3), single(4)])])
        => Resolved);

    // ==================== Resolution: Duplicated leaves and subtrees ====================

    test_try_future_resolve!(duplicated_leaf_in_all_completed:
        [success(1)], all_completed([single(1), single(1)])
        => Resolved);
    test_try_future_resolve!(duplicated_leaf_in_first_completed:
        [success(1)], first_completed([single(1), single(1), single(2)])
        => Resolved);
    test_try_future_resolve!(duplicated_leaf_failure_in_promise_all:
        [failure(1)], all_succeeded_or_first_failed([single(1), single(1), single(2)])
        => Resolved);
    test_try_future_resolve!(duplicated_leaf_success_in_promise_any:
        [success(1)], first_succeeded_or_all_failed([single(1), single(1)])
        => Resolved);
    test_try_future_resolve!(duplicated_leaf_across_nested_combinators:
        [success(1)],
        all_completed([first_completed([single(1), single(2)]), first_completed([single(1), single(3)])])
        => Resolved);
    test_try_future_resolve!(duplicated_subtree_all_succeeded:
        [success(1), success(2)],
        all_completed([
            all_succeeded_or_first_failed([single(1), single(2)]),
            all_succeeded_or_first_failed([single(1), single(2)])
        ])
        => Resolved);
    test_try_future_resolve!(duplicated_subtree_with_failure:
        [failure(1)],
        all_completed([
            all_succeeded_or_first_failed([single(1), single(2)]),
            all_succeeded_or_first_failed([single(1), single(2)])
        ])
        => Resolved);
    test_try_future_resolve!(duplicated_leaf_with_unknown_in_all_completed:
        [success(1)],
        all_completed([single(1), unknown([single(1), single(2)])])
        => Resolved);
    test_try_future_resolve!(duplicated_leaf_partial_resolution:
        [success(1)], all_completed([single(1), single(2), single(1)])
        => Unresolved(all_completed([single(2)])));

    // ==================== Restate-specific tests ====================

    #[test]
    fn flatten_and_is_empty_across_nesting() {
        let inner_all = all_succeeded_or_first_failed([
            single(3),
            first_succeeded_or_all_failed([single(1), single(2)]),
        ]);
        let mut fut = first_completed([single(4), inner_all]);

        assert_eq!(
            fut.flatten(),
            HashSet::from([nid(1), nid(2), nid(3), nid(4)])
        );
        assert!(!fut.is_empty());

        // Race resolves via the direct leaf — the nested `all` subtree is
        // not drained, so `is_empty()` still reports false.
        assert!(fut.resolve(&nid(4), OK));
        assert!(!fut.is_empty());
    }

    #[test]
    fn split_special_variants_return_empty_nested_and_flat_notifications() {
        // Single -> FirstCompleted with the single notification, no nested combinators.
        let (combinator, notifications, nested) = UnresolvedFuture::Single(nid(1)).split();
        assert_eq!(combinator, CombinatorType::FirstCompleted);
        assert_eq!(notifications, HashSet::from([nid(1)]));
        assert!(nested.is_empty());

        // Unknown fully flattens every nested subtree into the HashSet.
        let fut = unknown([
            single(1),
            first_completed([single(2), single(3)]),
            all_succeeded_or_first_failed([single(4), single(5)]),
        ]);
        let (combinator, notifications, nested) = fut.split();
        assert_eq!(combinator, CombinatorType::Unknown);
        assert_eq!(
            notifications,
            HashSet::from([nid(1), nid(2), nid(3), nid(4), nid(5)])
        );
        assert!(nested.is_empty());
    }

    #[test]
    fn split_separates_direct_singles_from_nested_combinators() {
        // all(1, race(3, 4), 2, any(5, 6)) -- direct singles are lifted into the HashSet;
        // nested combinators remain in the Vec untouched. Ordering is interleaved so
        // we also exercise the swap_remove path inside the while-loop.
        let fut = all_succeeded_or_first_failed([
            single(1),
            first_completed([single(3), single(4)]),
            single(2),
            first_succeeded_or_all_failed([single(5), single(6)]),
        ]);
        let (combinator, notifications, nested) = fut.split();
        assert_eq!(combinator, CombinatorType::AllSucceededOrFirstFailed);
        assert_eq!(notifications, HashSet::from([nid(1), nid(2)]));
        assert_eq!(nested.len(), 2);
        let preserved: HashSet<_> = nested.iter().flat_map(|f| f.flatten()).collect();
        assert_eq!(preserved, HashSet::from([nid(3), nid(4), nid(5), nid(6)]));

        // Empty combinator: the type is preserved but nothing is produced.
        let (combinator, notifications, nested) = UnresolvedFuture::AllCompleted(vec![]).split();
        assert_eq!(combinator, CombinatorType::AllCompleted);
        assert!(notifications.is_empty());
        assert!(nested.is_empty());
    }

    #[test]
    fn resolve_all_iterator_short_circuits_and_returns_true_once_completed() {
        let mut fut = first_completed([single(1), single(2), single(3)]);
        let batch = [(nid(1), ERR), (nid(2), OK)];
        assert!(fut.resolve_all(batch.iter().map(|(id, r)| (id, *r))));

        let mut fut = first_completed([single(1), single(2), single(3)]);
        let batch = [(nid(98), OK), (nid(99), OK)];
        assert!(!fut.resolve_all(batch.iter().map(|(id, r)| (id, *r))));
    }
}
