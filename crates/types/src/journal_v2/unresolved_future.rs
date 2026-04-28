// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};

use itertools::{Itertools, Position};
use serde::{Deserialize, Serialize};

use crate::journal_v2::{NotificationId, SignalId, raw::RawNotificationResultVariant};

/// A bit of theory on future resolution.
///
/// ## What is a future?
/// A Future represents one or more results the SDK is awaiting before resuming user code.
///
/// Futures are modeled as a tree: leaves reference Notifications in the restate service-protocol,
/// while intermediate nodes are combinators that compose child futures.
///
/// A future at any point in time can be in any of these 3 states:
/// * PENDING (initial state)
/// * SUCCEEDED (completed with success)
/// * FAILED (completed with failure)
///
/// Once in SUCCEEDED or FAILED, the Future is immutable, that is it cannot change state.
///
/// In the data model we have several types of combinators:
/// * SINGLE -> Resolve as soon as the operation is completed
/// * FIRST_COMPLETED -> Resolve as soon as any one child future completes with success, or with failure (same as JS Promise.race).
/// * ALL_COMPLETED -> Wait for every child to complete, regardless of success or failure. Completes always with success (same as JS Promise.allSettled).
/// * FIRST_SUCCEEDED_OR_ALL_FAILED -> Resolve on the first success; fail only if all children fail (same as JS Promise.any).
/// * ALL_SUCCEEDED_OR_FIRST_FAILED -> Resolve when all children succeed; short-circuit on the first failure (same as JS Promise.all).
/// * UNKNOWN -> Unknown combinator made of 1 or more children futures.
///   The SDK uses this for futures that are not representable in any of the above types.
///   A notable example if RestatePromise.map in Javascript or Java SDK
///
/// Implementation note: for performance reasons, the nested combinators use Vec to store children nodes, but they're conceptually sets.
/// Duplicates should not influence the resolution algorithm.
///
/// # SDK and restate-server expectations
/// The SDK will try to resolve the nodes of the tree it can resolve.
///
/// If the SDK can fully resolve the future with the local information, it will unblock the user code and move on
/// If the SDK cannot resolve the future, it will shave off the tree the Completed nodes, and propagate back the remaining part of the tree.
///      Propagation will happen depending on the situation, via AwaitingOnMessage or SuspensionMessage.
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

impl From<NotificationId> for UnresolvedFuture {
    fn from(value: NotificationId) -> Self {
        Self::Single(value)
    }
}

impl From<SignalId> for UnresolvedFuture {
    fn from(value: SignalId) -> Self {
        Self::Single(value.into())
    }
}

/// See [UnresolvedFuture::resolve] below
#[derive(Debug, Clone, PartialEq, Eq)]
enum ResolveResult {
    Pending,
    // This is used when it cannot be determined whether the result is success or failure.
    SuccessOrFailure,
    Success,
    Failure,
}

impl ResolveResult {
    fn is_completed(&self) -> bool {
        *self != ResolveResult::Pending
    }
}

/// See [UnresolvedFuture::resolve] below
struct Indeterminable;

/// Used by the [UnresolvedFuture::resolve] algorithm to read the result variant of a notification.
pub trait NotificationResultVariantLookup {
    fn read_result_variant(
        &self,
        notification_id: &NotificationId,
    ) -> Option<RawNotificationResultVariant>;
}

impl NotificationResultVariantLookup for &HashMap<NotificationId, RawNotificationResultVariant> {
    fn read_result_variant(
        &self,
        notification_id: &NotificationId,
    ) -> Option<RawNotificationResultVariant> {
        self.get(notification_id).cloned()
    }
}

impl NotificationResultVariantLookup for &RawNotification {
    fn read_result_variant(
        &self,
        notification_id: &NotificationId,
    ) -> Option<RawNotificationResultVariant> {
        if notification_id == &self.id() {
            Some(self.result_variant())
        } else {
            None
        }
    }
}

impl From<RawNotificationResultVariant> for ResolveResult {
    fn from(value: RawNotificationResultVariant) -> Self {
        if value.is_success() {
            ResolveResult::Success
        } else if value.is_unknown() {
            ResolveResult::SuccessOrFailure
        } else {
            ResolveResult::Failure
        }
    }
}

impl UnresolvedFuture {
    #[cfg(any(test, feature = "test-util"))]
    pub fn empty() -> Self {
        Self::Unknown(Vec::default())
    }

    pub fn combinator_type(&self) -> Option<CombinatorType> {
        match self {
            UnresolvedFuture::Single(_) => None,
            UnresolvedFuture::FirstCompleted(_) => Some(CombinatorType::FirstCompleted),
            UnresolvedFuture::AllCompleted(_) => Some(CombinatorType::AllCompleted),
            UnresolvedFuture::FirstSucceededOrAllFailed(_) => {
                Some(CombinatorType::FirstSucceededOrAllFailed)
            }
            UnresolvedFuture::AllSucceededOrFirstFailed(_) => {
                Some(CombinatorType::AllSucceededOrFirstFailed)
            }
            UnresolvedFuture::Unknown(_) => Some(CombinatorType::Unknown),
        }
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

    pub fn split(self) -> (CombinatorType, Vec<NotificationId>, Vec<UnresolvedFuture>) {
        let combinator_ty = self
            .combinator_type()
            // Let single be unknown, that's fine
            .unwrap_or(CombinatorType::Unknown);

        let mut nids = Vec::new();
        let mut nested_futs = Vec::new();

        // Flatten just the first level
        match self {
            UnresolvedFuture::Single(nid) => nids.push(nid),
            UnresolvedFuture::FirstCompleted(futs)
            | UnresolvedFuture::AllCompleted(futs)
            | UnresolvedFuture::FirstSucceededOrAllFailed(futs)
            | UnresolvedFuture::AllSucceededOrFirstFailed(futs)
            | UnresolvedFuture::Unknown(futs) => {
                for fut in futs {
                    match fut {
                        UnresolvedFuture::Single(nid) => nids.push(nid),
                        nested_fut => nested_futs.push(nested_fut),
                    }
                }
            }
        };

        (combinator_ty, nids, nested_futs)
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

    /// # Resolution algorithm
    /// Theorem: Given as input the future tree and the list of notifications with their result variant
    ///          (succeeded/failed/unknown), it is possible to establish whether the invocation can resume or not.
    /// Proof by induction:
    ///   Given a SINGLE future, the invocation can resume once the corresponding notification is received.
    ///      The propagated result tracks the notification variant: Success, Failure, or SuccessOrFailure for the Unknown variant.
    ///   Given an UNKNOWN combinator future, the invocation can resume once any of the children are completed.
    ///      The propagated result is always SuccessOrFailure: the combinator output cannot be determined locally from its children.
    ///   Given a FIRST_COMPLETED future, the invocation can resume once any of the children are completed.
    ///      The first completed child's result is propagated upward.
    ///   Given an ALL_COMPLETED future, the invocation can resume when all the children are completed.
    ///      The propagated result is always Success.
    ///   Given a FIRST_SUCCEEDED_OR_ALL_FAILED future, the invocation can resume when either any of the children succeeded, or all the children failed.
    ///      If any child resolves to SuccessOrFailure (e.g. nested UNKNOWN combinator, or Unknown notification variant),
    ///      success/failure cannot be determined: we shortcircuit and let the SDK decide how to proceed.
    ///   Given an ALL_SUCCEEDED_OR_FIRST_FAILED future, the invocation can resume when either all the children succeeded, or any of the children failed.
    ///      If any child resolves to SuccessOrFailure, we shortcircuit as above.
    ///   Recursion
    ///   QED
    pub fn resolve(&mut self, result_variant_lookup: impl NotificationResultVariantLookup) -> bool {
        self.resolve_inner(&result_variant_lookup)
            // In case of shortcircuit, we always want to resume
            .unwrap_or(ResolveResult::Success)
            .is_completed()
    }

    // This function uses Err to shortcircuit when we cannot determine a future output, and we really need the SDK to make progress.
    // For example, an UNKNOWN is deeply nested inside a FIRST_SUCCEEDED_OR_ALL_FAILED.
    fn resolve_inner(
        &mut self,
        result_variant_lookup: &impl NotificationResultVariantLookup,
    ) -> Result<ResolveResult, Indeterminable> {
        match self {
            Self::Single(inner) => Ok(
                if let Some(variant) = result_variant_lookup.read_result_variant(inner) {
                    variant.into()
                } else {
                    ResolveResult::Pending
                },
            ),
            Self::Unknown(futures) => {
                // Unknown on the first completed future returns ResolveResult::SuccessOrFailure,
                // because it cannot determine whether it will resolve as success or failure only from the ResolveResult of its child.
                for fut in futures.iter_mut() {
                    if fut.resolve_inner(result_variant_lookup)?.is_completed() {
                        return Ok(ResolveResult::SuccessOrFailure);
                    }
                }

                Ok(ResolveResult::Pending)
            }
            Self::FirstCompleted(futures) => {
                // FirstCompleted is different from Unknown because it propagates upward the ResolveResult of its first completed child.
                for fut in futures.iter_mut() {
                    let inner_result = fut.resolve_inner(result_variant_lookup)?;
                    if inner_result.is_completed() {
                        return Ok(inner_result);
                    }
                }

                Ok(ResolveResult::Pending)
            }
            Self::AllCompleted(futures) => {
                // Wait for every child to complete
                let mut i = 0;
                while i < futures.len() {
                    if futures[i]
                        .resolve_inner(result_variant_lookup)?
                        .is_completed()
                    {
                        futures.swap_remove(i);
                    } else {
                        i += 1;
                    }
                }
                if futures.is_empty() {
                    Ok(ResolveResult::Success)
                } else {
                    Ok(ResolveResult::Pending)
                }
            }
            Self::FirstSucceededOrAllFailed(futures) => {
                let mut i = 0;
                while i < futures.len() {
                    match futures[i].resolve_inner(result_variant_lookup)? {
                        ResolveResult::Success => {
                            return Ok(ResolveResult::Success);
                        }
                        ResolveResult::Failure => {
                            futures.swap_remove(i);
                        }
                        ResolveResult::Pending => i += 1,
                        ResolveResult::SuccessOrFailure => {
                            // I can't determine if one of the children is success or failure,
                            // so the only thing left to do here is to shortcircuit and let the SDK decide how to deal with this.
                            return Err(Indeterminable);
                        }
                    }
                }

                // If all nested futures have been evicted
                // without success, then resolve as failure.
                // otherwise, pending.
                Ok(if futures.is_empty() {
                    ResolveResult::Failure
                } else {
                    ResolveResult::Pending
                })
            }
            Self::AllSucceededOrFirstFailed(futures) => {
                let mut i = 0;
                while i < futures.len() {
                    match futures[i].resolve_inner(result_variant_lookup)? {
                        ResolveResult::Success => {
                            futures.swap_remove(i);
                        }
                        ResolveResult::Failure => return Ok(ResolveResult::Failure),
                        ResolveResult::Pending => i += 1,
                        ResolveResult::SuccessOrFailure => {
                            // I can't determine if one of the children is success or failure,
                            // so the only thing left to do here is to shortcircuit and let the SDK decide how to deal with this.
                            return Err(Indeterminable);
                        }
                    }
                }

                // If all nested futures have been evicted
                // without failure, then resolve as a success.
                // otherwise, pending.
                Ok(if futures.is_empty() {
                    ResolveResult::Success
                } else {
                    ResolveResult::Pending
                })
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
    pub fn first_succeeded_or_all_failed() -> Self {
        Self {
            inner: Vec::default(),
            combinator: CombinatorType::FirstSucceededOrAllFailed,
        }
    }

    /// All succeeded or first failed.
    pub fn all_succeeded_or_first_failed() -> Self {
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
    pub fn all_completed() -> Self {
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

#[doc(hidden)]
#[macro_export]
macro_rules! __unresolved_future_first_completed {
    ($($child:expr),+ $(,)?) => {
        $crate::journal_v2::UnresolvedFuture::FirstCompleted(
            vec![$(::std::convert::Into::<$crate::journal_v2::UnresolvedFuture>::into($child)),+]
        )
    };
}
pub use __unresolved_future_first_completed as first_completed;

#[doc(hidden)]
#[macro_export]
macro_rules! __unresolved_future_all_completed {
    ($($child:expr),+ $(,)?) => {
        $crate::journal_v2::UnresolvedFuture::AllCompleted(
            vec![$(::std::convert::Into::<$crate::journal_v2::UnresolvedFuture>::into($child)),+]
        )
    };
}
pub use __unresolved_future_all_completed as all_completed;

#[doc(hidden)]
#[macro_export]
macro_rules! __unresolved_future_first_succeeded_or_all_failed {
    ($($child:expr),+ $(,)?) => {
        $crate::journal_v2::UnresolvedFuture::FirstSucceededOrAllFailed(
            vec![$(::std::convert::Into::<$crate::journal_v2::UnresolvedFuture>::into($child)),+]
        )
    };
}
pub use __unresolved_future_first_succeeded_or_all_failed as first_succeeded_or_all_failed;

#[doc(hidden)]
#[macro_export]
macro_rules! __unresolved_future_all_succeeded_or_first_failed {
    ($($child:expr),+ $(,)?) => {
        $crate::journal_v2::UnresolvedFuture::AllSucceededOrFirstFailed(
            vec![$(::std::convert::Into::<$crate::journal_v2::UnresolvedFuture>::into($child)),+]
        )
    };
}
pub use __unresolved_future_all_succeeded_or_first_failed as all_succeeded_or_first_failed;

#[doc(hidden)]
#[macro_export]
macro_rules! __unresolved_future_unknown {
    ($($child:expr),+ $(,)?) => {
        $crate::journal_v2::UnresolvedFuture::Unknown(
            vec![$(::std::convert::Into::<$crate::journal_v2::UnresolvedFuture>::into($child)),+]
        )
    };
}
use crate::journal_v2::raw::RawNotification;
pub use __unresolved_future_unknown as unknown;

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::prelude::{assert_that, empty, eq};
    use googletest::{pat, unordered_elements_are};
    use paste::paste;
    use std::collections::HashSet;

    // --- Helpers ---

    fn id(id: u32) -> NotificationId {
        NotificationId::CompletionId(id)
    }

    const OK: RawNotificationResultVariant = RawNotificationResultVariant::Void;
    const ERR: RawNotificationResultVariant = RawNotificationResultVariant::Failure;
    const UNKNOWN: RawNotificationResultVariant = RawNotificationResultVariant::Unknown;

    fn success(i: u32) -> (NotificationId, RawNotificationResultVariant) {
        (id(i), OK)
    }

    fn failure(i: u32) -> (NotificationId, RawNotificationResultVariant) {
        (id(i), ERR)
    }

    fn unknown(i: u32) -> (NotificationId, RawNotificationResultVariant) {
        (id(i), UNKNOWN)
    }

    // --- Macros ---

    macro_rules! test_try_future_resolve {
        ($test_name:ident: [$($notif:expr),* $(,)?], $input:expr => Resolved) => {
            paste! {
                #[test]
                fn [<try_resolve_ $test_name>]() {
                    let mut fut: UnresolvedFuture = ($input).into();
                    let notifs = HashMap::from([$($notif),*]);
                    assert!(
                        fut.resolve(&notifs),
                        "expected Resolved, got Unresolved({fut:?})"
                    );
                }
            }
        };
        ($test_name:ident: [$($notif:expr),* $(,)?], $input:expr => Unresolved($expected:expr)) => {
            paste! {
                #[test]
                fn [<try_resolve_ $test_name>]() {
                    let mut fut: UnresolvedFuture = ($input).into();
                    let notifs = HashMap::from([$($notif),*]);
                    assert!(
                        !fut.resolve(&notifs),
                        "expected Unresolved, got Resolved"
                    );
                    let expected: UnresolvedFuture = ($expected).into();
                    assert_eq!(fut, expected, "remaining future mismatch");
                }
            }
        };
    }

    // ==================== Resolution: Single ====================

    test_try_future_resolve!(single_succeeded:
        [success(1)], id(1) => Resolved);
    test_try_future_resolve!(single_failed:
        [failure(1)], id(1) => Resolved);
    test_try_future_resolve!(single_pending:
        [], id(1) => Unresolved(id(1)));

    // ==================== Resolution: FirstCompleted ====================

    test_try_future_resolve!(first_completed_none_ready:
        [], first_completed!(id(1), id(2), id(3))
        => Unresolved(first_completed!(id(1), id(2), id(3))));
    test_try_future_resolve!(first_completed_one_succeeded:
        [success(2)], first_completed!(id(1), id(2), id(3))
        => Resolved);
    // Resolves on any completion, even failure
    test_try_future_resolve!(first_completed_one_failed:
        [failure(1)], first_completed!(id(1), id(2), id(3))
        => Resolved);
    // first_completed(1, unknown(2)) — unknown(2) completes
    test_try_future_resolve!(first_completed_with_unknown_resolves:
        [success(2)], first_completed!(id(1), unknown!(id(2)))
        => Resolved);
    // first_completed(unknown(all_completed(1, 2)), 3) — completing 3 resolves
    test_try_future_resolve!(first_completed_unknown_wrapping_combinator_resolves_on_leaf:
        [success(3)],
        first_completed!(unknown!(all_completed!(id(1), id(2))), id(3))
        => Resolved);
    // Completing 1 alone doesn't resolve: unknown(all_completed(1,2)) needs both
    test_try_future_resolve!(first_completed_unknown_wrapping_combinator_partial_inner:
        [success(1)],
        first_completed!(unknown!(all_completed!(id(1), id(2))), id(3))
        => Unresolved(first_completed!(unknown!(all_completed!(id(2))), id(3))));
    // Completing 1 and 2 resolves
    test_try_future_resolve!(first_completed_unknown_wrapping_combinator_inner_done:
        [success(1), success(2)],
        first_completed!(unknown!(all_completed!(id(1), id(2))), id(3))
        => Resolved);
    // Completing just 1 is NOT enough
    test_try_future_resolve!(deep_unknown_not_prematurely_resolved:
        [success(1)],
        first_completed!(unknown!(all_completed!(id(1), unknown!(id(2)))))
        => Unresolved(first_completed!(unknown!(all_completed!(unknown!(id(2)))))));
    test_try_future_resolve!(deep_unknown_resolves_when_all_done:
        [success(1), success(2)],
        first_completed!(unknown!(all_completed!(id(1), unknown!(id(2)))))
        => Resolved);

    // ==================== Resolution: AllCompleted ====================

    test_try_future_resolve!(all_completed_none_ready:
        [], all_completed!(id(1), id(2), id(3))
        => Unresolved(all_completed!(id(1), id(2), id(3))));
    test_try_future_resolve!(all_completed_partial:
        [success(1), failure(3)], all_completed!(id(1), id(2), id(3))
        => Unresolved(all_completed!(id(2))));
    test_try_future_resolve!(all_completed_all_done:
        [success(1), failure(2)], all_completed!(id(1), id(2))
        => Resolved);
    // Handle 1 completes but unknown(2) still pending
    test_try_future_resolve!(all_completed_with_unknown_partial:
        [success(1)], all_completed!(id(1), unknown!(id(2)))
        => Unresolved(all_completed!(unknown!(id(2)))));
    test_try_future_resolve!(all_completed_with_unknown_all_done:
        [success(1), success(2)], all_completed!(id(1), unknown!(id(2)))
        => Resolved);

    // ==================== Resolution: FirstSucceededOrAllFailed ====================

    test_try_future_resolve!(first_succeeded_or_all_failed_none_ready:
        [], first_succeeded_or_all_failed!(id(1), id(2), id(3))
        => Unresolved(first_succeeded_or_all_failed!(id(1), id(2), id(3))));
    test_try_future_resolve!(first_succeeded_or_all_failed_one_succeeded:
        [success(2)], first_succeeded_or_all_failed!(id(1), id(2), id(3))
        => Resolved);
    // swap_remove changes order
    test_try_future_resolve!(first_succeeded_or_all_failed_some_failed_some_pending:
        [failure(1)], first_succeeded_or_all_failed!(id(1), id(2), id(3))
        => Unresolved(first_succeeded_or_all_failed!(id(3), id(2))));
    test_try_future_resolve!(first_succeeded_or_all_failed_all_failed:
        [failure(1), failure(2)], first_succeeded_or_all_failed!(id(1), id(2))
        => Resolved);

    // fsaf(1, asff(2, unknown(3))) -> unknown(fsaf(1, 2), 3)
    test_try_future_resolve!(fsaf_asff_unknown_success:
        [success(3)],
        first_succeeded_or_all_failed!(id(1), all_succeeded_or_first_failed!(id(2), unknown!(id(3))))
        => Resolved);
    test_try_future_resolve!(fsaf_asff_unknown_failure:
        [failure(3)],
        first_succeeded_or_all_failed!(id(1), all_succeeded_or_first_failed!(id(2), unknown!(id(3))))
        => Resolved);
    // 1 fails -> fsaf prunes it, fsaf(2) still pending. 3 still pending.
    test_try_future_resolve!(nested_fsaf_asff_partial:
        [failure(1)],
        first_succeeded_or_all_failed!(id(1), all_succeeded_or_first_failed!(id(2), unknown!(id(3))))
        => Unresolved(first_succeeded_or_all_failed!(all_succeeded_or_first_failed!(id(2), unknown!(id(3))))));
    // fsaf(1, all_completed(2, unknown(3))) — deep extraction
    test_try_future_resolve!(fsaf_with_nested_unknown_in_all_completed:
        [success(3)],
        first_succeeded_or_all_failed!(id(1), all_completed!(id(2), unknown!(id(3))))
        => Unresolved(first_succeeded_or_all_failed!(id(1), all_completed!(id(2)))));
    test_try_future_resolve!(fsaf_with_nested_unknown_in_all_completed_pending:
        [],
        first_succeeded_or_all_failed!(id(1), all_completed!(id(2), unknown!(id(3))))
        => Unresolved(first_succeeded_or_all_failed!(id(1), all_completed!(id(2), unknown!(id(3))))));
    // fsaf(1, unknown(asff(2, 3))) -> unknown(1, asff(2, 3))
    test_try_future_resolve!(fsaf_unknown_asff_resolves_when_inner_done:
        [success(2), success(3)],
        first_succeeded_or_all_failed!(id(1), unknown!(all_succeeded_or_first_failed!(id(2), id(3))))
        => Resolved);
    test_try_future_resolve!(fsaf_unknown_asff_inner_failure_resolves:
        [failure(2)],
        first_succeeded_or_all_failed!(id(1), unknown!(all_succeeded_or_first_failed!(id(2), id(3))))
        => Resolved);
    test_try_future_resolve!(fsaf_unknown_asff_inner_partial:
        [success(2)],
        first_succeeded_or_all_failed!(id(1), unknown!(all_succeeded_or_first_failed!(id(2), id(3))))
        => Unresolved(first_succeeded_or_all_failed!(id(1), unknown!(all_succeeded_or_first_failed!(id(3))))));

    // ==================== Resolution: AllSucceededOrFirstFailed ====================

    test_try_future_resolve!(all_succeeded_or_first_failed_none_ready:
        [], all_succeeded_or_first_failed!(id(1), id(2), id(3))
        => Unresolved(all_succeeded_or_first_failed!(id(1), id(2), id(3))));
    test_try_future_resolve!(all_succeeded_or_first_failed_all_succeeded:
        [success(1), success(2)], all_succeeded_or_first_failed!(id(1), id(2))
        => Resolved);
    test_try_future_resolve!(all_succeeded_or_first_failed_one_failed:
        [failure(2)], all_succeeded_or_first_failed!(id(1), id(2), id(3))
        => Resolved);
    // swap_remove changes order
    test_try_future_resolve!(all_succeeded_or_first_failed_some_succeeded_some_pending:
        [success(1)], all_succeeded_or_first_failed!(id(1), id(2), id(3))
        => Unresolved(all_succeeded_or_first_failed!(id(3), id(2))));
    // Inner failure propagates up
    test_try_future_resolve!(promise_all_short_circuits_on_nested_failure:
        [failure(2)],
        all_succeeded_or_first_failed!(all_succeeded_or_first_failed!(id(1), id(2)), id(3))
        => Resolved);

    // ==================== Mix asff with first_completed/all_completed ====================
    // These check that first_completed/all_completed correctly propagate their success/failure state

    test_try_future_resolve!(asff_with_nested_first_completed:
        [failure(2)],
        all_succeeded_or_first_failed!(first_completed!(id(1), id(2)), first_completed!(id(3), id(4)))
        => Resolved);
    test_try_future_resolve!(asff_with_nested_all_completed:
        [failure(1), failure(2)],
        all_succeeded_or_first_failed!(all_completed!(id(1), id(2)), all_completed!(id(3), id(4)))
         => Unresolved(all_succeeded_or_first_failed!(all_completed!(id(3), id(4)))));
    test_try_future_resolve!(asff_with_nested_all_completed_only_one_resolved:
        [failure(1)],
        all_succeeded_or_first_failed!(all_completed!(id(1), id(2)), all_completed!(id(3), id(4)))
         => Unresolved(all_succeeded_or_first_failed!(all_completed!(id(2)), all_completed!(id(3), id(4)))));

    // ==================== Resolution: Unknown combinator ====================

    test_try_future_resolve!(unknown_none_ready:
        [], unknown!(id(1), id(2))
        => Unresolved(unknown!(id(1), id(2))));
    test_try_future_resolve!(unknown_one_ready:
        [success(2)], unknown!(id(1), id(2))
        => Resolved);
    test_try_future_resolve!(all_completed_with_unknowns_1:
        [failure(1)],
        all_completed!(id(1), unknown!(id(2)), unknown!(id(3)))
        // Unordered b/c of swap_remove
        => Unresolved(all_completed!(unknown!(id(3)), unknown!(id(2)))));
    test_try_future_resolve!(all_completed_with_unknowns_2:
        [success(2)],
        all_completed!(id(1), unknown!(id(2)), unknown!(id(3)))
        => Unresolved(all_completed!(id(1), unknown!(id(3)))));

    // ==================== Resolution: Unknown variant ====================

    test_try_future_resolve!(asff_with_unknown_variant:
        [unknown(1)],
        all_succeeded_or_first_failed!(id(1), id(2))
         => Resolved);
    test_try_future_resolve!(all_completed_with_unknown_variant:
        [unknown(1)],
        all_completed!(id(1), id(2))
         => Unresolved(all_completed!(id(2))));
    test_try_future_resolve!(asff_with_unknown_variant_not_part_of_the_future:
        [unknown(3)],
        all_succeeded_or_first_failed!(id(1), id(2))
         => Unresolved(all_succeeded_or_first_failed!(id(1), id(2))));
    test_try_future_resolve!(asff_with_first_completed_with_unknown_variant:
        [unknown(1)],
        all_succeeded_or_first_failed!(first_completed!(id(1), id(2)), id(3))
         => Resolved);

    // ==================== Resolution: Nested combinators ====================

    test_try_future_resolve!(nested_all_inside_first_completed:
        [success(3)], first_completed!(all_completed!(id(1), id(2)), id(3))
        => Resolved);
    test_try_future_resolve!(nested_first_completed_inside_all_partial:
        [success(1)],
        all_completed!(first_completed!(id(1), id(2)), first_completed!(id(3), id(4)))
        => Unresolved(all_completed!(first_completed!(id(3), id(4)))));
    test_try_future_resolve!(nested_first_completed_inside_all_complete:
        [success(1), success(4)],
        all_completed!(first_completed!(id(1), id(2)), first_completed!(id(3), id(4)))
        => Resolved);

    // ==================== Resolution: Duplicated leaves and subtrees ====================

    test_try_future_resolve!(duplicated_leaf_in_all_completed:
        [success(1)], all_completed!(id(1), id(1))
        => Resolved);
    test_try_future_resolve!(duplicated_leaf_in_first_completed:
        [success(1)], first_completed!(id(1), id(1), id(2))
        => Resolved);
    test_try_future_resolve!(duplicated_leaf_failure_in_promise_all:
        [failure(1)], all_succeeded_or_first_failed!(id(1), id(1), id(2))
        => Resolved);
    test_try_future_resolve!(duplicated_leaf_success_in_promise_any:
        [success(1)], first_succeeded_or_all_failed!(id(1), id(1))
        => Resolved);
    test_try_future_resolve!(duplicated_leaf_across_nested_combinators:
        [success(1)],
        all_completed!(first_completed!(id(1), id(2)), first_completed!(id(1), id(3)))
        => Resolved);
    test_try_future_resolve!(duplicated_subtree_all_succeeded:
        [success(1), success(2)],
        all_completed!(
            all_succeeded_or_first_failed!(id(1), id(2)),
            all_succeeded_or_first_failed!(id(1), id(2))
        )
        => Resolved);
    test_try_future_resolve!(duplicated_subtree_with_failure:
        [failure(1)],
        all_completed!(
            all_succeeded_or_first_failed!(id(1), id(2)),
            all_succeeded_or_first_failed!(id(1), id(2))
        )
        => Resolved);
    test_try_future_resolve!(duplicated_leaf_with_unknown_in_all_completed:
        [success(1)],
        all_completed!(id(1), unknown!(id(1), id(2)))
        => Resolved);
    test_try_future_resolve!(duplicated_leaf_partial_resolution:
        [success(1)], all_completed!(id(1), id(2), id(1))
        => Unresolved(all_completed!(id(2))));

    // ==================== Resolution: Not normalized future ====================
    // If a future is not normalized, we just wake up as we don't know how to move forward.

    test_try_future_resolve!(asff_not_normalized:
        [success(1)],
        all_succeeded_or_first_failed!(unknown!(id(1)), unknown!(id(2)))
         => Resolved);

    // asff(1, unknown(all_completed(2, unknown(3))))
    test_try_future_resolve!(asff_unknown_all_completed_with_unknown_resolves_on_leaf:
        [success(1)],
        all_succeeded_or_first_failed!(id(1), unknown!(all_completed!(id(2), unknown!(id(3)))))
        => Unresolved(all_succeeded_or_first_failed!(unknown!(all_completed!(id(2), unknown!(id(3)))))));
    test_try_future_resolve!(asff_unknown_all_completed_with_unknown_partial:
        [success(2)],
        all_succeeded_or_first_failed!(id(1), unknown!(all_completed!(id(2), unknown!(id(3)))))
        => Unresolved(all_succeeded_or_first_failed!(id(1), unknown!(all_completed!(unknown!(id(3)))))));
    test_try_future_resolve!(asff_unknown_all_completed_with_unknown_all_done:
        [success(2), success(3)],
        all_succeeded_or_first_failed!(id(1), unknown!(all_completed!(id(2), unknown!(id(3)))))
        => Resolved);

    // asff(1, unknown(2)) -> unknown(1, 2). Failure of 2 wakes.
    test_try_future_resolve!(normalization_asff_extracts_unknown:
        [failure(2)],
        all_succeeded_or_first_failed!(id(1), unknown!(id(2)))
        => Resolved);
    // asff(1, unknown(2, fsaf(3, 4))) -> unknown(1, 2, fsaf(3, 4))
    test_try_future_resolve!(asff_unknown_fsaf_resolves_on_leaf:
        [success(1)],
        all_succeeded_or_first_failed!(id(1), unknown!(id(2), first_succeeded_or_all_failed!(id(3), id(4))))
        => Unresolved(all_succeeded_or_first_failed!(unknown!(id(2), first_succeeded_or_all_failed!(id(3), id(4))))));
    test_try_future_resolve!(asff_unknown_fsaf_resolves_on_inner_fsaf_success:
        [success(3)],
        all_succeeded_or_first_failed!(id(1), unknown!(id(2), first_succeeded_or_all_failed!(id(3), id(4))))
        => Resolved);
    // 3 fails but 4 pending -> fsaf pending -> nothing completed
    test_try_future_resolve!(asff_unknown_fsaf_failure_doesnt_resolve:
        [failure(3)],
        all_succeeded_or_first_failed!(id(1), unknown!(id(2), first_succeeded_or_all_failed!(id(3), id(4))))
        => Unresolved(all_succeeded_or_first_failed!(id(1), unknown!(id(2), first_succeeded_or_all_failed!(id(4))))));
    // Both 3 and 4 fail -> fsaf completed -> unknown wakes
    test_try_future_resolve!(asff_unknown_fsaf_all_inner_fail:
        [failure(3), failure(4)],
        all_succeeded_or_first_failed!(id(1), unknown!(id(2), first_succeeded_or_all_failed!(id(3), id(4))))
        => Resolved);
    test_try_future_resolve!(asff_unknown_fsaf_pending:
        [],
        all_succeeded_or_first_failed!(id(1), unknown!(id(2), first_succeeded_or_all_failed!(id(3), id(4))))
        => Unresolved(all_succeeded_or_first_failed!(id(1), unknown!(id(2), first_succeeded_or_all_failed!(id(3), id(4))))));

    // ==================== Restate-specific tests ====================

    #[test]
    fn flatten_and_is_empty_across_nesting() {
        let inner_all =
            all_succeeded_or_first_failed!(id(3), first_succeeded_or_all_failed!(id(1), id(2)),);
        let mut fut = first_completed!(id(4), inner_all);

        assert_eq!(fut.flatten(), HashSet::from([id(1), id(2), id(3), id(4)]));
        assert!(!fut.is_empty());

        // Race resolves via the direct leaf — the nested `all` subtree is
        // not drained, so `is_empty()` still reports false.
        assert!(fut.resolve(&HashMap::from([(id(4), OK)])));
        assert!(!fut.is_empty());
    }

    #[test]
    fn split_single() {
        // Single -> FirstCompleted with the single notification, no nested combinators.
        let (combinator, notifications, nested) = UnresolvedFuture::Single(id(1)).split();

        assert_that!(combinator, eq(CombinatorType::Unknown));
        assert_that!(notifications, unordered_elements_are![eq(id(1))]);
        assert_that!(nested, empty());
    }

    #[test]
    fn split_unknown() {
        // Unknown correctly splits as any other combinator.
        let fut = unknown!(
            id(1),
            first_completed!(id(2), id(3)),
            all_succeeded_or_first_failed!(id(4), id(5)),
        );
        let (combinator, notifications, nested) = fut.split();
        assert_that!(combinator, eq(CombinatorType::Unknown));
        assert_that!(notifications, unordered_elements_are![eq(id(1))]);
        assert_that!(
            nested,
            unordered_elements_are![
                pat!(UnresolvedFuture::FirstCompleted(unordered_elements_are![
                    eq(UnresolvedFuture::Single(id(2))),
                    eq(UnresolvedFuture::Single(id(3)))
                ])),
                pat!(UnresolvedFuture::AllSucceededOrFirstFailed(
                    unordered_elements_are![
                        eq(UnresolvedFuture::Single(id(4))),
                        eq(UnresolvedFuture::Single(id(5)))
                    ]
                ))
            ]
        );
    }

    #[test]
    fn split_separates_direct_singles_from_nested_combinators() {
        // all(1, race(3, 4), 2, any(5, 6)) -- direct singles are lifted into the HashSet;
        // nested combinators remain in the Vec untouched. Ordering is interleaved so
        // we also exercise the swap_remove path inside the while-loop.
        let fut = all_succeeded_or_first_failed!(
            id(1),
            first_completed!(id(3), id(4)),
            id(2),
            first_succeeded_or_all_failed!(id(5), id(6)),
        );
        let (combinator, notifications, nested) = fut.split();
        assert_eq!(combinator, CombinatorType::AllSucceededOrFirstFailed);
        assert_eq!(notifications, vec![id(1), id(2)]);
        assert_eq!(nested.len(), 2);
        let preserved: HashSet<_> = nested.iter().flat_map(|f| f.flatten()).collect();
        assert_eq!(preserved, HashSet::from([id(3), id(4), id(5), id(6)]));

        // Empty combinator: the type is preserved but nothing is produced.
        let (combinator, notifications, nested) = UnresolvedFuture::AllCompleted(vec![]).split();
        assert_eq!(combinator, CombinatorType::AllCompleted);
        assert!(notifications.is_empty());
        assert!(nested.is_empty());
    }

    #[test]
    fn resolve_all_iterator_short_circuits_and_returns_true_once_completed() {
        let mut fut = first_completed!(id(1), id(2), id(3));
        let batch = HashMap::from([(id(1), ERR), (id(2), OK)]);
        assert!(fut.resolve(&batch));

        let mut fut = first_completed!(id(1), id(2), id(3));
        let batch = HashMap::from([(id(98), OK), (id(99), OK)]);
        assert!(!fut.resolve(&batch));
    }
}
