// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Leader-local fencing tokens used to drop stale invoker effects at write time.

use restate_platform::hash::HashMap;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::FencingToken;

/// Tracks, per in-flight invocation, the fencing token of the invoker attempt whose effects the
/// leader currently accepts.
///
/// A fresh token is [`mint`](Self::mint)ed on every (re)invoke and handed to the invoker, which
/// echoes it on every effect. Before self-proposing an invoker effect the leader checks
/// [`accepts`](Self::accepts): an effect whose token is not the invocation's current token is a
/// straggler from a previous attempt and is dropped. The token is [`clear`](Self::clear)ed when
/// an attempt ends (abort / terminal effect) or is fenced (pause). The whole structure is
/// leader-local and discarded on leadership loss (rebuilt on the next term).
///
/// The counter is global (per leader) and wraps: a straggler would have to survive 2^32
/// intervening invokes to alias a live token, which cannot happen (effects drain in
/// milliseconds).
#[derive(Default)]
pub(crate) struct FencingTokens {
    tokens: HashMap<InvocationId, FencingToken>,
    next: FencingToken,
}

impl FencingTokens {
    /// Mint a fresh token for a (re)invoke of `invocation_id` and record it as the accepted
    /// token, replacing any previous one.
    pub(crate) fn mint(&mut self, invocation_id: InvocationId) -> FencingToken {
        let token = self.next;
        self.next = self.next.wrapping_add(1);
        self.tokens.insert(invocation_id, token);
        token
    }

    /// Whether an invoker effect carrying `token` should be accepted: `true` iff `token` is
    /// `invocation_id`'s current (most recently minted, not since cleared) token.
    pub(crate) fn accepts(&self, invocation_id: &InvocationId, token: FencingToken) -> bool {
        self.tokens.get(invocation_id) == Some(&token)
    }

    /// Stop accepting `invocation_id`'s current token (the attempt ended or was fenced). A later
    /// [`mint`](Self::mint) starts accepting a fresh attempt.
    pub(crate) fn clear(&mut self, invocation_id: &InvocationId) {
        self.tokens.remove(invocation_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_the_current_token() {
        let mut ft = FencingTokens::default();
        let id = InvocationId::mock_random();

        let t1 = ft.mint(id); // invoke (attempt 1)
        assert!(ft.accepts(&id, t1)); // attempt-1 effect accepted

        ft.clear(&id); // pause
        assert!(!ft.accepts(&id, t1)); // straggler dropped while paused

        let t2 = ft.mint(id); // resume (attempt 2)
        assert_ne!(t1, t2);
        assert!(!ft.accepts(&id, t1)); // attempt-1 straggler still fenced
        assert!(ft.accepts(&id, t2)); // attempt-2 effect accepted
    }

    #[test]
    fn distinct_invocations_are_independent() {
        let mut ft = FencingTokens::default();
        let a = InvocationId::mock_random();
        let b = InvocationId::mock_random();

        let ta = ft.mint(a);
        let tb = ft.mint(b);
        assert!(ft.accepts(&a, ta));
        assert!(ft.accepts(&b, tb));

        ft.clear(&a);
        assert!(!ft.accepts(&a, ta)); // a is fenced
        assert!(ft.accepts(&b, tb)); // b is unaffected
    }

    #[test]
    fn unknown_invocation_never_accepts() {
        let ft = FencingTokens::default();
        assert!(!ft.accepts(&InvocationId::mock_random(), 0));
    }
}
