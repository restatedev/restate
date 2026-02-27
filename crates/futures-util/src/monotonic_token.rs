// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A monotonically increasing token system for tracking ordered completions.
//!
//! This module provides a lightweight mechanism for a producer to signal that
//! it has finished processing a *prefix* of sequentially issued work items.
//! The core abstraction is a [`Token<T>`], an opaque, ordered identifier
//! drawn from a [`Tokens<T>`] generator.
//!
//! # Roles
//!
//! | Type | Role |
//! |------|------|
//! | [`TokenOwner<T>`] | Root owner. Creates [`Tokens`] generators and retires tokens. |
//! | [`Tokens<T>`] | Generator. Produces new [`Token`]s and creates [`TokenListener`]s. |
//! | [`TokenListener<T>`] | Observer. Asynchronously waits for tokens to be retired. |
//!
//! # Semantics
//!
//! Tokens are monotonically increasing integers (starting at 1). Calling
//! [`retire_through(t)`](TokenOwner::retire_through) signals that all tokens
//! `<= t` have been processed. Listeners waiting on any token in that prefix
//! will be woken up.
//!
//! **Important:** if a caller acquires a token via [`Tokens::next`] but never
//! actually submits work for it, the token still occupies a position in the
//! sequence. When the owner later retires *through* a higher token, the unused
//! token is implicitly included. It is the caller's responsibility to decide
//! whether an unused token should be treated as processed or not. In practice
//! this means: always submit work for every token you acquire, or handle the
//! gap explicitly in your protocol.
//!
//! # Notification model
//!
//! [`TokenListener::changed`] returns the latest retired token at the moment
//! the listener wakes up. Multiple retirements between polls are *coalesced*:
//! the listener sees only the highest value, not each intermediate step. This
//! is intentional — it mirrors the behaviour of `tokio::sync::watch` and
//! keeps the fast path allocation-free.
//!
//! # Type parameter `T`
//!
//! The phantom type parameter `T` prevents accidental mixing of tokens from
//! unrelated systems. Define a zero-sized marker type per domain:
//!
//! ```ignore
//! struct Commit;      // tokens for durable commits
//! struct Compaction;  // tokens for compaction rounds
//! ```
//!
//! # Memory ordering
//!
//! * [`Tokens::next`] uses `Relaxed` — it is a pure counter with no
//!   data dependency.
//! * [`TokenOwner::retire_through`] uses `Release` — it publishes
//!   side-effects that happened before retirement.
//! * [`TokenListener::changed`] and [`TokenListener::retired_through`] use
//!   `Acquire` — they synchronize with the `Release` store and observe all
//!   preceding side-effects.
//!
//! # Example
//!
//! ```
//! # async fn example() {
//! use restate_futures_util::monotonic_token::TokenOwner;
//!
//! struct Flush;
//!
//! let owner = TokenOwner::<Flush>::new();
//! let tokens = owner.new_tokens();
//! let listener = tokens.new_listener();
//!
//! let t1 = tokens.next();
//! let t2 = tokens.next();
//!
//! // Retire through t2 — both t1 and t2 are now considered processed.
//! owner.retire_through(t2);
//!
//! // Listener sees t2 immediately (t1 is implicitly included).
//! let seen = listener.changed(None).await;
//! assert_eq!(seen, t2);
//! # }
//! ```

use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, atomic};

use tokio::sync::Notify;

/// An opaque, ordered token drawn from a [`Tokens`] generator.
///
/// Tokens are [`Copy`], [`Ord`], and [`Eq`]. The phantom type `T` prevents
/// accidental mixing of tokens from unrelated domains.
pub struct Token<T>(NonZeroUsize, PhantomData<T>);

impl<T> Clone for Token<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Token<T> {}
impl<T> Unpin for Token<T> {}

// SAFETY: Token<T> is a plain NonZeroUsize + PhantomData<T>. The phantom type
// parameter is a zero-sized marker with no data — Token is safe to send and
// share across threads regardless of T.
unsafe impl<T> Send for Token<T> {}
unsafe impl<T> Sync for Token<T> {}

impl<T> std::fmt::Debug for Token<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Token")
            .field(&self.0)
            .finish_non_exhaustive()
    }
}

impl<T> std::hash::Hash for Token<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<T> PartialOrd for Token<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Token<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl<T> PartialEq for Token<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<T> Eq for Token<T> {}

struct Inner {
    next: AtomicUsize,
    retired_through: AtomicUsize,
    notify: Notify,
}

/// Root owner of a token sequence.
///
/// The owner has two responsibilities:
/// 1. Create [`Tokens`] generators via [`new_tokens`](Self::new_tokens).
/// 2. Signal completion of a prefix via
///    [`retire_through`](Self::retire_through).
///
/// There should be exactly one `TokenOwner` per logical sequence. Multiple
/// [`Tokens`] generators and [`TokenListener`]s may be created from it.
pub struct TokenOwner<T> {
    inner: Arc<Inner>,
    _phantom: PhantomData<T>,
}

impl<T> TokenOwner<T> {
    /// Creates a new token sequence. The first token produced by any generator
    /// derived from this owner will have an internal value of 1.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                next: AtomicUsize::new(1),
                retired_through: AtomicUsize::new(0),
                notify: Notify::new(),
            }),
            _phantom: PhantomData,
        }
    }

    /// Creates a new [`Tokens`] generator that shares this owner's sequence.
    pub fn new_tokens(&self) -> Tokens<T> {
        Tokens {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }

    /// Marks all tokens `<= through_token` as retired.
    ///
    /// This signals that the entire prefix up to and including `through_token`
    /// has been processed and will not be processed again. All listeners whose
    /// `last_seen` token is less than `through_token` will be woken.
    ///
    /// # Ordering
    ///
    /// The caller must ensure that `through_token` is monotonically
    /// non-decreasing across successive calls. Retiring a token lower than a
    /// previously retired one has no effect (the atomic store will overwrite
    /// the value, but listeners only wake when the value exceeds their
    /// threshold).
    pub fn retire_through(&self, through_token: Token<T>) {
        self.inner
            .retired_through
            .store(through_token.0.get(), atomic::Ordering::Release);
        self.inner.notify.notify_waiters();
    }
}

impl<T> Default for TokenOwner<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A generator of monotonically increasing [`Token`]s.
///
/// Cloning a `Tokens` generator produces a handle to the *same* underlying
/// sequence — tokens produced by either clone are globally ordered.
///
/// `Tokens::next` is thread-safe and lock-free.
pub struct Tokens<T> {
    inner: Arc<Inner>,
    _phantom: PhantomData<T>,
}

impl<T> Clone for Tokens<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T> Tokens<T> {
    /// Produces the next token in the sequence.
    ///
    /// Tokens are guaranteed to be strictly increasing across all clones of
    /// this generator.
    pub fn next(&self) -> Token<T> {
        // SAFETY: `next` is initialised to 1 and only incremented, so the
        // value returned by `fetch_add` is always >= 1 (non-zero).
        unsafe {
            Token(
                NonZeroUsize::new_unchecked(
                    self.inner.next.fetch_add(1, atomic::Ordering::Relaxed),
                ),
                PhantomData,
            )
        }
    }

    /// Creates a new [`TokenListener`] that observes retirements on this
    /// sequence.
    pub fn new_listener(&self) -> TokenListener<T> {
        TokenListener {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

/// An observer that asynchronously waits for tokens to be retired.
///
/// Multiple listeners may exist for the same sequence; all are woken when the
/// owner retires tokens.
pub struct TokenListener<T> {
    inner: Arc<Inner>,
    _phantom: PhantomData<T>,
}

impl<T> Clone for TokenListener<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T> TokenListener<T> {
    /// Returns the highest token retired so far, or `None` if no token has
    /// been retired yet.
    pub fn retired_through(&self) -> Option<Token<T>> {
        let value = self.inner.retired_through.load(atomic::Ordering::Acquire);
        NonZeroUsize::new(value).map(|t| Token(t, PhantomData))
    }

    /// Waits until a token strictly greater than `last_seen` has been retired.
    ///
    /// If `last_seen` is `None`, waits for the very first retirement or returns
    /// the latest retired immediately if it was set.
    ///
    /// Returns the latest retired token at the time of wakeup. Due to
    /// coalescing, this may be greater than the token that triggered the
    /// wakeup.
    pub async fn changed(&self, last_seen: Option<Token<T>>) -> Token<T> {
        let threshold = last_seen.map_or(0, |t| t.0.get());
        let mut notified = std::pin::pin!(self.inner.notify.notified());

        loop {
            // The `Notified` future is guaranteed to receive wakeups from
            // `notify_waiters()` as soon as it has been created, even before
            // being polled. So no `enable()` call is needed here — creation
            // alone is sufficient to register the waiter.
            let current = self.inner.retired_through.load(atomic::Ordering::Acquire);
            if current > threshold {
                // SAFETY: current > threshold >= 0, so current >= 1 (non-zero).
                return Token(unsafe { NonZeroUsize::new_unchecked(current) }, PhantomData);
            }

            // Wait for the next notification, then re-create the future for
            // the next loop iteration (`Notified` is fused — once completed it
            // always returns `Ready` immediately).
            notified.as_mut().await;
            notified.set(self.inner.notify.notified());
        }
    }

}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use super::*;

    /// Marker type for tests.
    struct Test;

    /// Polls a future once and returns whether it resolved.
    fn poll_once<F: Future>(fut: Pin<&mut F>) -> Poll<F::Output> {
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        fut.poll(&mut cx)
    }

    #[tokio::test]
    async fn generator_produces_monotonic_tokens() {
        let owner = TokenOwner::<Test>::new();
        let tokens = owner.new_tokens();
        let t1 = tokens.next();
        let t2 = tokens.next();
        let t3 = tokens.next();
        assert!(t1 < t2);
        assert!(t2 < t3);
    }

    #[tokio::test]
    async fn changed_none_blocks_until_first_retirement() {
        let owner = TokenOwner::<Test>::new();
        let tokens = owner.new_tokens();
        let listener = tokens.new_listener();

        let t1 = tokens.next();

        let mut fut = std::pin::pin!(listener.changed(None));
        assert!(poll_once(fut.as_mut()).is_pending());

        owner.retire_through(t1);
        let result = fut.await;
        assert_eq!(result, t1);
    }

    #[tokio::test]
    async fn changed_resolves_immediately_if_already_retired() {
        let owner = TokenOwner::<Test>::new();
        let tokens = owner.new_tokens();
        let listener = tokens.new_listener();

        let t1 = tokens.next();
        let t2 = tokens.next();

        owner.retire_through(t2);

        // changed(None) resolves immediately since there's already a retirement.
        let result = listener.changed(None).await;
        assert_eq!(result, t2);

        // changed(Some(t1)) also resolves immediately since t2 > t1.
        let result = listener.changed(Some(t1)).await;
        assert_eq!(result, t2);

        // changed(Some(t2)) blocks — nothing newer than t2 yet.
        let mut fut = std::pin::pin!(listener.changed(Some(t2)));
        assert!(poll_once(fut.as_mut()).is_pending());
    }

    #[tokio::test]
    async fn changed_blocks_when_last_seen_equals_current() {
        let owner = TokenOwner::<Test>::new();
        let tokens = owner.new_tokens();
        let listener = tokens.new_listener();

        let t1 = tokens.next();
        let t2 = tokens.next();
        owner.retire_through(t1);

        // t1 is NOT strictly greater than t1, so this blocks.
        let mut fut = std::pin::pin!(listener.changed(Some(t1)));
        assert!(poll_once(fut.as_mut()).is_pending());

        owner.retire_through(t2);
        let result = fut.await;
        assert_eq!(result, t2);
    }

    #[tokio::test]
    async fn coalesced_retirements() {
        let owner = TokenOwner::<Test>::new();
        let tokens = owner.new_tokens();
        let listener = tokens.new_listener();

        let t1 = tokens.next();
        let t2 = tokens.next();
        let t3 = tokens.next();

        // Three rapid retirements coalesce — listener sees the latest.
        owner.retire_through(t1);
        owner.retire_through(t2);
        owner.retire_through(t3);

        let result = listener.changed(None).await;
        assert_eq!(result, t3);
    }

    #[tokio::test]
    async fn sequential_changed_calls_in_a_loop() {
        let owner = TokenOwner::<Test>::new();
        let tokens = owner.new_tokens();
        let listener = tokens.new_listener();

        let t1 = tokens.next();
        let t2 = tokens.next();
        let t3 = tokens.next();

        // First iteration.
        owner.retire_through(t1);
        let seen = listener.changed(None).await;
        assert_eq!(seen, t1);

        // Second iteration: blocks until t2.
        let task = tokio::spawn({
            let listener = listener.clone();
            async move { listener.changed(Some(t1)).await }
        });
        tokio::task::yield_now().await;
        owner.retire_through(t2);
        let seen = task.await.unwrap();
        assert_eq!(seen, t2);

        // Third iteration: retirement before changed call.
        owner.retire_through(t3);
        let seen = listener.changed(Some(t2)).await;
        assert_eq!(seen, t3);
    }

    #[tokio::test]
    async fn multiple_listeners_all_wake() {
        let owner = TokenOwner::<Test>::new();
        let tokens = owner.new_tokens();
        let l1 = tokens.new_listener();
        let l2 = tokens.new_listener();

        let t1 = tokens.next();

        let task1 = tokio::spawn({
            let l = l1.clone();
            async move { l.changed(None).await }
        });
        let task2 = tokio::spawn({
            let l = l2.clone();
            async move { l.changed(None).await }
        });

        tokio::task::yield_now().await;
        owner.retire_through(t1);

        assert_eq!(task1.await.unwrap(), t1);
        assert_eq!(task2.await.unwrap(), t1);
    }

    #[tokio::test]
    async fn concurrent_producer_consumer_no_missed_wakeup() {
        let owner = TokenOwner::<Test>::new();
        let tokens = owner.new_tokens();
        let listener = tokens.new_listener();

        const ITERATIONS: usize = 200;
        let mut all_tokens = Vec::with_capacity(ITERATIONS);
        for _ in 0..ITERATIONS {
            all_tokens.push(tokens.next());
        }
        let last_token = *all_tokens.last().unwrap();

        let producer = tokio::spawn({
            let inner = owner.inner.clone();
            let all_tokens = all_tokens.clone();
            async move {
                for token in all_tokens {
                    inner
                        .retired_through
                        .store(token.0.get(), atomic::Ordering::Release);
                    inner.notify.notify_waiters();
                    if token.0.get() % 10 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }
        });

        let consumer = tokio::spawn(async move {
            let mut last_seen = None;
            let mut wakeup_count = 0usize;
            loop {
                let token = listener.changed(last_seen).await;
                wakeup_count += 1;
                last_seen = Some(token);
                if token >= last_token {
                    break;
                }
            }
            (last_seen.unwrap(), wakeup_count)
        });

        producer.await.unwrap();

        let (final_token, wakeup_count) = tokio::time::timeout(Duration::from_secs(5), consumer)
            .await
            .expect("consumer should not deadlock")
            .unwrap();

        assert_eq!(final_token, last_token);
        assert!(wakeup_count <= ITERATIONS);
        assert!(wakeup_count >= 1);
    }

    #[tokio::test]
    async fn retired_through_reflects_latest() {
        let owner = TokenOwner::<Test>::new();
        let tokens = owner.new_tokens();
        let listener = tokens.new_listener();

        assert_eq!(listener.retired_through(), None);

        let t1 = tokens.next();
        owner.retire_through(t1);
        assert_eq!(listener.retired_through(), Some(t1));

        let _t2 = tokens.next();
        let t3 = tokens.next();
        owner.retire_through(t3);
        // Skipped _t2 is implicitly included in the retired prefix.
        assert_eq!(listener.retired_through(), Some(t3));

        let result = listener.changed(Some(t1)).await;
        assert_eq!(result, t3);
    }

    #[tokio::test(start_paused = true)]
    async fn no_busy_loop_after_wakeup() {
        // Verifies that after one wakeup, calling `changed()` again actually
        // blocks (not spins). With `start_paused = true`, time doesn't advance
        // unless we are truly waiting on IO / timers.
        let owner = TokenOwner::<Test>::new();
        let tokens = owner.new_tokens();
        let listener = tokens.new_listener();

        let t1 = tokens.next();
        let t2 = tokens.next();

        owner.retire_through(t1);
        let seen = listener.changed(None).await;
        assert_eq!(seen, t1);

        let task = tokio::spawn({
            let listener = listener.clone();
            async move { listener.changed(Some(t1)).await }
        });

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!task.is_finished(), "task should be blocked, not spinning");

        owner.retire_through(t2);
        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("should resolve after retirement")
            .unwrap();
        assert_eq!(result, t2);
    }

}
