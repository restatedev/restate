// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    num::NonZeroU32,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;

/// The internal state of a token bucket throttling mechanism.
#[derive(Debug)]
enum TokenStateInner {
    /// Waiting for the next token to become available after a specified duration.
    Waiting,
    /// Ready to consume a token immediately.
    Ready,
}

pin_project! {
    /// A token bucket throttling state that implements `Future` for asynchronous rate limiting.
    ///
    /// This struct manages the throttling state of a token bucket and must be actively polled
    /// to make progress. The future returns `Some(())` when the state transitions (e.g., from
    /// waiting to ready), and `None` when already in the ready state.
    ///
    /// # State Machine Behavior
    ///
    /// The implementation uses a state machine with two main states:
    /// - **Ready**: A token is immediately available for consumption
    /// - **Waiting**: Waiting for the scheduled time to elapse before a token becomes available
    ///
    /// # Polling Behavior
    ///
    /// The future must be actively polled to drive state transitions:
    /// - `Poll::Ready(true)` when transitioning from Waiting to Ready
    /// - `Poll::Ready(false)` when already in Ready state
    /// - `Poll::Pending` when waiting for time to elapse
    ///
    /// # Usage Pattern
    ///
    /// 1. Check if ready using `is_ready()`
    /// 2. If ready, consume the token with `consume()`
    /// 3. Always poll the future to ensure state transitions occur
    /// 4. The `step()` method automatically manages state transitions based on token availability
    pub struct TokenState<C = gardal::TokioClock> {
        bucket: super::TokenBucket<C>,
        state: TokenStateInner,
        #[pin]
        sleep: Option<tokio_hrtime::Sleep>,
    }
}

impl<C> TokenState<C>
where
    C: gardal::Clock,
{
    /// Creates a new `TokenState` with the given token bucket.
    ///
    /// The initial state is determined by calling `next()` to check if tokens are immediately
    /// available or if waiting is required.
    pub fn new(bucket: super::TokenBucket<C>) -> Self {
        let (state, sleep) = match bucket
            .consume_with_borrow(unsafe { NonZeroU32::new_unchecked(1) })
            .expect("1 is less than burst capacity")
        {
            None => (TokenStateInner::Ready, None),
            Some(wait_time) => (TokenStateInner::Waiting, Some(wait_time)),
        };

        Self {
            bucket,
            state,
            sleep: sleep.map(|wait_time| tokio_hrtime::sleep(wait_time.into())),
        }
    }

    /// Returns `true` if the token state is ready for immediate consumption.
    ///
    /// This method checks the current state without consuming any tokens.
    pub fn is_ready(&self) -> bool {
        matches!(self.state, TokenStateInner::Ready)
    }

    /// Consumes the current token and advances to the next state.
    ///
    /// This method will panic if called when the state is not ready. Always check
    /// `is_ready()` before calling this method.
    ///
    /// # Panics
    ///
    /// Panics if the token state is not ready (i.e., if `is_ready()` returns `false`).
    pub fn consume(self: Pin<&mut Self>) {
        assert!(
            matches!(self.state, TokenStateInner::Ready),
            "token state is not ready"
        );

        self.step();
    }

    /// Advances the token state based on current bucket availability.
    ///
    /// This method attempts to consume a token immediately. If successful, the state
    /// transitions to Ready. If the bucket is empty, it schedules a wait and transitions
    /// to Waiting state.
    fn step(self: Pin<&mut Self>) {
        // Try to drive the state in case we can move immediately to ready state. Otherwise go into waiting state.
        let mut this = self.project();
        match this
            .bucket
            .consume_with_borrow(unsafe { NonZeroU32::new_unchecked(1) })
            .expect("1 is less than burst capacity")
        {
            None => *this.state = TokenStateInner::Ready,
            Some(wait_time) => {
                *this.state = TokenStateInner::Waiting;
                this.sleep.set(Some(tokio_hrtime::sleep(wait_time.into())));
            }
        }
    }
}

impl<C> Future for TokenState<C>
where
    C: gardal::Clock,
{
    type Output = bool;

    /// Polls the token state to drive state transitions.
    ///
    /// This implementation handles the async state machine:
    /// - If in Waiting state, checks if the sleep timer has completed and transitions to Ready
    /// - If in Ready state, returns `None` to indicate no state change
    /// - Returns `Pending` while waiting for time to elapse
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        match this.state {
            TokenStateInner::Waiting => {
                if let Some(sleep) = this.sleep.as_mut().as_pin_mut() {
                    if let Poll::Ready(()) = sleep.poll(cx) {
                        *this.state = TokenStateInner::Ready;
                        this.sleep.set(None);
                        return Poll::Ready(true);
                    }
                }
            }
            TokenStateInner::Ready => {
                return Poll::Ready(false);
            }
        };

        Poll::Pending
    }
}

pub struct ThrottlingBucketsMut<'a> {
    pub invocations: Pin<&'a mut TokenState>,
    pub actions: Pin<&'a mut TokenState>,
}

/// A collection of token buckets for throttling invocations and actions.
pub struct ThrottlingBuckets {
    invocations: TokenState,
    actions: TokenState,
}

impl ThrottlingBuckets {
    pub fn new(invocations: TokenState, actions: TokenState) -> Self {
        Self {
            invocations,
            actions,
        }
    }

    pub fn as_mut(&mut self) -> ThrottlingBucketsMut {
        ThrottlingBucketsMut {
            invocations: Pin::new(&mut self.invocations),
            actions: Pin::new(&mut self.actions),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use gardal::ManualClock;
    use googletest::assert_that;
    use googletest::prelude::*;

    use super::*;

    fn create_token_bucket_with_tokens(
        tokens: u32,
    ) -> (
        super::super::TokenBucket<Arc<ManualClock>>,
        Arc<ManualClock>,
    ) {
        use gardal::{RateLimit, TokenBucket as GardalTokenBucket};

        let clock = Arc::new(gardal::ManualClock::new(0.0));
        let bucket = GardalTokenBucket::from_parts(
            RateLimit::per_second(unsafe { NonZeroU32::new_unchecked(tokens) }),
            Arc::clone(&clock),
        );
        (bucket, clock)
    }

    #[tokio::test]
    async fn test_token_state_new() {
        let (bucket, _) = create_token_bucket_with_tokens(10);
        // bucket is empty

        let token_state = TokenState::new(bucket);

        assert!(!token_state.is_ready());

        let (bucket, _) = create_token_bucket_with_tokens(10);
        // pre-fill the bucket with tokens
        bucket.add_tokens(10);

        let token_state = TokenState::new(bucket);

        assert!(token_state.is_ready());
    }

    #[tokio::test]
    async fn test_token_state_immediate_ready() {
        let (bucket, _) = create_token_bucket_with_tokens(10);
        bucket.add_tokens(10);

        let token_state = TokenState::new(bucket);
        tokio::pin!(token_state);
        // First poll should immediately transition to Ready since tokens are available
        let result = token_state
            .as_mut()
            .poll(&mut std::task::Context::from_waker(
                futures::task::noop_waker_ref(),
            ));

        // polling should be ready immediately but with None
        // since the state did not change
        assert_that!(result, eq(Poll::Ready(false)));
        assert!(token_state.is_ready());
    }

    #[tokio::test]
    async fn test_token_state_empty_to_waiting() {
        let (bucket, _) = create_token_bucket_with_tokens(10);
        let token_state = TokenState::new(bucket);
        tokio::pin!(token_state);

        // First poll should transition to Waiting since no tokens are available
        let result = token_state
            .as_mut()
            .poll(&mut std::task::Context::from_waker(
                futures::task::noop_waker_ref(),
            ));

        assert_that!(result, eq(Poll::Pending));
        assert!(!token_state.is_ready());
    }

    #[tokio::test]
    async fn test_token_state_consume_ready_then_wait() {
        let (bucket, _) = create_token_bucket_with_tokens(10);
        bucket.add_tokens(2);

        let token_state = TokenState::new(bucket);
        tokio::pin!(token_state);

        assert!(token_state.is_ready());
        token_state.as_mut().consume();

        // Since we have 10 tokens, we should be ready again immediately
        assert!(token_state.is_ready());

        token_state.as_mut().consume();
        assert!(!token_state.is_ready());

        token_state.as_mut().await;
        assert!(token_state.is_ready());
    }

    #[tokio::test]
    #[should_panic(expected = "token state is not ready")]
    async fn test_token_state_consume_not_ready_panics() {
        let (bucket, _) = create_token_bucket_with_tokens(10);
        let token_state = TokenState::new(bucket);
        tokio::pin!(token_state);
        // Try to consume without being ready - should panic
        token_state.consume();
    }

    #[tokio::test]
    async fn test_token_state_multiple_consumes() {
        let (bucket, _) = create_token_bucket_with_tokens(5);
        bucket.add_tokens(5);
        let token_state = TokenState::new(bucket);
        tokio::pin!(token_state);

        // Consume tokens multiple times
        for i in 0..5 {
            // Poll to get to Ready state
            let _ = token_state
                .as_mut()
                .poll(&mut std::task::Context::from_waker(
                    futures::task::noop_waker_ref(),
                ));

            assert!(
                token_state.is_ready(),
                "Should be ready for consumption {i}"
            );
            token_state.as_mut().consume();
        }

        // After consuming all tokens, next poll should go to Waiting
        let result = token_state
            .as_mut()
            .poll(&mut std::task::Context::from_waker(
                futures::task::noop_waker_ref(),
            ));

        assert_that!(result, eq(Poll::Pending));
    }
}
