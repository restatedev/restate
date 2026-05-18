// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A core aspect of Restate is its ability to retry invocations. This module contains the types defining retries.

use std::borrow::Cow;
use std::cmp;
use std::future::Future;
use std::num::NonZeroUsize;
use std::time::Duration;

use restate_util_time::FriendlyDuration;

use rand::Rng;

const DEFAULT_JITTER_MULTIPLIER: f32 = 0.3;

/// This struct represents the policy to execute retries.
///
/// It can be used in components which needs to configure policies to execute retries.
///
/// To use it:
///
/// ```rust
/// use std::time::Duration;
/// use restate_types::retries::RetryPolicy;
///
/// // Define the retry policy
/// let retry_policy = RetryPolicy::fixed_delay(Duration::from_millis(100), Some(10));
///
/// // Transform it in an iterator
/// let mut retry_iter = retry_policy.into_iter();
///
/// // Now use it
/// loop {
///     // Do some operation
/// # let operation_succeeded = true;
///     if operation_succeeded {
///         // Our operation succeeded, we can exit the loop
///         break;
///     }
///
///     let next_retry = retry_iter.next();
///     if let Some(next_timer) = next_retry {
///         // Sleep for next_timer
///     } else {
///         // Retries exhausted
///         break;
///     }
/// }
/// ```
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(title = "Retry policy", description = "Definition of a retry policy")
)]
pub enum RetryPolicy {
    /// # None
    ///
    /// No retry strategy.
    #[default]
    None,
    /// # Fixed delay
    ///
    /// Retry with a fixed delay strategy.
    FixedDelay {
        /// # Interval
        ///
        /// Interval between retries.
        ///
        /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
        #[serde(with = "serde_with::As::<FriendlyDuration>")]
        #[cfg_attr(feature = "schemars", schemars(with = "FriendlyDuration"))]
        interval: Duration,
        /// # Max attempts
        ///
        /// Number of maximum attempts before giving up. Infinite retries if unset.
        max_attempts: Option<NonZeroUsize>,
    },
    /// # Exponential
    ///
    /// Retry with an exponential strategy. The next retry is computed as `min(last_retry_interval * factor, max_interval)`.
    Exponential {
        /// # Initial Interval
        ///
        /// Initial interval for the first retry attempt.
        ///
        /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
        #[serde(with = "serde_with::As::<FriendlyDuration>")]
        #[cfg_attr(feature = "schemars", schemars(with = "FriendlyDuration"))]
        initial_interval: Duration,

        /// # Factor
        ///
        /// The factor to use to compute the next retry attempt.
        factor: f32,

        /// # Max attempts
        ///
        /// Number of maximum attempts before giving up. Infinite retries if unset.
        max_attempts: Option<NonZeroUsize>,

        /// # Max interval
        ///
        /// Maximum interval between retries.
        ///
        /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
        #[serde(with = "serde_with::As::<Option<FriendlyDuration>>")]
        #[cfg_attr(feature = "schemars", schemars(with = "Option<FriendlyDuration>"))]
        max_interval: Option<Duration>,
    },
}

impl RetryPolicy {
    pub fn fixed_delay(interval: Duration, max_attempts: Option<usize>) -> Self {
        Self::FixedDelay {
            interval,
            max_attempts: max_attempts.map(|m| NonZeroUsize::new(m).expect("non-zero")),
        }
    }

    pub fn exponential(
        initial_interval: Duration,
        factor: f32,
        max_attempts: Option<usize>,
        max_interval: Option<Duration>,
    ) -> Self {
        // Formula to compute the time based on number of retries:
        // y = \sum_{n=0}^{x} \min \left( max_interval,\ initial_interval \cdot factor x \right)
        Self::Exponential {
            initial_interval,
            factor,
            max_attempts: max_attempts.map(|m| NonZeroUsize::new(m).expect("non-zero")),
            max_interval,
        }
    }

    pub fn max_attempts(&self) -> Option<NonZeroUsize> {
        match self {
            RetryPolicy::None => None,
            RetryPolicy::FixedDelay { max_attempts, .. }
            | RetryPolicy::Exponential { max_attempts, .. } => *max_attempts,
        }
    }

    /// Retry the provided closure respecting this retry policy.
    pub async fn retry<T, E, Fn, Fut>(self, mut operation: Fn) -> Result<T, E>
    where
        Fn: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut retry_iter = self.into_iter();
        loop {
            match (operation().await, retry_iter.next()) {
                (Ok(res), _) => return Ok(res),
                (Err(e), None) => return Err(e),
                (Err(_), Some(timer)) => {
                    tokio::time::sleep(timer).await;
                }
            }
        }
    }

    /// Retry the provided closure respecting this retry policy.
    /// Calls inspect with the number of attempts and the error on each failed try,
    /// except for the last retry.
    pub async fn retry_with_inspect<T, E, F, Fut, I>(
        self,
        mut operation: F,
        mut inspect: I,
    ) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        I: FnMut(u32, E),
    {
        let mut retry_iter = self.into_iter();
        let mut attempts = 0;
        loop {
            attempts += 1;
            match (operation().await, retry_iter.next()) {
                (Ok(res), _) => return Ok(res),
                (Err(e), None) => {
                    return Err(e);
                }
                (Err(e), Some(timer)) => {
                    inspect(attempts, e);
                    tokio::time::sleep(timer).await;
                }
            }
        }
    }

    /// Retry the provided closure respecting this retry policy and the retry condition.
    pub async fn retry_if<T, E, Fn, Fut, C>(
        self,
        mut operation: Fn,
        mut condition: C,
    ) -> Result<T, E>
    where
        Fn: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        C: FnMut(&E) -> bool,
    {
        let mut retry_iter = self.into_iter();
        loop {
            match operation().await {
                Ok(res) => return Ok(res),
                Err(err) => {
                    if condition(&err) {
                        if let Some(pause) = retry_iter.next() {
                            tokio::time::sleep(pause).await;
                        } else {
                            return Err(err);
                        }
                    } else {
                        return Err(err);
                    }
                }
            }
        }
    }

    pub fn iter(&self) -> RetryIter<'_> {
        let policy = Cow::Borrowed(self);
        RetryIter {
            policy,
            attempts: 0,
            last_retry: None,
        }
    }

    /// Build a `RetryIter` as if `attempts_made` attempts already happened.
    /// The next delay returned will correspond to attempt `attempts_made + 1`,
    /// honoring the configured policy (including per-step `max_interval` clamping
    /// for exponential policies).
    pub fn iter_after(&self, attempts_made: usize) -> RetryIter<'_> {
        let mut iter = self.iter();
        iter.fast_forward(attempts_made);
        iter
    }
}

impl IntoIterator for RetryPolicy {
    type Item = Duration;
    type IntoIter = RetryIter<'static>;

    fn into_iter(self) -> Self::IntoIter {
        RetryIter {
            policy: Cow::Owned(self),
            attempts: 0,
            last_retry: None,
        }
    }
}

/// Possible outcomes for calculating the duration of a retry policy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitDuration {
    Infinite,
    Finite(Duration),
    // No retries left, time left is ZERO.
    None,
}
impl WaitDuration {
    pub fn is_infinite(&self) -> bool {
        matches!(self, WaitDuration::Infinite)
    }

    pub fn is_none(&self) -> bool {
        matches!(self, WaitDuration::None)
    }

    /// Returns the duration if it is finite, otherwise panics.
    pub fn unwrap(self) -> Duration {
        match self {
            WaitDuration::Finite(d) => d,
            WaitDuration::Infinite => panic!("Infinite duration"),
            WaitDuration::None => panic!("No duration left"),
        }
    }

    pub fn unwrap_or(self, default: Duration) -> Duration {
        match self {
            WaitDuration::Finite(d) => d,
            WaitDuration::Infinite => default,
            WaitDuration::None => default,
        }
    }

    /// Subtracts a duration from the current wait duration.
    pub fn subtract(mut self, duration: Duration) -> Self {
        match self {
            WaitDuration::Finite(d) => {
                if d > duration {
                    self = WaitDuration::Finite(d - duration);
                } else {
                    self = WaitDuration::None;
                }
            }
            WaitDuration::Infinite => {}
            WaitDuration::None => {}
        }
        self
    }

    /// returns None if remaining time is zero
    pub fn min(&self, other: Duration) -> Option<Duration> {
        let remaining = match self {
            WaitDuration::Finite(d) => (*d).min(other),
            WaitDuration::Infinite => other,
            WaitDuration::None => return None,
        };
        if remaining.is_zero() {
            None
        } else {
            Some(remaining)
        }
    }
}

#[derive(Debug)]
pub struct RetryIter<'a> {
    policy: Cow<'a, RetryPolicy>,
    attempts: usize,
    last_retry: Option<Duration>,
}

impl RetryIter<'_> {
    /// The number of attempts on this retry iterator so far
    pub fn attempts(&self) -> usize {
        self.attempts
    }

    pub fn max_attempts(&self) -> usize {
        let max_attempts = match self.policy.as_ref() {
            RetryPolicy::None => return 0,
            RetryPolicy::FixedDelay { max_attempts, .. } => max_attempts,
            RetryPolicy::Exponential { max_attempts, .. } => max_attempts,
        };
        max_attempts.unwrap_or(NonZeroUsize::MAX).into()
    }

    pub fn remaining_attempts(&self) -> usize {
        self.max_attempts().saturating_sub(self.attempts())
    }

    pub fn is_infinite(&self) -> bool {
        match self.policy.as_ref() {
            RetryPolicy::None => false,
            RetryPolicy::FixedDelay { max_attempts, .. } => max_attempts.is_none(),
            RetryPolicy::Exponential { max_attempts, .. } => max_attempts.is_none(),
        }
    }

    /// Calculates the total remaining duration of all subsequent retries combined.
    ///
    /// This will return `WaitDuration::Infinite` if this retry policy does not have a maximum number of attempts
    /// (infinite), or `WaitDuration::None` if no remaining retries are available.
    pub fn remaining_cumulative_duration(&self) -> WaitDuration {
        if self.is_infinite() {
            return WaitDuration::Infinite;
        }
        let Some(next_delay) = self.peek_next() else {
            return WaitDuration::None;
        };

        match self.policy.as_ref() {
            RetryPolicy::None => WaitDuration::None,
            RetryPolicy::FixedDelay { interval, .. } => {
                WaitDuration::Finite(interval.mul_f64(self.remaining_attempts() as f64))
            }
            RetryPolicy::Exponential {
                factor,
                max_interval,
                ..
            } => {
                let retries_left = self.remaining_attempts();

                //-------------------------------------------------------------
                // Put all arithmetic in f64 milliseconds for convenience
                //-------------------------------------------------------------
                let r = *factor as f64;
                let d1_ms = next_delay.as_secs_f64() * 1_000.0; // d₁
                let cap_ms = max_interval.map(|d| d.as_secs_f64() * 1_000.0); // M

                //-------------------------------------------------------------
                // How many future delays remain purely exponential (< cap)?
                //-------------------------------------------------------------
                let n_exp = match cap_ms {
                    None => retries_left,       // no cap at all
                    Some(m) if d1_ms >= m => 0, // already above / at the cap
                    Some(m) => {
                        // smallest j s.t. d₁·rʲ ≥ M  →  j = ceil(log_r(M/d₁))
                        let ceil_j = ((m / d1_ms).ln() / r.ln()).ceil() as usize;
                        retries_left.min(ceil_j)
                    }
                };

                //-------------------------------------------------------------
                // Geometric part (those still < cap)
                //-------------------------------------------------------------
                let geom_ms = if n_exp == 0 {
                    0.0
                } else {
                    d1_ms * (r.powi(n_exp as i32) - 1.0) / (r - 1.0)
                };

                //-------------------------------------------------------------
                // Flat tail at the cap, if any
                //-------------------------------------------------------------
                let cap_tail_ms = match cap_ms {
                    Some(m) => (retries_left - n_exp) as f64 * m,
                    None => 0.0,
                };

                WaitDuration::Finite(Duration::from_secs_f64((geom_ms + cap_tail_ms) / 1_000.0))
            }
        }
    }

    pub fn last_retry(&self) -> Option<Duration> {
        self.last_retry
    }

    /// Advance internal state by `n` attempts without producing delays.
    /// For exponential policies this replays the per-step `max_interval` clamp
    /// so subsequent delays match the configured progression exactly.
    pub fn fast_forward(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        let steps_to_replay = n.min(self.remaining_attempts());
        if let RetryPolicy::Exponential {
            initial_interval,
            factor,
            max_interval,
            ..
        } = self.policy.as_ref()
            && steps_to_replay > 0
        {
            let cap = max_interval.unwrap_or(Duration::MAX);
            // Replay: a fresh iter's first `next()` sets last_retry = initial_interval;
            // each subsequent `next()` multiplies by `factor` and clamps to `cap`.
            let (mut d, mult_steps) = match self.last_retry {
                None => (*initial_interval, steps_to_replay - 1),
                Some(last) => (last, steps_to_replay),
            };
            for _ in 0..mult_steps {
                d = cmp::min(saturating_mul_f32(d, *factor), cap);
                if d >= cap {
                    // Further steps would all stay at cap; stop early.
                    break;
                }
            }
            self.last_retry = Some(d);
        }
        self.attempts = self.attempts.saturating_add(n);
        // Clamp to max_attempts for bounded policies so subsequent
        // `peek_next`/`next` arithmetic (`self.attempts + 1`) cannot overflow.
        if let Some(limit) = self.policy.max_attempts() {
            self.attempts = self.attempts.min(limit.into());
        }
    }

    /// peeks the next delay without adding jitter
    pub fn peek_next(&self) -> Option<Duration> {
        match self.policy.as_ref() {
            RetryPolicy::None => None,
            RetryPolicy::FixedDelay {
                interval,
                max_attempts,
            } => {
                if max_attempts.is_some_and(|limit| (self.attempts + 1) > limit.into()) {
                    None
                } else {
                    Some(*interval)
                }
            }
            RetryPolicy::Exponential {
                initial_interval,
                factor,
                max_attempts,
                max_interval,
            } => {
                if max_attempts.is_some_and(|limit| (self.attempts + 1) > limit.into()) {
                    None
                } else if self.last_retry.is_some() {
                    let new_retry = cmp::min(
                        saturating_mul_f32(self.last_retry.unwrap(), *factor),
                        max_interval.map(Into::into).unwrap_or(Duration::MAX),
                    );
                    Some(new_retry)
                } else {
                    Some(*initial_interval)
                }
            }
        }
    }
}

impl Iterator for RetryIter<'_> {
    type Item = Duration;

    /// adds up to 1/3 target duration as jitter
    fn next(&mut self) -> Option<Self::Item> {
        self.attempts = self.attempts.saturating_add(1);
        match self.policy.as_ref() {
            RetryPolicy::None => None,
            RetryPolicy::FixedDelay {
                interval,
                max_attempts,
            } => {
                if max_attempts.is_some_and(|limit| self.attempts > limit.into()) {
                    None
                } else {
                    Some(with_jitter(*interval, DEFAULT_JITTER_MULTIPLIER))
                }
            }
            RetryPolicy::Exponential {
                initial_interval,
                factor,
                max_attempts,
                max_interval,
            } => {
                if max_attempts.is_some_and(|limit| self.attempts > limit.into()) {
                    None
                } else if self.last_retry.is_some() {
                    let new_retry = cmp::min(
                        saturating_mul_f32(self.last_retry.unwrap(), *factor),
                        max_interval.map(Into::into).unwrap_or(Duration::MAX),
                    );
                    self.last_retry = Some(new_retry);
                    Some(with_jitter(new_retry, DEFAULT_JITTER_MULTIPLIER))
                } else {
                    self.last_retry = Some(*initial_interval);
                    Some(with_jitter(*initial_interval, DEFAULT_JITTER_MULTIPLIER))
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let max_attempts = match self.policy.as_ref() {
            RetryPolicy::None => return (0, Some(0)),
            RetryPolicy::FixedDelay { max_attempts, .. } => max_attempts,
            RetryPolicy::Exponential { max_attempts, .. } => max_attempts,
        };
        let max_attempts: usize = max_attempts.unwrap_or(NonZeroUsize::MAX).into();
        let remaining_attempts = max_attempts.saturating_sub(self.attempts);
        (remaining_attempts, Some(remaining_attempts))
    }
}

/// Multiplies a [`Duration`] by an `f32` factor, saturating at [`Duration::MAX`]
/// instead of panicking on overflow, non-finite, or negative results.
fn saturating_mul_f32(d: Duration, factor: f32) -> Duration {
    Duration::try_from_secs_f32(d.as_secs_f32() * factor).unwrap_or(Duration::MAX)
}

// Jitter is a random duration added to the desired target, it ranges from 3ms to
// (max_multiplier * duration) of the original requested delay. The minimum of +3ms
// is to avoid falling into common zero-ending values (0, 10, 100, etc.) which are
// common cause of harmonics in systems (avoiding resonance frequencies)
static MIN_JITTER: Duration = Duration::from_millis(3);

pub fn with_jitter(duration: Duration, max_multiplier: f32) -> Duration {
    let max_jitter = duration.mul_f32(max_multiplier);
    if max_jitter <= MIN_JITTER {
        // We can't get a random value unless max_jitter is higher than MIN_JITTER.
        duration + MIN_JITTER
    } else {
        let jitter = rand::rng().random_range(MIN_JITTER..max_jitter);
        duration + jitter
    }
}

impl ExactSizeIterator for RetryIter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn no_retry_policy() {
        assert_eq!(
            Vec::<Duration>::new(),
            RetryPolicy::None.into_iter().collect::<Vec<_>>()
        )
    }

    #[test]
    fn fixed_delay_retry_policy() {
        let expected = [Duration::from_millis(100); 10];
        let actuals = RetryPolicy::fixed_delay(Duration::from_millis(100), Some(10))
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(actuals.len(), expected.len());
        for (expected, actual) in expected.iter().zip(actuals.iter()) {
            assert!(within_jitter(*expected, *actual, DEFAULT_JITTER_MULTIPLIER));
        }
    }

    #[test]
    fn exponential_retry_policy() {
        let expected = [
            // Manually building this powers to avoid rounding issues :)
            Duration::from_millis(100),
            Duration::from_millis(100).mul_f32(2.0),
            Duration::from_millis(100).mul_f32(2.0).mul_f32(2.0),
            Duration::from_millis(100)
                .mul_f32(2.0)
                .mul_f32(2.0)
                .mul_f32(2.0),
            Duration::from_millis(100)
                .mul_f32(2.0)
                .mul_f32(2.0)
                .mul_f32(2.0)
                .mul_f32(2.0),
        ];

        let actuals = RetryPolicy::exponential(Duration::from_millis(100), 2.0, Some(5), None)
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(actuals.len(), expected.len());
        for (expected, actual) in expected.iter().zip(actuals.iter()) {
            assert!(within_jitter(*expected, *actual, DEFAULT_JITTER_MULTIPLIER));
        }
    }

    #[test]
    fn exponential_retry_policy_with_max_interval() {
        let expected = [
            // Manually building this powers to avoid rounding issues :)
            Duration::from_millis(100),
            Duration::from_millis(100).mul_f32(2.0),
            Duration::from_millis(100).mul_f32(2.0).mul_f32(2.0),
            Duration::from_millis(100)
                .mul_f32(2.0)
                .mul_f32(2.0)
                .mul_f32(2.0),
            Duration::from_secs(1),
        ];
        let actuals = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            Some(5),
            Some(Duration::from_secs(1)),
        )
        .into_iter()
        .collect::<Vec<_>>();
        assert_eq!(actuals.len(), expected.len());
        for (expected, actual) in expected.iter().zip(actuals.iter()) {
            assert!(within_jitter(*expected, *actual, DEFAULT_JITTER_MULTIPLIER));
        }
    }

    fn within_jitter(expected: Duration, actual: Duration, max_multiplier: f32) -> bool {
        let min_inc_jitter = expected + MIN_JITTER;
        let max_inc_jitter = expected + expected.mul_f32(max_multiplier);
        actual >= min_inc_jitter && actual <= max_inc_jitter
    }

    fn within_rounding_error(expected: Duration, actual: Duration) -> bool {
        let min_inc_jitter = expected - Duration::from_millis(1);
        let max_inc_jitter = expected + Duration::from_millis(1);
        actual >= min_inc_jitter && actual <= max_inc_jitter
    }

    #[tokio::test(start_paused = true)]
    async fn conditional_retry() {
        let retry_policy = RetryPolicy::fixed_delay(Duration::from_millis(100), Some(10));

        let attempts = Arc::new(AtomicU64::new(0));

        let result = retry_policy
            .retry_if(
                || {
                    let previous_value = attempts.fetch_add(1, Ordering::Relaxed);
                    future::ready(Err::<(), _>(previous_value))
                },
                |err| *err < 5,
            )
            .await;

        assert_eq!(result, Err(5));
    }

    #[test]
    fn remaining_duration() {
        // no max attempts
        let iter =
            RetryPolicy::exponential(Duration::from_millis(100), 2.0, None, None).into_iter();
        assert_eq!(iter.remaining_cumulative_duration(), WaitDuration::Infinite);

        // 10 fixed attempts
        let mut iter = RetryPolicy::fixed_delay(Duration::from_millis(100), Some(10)).into_iter();
        assert!(within_rounding_error(
            Duration::from_millis(1000),
            iter.remaining_cumulative_duration().unwrap()
        ));
        iter.next();
        assert!(within_rounding_error(
            iter.remaining_cumulative_duration().unwrap(),
            Duration::from_millis(900)
        ));

        // exponential with max attempts, no max interval
        let mut iter =
            RetryPolicy::exponential(Duration::from_millis(100), 2.0, Some(5), None).into_iter();
        // 100 + 200 + 400 + 800 + 1600 = 3100
        assert!(within_rounding_error(
            iter.remaining_cumulative_duration().unwrap(),
            Duration::from_millis(3100)
        ));
        // skip first two
        iter.next();
        iter.next();
        // _ + _ + 400 + 800 + 1600 = 2800
        assert!(within_rounding_error(
            iter.remaining_cumulative_duration().unwrap(),
            Duration::from_millis(2800)
        ));

        // capped at 500ms
        let mut iter = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            Some(5),
            Some(Duration::from_millis(500)),
        )
        .into_iter();

        // 100 + 200 + 400 + 500 + 500 = 1700
        assert!(within_rounding_error(
            iter.remaining_cumulative_duration().unwrap(),
            Duration::from_millis(1700)
        ));
        // skip first two
        iter.next();
        iter.next();
        // _ + _ + 400 + 500 + 500 = 1400
        assert!(within_rounding_error(
            iter.remaining_cumulative_duration().unwrap(),
            Duration::from_millis(1400)
        ));
        iter.next();
        iter.next();
        assert!(within_rounding_error(
            iter.remaining_cumulative_duration().unwrap(),
            Duration::from_millis(500)
        ));

        iter.next();
        // no more left
        assert_eq!(iter.remaining_cumulative_duration(), WaitDuration::None);
    }

    #[test]
    fn fast_forward_fixed_delay() {
        let policy = RetryPolicy::fixed_delay(Duration::from_millis(100), Some(10));

        // After 5 attempts, the 6th delay should still be the configured interval.
        let mut iter = policy.iter_after(5);
        assert_eq!(iter.attempts(), 5);
        assert_eq!(iter.remaining_attempts(), 5);
        assert_eq!(iter.peek_next(), Some(Duration::from_millis(100)));
        let next = iter.next().expect("delay available");
        assert!(within_jitter(
            Duration::from_millis(100),
            next,
            DEFAULT_JITTER_MULTIPLIER
        ));
        assert_eq!(iter.attempts(), 6);

        // Drains until max_attempts reached.
        let remaining: Vec<_> = iter.collect();
        assert_eq!(remaining.len(), 4);
    }

    #[test]
    fn fast_forward_exponential_matches_step_by_step() {
        let make = || RetryPolicy::exponential(Duration::from_millis(100), 2.0, Some(10), None);

        // Step-by-step reference path.
        let mut reference = make().into_iter();
        for _ in 0..5 {
            reference.next();
        }
        let reference_last = reference.last_retry();

        // Fast-forwarded path.
        let policy = make();
        let mut iter = policy.iter_after(5);
        assert_eq!(iter.attempts(), 5);
        assert_eq!(iter.last_retry(), reference_last);

        // 6th delay (un-jittered) = 100ms * 2^5 = 3200ms (allow f32 rounding slack)
        let peeked = iter.peek_next().expect("delay available");
        assert!(within_rounding_error(Duration::from_millis(3200), peeked));
        let actual = iter.next().expect("delay");
        assert!(within_jitter(peeked, actual, DEFAULT_JITTER_MULTIPLIER));
    }

    #[test]
    fn fast_forward_exponential_respects_max_interval_clamp() {
        // initial=100ms, factor=2.0, cap=500ms. Per-step clamp: 100,200,400,500,500,500,...
        let policy = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            Some(10),
            Some(Duration::from_millis(500)),
        );

        // After 4 attempts the per-step clamp has settled at 500ms.
        let iter = policy.iter_after(4);
        assert_eq!(iter.last_retry(), Some(Duration::from_millis(500)));
        assert_eq!(iter.peek_next(), Some(Duration::from_millis(500)));

        // After 7 attempts, still clamped at 500ms (closed-form 100*2^6=6400 would be wrong).
        let iter = policy.iter_after(7);
        assert_eq!(iter.last_retry(), Some(Duration::from_millis(500)));
        assert_eq!(iter.peek_next(), Some(Duration::from_millis(500)));
    }

    #[test]
    fn fast_forward_beyond_max_attempts_exhausts_iter() {
        let policy = RetryPolicy::fixed_delay(Duration::from_millis(100), Some(3));
        let mut iter = policy.iter_after(3);
        assert_eq!(iter.peek_next(), None);
        assert_eq!(iter.next(), None);

        // Overshoot is allowed; iterator stays exhausted.
        let mut iter = policy.iter_after(99);
        assert_eq!(iter.next(), None);

        let policy = RetryPolicy::exponential(Duration::from_secs(1), 2.0, Some(3), None);
        let mut iter = policy.iter_after(99);
        // Bounded policy: overshoot is clamped to max_attempts so that subsequent
        // arithmetic on `self.attempts + 1` cannot overflow.
        assert_eq!(iter.attempts(), 3);
        assert_eq!(iter.remaining_attempts(), 0);
        assert_eq!(iter.peek_next(), None);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }

    #[test]
    fn fast_forward_usize_max_on_bounded_policy_does_not_overflow() {
        // `iter_after(usize::MAX)` on a bounded policy must not cause
        // `self.attempts + 1` to overflow inside `peek_next`/`next`.
        let policy = RetryPolicy::fixed_delay(Duration::from_millis(100), Some(5));
        let mut iter = policy.iter_after(usize::MAX);
        assert_eq!(iter.attempts(), 5);
        assert_eq!(iter.remaining_attempts(), 0);
        assert_eq!(iter.peek_next(), None);
        assert_eq!(iter.next(), None);

        let policy = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            Some(5),
            Some(Duration::from_millis(500)),
        );
        let mut iter = policy.iter_after(usize::MAX);
        assert_eq!(iter.attempts(), 5);
        assert_eq!(iter.peek_next(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn fast_forward_unbounded_does_not_panic_on_mul_f32_overflow() {
        // factor=2.0 with no cap and unbounded max_attempts would overflow
        // `Duration::mul_f32` after ~64 steps. Replay must saturate, not panic.
        let policy = RetryPolicy::exponential(Duration::from_millis(100), 2.0, None, None);
        let iter = policy.iter_after(1_000);
        // Reached Duration::MAX saturation rather than panicking.
        assert_eq!(iter.last_retry(), Some(Duration::MAX));
        assert_eq!(iter.peek_next(), Some(Duration::MAX));
    }

    #[test]
    fn pathological_factor_does_not_panic() {
        // NaN / negative factor must saturate to Duration::MAX instead of panicking
        // inside `mul_f32`.
        for factor in [f32::NAN, -1.0, f32::INFINITY] {
            let policy = RetryPolicy::exponential(
                Duration::from_millis(100),
                factor,
                Some(5),
                Some(Duration::from_secs(10)),
            );
            // Both replay path and iterator path must complete without panic.
            let _ = policy.clone().iter_after(3);
            let _ = policy.into_iter().collect::<Vec<_>>();
        }
    }

    #[test]
    fn fast_forward_zero_is_noop() {
        let policy = RetryPolicy::exponential(Duration::from_millis(100), 2.0, Some(5), None);
        let mut iter = policy.iter();
        iter.fast_forward(0);
        assert_eq!(iter.attempts(), 0);
        assert_eq!(iter.last_retry(), None);
        // First delay equals the initial interval.
        assert_eq!(iter.peek_next(), Some(Duration::from_millis(100)));
    }

    #[test]
    fn fast_forward_is_composable() {
        // Calling fast_forward repeatedly should be equivalent to one big jump.
        let policy = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            Some(20),
            Some(Duration::from_millis(1000)),
        );

        let a = policy.iter_after(7);

        let mut b = policy.iter();
        b.fast_forward(3);
        b.fast_forward(4);

        assert_eq!(a.attempts(), b.attempts());
        assert_eq!(a.last_retry(), b.last_retry());
        assert_eq!(a.peek_next(), b.peek_next());
    }
}
