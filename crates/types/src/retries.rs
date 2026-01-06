// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use restate_time_util::FriendlyDuration;

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
        self.max_attempts() - self.attempts()
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
                        self.last_retry.unwrap().mul_f32(*factor),
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
        self.attempts += 1;
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
                        self.last_retry.unwrap().mul_f32(*factor),
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
        (
            max_attempts - self.attempts,
            Some(max_attempts - self.attempts),
        )
    }
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
}
