// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A core aspect of Restate is its ability to retry invocations. This module contains the types defining retries.

use std::cmp;
use std::future::Future;
use std::num::NonZeroUsize;
use std::time::Duration;

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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
    None,
    /// # Fixed delay
    ///
    /// Retry with a fixed delay strategy.
    FixedDelay {
        /// # Interval
        ///
        /// Interval between retries.
        ///
        /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        interval: humantime::Duration,
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
        /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        initial_interval: humantime::Duration,

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
        #[serde(with = "serde_with::As::<Option<serde_with::DisplayFromStr>>")]
        #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
        max_interval: Option<humantime::Duration>,
    },
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::None
    }
}

impl RetryPolicy {
    pub fn fixed_delay(interval: Duration, max_attempts: Option<usize>) -> Self {
        Self::FixedDelay {
            interval: interval.into(),
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
            initial_interval: initial_interval.into(),
            factor,
            max_attempts: max_attempts.map(|m| NonZeroUsize::new(m).expect("non-zero")),
            max_interval: max_interval.map(Into::into),
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
}

impl IntoIterator for RetryPolicy {
    type Item = Duration;
    type IntoIter = RetryIter;

    fn into_iter(self) -> Self::IntoIter {
        RetryIter {
            policy: self,
            attempts: 0,
            last_retry: None,
        }
    }
}

#[derive(Debug)]
pub struct RetryIter {
    policy: RetryPolicy,
    attempts: usize,
    last_retry: Option<Duration>,
}

impl Iterator for RetryIter {
    type Item = Duration;

    /// adds up to 1/3 target duration as jitter
    fn next(&mut self) -> Option<Self::Item> {
        self.attempts += 1;
        match self.policy {
            RetryPolicy::None => None,
            RetryPolicy::FixedDelay {
                interval,
                max_attempts,
            } => {
                if max_attempts.is_some_and(|limit| self.attempts > limit.into()) {
                    None
                } else {
                    Some(with_jitter(interval.into(), DEFAULT_JITTER_MULTIPLIER))
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
                        self.last_retry.unwrap().mul_f32(factor),
                        max_interval.map(Into::into).unwrap_or(Duration::MAX),
                    );
                    self.last_retry = Some(new_retry);
                    return Some(with_jitter(new_retry, DEFAULT_JITTER_MULTIPLIER));
                } else {
                    self.last_retry = Some(*initial_interval);
                    return Some(with_jitter(*initial_interval, DEFAULT_JITTER_MULTIPLIER));
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let max_attempts = match self.policy {
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
        let jitter = rand::thread_rng().gen_range(MIN_JITTER..max_jitter);
        duration + jitter
    }
}

impl ExactSizeIterator for RetryIter {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

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
}
