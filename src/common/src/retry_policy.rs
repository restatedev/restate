use std::cmp;
use std::future::Future;
use std::time::Duration;

/// This struct represents the policy to execute retries.
///
/// It can be used in components which needs to configure policies to execute retries.
///
/// To use it:
///
/// ```rust
/// use std::time::Duration;
/// use restate_common::retry_policy::RetryPolicy;
///
/// // Define the retry policy
/// let retry_policy = RetryPolicy::fixed_delay(Duration::from_millis(100), 10);
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
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "type"))]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(title = "Retry policy", description = "Definition of a retry policy")
)]
pub enum RetryPolicy {
    /// # None
    ///
    /// No retries strategy.
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
        #[cfg_attr(
            feature = "serde",
            serde(with = "serde_with::As::<serde_with::DisplayFromStr>")
        )]
        #[cfg_attr(feature = "options_schema", schemars(with = "String"))]
        interval: humantime::Duration,
        /// # Max attempts
        ///
        /// Number of maximum attempts before giving up.
        max_attempts: usize,
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
        #[cfg_attr(
            feature = "serde",
            serde(with = "serde_with::As::<serde_with::DisplayFromStr>")
        )]
        #[cfg_attr(feature = "options_schema", schemars(with = "String"))]
        initial_interval: humantime::Duration,

        /// # Factor
        ///
        /// The factor to use to compute the next retry attempt.
        factor: f32,

        /// # Max attempts
        ///
        /// Number of maximum attempts before giving up.
        max_attempts: usize,

        /// # Max interval
        ///
        /// Maximum interval between retries.
        #[cfg_attr(
            feature = "serde",
            serde(with = "serde_with::As::<Option<serde_with::DisplayFromStr>>")
        )]
        #[cfg_attr(feature = "options_schema", schemars(with = "Option<String>"))]
        max_interval: Option<humantime::Duration>,
    },
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::None
    }
}

impl RetryPolicy {
    pub fn fixed_delay(interval: Duration, max_attempts: usize) -> Self {
        Self::FixedDelay {
            interval: interval.into(),
            max_attempts,
        }
    }

    pub fn exponential(
        initial_interval: Duration,
        factor: f32,
        max_attempts: usize,
        max_interval: Option<Duration>,
    ) -> Self {
        // Formula to compute the time based on number of retries:
        // y = \sum_{n=0}^{x} \min \left( max_interval,\ initial_interval \cdot factor x \right)
        Self::Exponential {
            initial_interval: initial_interval.into(),
            factor,
            max_attempts,
            max_interval: max_interval.map(Into::into),
        }
    }

    /// Retry the provided closure respecting this retry policy.
    pub async fn retry_operation<T, E, Fn, Fut>(self, mut operation: Fn) -> Result<T, E>
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
}

impl IntoIterator for RetryPolicy {
    type Item = Duration;
    type IntoIter = Iter;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            policy: self,
            attempts: 0,
            last_retry: None,
        }
    }
}

#[derive(Debug)]
pub struct Iter {
    policy: RetryPolicy,
    attempts: usize,
    last_retry: Option<Duration>,
}

impl Iterator for Iter {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        self.attempts += 1;
        match self.policy {
            RetryPolicy::None => None,
            RetryPolicy::FixedDelay {
                interval,
                max_attempts,
            } => {
                if self.attempts > max_attempts {
                    None
                } else {
                    Some(interval.into())
                }
            }
            RetryPolicy::Exponential {
                initial_interval,
                factor,
                max_attempts,
                max_interval,
            } => {
                if self.attempts > max_attempts {
                    None
                } else if self.last_retry.is_some() {
                    let new_retry = cmp::min(
                        self.last_retry.unwrap().mul_f32(factor),
                        max_interval.map(Into::into).unwrap_or(Duration::MAX),
                    );
                    self.last_retry = Some(new_retry);
                    return Some(new_retry);
                } else {
                    self.last_retry = Some(*initial_interval);
                    return Some(*initial_interval);
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
        (
            max_attempts - self.attempts,
            Some(max_attempts - self.attempts),
        )
    }
}

impl ExactSizeIterator for Iter {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_retry_policy() {
        assert_eq!(
            Vec::<Duration>::new(),
            RetryPolicy::None.into_iter().collect::<Vec<_>>()
        )
    }

    #[test]
    fn fixed_delay_retry_policy() {
        assert_eq!(
            vec![Duration::from_millis(100); 10],
            RetryPolicy::fixed_delay(Duration::from_millis(100), 10)
                .into_iter()
                .collect::<Vec<_>>()
        )
    }

    #[test]
    fn exponential_retry_policy() {
        assert_eq!(
            vec![
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
                    .mul_f32(2.0)
            ],
            RetryPolicy::exponential(Duration::from_millis(100), 2.0, 5, None)
                .into_iter()
                .collect::<Vec<_>>()
        )
    }

    #[test]
    fn exponential_retry_policy_with_max_interval() {
        assert_eq!(
            vec![
                // Manually building this powers to avoid rounding issues :)
                Duration::from_millis(100),
                Duration::from_millis(100).mul_f32(2.0),
                Duration::from_millis(100).mul_f32(2.0).mul_f32(2.0),
                Duration::from_millis(100)
                    .mul_f32(2.0)
                    .mul_f32(2.0)
                    .mul_f32(2.0),
                Duration::from_secs(1)
            ],
            RetryPolicy::exponential(
                Duration::from_millis(100),
                2.0,
                5,
                Some(Duration::from_secs(1))
            )
            .into_iter()
            .collect::<Vec<_>>()
        )
    }
}
