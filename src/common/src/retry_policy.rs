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
        }
    }
}

#[derive(Debug)]
pub struct Iter {
    policy: RetryPolicy,
    attempts: usize,
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
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self.policy {
            RetryPolicy::None => (0, Some(0)),
            RetryPolicy::FixedDelay { max_attempts, .. } => (
                max_attempts - self.attempts,
                Some(max_attempts - self.attempts),
            ),
        }
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
}
