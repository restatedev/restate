// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module exports macros for rate-limiting log lines. Those macros will only log the event if the
//! rate limit has not been exceeded.
//!
//! For example, for logging a log line at most 10 times per second:
//!
//! ```rust
//!    info_ratelimited!(10, Duration::from_secs(1), "This event will be logged at most 10 times per second");
//! ```
//! After the first 10 log lines, new log lines will be suppressed. The first log line after the window expiry
//! will get a label injected (`restate.logging.suppressed`) with the number of suppressed events in the previous
//! window.
//!
//! Notes:
//! - As with the tracing crate, if the event level is not enabled, field values won't be evaluated. Similarly, if
//!   the rate limit is exceeded, the field values will also not be evaluated.
//! - To reduce the overhead of this crate, if there are multiple concurrent calls to the same log line, only one
//!   of them will be evaluated, and others will just move on (and not even get counted as suppressed). No caller
//!   ever waits for the lock. This keeps the cost of this ratelimiting minimal (mostly two atomic operations), but
//!   sacrifices on accuracy.

#[doc(hidden)]
pub mod __private {
    use std::cell::UnsafeCell;
    use std::num::NonZeroU32;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    pub use tracing;

    use restate_clock::time::MillisSinceEpoch;

    struct Inner {
        current: u16,
        period_start: MillisSinceEpoch,
        suppressed: u32,
    }

    struct Unlock<'a>(&'a AtomicBool);

    impl Drop for Unlock<'_> {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Release);
        }
    }

    pub struct RateLimiter {
        max: u16,
        locked: AtomicBool,
        inner: UnsafeCell<Inner>,
    }

    unsafe impl Sync for RateLimiter {}

    impl RateLimiter {
        pub const fn new(max: u16) -> Self {
            Self {
                max,
                locked: AtomicBool::new(false),
                inner: UnsafeCell::new(Inner {
                    current: 0,
                    period_start: MillisSinceEpoch::UNIX_EPOCH,
                    suppressed: 0,
                }),
            }
        }

        #[inline]
        pub fn should_log(&self, dur: &Duration) -> (bool, Option<NonZeroU32>) {
            if self
                .locked
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                // Lock is already held, let's just skip this log line
                return (false, None);
            }
            let _unlock = Unlock(&self.locked);
            let lock = unsafe { &mut *self.inner.get() };

            if lock.current == 0 || lock.period_start.elapsed() >= *dur {
                lock.current = 1;
                lock.period_start = MillisSinceEpoch::now();
                let suppressed = NonZeroU32::new(std::mem::take(&mut lock.suppressed));
                (true, suppressed)
            } else if lock.current < self.max {
                lock.current += 1;
                (true, None)
            } else {
                lock.suppressed = lock.suppressed.saturating_add(1);
                (false, None)
            }
        }
    }
}

#[macro_export]
macro_rules! event_ratelimited {
    ($lvl:expr, $count:expr, $dur:expr, $($field:tt)*) => {
        {
            if $crate::__private::tracing::event_enabled!(
                $lvl,
                restate.logging.suppressed =
                    $crate::__private::tracing::field::Empty,
                $($field)*
            ) {
                const DUR: ::std::time::Duration = $dur;
                static LIMIT: $crate::__private::RateLimiter =
                    $crate::__private::RateLimiter::new($count);

                let (should_log, suppressed) = LIMIT.should_log(&DUR);
                if should_log {
                    $crate::__private::tracing::event!(
                        $lvl,
                        restate.logging.suppressed = suppressed,
                        $($field)*
                    );
                }
            }
        }
    };
}

#[macro_export]
macro_rules! info_ratelimited {
    ($count:expr, $dur:expr, $($field:tt)*) => {
        $crate::event_ratelimited!(
            $crate::__private::tracing::Level::INFO,
            $count,
            $dur,
            $($field)*
        );
    }
}

#[macro_export]
macro_rules! warn_ratelimited {
    ($count:expr, $dur:expr, $($field:tt)*) => {
        $crate::event_ratelimited!(
            $crate::__private::tracing::Level::WARN,
            $count,
            $dur,
            $($field)*
        );
    }
}

#[macro_export]
macro_rules! error_ratelimited {
    ($count:expr, $dur:expr, $($field:tt)*) => {
        $crate::event_ratelimited!(
            $crate::__private::tracing::Level::ERROR,
            $count,
            $dur,
            $($field)*
        );
    }
}
