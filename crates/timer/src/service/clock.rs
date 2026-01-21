// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::time::MillisSinceEpoch;
use std::future::Future;
use std::ops::Add;
use std::time::{Duration, SystemTime};

pub trait Clock {
    type SleepFuture: Future<Output = ()>;

    /// Returns a sleep future that completes when `wake_up_time` is reached. None if this moment
    /// has already passed.
    fn sleep_until(&mut self, wake_up_time: MillisSinceEpoch) -> Option<Self::SleepFuture>;
}

pub struct TokioClock;

impl Clock for TokioClock {
    type SleepFuture = tokio::time::Sleep;

    fn sleep_until(&mut self, wake_up_time: MillisSinceEpoch) -> Option<Self::SleepFuture> {
        let now = SystemTime::now();

        if let Ok(duration) = SystemTime::UNIX_EPOCH
            .add(Duration::from_millis(wake_up_time.as_u64()))
            .duration_since(now)
        {
            Some(tokio::time::sleep(duration))
        } else {
            None
        }
    }
}

#[cfg(test)]
pub mod tests {
    use crate::service::clock::Clock;
    use futures_util::future::{BoxFuture, FutureExt};
    use restate_types::time::MillisSinceEpoch;
    use std::cmp::{Ordering, Reverse};
    use std::collections::BinaryHeap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[derive(Debug, Clone)]
    pub struct ManualClock {
        inner: Arc<Mutex<InnerManualClock>>,
    }

    impl ManualClock {
        pub fn new(time: MillisSinceEpoch) -> Self {
            Self {
                inner: Arc::new(Mutex::new(InnerManualClock::new(time))),
            }
        }

        pub fn advance_time_by(&mut self, duration: Duration) {
            self.inner.lock().unwrap().advance_time(duration);
        }

        pub fn advance_time_to(&mut self, time: MillisSinceEpoch) {
            let mut inner = self.inner.lock().unwrap();
            assert!(inner.time <= time);

            inner.time = time;
            inner.wake_up_sleeps();
        }
    }

    impl Clock for ManualClock {
        type SleepFuture = BoxFuture<'static, ()>;

        fn sleep_until(&mut self, wake_up_time: MillisSinceEpoch) -> Option<Self::SleepFuture> {
            self.inner
                .lock()
                .unwrap()
                .sleep_until(wake_up_time)
                .map(|rx| rx.map(|result| result.unwrap_or_default()).boxed())
        }
    }

    #[derive(Debug)]
    struct InnerManualClock {
        current_sleep_future_id: usize,
        time: MillisSinceEpoch,
        pending_sleep_futures: BinaryHeap<Reverse<SleepFuture>>,
    }

    impl InnerManualClock {
        fn new(time: MillisSinceEpoch) -> Self {
            Self {
                current_sleep_future_id: 0,
                time,
                pending_sleep_futures: BinaryHeap::new(),
            }
        }

        fn advance_time(&mut self, duration: Duration) {
            self.time = MillisSinceEpoch::new(self.time.as_u64() + duration.as_millis() as u64);

            self.wake_up_sleeps();
        }

        fn wake_up_sleeps(&mut self) {
            while let Some(sleep_future) = self.pending_sleep_futures.peek() {
                if sleep_future.0.wake_up_time > self.time {
                    break;
                }

                let Reverse(sleep_future) = self.pending_sleep_futures.pop().unwrap();
                let _ = sleep_future.waker.send(());
            }
        }

        fn sleep_until(
            &mut self,
            wake_up_time: MillisSinceEpoch,
        ) -> Option<tokio::sync::oneshot::Receiver<()>> {
            if wake_up_time <= self.time {
                None
            } else {
                let (waker, rx) = tokio::sync::oneshot::channel();
                self.current_sleep_future_id += 1;
                self.pending_sleep_futures.push(Reverse(SleepFuture {
                    id: self.current_sleep_future_id,
                    wake_up_time,
                    waker,
                }));

                Some(rx)
            }
        }
    }

    #[derive(Debug)]
    pub struct SleepFuture {
        id: usize,
        wake_up_time: MillisSinceEpoch,
        waker: tokio::sync::oneshot::Sender<()>,
    }

    impl PartialEq for SleepFuture {
        fn eq(&self, other: &Self) -> bool {
            self.id == other.id
        }
    }

    impl Eq for SleepFuture {}

    impl PartialOrd for SleepFuture {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for SleepFuture {
        fn cmp(&self, other: &Self) -> Ordering {
            self.wake_up_time
                .cmp(&other.wake_up_time)
                .then_with(|| self.id.cmp(&other.id))
        }
    }
}
