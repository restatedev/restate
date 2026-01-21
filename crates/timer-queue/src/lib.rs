// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::time::SystemTime;

#[derive(Debug)]
pub struct Timer<T> {
    sleep_until: SystemTime,
    payload: T,
}

impl<T> Timer<T> {
    pub fn into_inner(self) -> T {
        self.payload
    }
}

impl<T> Ord for Timer<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sleep_until.cmp(&other.sleep_until)
    }
}

impl<T> PartialOrd for Timer<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for Timer<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T> Eq for Timer<T> {}

#[derive(Debug)]
pub struct TimerQueue<T>(BinaryHeap<Reverse<Timer<T>>>);

impl<T> Default for TimerQueue<T> {
    fn default() -> Self {
        TimerQueue::new()
    }
}

impl<T> TimerQueue<T> {
    pub fn new() -> Self {
        Self(BinaryHeap::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn sleep_until(&mut self, sleep_until: SystemTime, payload: T) {
        self.0.push(Reverse(Timer {
            sleep_until,
            payload,
        }))
    }

    pub async fn await_timer(&mut self) -> Timer<T> {
        if let Some(Reverse(Timer { sleep_until, .. })) = self.0.peek() {
            let system_now = SystemTime::now();
            if let Ok(sleep) = sleep_until.duration_since(system_now) {
                tokio::time::sleep(sleep).await;
            }

            self.0.pop().unwrap().0
        } else {
            futures::future::pending().await
        }
    }
}

impl<T> FromIterator<(SystemTime, T)> for TimerQueue<T> {
    fn from_iter<IT: IntoIterator<Item = (SystemTime, T)>>(iter: IT) -> Self {
        let mut tq = TimerQueue::new();
        for (time, payload) in iter {
            tq.sleep_until(time, payload);
        }
        tq
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ops::Add;
    use std::time::{Duration, Instant, SystemTime};

    #[tokio::test]
    async fn test_simple_timer() {
        let mut timer_queue = TimerQueue::new();

        let now = Instant::now();

        let sleep_duration = Duration::from_millis(1);
        timer_queue.sleep_until(SystemTime::now().add(sleep_duration), 1);

        let result = timer_queue.await_timer().await;

        assert_eq!(result.payload, 1);
        assert!(now.elapsed() >= sleep_duration);
    }

    #[tokio::test]
    async fn test_timer_ordering() {
        let now = SystemTime::now();

        let mut timer_queue: TimerQueue<i32> = [
            (now + Duration::from_millis(3600), 1),
            (now + Duration::from_millis(1), 2),
        ]
        .into_iter()
        .collect();

        let result = timer_queue.await_timer().await;

        assert_eq!(result.payload, 2);
    }

    #[tokio::test]
    async fn test_completed_timers() {
        let now = SystemTime::now();

        let mut timer_queue: TimerQueue<i32> = [
            (now + Duration::from_millis(1), 1),
            (now + Duration::from_millis(2), 2),
        ]
        .into_iter()
        .collect();

        tokio::time::sleep(Duration::from_millis(5)).await;

        assert_eq!(timer_queue.await_timer().await.payload, 1);
        assert_eq!(timer_queue.await_timer().await.payload, 2);
    }
}
