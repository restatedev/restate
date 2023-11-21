// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::service::clock::tests::ManualClock;
use crate::service::clock::TokioClock;
use crate::{Timer, TimerKey, TimerReader, TimerService};
use futures_util::stream;
use restate_test_util::{let_assert, test};
use restate_types::time::MillisSinceEpoch;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone)]
struct MockTimerReader<T>
where
    T: Timer,
{
    timers: Arc<Mutex<BTreeMap<T::TimerKey, T>>>,
}

impl<T> MockTimerReader<T>
where
    T: Timer,
{
    fn new() -> Self {
        Self {
            timers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn add_timer(&self, timer: T) {
        self.timers.lock().unwrap().insert(timer.timer_key(), timer);
    }

    fn add_timers(&self, timers: impl IntoIterator<Item = T>) {
        for timer in timers {
            self.add_timer(timer);
        }
    }
}

impl<T> TimerReader<T> for MockTimerReader<T>
where
    T: Timer + Send + Ord + Clone,
{
    type TimerStream<'a> = stream::Iter<std::vec::IntoIter<T>> where T: 'a;

    fn scan_timers(
        &self,
        num_timers: usize,
        previous_timer_key: Option<T::TimerKey>,
    ) -> Self::TimerStream<'_> {
        let result: Vec<_> = if let Some(previous_timer_key) = previous_timer_key {
            self.timers
                .lock()
                .unwrap()
                .range(previous_timer_key..)
                .skip(1)
                .take(num_timers)
                .map(|(_, value)| value.clone())
                .collect()
        } else {
            self.timers
                .lock()
                .unwrap()
                .iter()
                .take(num_timers)
                .map(|(_, value)| value.clone())
                .collect()
        };

        stream::iter(result)
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Hash, Eq)]
struct TimerValue {
    value: u64,
    wake_up_time: MillisSinceEpoch,
}

impl PartialOrd for TimerValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.wake_up_time
            .cmp(&other.wake_up_time)
            .then_with(|| self.value.cmp(&other.value))
    }
}

impl TimerValue {
    fn new(value: u64, wake_up_time: MillisSinceEpoch) -> Self {
        Self {
            value,
            wake_up_time,
        }
    }
}

impl Timer for TimerValue {
    type TimerKey = TimerValue;

    fn timer_key(&self) -> Self {
        *self
    }
}

impl TimerKey for TimerValue {
    fn wake_up_time(&self) -> MillisSinceEpoch {
        self.wake_up_time
    }
}

#[test(tokio::test)]
async fn no_timer_is_dropped() {
    let timer_reader = MockTimerReader::new();
    let service = TimerService::new(TokioClock, None, &timer_reader);
    tokio::pin!(service);

    let timer_1 = TimerValue::new(0, 0.into());
    let timer_2 = TimerValue::new(1, 1.into());
    let timer_3 = TimerValue::new(2, 2.into());

    service.as_mut().add_timer(timer_1);
    service.as_mut().add_timer(timer_2);
    service.as_mut().add_timer(timer_3);

    assert_eq!(service.as_mut().next_timer().await, timer_1);
    assert_eq!(service.as_mut().next_timer().await, timer_2);
    assert_eq!(service.as_mut().next_timer().await, timer_3);
}

#[test(tokio::test)]
async fn timers_fire_in_wake_up_order() {
    let num_timers = 10;
    let timer_reader = MockTimerReader::new();
    let service = TimerService::new(TokioClock, None, &timer_reader);
    tokio::pin!(service);

    let now = u64::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();

    for i in 0..num_timers {
        let wake_up_time = (now + num_timers - i).into();
        service.as_mut().add_timer(TimerValue::new(i, wake_up_time));
    }

    for i in (0..num_timers).rev() {
        assert_eq!(
            service.as_mut().next_timer().await,
            TimerValue::new(i, (now + num_timers - i).into())
        );
    }
}

#[test(tokio::test)]
async fn loading_timers_from_reader() {
    let mut clock = ManualClock::new(MillisSinceEpoch::UNIX_EPOCH);
    let timer_reader = MockTimerReader::<TimerValue>::new();
    let num_timers = 10;

    for i in 0..num_timers {
        timer_reader.add_timer(TimerValue::new(i, i.into()))
    }

    let service = TimerService::new(clock.clone(), Some(1), &timer_reader);
    tokio::pin!(service);

    // trigger all timers
    clock.advance_time_by(Duration::from_millis(num_timers - 1));

    for i in 0..num_timers {
        assert_eq!(
            service.as_mut().next_timer().await,
            TimerValue::new(i, i.into())
        );
    }
}

#[test(tokio::test)]
async fn advancing_time_triggers_timer() {
    let mut clock = ManualClock::new(MillisSinceEpoch::UNIX_EPOCH);
    let timer_reader = MockTimerReader::<TimerValue>::new();
    let num_timers = 10;

    for i in 0..num_timers {
        timer_reader.add_timer(TimerValue::new(i, i.into()));
    }

    let service = TimerService::new(clock.clone(), Some(1), &timer_reader);
    tokio::pin!(service);

    // trigger half of the timers
    clock.advance_time_by(Duration::from_millis(num_timers / 2 - 1));

    for i in 0..num_timers / 2 {
        assert_eq!(
            service.as_mut().next_timer().await,
            TimerValue::new(i, i.into())
        );
    }

    // no other timer should fire
    assert!(
        tokio::time::timeout(Duration::from_millis(10), service.as_mut().next_timer())
            .await
            .is_err()
    );

    // trigger the remaining half
    clock.advance_time_by(Duration::from_millis(num_timers / 2));

    for i in num_timers / 2..num_timers {
        assert_eq!(
            service.as_mut().next_timer().await,
            TimerValue::new(i, i.into())
        );
    }
}

#[test(tokio::test)]
async fn add_new_timers() {
    let mut clock = ManualClock::new(MillisSinceEpoch::UNIX_EPOCH);
    let timer_reader = MockTimerReader::<TimerValue>::new();

    timer_reader.add_timers(vec![
        TimerValue::new(0, 0.into()),
        TimerValue::new(1, 1.into()),
        TimerValue::new(3, 10.into()),
    ]);

    let service = TimerService::new(clock.clone(), Some(1), &timer_reader);
    tokio::pin!(service);

    clock.advance_time_to(MillisSinceEpoch::new(5));
    let new_timer = TimerValue::new(2, 5.into());
    timer_reader.add_timer(new_timer);

    // notify timer about new timer
    service.as_mut().add_timer(new_timer);

    clock.advance_time_to(MillisSinceEpoch::new(10));

    for i in 0..4 {
        let_assert!(TimerValue { value, .. } = service.as_mut().next_timer().await);
        assert_eq!(value, i);
    }
}

#[test(tokio::test)]
async fn earlier_timers_replace_older_ones() {
    let mut clock = ManualClock::new(MillisSinceEpoch::UNIX_EPOCH);
    let timer_reader = MockTimerReader::<TimerValue>::new();
    timer_reader.add_timer(TimerValue::new(1, 10.into()));

    let service = TimerService::new(clock.clone(), Some(1), &timer_reader);
    tokio::pin!(service);

    // give timer service chance to load timers
    yield_to_timer_service(&mut service).await;

    let new_timer = TimerValue::new(0, 5.into());
    timer_reader.add_timer(new_timer);
    service.as_mut().add_timer(new_timer);

    clock.advance_time_to(MillisSinceEpoch::new(10));

    for i in 0..2 {
        let_assert!(TimerValue { value, .. } = service.as_mut().next_timer().await);
        assert_eq!(value, i);
    }
}

async fn yield_to_timer_service<
    'a,
    Timer: crate::Timer + Debug,
    Clock: crate::Clock,
    TimerReader: crate::TimerReader<Timer>,
>(
    timer_service: &mut Pin<&mut TimerService<'a, Timer, Clock, TimerReader>>,
) {
    assert!(tokio::time::timeout(
        Duration::from_millis(10),
        timer_service.as_mut().next_timer()
    )
    .await
    .is_err());
}

#[test(tokio::test)]
async fn earlier_timers_wont_trigger_reemission_of_fired_timers() {
    let mut clock = ManualClock::new(MillisSinceEpoch::UNIX_EPOCH);
    let timer_reader = MockTimerReader::<TimerValue>::new();
    timer_reader.add_timer(TimerValue::new(0, 2.into()));
    timer_reader.add_timer(TimerValue::new(2, 5.into()));

    let service = TimerService::new(clock.clone(), Some(1), &timer_reader);
    tokio::pin!(service);

    // give timer service the chance to load the initial timers
    yield_to_timer_service(&mut service).await;

    clock.advance_time_to(MillisSinceEpoch::new(3));

    let_assert!(TimerValue { value: 0, .. } = service.as_mut().next_timer().await);

    let new_timer = TimerValue::new(1, 0.into());
    timer_reader.add_timer(new_timer);
    service.as_mut().add_timer(new_timer);

    clock.advance_time_to(MillisSinceEpoch::new(10));

    for i in 1..3 {
        let_assert!(TimerValue { value, .. } = service.as_mut().next_timer().await);
        assert_eq!(value, i);
    }
}
