use crate::service::clock::tests::ManualClock;
use crate::service::clock::TokioClock;
use crate::{Output, Timer, TimerKey, TimerReader, TimerService};
use common::types::MillisSinceEpoch;
use futures_util::stream;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use test_utils::{let_assert, test};
use tokio::sync::mpsc;

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
    let (output_tx, mut output_rx) = mpsc::channel(1);
    let service = TimerService::new(None, output_tx, MockTimerReader::new(), TokioClock);
    let timer_handle = service.create_timer_handle();

    let (shutdown_signal, shutdown_watch) = drain::channel();

    let service_task = tokio::spawn(service.run(shutdown_watch));

    let timer_1 = TimerValue::new(0, 0.into());
    let timer_2 = TimerValue::new(1, 1.into());
    let timer_3 = TimerValue::new(2, 2.into());

    timer_handle.add_timer(timer_1).await.unwrap();
    timer_handle.add_timer(timer_2).await.unwrap();
    tokio::task::yield_now().await;

    timer_handle.add_timer(timer_3).await.unwrap();
    tokio::task::yield_now().await;

    let_assert!(Some(Output::TimerFired(fired_timer_1)) = output_rx.recv().await);
    assert_eq!(fired_timer_1, timer_1);
    let_assert!(Some(Output::TimerFired(fired_timer_2)) = output_rx.recv().await);
    assert_eq!(fired_timer_2, timer_2);
    let_assert!(Some(Output::TimerFired(fired_timer_3)) = output_rx.recv().await);
    assert_eq!(fired_timer_3, timer_3);

    shutdown_signal.drain().await;

    service_task.await.unwrap().unwrap();
}

#[test(tokio::test)]
async fn timers_fire_in_wake_up_order() {
    let num_timers = 10;
    let (output_tx, mut output_rx) = mpsc::channel(num_timers as usize);
    let service = TimerService::new(None, output_tx, MockTimerReader::new(), TokioClock);

    let timer_handle = service.create_timer_handle();
    let (shutdown_signal, shutdown_watch) = drain::channel();

    let timer_join_handle = tokio::spawn(service.run(shutdown_watch));

    let now = u64::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();

    for i in 0..num_timers {
        let wake_up_time = (now + num_timers - i).into();
        timer_handle
            .add_timer(TimerValue::new(i, wake_up_time))
            .await
            .unwrap();
    }

    for i in (0..num_timers).rev() {
        let_assert!(Some(Output::TimerFired(fired_timer)) = output_rx.recv().await);
        assert_eq!(
            fired_timer,
            TimerValue::new(i, (now + num_timers - i).into())
        );
    }

    shutdown_signal.drain().await;
    timer_join_handle.await.unwrap().unwrap();
}

#[test(tokio::test)]
async fn loading_timers_from_reader() {
    let mut clock = ManualClock::new(MillisSinceEpoch::UNIX_EPOCH);
    let (output_tx, mut output_rx) = mpsc::channel(1);
    let timer_reader = MockTimerReader::<TimerValue>::new();
    let num_timers = 10;

    for i in 0..num_timers {
        timer_reader.add_timer(TimerValue::new(i, i.into()))
    }

    let service = TimerService::new(Some(1), output_tx, timer_reader, clock.clone());

    let (shutdown_signal, shutdown_watch) = drain::channel();
    let join_handle = tokio::spawn(service.run(shutdown_watch));

    // trigger all timers
    clock.advance_time_by(Duration::from_millis(num_timers - 1));

    for i in 0..num_timers {
        assert_eq!(
            output_rx.recv().await,
            Some(Output::TimerFired(TimerValue::new(i, i.into())))
        );
    }

    shutdown_signal.drain().await;
    join_handle.await.unwrap().unwrap();
}

#[test(tokio::test)]
async fn advancing_time_triggers_timer() {
    let mut clock = ManualClock::new(MillisSinceEpoch::UNIX_EPOCH);
    let (output_tx, mut output_rx) = mpsc::channel(1);
    let timer_reader = MockTimerReader::<TimerValue>::new();
    let num_timers = 10;

    for i in 0..num_timers {
        timer_reader.add_timer(TimerValue::new(i, i.into()))
    }

    let service = TimerService::new(Some(1), output_tx, timer_reader, clock.clone());

    let (shutdown_signal, shutdown_watch) = drain::channel();
    let join_handle = tokio::spawn(service.run(shutdown_watch));

    // trigger half of the timers
    clock.advance_time_by(Duration::from_millis(num_timers / 2 - 1));

    for i in 0..num_timers / 2 {
        assert_eq!(
            output_rx.recv().await,
            Some(Output::TimerFired(TimerValue::new(i, i.into())))
        );
    }

    // no other timer should fire
    assert!(
        tokio::time::timeout(Duration::from_millis(10), output_rx.recv())
            .await
            .is_err()
    );

    // trigger the remaining half
    clock.advance_time_by(Duration::from_millis(num_timers / 2));

    for i in num_timers / 2..num_timers {
        assert_eq!(
            output_rx.recv().await,
            Some(Output::TimerFired(TimerValue::new(i, i.into())))
        );
    }

    shutdown_signal.drain().await;
    join_handle.await.unwrap().unwrap();
}

#[test(tokio::test)]
async fn add_new_timers() {
    let mut clock = ManualClock::new(MillisSinceEpoch::UNIX_EPOCH);
    let (output_tx, mut output_rx) = mpsc::channel(1);
    let timer_reader = MockTimerReader::<TimerValue>::new();

    timer_reader.add_timers(vec![
        TimerValue::new(0, 0.into()),
        TimerValue::new(1, 1.into()),
        TimerValue::new(3, 10.into()),
    ]);

    let service = TimerService::new(Some(1), output_tx, timer_reader.clone(), clock.clone());
    let timer_handle = service.create_timer_handle();

    let (shutdown_signal, shutdown_watch) = drain::channel();
    let join_handle = tokio::spawn(service.run(shutdown_watch));

    clock.advance_time_to(MillisSinceEpoch::new(5));
    let new_timer = TimerValue::new(2, 5.into());
    timer_reader.add_timer(new_timer);

    // notify timer about new timer
    timer_handle.add_timer(new_timer).await.unwrap();

    clock.advance_time_to(MillisSinceEpoch::new(10));

    for i in 0..4 {
        let_assert!(Some(Output::TimerFired(TimerValue { value, .. })) = output_rx.recv().await);
        assert_eq!(value, i);
    }

    shutdown_signal.drain().await;
    join_handle.await.unwrap().unwrap();
}

#[test(tokio::test)]
async fn earlier_timers_replace_older_ones() {
    let mut clock = ManualClock::new(MillisSinceEpoch::UNIX_EPOCH);
    let (output_tx, mut output_rx) = mpsc::channel(1);
    let timer_reader = MockTimerReader::<TimerValue>::new();
    timer_reader.add_timer(TimerValue::new(1, 10.into()));

    let service = TimerService::new(Some(1), output_tx, timer_reader.clone(), clock.clone());
    let timer_handle = service.create_timer_handle();

    let (shutdown_signal, shutdown_watch) = drain::channel();
    let join_handle = tokio::spawn(service.run(shutdown_watch));

    // give timer service the chance to load the initial timers
    tokio::task::yield_now().await;

    let new_timer = TimerValue::new(0, 5.into());
    timer_reader.add_timer(new_timer);
    timer_handle.add_timer(new_timer).await.unwrap();

    clock.advance_time_to(MillisSinceEpoch::new(10));

    for i in 0..2 {
        let_assert!(Some(Output::TimerFired(TimerValue { value, .. })) = output_rx.recv().await);
        assert_eq!(value, i);
    }

    shutdown_signal.drain().await;
    join_handle.await.unwrap().unwrap();
}
