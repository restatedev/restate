use common::types::MillisSinceEpoch;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::time::{Duration, SystemTime};
use timer::{Clock, TokioClock};
use tokio::runtime::Builder;

async fn await_same_timers(timers: usize) {
    let mut clock = TokioClock::default();

    let now = MillisSinceEpoch::new(
        u64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap(),
    );

    for _ in 0..timers {
        black_box(clock.sleep_until(now));
    }
}

async fn await_increasing_timers(timers: usize) {
    let mut clock = TokioClock::default();

    let mut timer = MillisSinceEpoch::new(
        u64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap(),
    );

    for _ in 0..timers {
        black_box(clock.sleep_until(timer));
        timer = MillisSinceEpoch::new(timer.as_u64() + 1);
    }
}

fn clock_time_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Clock");
    let timers = 1000000;
    group
        // .sample_size(20)
        // .measurement_time(Duration::from_secs(20))
        .bench_function("Await the same timers", |bencher| {
            bencher
                .to_async(Builder::new_multi_thread().enable_all().build().unwrap())
                .iter(|| await_same_timers(timers));
        })
        .bench_function("Await increasing timers", |bencher| {
            bencher
                .to_async(Builder::new_multi_thread().enable_all().build().unwrap())
                .iter(|| await_increasing_timers(timers));
        });
    group.finish();
}

criterion_group!(benches, clock_time_benchmark);
criterion_main!(benches);
