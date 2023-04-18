use criterion::{black_box, criterion_group, criterion_main, Criterion};
use restate_queue::SegmentQueue;
use std::path;
use std::time::Duration;
use tempfile::tempdir;
use tokio::runtime::Builder;

async fn writing_to_queue_reading_from_queue(base_path: &path::Path) {
    let segment_size = 1024 * 256;
    let mut queue = SegmentQueue::new(base_path, segment_size);

    let number_values = segment_size * 100;
    for value in 0..number_values {
        queue.enqueue(value).await;
    }

    let mut counter = 0;

    for _ in 0..number_values {
        let value = queue.dequeue().await;

        if let Some(value) = value {
            counter += value;
        }
    }

    black_box(counter);
}

fn queue_writing_reading_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Queue");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(20))
        .bench_function("Writing_reading", |bencher| {
            let temp_dir = tempdir().unwrap();
            bencher
                .to_async(Builder::new_multi_thread().enable_all().build().unwrap())
                .iter(|| writing_to_queue_reading_from_queue(temp_dir.path()));
        });
    group.finish();
}

criterion_group!(benches, queue_writing_reading_benchmark);
criterion_main!(benches);
