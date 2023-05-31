use criterion::{criterion_group, criterion_main, Criterion};
use restate_storage_api::deduplication_table::DeduplicationTable;
use restate_storage_api::Transaction;
use std::path;
use tempfile::tempdir;
use tokio::runtime::Builder;

async fn writing_to_rocksdb(base_path: &path::Path) {
    //
    // setup
    //
    let opts = restate_storage_rocksdb::Options {
        path: base_path.to_str().unwrap().into(),
        ..Default::default()
    };
    let rocksdb = opts
        .build()
        .expect("RocksDB storage creation should succeed");

    //
    // write
    //
    for i in 0..100000 {
        let mut txn = rocksdb.transaction();
        for j in 0..10 {
            txn.put_sequence_number(i, j, 0).await;
        }
        txn.commit().await.unwrap();
    }
}

fn basic_writing_reading_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("RocksDB");
    group.sample_size(10).bench_function("writing", |bencher| {
        let temp_dir = tempdir().unwrap();
        bencher
            .to_async(Builder::new_multi_thread().enable_all().build().unwrap())
            .iter(|| writing_to_rocksdb(temp_dir.path()));
    });

    group.finish();
}

criterion_group!(benches, basic_writing_reading_benchmark);
criterion_main!(benches);
