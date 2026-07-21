// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    hint::black_box,
    sync::atomic::{AtomicU32, Ordering},
};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use restate_types::{
    logs::{Keys, LogletId, LogletOffset, Record, RecordCache},
    storage::PolyBytes,
    time::NanosSinceEpoch,
};

const LOGLET_ID: LogletId = LogletId::new_unchecked(1);

fn record(body_size: usize) -> Record {
    Record::from_parts(
        NanosSinceEpoch::now(),
        Keys::None,
        PolyBytes::Bytes(Bytes::from(vec![42; body_size])),
    )
}

fn record_cache_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("record-cache-add");

    for body_size in [1024, 64 * 1024, 1024 * 1024] {
        let record = record(body_size);
        group.throughput(Throughput::Bytes(body_size as u64));

        let PolyBytes::Bytes(body) = record.body() else {
            unreachable!("benchmark record has a raw body");
        };

        group.bench_with_input(
            BenchmarkId::new("raw-body-clone", body_size),
            &body_size,
            |b, _| b.iter(|| black_box(Bytes::clone(body))),
        );

        group.bench_with_input(
            BenchmarkId::new("raw-body-copy", body_size),
            &body_size,
            |b, _| b.iter(|| black_box(Bytes::copy_from_slice(body))),
        );

        group.bench_with_input(BenchmarkId::new("miss", body_size), &body_size, |b, _| {
            let cache = RecordCache::new(body_size * 128);
            let offset = AtomicU32::new(1);

            b.iter(|| {
                cache.add(
                    LOGLET_ID,
                    LogletOffset::new(offset.fetch_add(1, Ordering::Relaxed)),
                    black_box(&record),
                );
            });
        });

        group.bench_with_input(
            BenchmarkId::new("duplicate", body_size),
            &body_size,
            |b, _| {
                let cache = RecordCache::new(body_size * 128);
                let offset = LogletOffset::new(1);
                cache.add(LOGLET_ID, offset, &record);

                b.iter(|| cache.add(LOGLET_ID, offset, black_box(&record)));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, record_cache_add);
criterion_main!(benches);
