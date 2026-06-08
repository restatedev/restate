// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use pprof::criterion::{Output, PProfProfiler};
use pprof::flamegraph::Options;

use restate_limiter::LimitKey;
use restate_types::Scope;
use restate_types::identifiers::PartitionKey;
use restate_util_string::ReString;
use restate_vqueues::generate_vqueue_id;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub fn flamegraph_options<'a>() -> Options<'a> {
    #[allow(unused_mut)]
    let mut options = Options::default();
    if cfg!(target_os = "macos") {
        // Ignore different thread origins to merge traces. This seems not needed on Linux.
        options.base = vec!["__pthread_joiner_wake".to_string(), "_main".to_string()];
    }
    options
}

fn bench_generate_vqueue_id(c: &mut Criterion) {
    let partition_key: PartitionKey = 0xDEAD_BEEF_CAFE_F00D;
    let scope = Scope::try_non_interned("my_scope.v2").unwrap();
    let limit_key_none: LimitKey<ReString> = LimitKey::None;
    let limit_key_l1: LimitKey<ReString> = "tenant_42".parse().unwrap();
    let limit_key_l2: LimitKey<ReString> = "tenant_42/workflow_77".parse().unwrap();
    let service_name = "MyWonderfulService";
    let key = "user-key-1234567890";

    let mut group = c.benchmark_group("generate_vqueue_id");
    group
        .sample_size(200)
        .measurement_time(Duration::from_secs(5));

    group.bench_function("shared/no-scope/no-limit", |b| {
        b.iter(|| {
            black_box(generate_vqueue_id(
                black_box(partition_key),
                black_box(None),
                black_box(&limit_key_none),
                black_box(false),
                black_box(service_name),
                black_box(None),
            ))
        });
    });

    group.bench_function("shared/scope/no-limit", |b| {
        b.iter(|| {
            black_box(generate_vqueue_id(
                black_box(partition_key),
                black_box(Some(&scope)),
                black_box(&limit_key_none),
                black_box(false),
                black_box(service_name),
                black_box(None),
            ))
        });
    });

    group.bench_function("shared/scope/l1", |b| {
        b.iter(|| {
            black_box(generate_vqueue_id(
                black_box(partition_key),
                black_box(Some(&scope)),
                black_box(&limit_key_l1),
                black_box(false),
                black_box(service_name),
                black_box(None),
            ))
        });
    });

    group.bench_function("shared/scope/l2", |b| {
        b.iter(|| {
            black_box(generate_vqueue_id(
                black_box(partition_key),
                black_box(Some(&scope)),
                black_box(&limit_key_l2),
                black_box(false),
                black_box(service_name),
                black_box(None),
            ))
        });
    });

    group.bench_function("exclusive/scope/l1/key", |b| {
        b.iter(|| {
            black_box(generate_vqueue_id(
                black_box(partition_key),
                black_box(Some(&scope)),
                black_box(&limit_key_l1),
                black_box(true),
                black_box(service_name),
                black_box(Some(key)),
            ))
        });
    });

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(997, Output::Flamegraph(Some(flamegraph_options()))));
    targets = bench_generate_vqueue_id
);
criterion_main!(benches);
