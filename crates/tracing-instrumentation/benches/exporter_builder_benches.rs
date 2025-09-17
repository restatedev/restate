// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use criterion::{Criterion, criterion_group, criterion_main};
use restate_serde_util::SerdeableHeaderHashMap;
use std::time::Duration;
use tokio::runtime::Builder;

fn builder_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("exporter-builder");

    let rt = Builder::new_multi_thread().enable_all().build().unwrap();

    let builder = rt.block_on(async {
        restate_tracing_instrumentation::ExporterBuilder::new(
            "http://127.0.0.1:4317",
            SerdeableHeaderHashMap::default(),
        )
        .expect("to work")
    });

    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(20))
        .bench_function("build exporter", |bencher| {
            bencher.iter(|| builder.build().unwrap());
        });

    group.finish();
}

criterion_group!(benches, builder_benchmark);
criterion_main!(benches);

// results on my machine:
//
// exporter-builder/build exporter
//                         time:   [558.07 ns 558.96 ns 560.06 ns]
//                         change: [-0.1904% -0.0302% +0.1255%] (p = 0.73 > 0.05)
//                         No change in performance detected.
//
