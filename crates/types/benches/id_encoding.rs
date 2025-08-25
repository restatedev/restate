// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Write;

use criterion::{Criterion, criterion_group, criterion_main};

use restate_types::{IdEncoder, identifiers::InvocationId};

pub fn id_encoding(c: &mut Criterion) {
    c.bench_function("invocation-id-display", |b| {
        let mut buf = String::with_capacity(IdEncoder::<InvocationId>::estimate_buf_capacity());
        b.iter_batched(
            InvocationId::mock_random,
            |id| {
                buf.clear();
                write!(&mut buf, "{id}")
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    name=benches;
    config = Criterion::default();
    targets=id_encoding
);

criterion_main!(benches);
