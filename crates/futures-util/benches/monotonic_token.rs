// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for drawing and observing monotonic tokens.
//!
//! Drawing a token and retiring one touch different counters, usually from
//! different threads. These benchmarks show whether the two sides slow each
//! other down by sharing a cache line:
//!
//! - **Uncontended:** one thread draws tokens with nothing else running.
//! - **Next while retiring:** one thread draws tokens while others keep
//!   retiring and polling for retirements.
//! - **Retired-through while drawing:** a listener polls for the latest
//!   retired token while another thread keeps drawing.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};

use criterion::{Criterion, criterion_group, criterion_main};

use restate_futures_util::monotonic_token::TokenOwner;

struct Bench;

fn uncontended(c: &mut Criterion) {
    let owner = TokenOwner::<Bench>::new();
    let tokens = owner.new_tokens();
    c.bench_function("monotonic_token/next/uncontended", |b| {
        b.iter(|| black_box(tokens.next()))
    });
}

fn next_while_retiring(c: &mut Criterion) {
    let owner = TokenOwner::<Bench>::new();
    let tokens = owner.new_tokens();
    let listener = tokens.new_listener();
    let retired = tokens.next();
    let stop = AtomicBool::new(false);
    std::thread::scope(|s| {
        s.spawn(|| {
            while !stop.load(Ordering::Relaxed) {
                owner.retire_through(retired);
            }
        });
        s.spawn(|| {
            while !stop.load(Ordering::Relaxed) {
                black_box(listener.retired_through());
            }
        });
        c.bench_function("monotonic_token/next/while_retiring", |b| {
            b.iter(|| black_box(tokens.next()))
        });
        stop.store(true, Ordering::Relaxed);
    });
}

fn retired_through_while_drawing(c: &mut Criterion) {
    let owner = TokenOwner::<Bench>::new();
    let tokens = owner.new_tokens();
    let listener = tokens.new_listener();
    owner.retire_through(tokens.next());
    let stop = AtomicBool::new(false);
    std::thread::scope(|s| {
        s.spawn(|| {
            while !stop.load(Ordering::Relaxed) {
                black_box(tokens.next());
            }
        });
        c.bench_function("monotonic_token/retired_through/while_drawing", |b| {
            b.iter(|| black_box(listener.retired_through()))
        });
        stop.store(true, Ordering::Relaxed);
    });
}

criterion_group!(
    benches,
    uncontended,
    next_while_retiring,
    retired_through_while_drawing
);
criterion_main!(benches);
