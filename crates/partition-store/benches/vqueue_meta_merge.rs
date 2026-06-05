// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bilrost::Message;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};

use restate_clock::UniqueTimestamp;
use restate_clock::time::MillisSinceEpoch;
use restate_limiter::LimitKey;
use restate_partition_store::vqueue_table::{MetaKey, vqueue_meta_merge};
use restate_storage_api::vqueue_table::Stage;
use restate_storage_api::vqueue_table::metadata::{
    Action, MoveMetrics, Update, VQueueLink, VQueueMeta,
};
use restate_storage_api::vqueue_table::stats::WaitStats;
use restate_types::vqueues::VQueueId;

const BASE_TS_MS: u64 = 1_744_000_000_000;
const MERGE_OPERANDS: usize = 10_000;

fn ts(offset_ms: u64) -> UniqueTimestamp {
    UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(BASE_TS_MS + offset_ms))
}

fn move_metrics(
    last_transition_at: UniqueTimestamp,
    first_runnable_at: MillisSinceEpoch,
    has_started: bool,
    blocked_on_concurrency_rules_ms: u32,
    blocked_on_invoker_throttling_ms: u32,
) -> MoveMetrics {
    MoveMetrics {
        last_transition_at,
        has_started,
        first_runnable_at,
        scheduler_wait_stats: Some(WaitStats {
            blocked_on_concurrency_rules_ms,
            blocked_on_invoker_throttling_ms,
            ..WaitStats::default()
        }),
    }
}

fn update_operands() -> Vec<Vec<u8>> {
    let mut operands = Vec::with_capacity(MERGE_OPERANDS);

    for cycle in 0..(MERGE_OPERANDS / 4) {
        let base_offset = (cycle as u64) * 4;
        let enqueued_at = ts(base_offset);
        let started_at = ts(base_offset + 1);
        let finished_at = ts(base_offset + 2);
        let first_runnable_at = enqueued_at.to_unix_millis();

        operands.push(
            Update::new(
                enqueued_at,
                Action::Move {
                    prev_stage: None,
                    next_stage: Stage::Inbox,
                    metrics: move_metrics(enqueued_at, first_runnable_at, false, 0, 0),
                },
            )
            .encode_contiguous()
            .into_vec(),
        );

        operands.push(
            Update::new(
                started_at,
                Action::Move {
                    prev_stage: Some(Stage::Inbox),
                    next_stage: Stage::Running,
                    metrics: move_metrics(
                        enqueued_at,
                        first_runnable_at,
                        false,
                        (cycle % 1_000) as u32,
                        (cycle % 100) as u32,
                    ),
                },
            )
            .encode_contiguous()
            .into_vec(),
        );

        operands.push(
            Update::new(
                finished_at,
                Action::Move {
                    prev_stage: Some(Stage::Running),
                    next_stage: Stage::Finished,
                    metrics: move_metrics(started_at, first_runnable_at, true, 0, 0),
                },
            )
            .encode_contiguous()
            .into_vec(),
        );

        operands.push(
            Update::new(
                ts(base_offset + 3),
                Action::RemoveEntry {
                    stage: Stage::Finished,
                },
            )
            .encode_contiguous()
            .into_vec(),
        );
    }

    operands
}

fn initial_vqueue_meta() -> Vec<u8> {
    VQueueMeta::new(ts(0), None, LimitKey::None, VQueueLink::None)
        .encode_contiguous()
        .into_vec()
}

fn vqueue_meta_key() -> Vec<u8> {
    let qid = VQueueId::custom(1, "vqueue-meta-merge-benchmark");
    MetaKey::from(&qid).to_bytes().to_vec()
}

fn vqueue_meta_merge_benchmark(c: &mut Criterion) {
    let key = vqueue_meta_key();
    let existing_value = initial_vqueue_meta();
    let operands = update_operands();
    let operand_slices = operands.iter().map(Vec::as_slice).collect::<Vec<_>>();

    let mut group = c.benchmark_group("vqueue_meta_merge");
    group.throughput(Throughput::Elements(MERGE_OPERANDS as u64));
    group.bench_function("full_merge_10000_operands", |bencher| {
        bencher.iter(|| {
            let operands = black_box(operand_slices.as_slice()).iter().copied();
            let merged = vqueue_meta_merge::full_merge_slices(
                black_box(key.as_slice()),
                Some(black_box(existing_value.as_slice())),
                operands,
            )
            .expect("vqueue metadata merge succeeds");
            black_box(merged);
        });
    });
    group.finish();
}

criterion_group!(benches, vqueue_meta_merge_benchmark);
criterion_main!(benches);
