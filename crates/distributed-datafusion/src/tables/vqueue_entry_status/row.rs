// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::RoughTimestamp;
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{EntryMetadataRef, RawStatusHeaderRef, Stage, Status};
use restate_storage_query_api::tables::vqueue_entry_status::VqueueEntryStatusRow;
use restate_types::identifiers::BaseEntryId;
use restate_types::vqueues::{Seq, VQueueId, VQueueIdRef};

use super::schema::SysVqueueEntryStatusBuilder;

/// An entry's place in its queue: the columns of its stage key.
pub(crate) struct EntryPlace<'a> {
    pub(crate) qid: &'a VQueueIdRef<'a>,
    pub(crate) stage: Stage,
    pub(crate) has_lock: bool,
    pub(crate) next_run_at: RoughTimestamp,
    pub(crate) seq: Seq,
}

/// An entry's status, metadata and statistics.
pub(crate) struct EntryState<'a> {
    pub(crate) status: Status,
    pub(crate) metadata: &'a EntryMetadataRef<'a>,
    pub(crate) stats: &'a EntryStatistics,
}

/// The native row of an entry, for checking conditions on it, with the columns
/// [`append_entry_row`] writes from the same parts, except the metadata, which
/// is left empty: the row is for conditions on other columns.
pub(crate) fn covered_row(
    id: &BaseEntryId,
    place: &EntryPlace<'_>,
    state: &EntryState<'_>,
) -> VqueueEntryStatusRow {
    let stats = state.stats;
    let latest = stats.latest_attempt_wait_stats;
    let total = stats.total_wait_stats;
    VqueueEntryStatusRow {
        partition_key: id.partition_key(),
        entry_id: *id,
        vqueue_id: VQueueId::from_raw_bytes(&mut place.qid.as_raw_bytes()),
        stage: place.stage,
        status: state.status,
        has_lock: place.has_lock,
        next_at: place.next_run_at.as_unix_millis(),
        sequence_number: place.seq.as_u64(),
        entry_kind: id.kind(),
        created_at: stats.created_at.to_unix_millis(),
        transitioned_at: stats.transitioned_at.to_unix_millis(),
        num_attempts: stats.num_attempts,
        num_errors: stats.num_errors,
        num_pauses: stats.num_paused,
        num_suspensions: stats.num_suspensions,
        num_yields: stats.num_yields,
        first_attempt_at: stats.first_attempt_at.map(|at| at.to_unix_millis()),
        latest_attempt_at: stats.latest_attempt_at.map(|at| at.to_unix_millis()),
        first_runnable_at: stats.first_runnable_at,
        deployment: None,
        needed_memory: None,
        retry_attempts: 0,
        retry_count_since_last_stored_command: 0,
        latest_attempt_blocked_on_invoker_concurrency: latest.blocked_on_invoker_concurrency_ms
            as i64,
        latest_attempt_blocked_on_throttling_rules: latest.blocked_on_throttling_rules_ms as i64,
        latest_attempt_blocked_on_invoker_throttling: latest.blocked_on_invoker_throttling_ms
            as i64,
        latest_attempt_blocked_on_invoker_memory: latest.blocked_on_invoker_memory_ms as i64,
        latest_attempt_blocked_on_concurrency_rules: latest.blocked_on_concurrency_rules_ms as i64,
        latest_attempt_blocked_on_lock: latest.blocked_on_lock_ms as i64,
        latest_attempt_blocked_on_deployment_concurrency: latest
            .blocked_on_deployment_concurrency_ms
            as i64,
        total_blocked_on_invoker_concurrency: total.blocked_on_invoker_concurrency_ms as i64,
        total_blocked_on_throttling_rules: total.blocked_on_throttling_rules_ms as i64,
        total_blocked_on_invoker_throttling: total.blocked_on_invoker_throttling_ms as i64,
        total_blocked_on_invoker_memory: total.blocked_on_invoker_memory_ms as i64,
        total_blocked_on_concurrency_rules: total.blocked_on_concurrency_rules_ms as i64,
        total_blocked_on_lock: total.blocked_on_lock_ms as i64,
        total_blocked_on_deployment_concurrency: total.blocked_on_deployment_concurrency_ms as i64,
        canonical_id: id.canonicalize(place.seq),
    }
}

#[inline]
pub(crate) fn append_vqueue_entry_status_row(
    builder: &mut SysVqueueEntryStatusBuilder,
    id: &BaseEntryId,
    header: &RawStatusHeaderRef<'_>,
) {
    append_entry_row(
        builder,
        id,
        &EntryPlace {
            qid: &header.qid,
            stage: header.stage,
            has_lock: header.has_lock,
            next_run_at: header.next_run_at,
            seq: header.seq,
        },
        Some(&EntryState {
            status: header.status,
            metadata: &header.metadata,
            stats: &header.stats,
        }),
    );
}

/// Appends an entry's row. Without `state`, only the columns of `place` and
/// `id` are set: the caller projects no other column.
#[inline]
pub(crate) fn append_entry_row(
    builder: &mut SysVqueueEntryStatusBuilder,
    id: &BaseEntryId,
    place: &EntryPlace<'_>,
    state: Option<&EntryState<'_>>,
) {
    let mut row = builder.row();

    if row.is_partition_key_defined() {
        row.partition_key(id.partition_key());
    }

    if row.is_entry_id_defined() {
        row.fmt_entry_id(id);
    }
    if row.is_canonical_id_defined() {
        row.fmt_canonical_id(id.canonicalize(place.seq));
    }
    if row.is_vqueue_id_defined() {
        row.fmt_vqueue_id(place.qid);
    }
    if row.is_stage_defined() {
        row.stage(place.stage.as_str());
    }
    if row.is_has_lock_defined() {
        row.has_lock(place.has_lock);
    }
    if row.is_next_at_defined() {
        row.next_at(place.next_run_at.as_unix_millis().as_u64() as i64);
    }
    if row.is_sequence_number_defined() {
        row.sequence_number(place.seq.as_u64());
    }

    if row.is_entry_kind_defined() {
        row.fmt_entry_kind(id.kind());
    }

    let Some(EntryState {
        status,
        metadata,
        stats,
    }) = state
    else {
        return;
    };

    if row.is_status_defined() {
        row.status(status.as_str());
    }

    if row.is_created_at_defined() {
        row.created_at(stats.created_at.to_unix_millis().as_u64() as i64);
    }

    if row.is_transitioned_at_defined() {
        row.transitioned_at(stats.transitioned_at.to_unix_millis().as_u64() as i64);
    }

    if row.is_num_attempts_defined() {
        row.num_attempts(stats.num_attempts);
    }

    if row.is_num_errors_defined() {
        row.num_errors(stats.num_errors);
    }

    if row.is_num_pauses_defined() {
        row.num_pauses(stats.num_paused);
    }

    if row.is_num_suspensions_defined() {
        row.num_suspensions(stats.num_suspensions);
    }

    if row.is_num_yields_defined() {
        row.num_yields(stats.num_yields);
    }

    if row.is_first_attempt_at_defined()
        && let Some(first_attempt_at) = stats.first_attempt_at
    {
        row.first_attempt_at(first_attempt_at.to_unix_millis().as_u64() as i64);
    }

    if row.is_latest_attempt_at_defined()
        && let Some(latest_attempt_at) = stats.latest_attempt_at
    {
        row.latest_attempt_at(latest_attempt_at.to_unix_millis().as_u64() as i64);
    }

    if row.is_first_runnable_at_defined() {
        row.first_runnable_at(stats.first_runnable_at.as_u64() as i64);
    }

    if row.is_deployment_defined()
        && let Some(deployment) = metadata.deployment
    {
        row.fmt_deployment(deployment);
    }

    if row.is_needed_memory_defined()
        && let Some(needed_memory) = metadata.needed_memory
    {
        row.needed_memory(needed_memory.as_u64());
    }

    if row.is_retry_attempts_defined() {
        row.retry_attempts(metadata.retry_attempts);
    }

    if row.is_retry_count_since_last_stored_command_defined() {
        row.retry_count_since_last_stored_command(metadata.retry_count_since_last_stored_command);
    }

    let latest_wait_stats = stats.latest_attempt_wait_stats;
    if row.is_latest_attempt_blocked_on_invoker_concurrency_defined() {
        row.latest_attempt_blocked_on_invoker_concurrency(
            latest_wait_stats.blocked_on_invoker_concurrency_ms as i64,
        );
    }
    if row.is_latest_attempt_blocked_on_throttling_rules_defined() {
        row.latest_attempt_blocked_on_throttling_rules(
            latest_wait_stats.blocked_on_throttling_rules_ms as i64,
        );
    }
    if row.is_latest_attempt_blocked_on_invoker_throttling_defined() {
        row.latest_attempt_blocked_on_invoker_throttling(
            latest_wait_stats.blocked_on_invoker_throttling_ms as i64,
        );
    }
    if row.is_latest_attempt_blocked_on_invoker_memory_defined() {
        row.latest_attempt_blocked_on_invoker_memory(
            latest_wait_stats.blocked_on_invoker_memory_ms as i64,
        );
    }
    if row.is_latest_attempt_blocked_on_concurrency_rules_defined() {
        row.latest_attempt_blocked_on_concurrency_rules(
            latest_wait_stats.blocked_on_concurrency_rules_ms as i64,
        );
    }
    if row.is_latest_attempt_blocked_on_lock_defined() {
        row.latest_attempt_blocked_on_lock(latest_wait_stats.blocked_on_lock_ms as i64);
    }
    if row.is_latest_attempt_blocked_on_deployment_concurrency_defined() {
        row.latest_attempt_blocked_on_deployment_concurrency(
            latest_wait_stats.blocked_on_deployment_concurrency_ms as i64,
        );
    }

    let total_wait_stats = stats.total_wait_stats;
    if row.is_total_blocked_on_invoker_concurrency_defined() {
        row.total_blocked_on_invoker_concurrency(
            total_wait_stats.blocked_on_invoker_concurrency_ms as i64,
        );
    }
    if row.is_total_blocked_on_throttling_rules_defined() {
        row.total_blocked_on_throttling_rules(
            total_wait_stats.blocked_on_throttling_rules_ms as i64,
        );
    }
    if row.is_total_blocked_on_invoker_throttling_defined() {
        row.total_blocked_on_invoker_throttling(
            total_wait_stats.blocked_on_invoker_throttling_ms as i64,
        );
    }
    if row.is_total_blocked_on_invoker_memory_defined() {
        row.total_blocked_on_invoker_memory(total_wait_stats.blocked_on_invoker_memory_ms as i64);
    }
    if row.is_total_blocked_on_concurrency_rules_defined() {
        row.total_blocked_on_concurrency_rules(
            total_wait_stats.blocked_on_concurrency_rules_ms as i64,
        );
    }
    if row.is_total_blocked_on_lock_defined() {
        row.total_blocked_on_lock(total_wait_stats.blocked_on_lock_ms as i64);
    }
    if row.is_total_blocked_on_deployment_concurrency_defined() {
        row.total_blocked_on_deployment_concurrency(
            total_wait_stats.blocked_on_deployment_concurrency_ms as i64,
        );
    }
}
