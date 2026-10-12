// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_sharding::PartitionKey;
use restate_storage_api::vqueue_table::RawStatusHeaderRef;
use restate_types::vqueues::EntryId;

use super::schema::SysVqueueEntryStatusBuilder;

#[inline]
pub(crate) fn append_vqueue_entry_status_row(
    builder: &mut SysVqueueEntryStatusBuilder,
    partition_key: PartitionKey,
    entry_id: &EntryId,
    header: &RawStatusHeaderRef<'_>,
) {
    let stats = &header.stats;
    let metadata = &header.metadata;

    let mut row = builder.row();

    if row.is_partition_key_defined() {
        row.partition_key(partition_key);
    }

    if row.is_entry_id_defined() {
        row.fmt_entry_id(entry_id.display(partition_key));
    }
    if row.is_vqueue_id_defined() {
        row.fmt_vqueue_id(&header.qid);
    }
    if row.is_stage_defined() {
        row.fmt_stage(header.stage);
    }
    if row.is_status_defined() {
        row.fmt_status(header.status);
    }
    if row.is_has_lock_defined() {
        row.has_lock(header.has_lock);
    }
    if row.is_next_at_defined() {
        row.next_at(header.next_run_at.as_unix_millis().as_u64() as i64);
    }
    if row.is_sequence_number_defined() {
        row.sequence_number(header.seq.as_u64());
    }

    if row.is_entry_kind_defined() {
        row.fmt_entry_kind(entry_id.kind());
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
