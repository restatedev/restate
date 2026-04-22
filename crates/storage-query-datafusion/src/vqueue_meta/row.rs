// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::vqueue_table::metadata::VQueueMetaRef;
use restate_types::vqueues::VQueueId;

use super::schema::SysVqueueMetaBuilder;

#[inline]
pub(crate) fn append_vqueues_meta_row(
    builder: &mut SysVqueueMetaBuilder,
    qid: &VQueueId,
    meta: &VQueueMetaRef<'_>,
) {
    let mut row = builder.row();

    row.partition_key(qid.partition_key());
    if row.is_id_defined() {
        row.fmt_id(qid);
    }
    if row.is_scope_defined()
        && let Some(scope) = meta.scope
    {
        row.scope(scope);
    }

    if row.is_service_name_defined()
        && let Some(service_name) = meta.service_name()
    {
        row.service_name(service_name);
    }

    if row.is_is_active_defined() {
        row.is_active(meta.is_active());
    }
    if row.is_queue_is_paused_defined() {
        row.queue_is_paused(meta.queue_is_paused);
    }

    if row.is_limit_key_defined() {
        row.fmt_limit_key(&meta.limit_key);
    }

    if row.is_lock_name_defined()
        && let Some(lock_name) = meta.lock_name()
    {
        row.fmt_lock_name(lock_name);
    }

    if row.is_created_at_defined() {
        row.created_at(meta.stats.created_at().as_u64() as i64);
    }

    if row.is_last_enqueued_at_defined()
        && let Some(last_enqueued_at) = meta.stats.last_enqueued_at()
    {
        row.last_enqueued_at(last_enqueued_at.as_u64() as i64);
    }

    if row.is_last_start_at_defined()
        && let Some(last_start_at) = meta.stats.last_start_at()
    {
        row.last_start_at(last_start_at.as_u64() as i64);
    }

    if row.is_last_attempt_at_defined()
        && let Some(last_attempt_at) = meta.stats.last_attempt_at()
    {
        row.last_attempt_at(last_attempt_at.as_u64() as i64);
    }

    if row.is_last_finish_at_defined()
        && let Some(last_finish_at) = meta.stats.last_finish_at()
    {
        row.last_finish_at(last_finish_at.as_u64() as i64);
    }

    if row.is_avg_queue_duration_defined() {
        row.avg_queue_duration(meta.stats.avg_queue_duration_ms() as i64);
    }

    if row.is_avg_inbox_duration_defined() {
        row.avg_inbox_duration(meta.stats.avg_inbox_duration_ms() as i64);
    }

    if row.is_avg_run_duration_defined() {
        row.avg_run_duration(meta.stats.avg_run_duration_ms() as i64);
    }

    if row.is_avg_suspension_duration_defined() {
        row.avg_suspension_duration(meta.stats.avg_suspension_duration_ms() as i64);
    }

    if row.is_avg_end_to_end_duration_defined() {
        row.avg_end_to_end_duration(meta.stats.avg_end_to_end_duration_ms() as i64);
    }

    if row.is_num_inbox_defined() {
        row.num_inbox(meta.stats.num_inbox());
    }

    if row.is_num_running_defined() {
        row.num_running(meta.stats.num_running());
    }

    if row.is_num_suspended_defined() {
        row.num_suspended(meta.stats.num_suspended());
    }

    if row.is_num_paused_defined() {
        row.num_paused(meta.stats.num_paused());
    }

    if row.is_num_finished_defined() {
        row.num_finished(meta.stats.num_finished());
    }
}
