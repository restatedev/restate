// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::vqueue_table::{EntryKey, EntryKind, EntryValue, Stage};
use restate_types::vqueues::VQueueId;

use super::schema::SysVqueuesBuilder;

#[inline]
pub(crate) fn append_vqueues_row<'a>(
    builder: &mut SysVqueuesBuilder,
    qid: &'a VQueueId,
    stage: Stage,
    entry_key: &'a EntryKey,
    entry: &'a EntryValue,
) {
    let mut row = builder.row();

    row.partition_key(qid.partition_key());
    if row.is_id_defined() {
        row.fmt_id(qid);
    }
    if row.is_stage_defined() {
        row.fmt_stage(stage);
    }
    if row.is_status_defined() {
        row.fmt_status(entry.status);
    }

    if row.is_has_lock_defined() {
        row.has_lock(entry_key.has_lock());
    }
    if matches!(stage, Stage::Inbox) && row.is_run_at_defined() {
        row.run_at(entry_key.run_at().as_unix_millis().as_u64() as i64);
    }
    if row.is_sequence_number_defined() {
        row.sequence_number(entry_key.seq().as_u64());
    }

    if row.is_entry_id_defined() {
        row.fmt_entry_id(entry_key.entry_id().display(qid.partition_key()));
    }

    if row.is_entry_kind_defined() {
        row.entry_kind(match entry_key.kind() {
            EntryKind::Invocation => "invocation",
            EntryKind::StateMutation => "state-mutation",
            EntryKind::Unknown => "unknown",
        });
    }

    if row.is_created_at_defined() {
        row.created_at(entry.stats.created_at.to_unix_millis().as_u64() as i64);
    }

    if row.is_transitioned_at_defined() {
        row.transitioned_at(entry.stats.transitioned_at.to_unix_millis().as_u64() as i64);
    }

    if row.is_num_attempts_defined() {
        row.num_attempts(entry.stats.num_attempts);
    }
    if row.is_num_paused_defined() {
        row.num_paused(entry.stats.num_paused);
    }
    if row.is_num_suspensions_defined() {
        row.num_suspensions(entry.stats.num_suspensions);
    }
    if row.is_num_yields_defined() {
        row.num_yields(entry.stats.num_yields);
    }

    if row.is_first_attempt_at_defined()
        && let Some(first_attempt_at) = entry.stats.first_attempt_at
    {
        row.first_attempt_at(first_attempt_at.to_unix_millis().as_u64() as i64);
    }

    if row.is_latest_attempt_at_defined()
        && let Some(latest_attempt_at) = entry.stats.latest_attempt_at
    {
        row.latest_attempt_at(latest_attempt_at.to_unix_millis().as_u64() as i64);
    }

    if row.is_first_runnable_at_defined() {
        row.first_runnable_at(entry.stats.first_runnable_at.as_u64() as i64);
    }

    if row.is_deployment_defined()
        && let Some(deployment) = &entry.metadata.deployment
    {
        row.fmt_deployment(deployment);
    }
}
