// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;

use restate_sharding::PartitionKey;
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryId, EntryKey, EntryValue, RawStatusHeaderRef, Stage, Status,
};
use restate_types::vqueues::{Seq, VQueueId};

use super::schema::SysVqueuesBuilder;

#[inline]
pub(crate) fn append_vqueues_row<'a>(
    builder: &mut SysVqueuesBuilder,
    qid: &'a VQueueId,
    stage: Stage,
    entry_key: &'a EntryKey,
    entry: &'a EntryValue,
) {
    append_vqueues_row_inner(
        builder,
        qid.partition_key(),
        qid,
        stage,
        entry.status,
        entry_key.has_lock(),
        entry_key.run_at().as_unix_millis().as_u64() as i64,
        entry_key.seq(),
        entry_key.entry_id(),
        &entry.stats,
        entry.metadata.deployment.as_deref(),
    );
}

#[inline]
pub(crate) fn append_vqueues_status_row(
    builder: &mut SysVqueuesBuilder,
    partition_key: PartitionKey,
    entry_id: &EntryId,
    header: &RawStatusHeaderRef<'_>,
) {
    append_vqueues_row_inner(
        builder,
        partition_key,
        &header.qid,
        header.stage,
        header.status,
        header.has_lock,
        header.next_run_at.as_unix_millis().as_u64() as i64,
        header.seq,
        entry_id,
        &header.stats,
        header.metadata.deployment,
    );
}

#[allow(clippy::too_many_arguments)]
fn append_vqueues_row_inner(
    builder: &mut SysVqueuesBuilder,
    partition_key: PartitionKey,
    qid: impl Display,
    stage: Stage,
    status: Status,
    has_lock: bool,
    run_at: i64,
    seq: Seq,
    entry_id: &EntryId,
    stats: &EntryStatistics,
    deployment: Option<&str>,
) {
    let mut row = builder.row();

    row.partition_key(partition_key);
    if row.is_id_defined() {
        row.fmt_id(qid);
    }
    if row.is_stage_defined() {
        row.fmt_stage(stage);
    }
    if row.is_status_defined() {
        row.fmt_status(status);
    }

    if row.is_has_lock_defined() {
        row.has_lock(has_lock);
    }
    if matches!(stage, Stage::Inbox) && row.is_run_at_defined() {
        row.run_at(run_at);
    }
    if row.is_sequence_number_defined() {
        row.sequence_number(seq.as_u64());
    }

    if row.is_entry_id_defined() {
        row.fmt_entry_id(entry_id.display(partition_key));
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
        && let Some(deployment) = deployment
    {
        row.fmt_deployment(deployment);
    }
}
