// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::invocation_status::schema::{InvocationStatusBuilder, InvocationStatusRowBuilder};
use crate::table_util::format_using;
use restate_storage_api::invocation_status_table::{
    InvocationMetadata, InvocationStatus, JournalMetadata, StatusTimestamps,
};
use restate_storage_rocksdb::invocation_status_table::OwnedInvocationStatusRow;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::{Source, TraceId};

#[inline]
pub(crate) fn append_invocation_status_row(
    builder: &mut InvocationStatusBuilder,
    output: &mut String,
    status_row: OwnedInvocationStatusRow,
) {
    let mut row = builder.row();

    row.partition_key(status_row.partition_key);
    if let Some(service_id) = status_row.invocation_status.service_id() {
        row.component(&service_id.service_name);
        row.component_key(std::str::from_utf8(&service_id.key).expect("The key must be a string!"));
    }

    // Invocation id
    if row.is_id_defined() {
        row.id(format_using(
            output,
            &InvocationId::new(status_row.partition_key, status_row.invocation_uuid),
        ));
    }

    // Journal metadata
    if let Some(journal_metadata) = status_row.invocation_status.get_journal_metadata() {
        fill_journal_metadata(&mut row, output, journal_metadata)
    }

    // Stat
    if let Some(timestamps) = status_row.invocation_status.get_timestamps() {
        fill_timestamps(&mut row, timestamps);
    }

    // Additional invocation metadata
    let metadata = match status_row.invocation_status {
        InvocationStatus::Invoked(metadata) => {
            row.status("invoked");
            Some(metadata)
        }
        InvocationStatus::Suspended { metadata, .. } => {
            row.status("suspended");
            Some(metadata)
        }
        InvocationStatus::Virtual { .. } => {
            row.status("virtual");
            None
        }
        InvocationStatus::Free => {
            row.status("free");
            None
        }
    };
    if let Some(metadata) = metadata {
        fill_invocation_metadata(&mut row, output, metadata);
    }
}

#[inline]
fn fill_invocation_metadata(
    row: &mut InvocationStatusRowBuilder,
    output: &mut String,
    meta: InvocationMetadata,
) {
    // journal_metadata and stats are filled by other functions
    row.handler(meta.method);
    if let Some(deployment_id) = meta.deployment_id {
        row.pinned_deployment_id(deployment_id.to_string());
    }
    match meta.source {
        Source::Service(caller) => {
            row.invoked_by("component");
            row.invoked_by_component(&caller.service_id.service_name);
            if row.is_invoked_by_id_defined() {
                row.invoked_by_id(format_using(output, &caller));
            }
        }
        Source::Ingress => {
            row.invoked_by("ingress");
        }
        Source::Internal => {
            row.invoked_by("restate");
        }
    }
}

#[inline]
fn fill_timestamps(row: &mut InvocationStatusRowBuilder, stat: &StatusTimestamps) {
    row.created_at(stat.creation_time().as_u64() as i64);
    row.modified_at(stat.modification_time().as_u64() as i64);
}

#[inline]
fn fill_journal_metadata(
    row: &mut InvocationStatusRowBuilder,
    output: &mut String,
    journal_metadata: &JournalMetadata,
) {
    if row.is_trace_id_defined() {
        let tid = journal_metadata.span_context.trace_id();
        if tid != TraceId::INVALID {
            row.trace_id(format_using(output, &tid));
        }
    }

    row.journal_size(journal_metadata.length);
}
