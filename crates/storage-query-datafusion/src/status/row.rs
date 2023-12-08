// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::status::schema::{StatusBuilder, StatusRowBuilder};
use crate::table_util::format_using;
use restate_storage_api::status_table::{
    InvocationMetadata, InvocationStatus, JournalMetadata, StatusTimestamps,
};
use restate_storage_rocksdb::status_table::OwnedStatusRow;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::ServiceInvocationResponseSink;

#[inline]
pub(crate) fn append_status_row(
    builder: &mut StatusBuilder,
    output: &mut String,
    status_row: OwnedStatusRow,
) {
    let mut row = builder.row();

    row.partition_key(status_row.partition_key);
    row.service(&status_row.service);
    row.service_key(
        std::str::from_utf8(&status_row.service_key).expect("The key must be a string!"),
    );

    // Invocation id
    if row.is_id_defined() {
        if let Some(invocation_uuid) = status_row.invocation_status.invocation_uuid() {
            row.id(format_using(
                output,
                &InvocationId::new(status_row.partition_key, invocation_uuid),
            ));
        }
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
    row: &mut StatusRowBuilder,
    output: &mut String,
    meta: InvocationMetadata,
) {
    // journal_metadata and stats are filled by other functions
    row.method(meta.method);
    if let Some(endpoint_id) = meta.endpoint_id {
        row.pinned_endpoint_id(endpoint_id);
    }
    match meta.response_sink {
        Some(ServiceInvocationResponseSink::PartitionProcessor { caller, .. }) => {
            row.invoked_by("service");
            row.invoked_by_service(&caller.service_id.service_name);
            if row.is_invoked_by_id_defined() {
                row.invoked_by_id(format_using(output, &caller));
            }
        }
        Some(ServiceInvocationResponseSink::Ingress(..)) => {
            row.invoked_by("ingress");
        }
        _ => {
            row.invoked_by("unknown");
        }
    }
}

#[inline]
fn fill_timestamps(row: &mut StatusRowBuilder, stat: &StatusTimestamps) {
    row.created_at(stat.creation_time().as_u64() as i64);
    row.modified_at(stat.modification_time().as_u64() as i64);
}

#[inline]
fn fill_journal_metadata(
    row: &mut StatusRowBuilder,
    output: &mut String,
    journal_metadata: &JournalMetadata,
) {
    if row.is_trace_id_defined() {
        let tid = journal_metadata.span_context.trace_id();
        row.trace_id(format_using(output, &tid));
    }

    row.journal_size(journal_metadata.length);
}
