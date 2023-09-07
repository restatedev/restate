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
use crate::udfs::restate_keys;
use restate_storage_api::status_table::{InvocationMetadata, InvocationStatus};
use restate_storage_rocksdb::status_table::OwnedStatusRow;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::ServiceInvocationResponseSink;
use std::fmt;
use std::fmt::Write;
use uuid::Uuid;

#[inline]
pub(crate) fn append_status_row(
    builder: &mut StatusBuilder,
    output: &mut String,
    status_row: OwnedStatusRow,
) {
    let mut row = builder.row();
    let metadata = match status_row.invocation_status {
        InvocationStatus::Invoked(metadata) => {
            row.status("invoked");
            Some(metadata)
        }
        InvocationStatus::Suspended { metadata, .. } => {
            row.status("suspended");
            Some(metadata)
        }
        InvocationStatus::Free => {
            row.status("free");
            None
        }
    };

    row.partition_key(status_row.partition_key);
    row.service(&status_row.service);
    row.service_key(&status_row.service_key);
    if row.is_service_key_utf8_defined() {
        if let Some(utf8) = restate_keys::try_decode_restate_key_as_utf8(&status_row.service_key) {
            row.service_key_utf8(utf8);
        }
    }
    if row.is_service_key_int32_defined() {
        if let Some(key) = restate_keys::try_decode_restate_key_as_int32(&status_row.service_key) {
            row.service_key_int32(key);
        }
    }
    if row.is_service_key_uuid_defined() {
        let mut buffer = Uuid::encode_buffer();
        if let Some(key) =
            restate_keys::try_decode_restate_key_as_uuid(&status_row.service_key, &mut buffer)
        {
            row.service_key_uuid(key);
        }
    }
    if let Some(metadata) = metadata {
        if row.is_id_defined() {
            row.id(format_using(
                output,
                &InvocationId::new(status_row.partition_key, metadata.invocation_uuid),
            ));
        }

        fill_invocation_metadata(&mut row, output, metadata);
    }
}

#[inline]
fn fill_invocation_metadata(
    row: &mut StatusRowBuilder,
    output: &mut String,
    meta: InvocationMetadata,
) {
    let InvocationMetadata {
        journal_metadata,
        response_sink,
        creation_time,
        modification_time,
        ..
    } = meta;

    row.created_at(creation_time.as_u64() as i64);
    row.modified_at(modification_time.as_u64() as i64);
    row.method(journal_metadata.method);

    if row.is_trace_id_defined() {
        let tid = journal_metadata.span_context.trace_id();
        row.trace_id(format_using(output, &tid));
    }

    row.journal_size(journal_metadata.length);

    match response_sink {
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
        None => {
            row.invoked_by("unknown");
        }
    }
}

#[inline]
fn format_using<'a>(output: &'a mut String, what: &impl fmt::Display) -> &'a str {
    output.clear();
    write!(output, "{}", what).expect("Error occurred while trying to write in String");
    output
}
