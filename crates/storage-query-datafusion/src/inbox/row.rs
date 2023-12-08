// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::InboxBuilder;
use crate::table_util::format_using;
use crate::udfs::restate_keys;
use restate_schema_api::key::RestateKeyConverter;
use restate_storage_api::inbox_table::InboxEntry;
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::invocation::{ServiceInvocation, ServiceInvocationResponseSink};
use std::time::Duration;
use uuid::Uuid;

#[inline]
pub(crate) fn append_inbox_row(
    builder: &mut InboxBuilder,
    output: &mut String,
    inbox_entry: InboxEntry,
    resolver: impl RestateKeyConverter,
) {
    let InboxEntry {
        inbox_sequence_number,
        service_invocation:
            ServiceInvocation {
                fid,
                method_name,
                response_sink,
                span_context,
                ..
            },
    } = inbox_entry;

    let mut row = builder.row();
    row.partition_key(fid.partition_key());

    row.service_name(&fid.service_id.service_name);
    row.method(&method_name);

    row.service_key(&fid.service_id.key);
    if row.is_service_key_utf8_defined() {
        if let Some(utf8) = restate_keys::try_decode_restate_key_as_utf8(&fid.service_id.key) {
            row.service_key_utf8(utf8);
        }
    }
    if row.is_service_key_int32_defined() {
        if let Some(key) = restate_keys::try_decode_restate_key_as_int32(&fid.service_id.key) {
            row.service_key_int32(key);
        }
    }
    if row.is_service_key_uuid_defined() {
        let mut buffer = Uuid::encode_buffer();
        if let Some(key) =
            restate_keys::try_decode_restate_key_as_uuid(&fid.service_id.key, &mut buffer)
        {
            row.service_key_uuid(key);
        }
    }
    if row.is_service_key_json_defined() {
        if let Some(key) = restate_keys::try_decode_restate_key_as_json(
            &fid.service_id.service_name,
            &fid.service_id.key,
            output,
            resolver,
        ) {
            row.service_key_json(key);
        }
    }

    if row.is_id_defined() {
        row.id(format_using(output, &InvocationId::from(&fid)));
    }

    row.sequence_number(inbox_sequence_number);

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
        _ => {
            row.invoked_by("unknown");
        }
    }
    if row.is_trace_id_defined() {
        let tid = span_context.trace_id();
        row.trace_id(format_using(output, &tid));
    }

    if row.is_created_at_defined() {
        let (secs, nanos) = Uuid::from(fid.invocation_uuid)
            .get_timestamp()
            .expect("The UUID must be a v7 uuid")
            .to_unix();
        row.created_at(Duration::new(secs, nanos).as_millis() as i64);
    }
}
