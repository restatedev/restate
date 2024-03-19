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
use restate_storage_api::inbox_table::{InboxEntry, SequenceNumberInboxEntry};
use restate_types::identifiers::{InvocationId, TimestampAwareId, WithPartitionKey};
use restate_types::invocation::{ServiceInvocation, Source, TraceId};

#[inline]
pub(crate) fn append_inbox_row(
    builder: &mut InboxBuilder,
    output: &mut String,
    inbox_entry: SequenceNumberInboxEntry,
) {
    let SequenceNumberInboxEntry {
        inbox_sequence_number,
        inbox_entry,
    } = inbox_entry;

    if let InboxEntry::Invocation(ServiceInvocation {
        fid,
        method_name,
        source: caller,
        span_context,
        ..
    }) = inbox_entry
    {
        let mut row = builder.row();
        row.partition_key(fid.partition_key());

        row.component(&fid.service_id.service_name);
        row.handler(&method_name);

        row.component_key(
            std::str::from_utf8(&fid.service_id.key).expect("The key must be a string!"),
        );

        if row.is_id_defined() {
            row.id(format_using(output, &InvocationId::from(&fid)));
        }

        row.sequence_number(inbox_sequence_number);

        match caller {
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
        if row.is_trace_id_defined() {
            let tid = span_context.trace_id();
            if tid != TraceId::INVALID {
                row.trace_id(format_using(output, &tid));
            }
        }

        if row.is_created_at_defined() {
            let ts = fid.invocation_uuid.timestamp();
            row.created_at(ts.as_u64() as i64);
        }
    } else {
        // todo think about how to present other inbox entries via datafusion: https://github.com/restatedev/restate/issues/1101
    }
}
