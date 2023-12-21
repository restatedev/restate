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
use restate_storage_api::inbox_table::InboxEntry;
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::invocation::{ServiceInvocation, Source};
use std::time::Duration;
use uuid::Uuid;

#[inline]
pub(crate) fn append_inbox_row(
    builder: &mut InboxBuilder,
    output: &mut String,
    inbox_entry: InboxEntry,
) {
    let InboxEntry {
        inbox_sequence_number,
        service_invocation:
            ServiceInvocation {
                fid,
                method_name,
                source: caller,
                span_context,
                ..
            },
    } = inbox_entry;

    let mut row = builder.row();
    row.partition_key(fid.partition_key());

    row.service(&fid.service_id.service_name);
    row.method(&method_name);

    row.service_key(std::str::from_utf8(&fid.service_id.key).expect("The key must be a string!"));

    if row.is_id_defined() {
        row.id(format_using(output, &InvocationId::from(&fid)));
    }

    row.sequence_number(inbox_sequence_number);

    match caller {
        Source::Service(caller) => {
            row.invoked_by("service");
            row.invoked_by_service(&caller.service_id.service_name);
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
