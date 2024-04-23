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
use restate_types::identifiers::{TimestampAwareId, WithPartitionKey};

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

    if let InboxEntry::Invocation(service_id, invocation_id) = inbox_entry {
        let mut row = builder.row();
        row.partition_key(invocation_id.partition_key());

        row.service_name(&service_id.service_name);

        row.service_key(&service_id.key);

        if row.is_id_defined() {
            row.id(format_using(output, &invocation_id));
        }

        row.sequence_number(inbox_sequence_number);

        if row.is_created_at_defined() {
            let ts = invocation_id.timestamp();
            row.created_at(ts.as_u64() as i64);
        }
    } else {
        // todo think about how to present other inbox entries via datafusion: https://github.com/restatedev/restate/issues/1101
    }
}
