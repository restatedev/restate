// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::SysInboxBuilder;
use restate_storage_api::inbox_table::{InboxEntry, SequenceNumberInboxEntry};
use restate_types::identifiers::WithPartitionKey;

#[inline]
pub(crate) fn append_inbox_row(
    builder: &mut SysInboxBuilder,
    inbox_entry: SequenceNumberInboxEntry,
) {
    let mut row = builder.row();

    let SequenceNumberInboxEntry {
        inbox_sequence_number,
        inbox_entry,
    } = inbox_entry;

    if let InboxEntry::Invocation(service_id, invocation_id) = inbox_entry {
        row.partition_key(invocation_id.partition_key());
        row.service_name(&service_id.service_name);
        row.service_key(&service_id.key);

        if row.is_id_defined() {
            row.fmt_id(invocation_id);
        }

        row.sequence_number(inbox_sequence_number);
    } else {
        // todo think about how to present other inbox entries via datafusion: https://github.com/restatedev/restate/issues/1101
    }
}
