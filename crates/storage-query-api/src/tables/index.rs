// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::DataType;

use restate_storage_api::vqueue_table::Stage;
use restate_types::identifiers::CanonicalEntryId;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::VQueueId;
use restate_util_string::ReString;

use super::vqueue_entry_status;

crate::define_schema! {
    /// Persisted entries ordered by stage, service, next transition time, and sequence.
    pub EntryNextAtByService {
        #[starts_with]
        stage: Stage,
        #[starts_with]
        service_name: ReString,
        next_at: MillisSinceEpoch,
        seq: u64,
        canonical_id: CanonicalEntryId,
    }
}

crate::define_schema! {
    /// Persisted virtual-object entries ordered by identity, stage, and newest transition.
    pub EntryByVirtualObject {
        #[starts_with]
        service_name: ReString,
        #[starts_with]
        scope: Option<ReString>,
        #[starts_with]
        key: ReString,
        #[starts_with]
        stage: Stage,
        transitioned_at: MillisSinceEpoch,
        canonical_id: CanonicalEntryId,
    }
}

crate::define_schema! {
    /// Persisted virtual-object entries ordered by identity, stage, next time, and sequence.
    pub EntryNextAtByVirtualObject {
        #[starts_with]
        service_name: ReString,
        #[starts_with]
        scope: Option<ReString>,
        #[starts_with]
        key: ReString,
        #[starts_with]
        stage: Stage,
        next_at: MillisSinceEpoch,
        seq: u64,
        canonical_id: CanonicalEntryId,
    }
}

crate::define_schema! {
    /// Persisted VQueues ordered by descending non-completed count and recency.
    pub BusyVQueue {
        total_non_completed: u64,
        last_modified: MillisSinceEpoch,
        #[starts_with]
        scope: Option<ReString>,
        vqueue_id: VQueueId,
    }
}

crate::define_schema! {
    /// Persisted entry-index records ordered by stage and newest transition first.
    pub EntryByStage {
        #[starts_with]
        stage: Stage,
        transitioned_at: MillisSinceEpoch,
        canonical_id: CanonicalEntryId,
    }
}

crate::define_schema! {
    /// Persisted entry-index records ordered by stage and next transition time.
    pub EntryNextAtByStage {
        #[starts_with]
        stage: Stage,
        next_at: MillisSinceEpoch,
        seq: u64,
        canonical_id: CanonicalEntryId,
    }
}

crate::define_schema! {
    /// Persisted entry-index records ordered by stage, service, and transition time.
    /// Inspection includes only indexed records; entries predating enablement may be absent.
    entry_by_service {
        name: "_idx_entry_by_service",
        primary: vqueue_entry_status::VqueueEntryStatus,
        reference: canonical_id => |id| id.to_base_entry_id(),
        row: for<'a> EntryByServiceView<'a>, append: append_entry_by_service,
        columns {
            /// The entry's service stored in the index.
            #[starts_with]
            service_name: Option<ReString> => DataType::LargeUtf8,
            /// The VQueue stage, compared by its string representation.
            #[starts_with]
            stage => DataType::LargeUtf8,
            /// The entry's last stage transition, at millisecond precision.
            transitioned_at => TimestampMillisecond,
            /// The complete indexed entry incarnation.
            canonical_id => DataType::LargeUtf8,
            /// Resource ID without its sequence suffix.
            entry_id => DataType::LargeUtf8,
            /// Partition key encoded in the canonical ID.
            partition_key => DataType::UInt64,
        }
        key {
            stage: asc nulls_last,
            service_name: asc nulls_last,
            transitioned_at: desc nulls_last,
        }
    }
}

/// A covering index row; formatted IDs are written only when projected.
pub struct EntryByServiceView<'a> {
    pub service_name: &'a str,
    pub stage: Stage,
    pub transitioned_at: MillisSinceEpoch,
    pub canonical_id: CanonicalEntryId,
}

fn append_entry_by_service(
    builder: &mut EntryByServiceBatchBuilder,
    value: EntryByServiceView<'_>,
) {
    let mut row = builder.row();
    row.service_name(value.service_name);
    row.stage(value.stage.as_str());
    row.transitioned_at(value.transitioned_at.as_u64() as i64);
    row.fmt_canonical_id(value.canonical_id);
    row.fmt_entry_id(value.canonical_id.to_base_entry_id());
    row.partition_key(value.canonical_id.partition_key());
}

crate::define_schema! {
    /// Vqueue stage entries, the scheduler's per-stage queues, as an index of entry
    /// status. Only key-derived columns: current status, metadata, and statistics
    /// are in `sys_vqueue_entry_status`. The physical suffix after the queue is
    /// `has_lock | next_at | sequence_number | entry_id`; the declared key order is
    /// not an SQL ordering guarantee (stage and queue bytes are not text order).
    entry_by_queue {
        name: "_idx_entry_by_queue",
        primary: vqueue_entry_status::VqueueEntryStatus,
        reference: canonical_id => |id| id.to_base_entry_id(),
        row: for<'a> EntryByQueueView<'a>, append: append_entry_by_queue,
        columns {
            /// The stage holding the entry.
            stage => DataType::LargeUtf8,
            /// The entry's queue.
            vqueue_id => DataType::LargeUtf8,
            /// Whether the entry holds its lock. Lock holders sort first in a queue.
            has_lock => DataType::Boolean,
            /// The entry's next run time, at second precision.
            next_at => TimestampMillisecond,
            /// Sequence number in the queue ordering key.
            sequence_number => DataType::UInt64,
            /// The complete indexed entry incarnation.
            canonical_id => DataType::LargeUtf8,
            /// Resource ID without its sequence suffix.
            entry_id => DataType::LargeUtf8,
            /// Partition key of the entry's queue.
            partition_key => DataType::UInt64,
        }
        key {
            stage: asc nulls_last,
            vqueue_id: asc nulls_last,
            has_lock: desc nulls_last,
            next_at: asc nulls_last,
            sequence_number: asc nulls_last,
        }
    }
}

/// A stage-index row, decoded from the key alone.
pub struct EntryByQueueView<'a> {
    pub stage: Stage,
    pub vqueue_id: &'a VQueueId,
    pub has_lock: bool,
    pub next_at: MillisSinceEpoch,
    pub canonical_id: CanonicalEntryId,
}

fn append_entry_by_queue(builder: &mut EntryByQueueBatchBuilder, value: EntryByQueueView<'_>) {
    let mut row = builder.row();
    row.stage(value.stage.as_str());
    row.fmt_vqueue_id(value.vqueue_id);
    row.has_lock(value.has_lock);
    row.next_at(value.next_at.as_u64() as i64);
    row.sequence_number(value.canonical_id.seq().as_u64());
    row.fmt_canonical_id(value.canonical_id);
    row.fmt_entry_id(value.canonical_id.to_base_entry_id());
    row.partition_key(value.canonical_id.partition_key());
}
