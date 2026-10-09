// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::time::MillisSinceEpoch;
use restate_types::identifiers::CanonicalEntryId;
use restate_types::vqueues::VQueueId;
use restate_util_string::ReString;

crate::define_table! {
    /// Persisted entries ordered by stage, service, next transition time, and sequence.
    pub EntryNextAtByService;
}

crate::define_filter! {
    pub EntryNextAtByService {
        stage: ReString => starts_with,
        service_name: ReString => starts_with,
        next_at: MillisSinceEpoch,
        seq: u64,
        canonical_id: CanonicalEntryId,
    }
}

crate::define_table! {
    /// Persisted virtual-object entries ordered by identity, stage, and newest transition.
    pub EntryByVirtualObject;
}

crate::define_filter! {
    pub EntryByVirtualObject {
        service_name: ReString => starts_with,
        scope: Option<ReString> => starts_with,
        key: ReString => starts_with,
        stage: ReString => starts_with,
        transitioned_at: MillisSinceEpoch,
        canonical_id: CanonicalEntryId,
    }
}

crate::define_table! {
    /// Persisted virtual-object entries ordered by identity, stage, next time, and sequence.
    pub EntryNextAtByVirtualObject;
}

crate::define_filter! {
    pub EntryNextAtByVirtualObject {
        service_name: ReString => starts_with,
        scope: Option<ReString> => starts_with,
        key: ReString => starts_with,
        stage: ReString => starts_with,
        next_at: MillisSinceEpoch,
        seq: u64,
        canonical_id: CanonicalEntryId,
    }
}

crate::define_table! {
    /// Persisted VQueues ordered by descending non-completed count and recency.
    pub BusyVQueue;
}

crate::define_filter! {
    pub BusyVQueue {
        total_non_completed: u64,
        last_modified: MillisSinceEpoch,
        scope: Option<ReString> => starts_with,
        vqueue_id: VQueueId,
    }
}

crate::define_table! {
    /// Persisted entry-index records ordered by stage, service, and transition time.
    /// This inspection surface does not imply completeness for pre-existing stores.
    pub EntryByService;
}

crate::define_filter! {
    pub EntryByService {
        /// The service name.
        service_name: ReString => starts_with,
        /// The VQueue stage, compared by its string representation.
        stage: ReString => starts_with,
        /// The entry's last stage transition as a Unix timestamp in milliseconds.
        transitioned_at: MillisSinceEpoch,
        /// The indexed entry's incarnation identity.
        canonical_id: CanonicalEntryId,
    }
}

crate::define_table! {
    /// Persisted entry-index records ordered by stage and newest transition first.
    pub EntryByStage;
}

crate::define_filter! {
    pub EntryByStage {
        /// The VQueue stage, compared by its string representation.
        stage: ReString => starts_with,
        /// The entry's last stage transition as a Unix timestamp in milliseconds.
        transitioned_at: MillisSinceEpoch,
        /// The indexed entry's incarnation identity.
        canonical_id: CanonicalEntryId,
    }
}

crate::define_table! {
    /// Persisted entry-index records ordered by stage and next transition time.
    pub EntryNextAtByStage;
}

crate::define_filter! {
    pub EntryNextAtByStage {
        /// The VQueue stage, compared by its string representation.
        stage: ReString => starts_with,
        /// The entry's scheduled transition time as a Unix timestamp in milliseconds.
        next_at: MillisSinceEpoch,
        /// Sequence used to order transitions within the same second.
        seq: u64,
        /// The indexed entry's incarnation identity.
        canonical_id: CanonicalEntryId,
    }
}
