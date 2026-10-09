// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;

use restate_clock::UniqueTimestamp;
use restate_types::vqueues::VQueueId;
use restate_util_string::ReString;

use super::macros::define_secondary_index;

define_secondary_index!(
    /// Find the busiest VQueues in a partition, preferring recently modified queues on ties.
    ///
    /// Ordering within a partition:
    /// `total_non_completed DESC -> last_modified DESC -> scope ASC -> vqueue_id ASC`.
    /// Values contain sparse `StageCounts`, including finished entries, so per-stage
    /// counts can be read without fetching queue metadata. Empty queues remain indexed.
    BusyVQueue,
    key: BusyVQueueKey {
        // (sorting key)
        total_non_completed: Reverse<u64>,
        last_modified: Reverse<UniqueTimestamp>,
        scope: Option<ReString> => str,
        vqueue_id: VQueueId (primary_key),
    }
);

crate::keys::macros::define_index_key_filter!(
    BusyVQueueKey; [restate_storage_api::index::BusyVQueue];
    total_non_completed: Reverse<u64>,
    last_modified: Reverse<UniqueTimestamp>,
    scope: Option<ReString>,
    vqueue_id: VQueueId,
);
