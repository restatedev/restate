// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::keys::codecs::{
    ReverseU64Codec, ReverseUniqueTimestampCodec, StringCodec, VQueueIdCodec,
};

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
        total_non_completed: ReverseU64Codec,
        last_modified: ReverseUniqueTimestampCodec,
        scope: Option<StringCodec> => str,
        vqueue_id: VQueueIdCodec (primary_key),
    }
);

crate::keys::macros::define_key_selection!(
    BusyVQueueKey; [restate_storage_query_api::tables::index::BusyVQueue];
    total_non_completed, last_modified, scope, vqueue_id,
);
