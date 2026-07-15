// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::range::RangeInclusive;

use restate_sharding::KeyRange;
use restate_types::vqueues::VQueueEntryId;

/// Filter vqueue entries by partition keys, entry ID range, or an exact set of entry IDs.
#[derive(Debug, Clone)]
pub enum ScanEntryIdFilter {
    PartitionKey(KeyRange),
    EntryIdRange(RangeInclusive<VQueueEntryId>),
    /// A known, bounded set of entry IDs served via a batched multi-get instead
    /// of a range scan. The set is sorted in on-disk key order (see the ordering
    /// invariant asserted in `partition-store`'s `entry` tests).
    EntryIdSet(BTreeSet<VQueueEntryId>),
}
