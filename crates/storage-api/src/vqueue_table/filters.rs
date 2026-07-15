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
use restate_types::vqueues::{VQueueEntryId, VQueueId};

/// Filter vqueue entries by partition keys, entry ID range, or an exact set of entry IDs.
#[derive(Debug, Clone)]
pub enum ScanEntryIdFilter {
    PartitionKey(KeyRange),
    EntryIdRange(RangeInclusive<VQueueEntryId>),
    /// A known set of entry IDs served via batched multi-get calls instead of a
    /// range scan. The set is sorted in on-disk key order.
    EntryIdSet(BTreeSet<VQueueEntryId>),
}

/// Filter vqueue metadata rows by partition keys, vqueue ID range, or an exact
/// set of vqueue IDs.
///
/// Each vqueue id maps to exactly one metadata row, so [`ScanMetaFilter::MetaIdSet`]
/// is served via a batched multi-get instead of a partition-key-range scan.
#[derive(Debug, Clone)]
pub enum ScanMetaFilter {
    PartitionKey(KeyRange),
    MetaIdRange(RangeInclusive<VQueueId>),
    /// A known set of vqueue IDs served via batched multi-get calls. The set is
    /// sorted in on-disk key order (`VQueueId`'s `Ord` matches its key byte
    /// encoding, which is prefixed by the partition key).
    MetaIdSet(BTreeSet<VQueueId>),
}
