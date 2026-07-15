// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::range::RangeInclusive;

use restate_sharding::KeyRange;
use restate_types::vqueues::VQueueEntryId;

/// Filter vqueue entries by partition keys or entry ID range
#[derive(Debug, Clone)]
pub enum ScanEntryIdFilter {
    PartitionKey(KeyRange),
    EntryIdRange(RangeInclusive<VQueueEntryId>),
}
