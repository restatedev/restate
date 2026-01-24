// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::vqueue_table::{EntryId, EntryKind};
use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::PartitionKey;
use restate_types::vqueue::{VQueueInstance, VQueueParent};

use crate::TableKind::VQueue;
use crate::keys::{KeyKind, define_table_key};

// Vqueue items are stored under the qid they belong to and their creation timestamp to maintain
// the order in which they were inserted into the vqueue.
// 'qI' | PKEY | VQUEUE_PARENT | VQUEUE_INSTANCE | CREATED_AT | ENTRY_KIND | ENTRY_ID
define_table_key!(
    VQueue,
    KeyKind::VQueueItems,
    ItemsKey (
        partition_key: PartitionKey,
        parent: VQueueParent,
        instance: VQueueInstance,
        created_at: UniqueTimestamp,
        kind: EntryKind,
        id: EntryId,
    )
);

static_assertions::const_assert_eq!(ItemsKey::serialized_length_fixed(), 43);

impl ItemsKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + size_of::<PartitionKey>()
            + size_of::<VQueueParent>()
            + size_of::<VQueueInstance>()
            + size_of::<UniqueTimestamp>()
            + size_of::<EntryKind>()
            + size_of::<EntryId>()
    }
}
