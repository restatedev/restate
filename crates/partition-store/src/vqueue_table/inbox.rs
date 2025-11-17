// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::vqueue_table::{EntryCard, EntryId, EntryKind, Stage, VisibleAt};
use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::PartitionKey;
use restate_types::vqueue::{EffectivePriority, VQueueInstance, VQueueParent};

use crate::TableKind::VQueue;
use crate::keys::{KeyKind, TableKey, define_table_key};

// 'qi' | PKEY | QID | STAGE | PRIORITY | VISIBLE_AT | CREATED_AT | ENTRY_KIND | ENTRY_ID
define_table_key!(
    VQueue,
    KeyKind::VQueueInbox,
    InboxKey(
        partition_key: PartitionKey,
        parent: VQueueParent,
        instance: VQueueInstance,
        stage: Stage,
        priority: EffectivePriority,
        visible_at: VisibleAt,
        created_at: UniqueTimestamp,
        kind: EntryKind,
        id: EntryId,
    )
);

static_assertions::const_assert_eq!(InboxKey::serialized_length_fixed(), 53);

impl InboxKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + std::mem::size_of::<PartitionKey>()
            // vq parent
            + std::mem::size_of::<VQueueParent>()
            // vq instance
            + std::mem::size_of::<VQueueInstance>()
            // stage
            + std::mem::size_of::<Stage>()
            // priority
            + std::mem::size_of::<EffectivePriority>()
            // visible at
            + std::mem::size_of::<VisibleAt>()
            // created_at
            + std::mem::size_of::<UniqueTimestamp>()
            // entry kind
            + std::mem::size_of::<EntryKind>()
            // entry id
            + std::mem::size_of::<EntryId>()
    }

    pub const fn by_stage_prefix_len() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + std::mem::size_of::<PartitionKey>()
            // vq parent
            + std::mem::size_of::<VQueueParent>()
            // vq instance
            + std::mem::size_of::<VQueueInstance>()
            // stage
            + std::mem::size_of::<Stage>()
    }

    #[inline]
    pub fn to_bytes(&self) -> [u8; Self::serialized_length_fixed()] {
        let mut buf = [0u8; Self::serialized_length_fixed()];
        self.serialize_to(&mut buf.as_mut());
        buf
    }
}

impl From<InboxKey> for EntryCard {
    #[inline(always)]
    fn from(value: InboxKey) -> Self {
        let InboxKey {
            priority,
            visible_at,
            created_at,
            kind,
            id,
            ..
        } = value;
        Self {
            priority,
            visible_at,
            created_at,
            kind,
            id,
        }
    }
}

// 'qa' | PKEY | QID
define_table_key!(
    VQueue,
    KeyKind::VQueueActive,
    ActiveKey(
        partition_key: PartitionKey,
        parent: VQueueParent,
        instance: VQueueInstance,
    )
);

static_assertions::const_assert_eq!(ActiveKey::serialized_length_fixed(), 18);

impl ActiveKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + std::mem::size_of::<PartitionKey>()
            + std::mem::size_of::<VQueueParent>()
            + std::mem::size_of::<VQueueInstance>()
    }

    pub const fn by_partition_prefix_len() -> usize {
        KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<PartitionKey>()
    }
}
