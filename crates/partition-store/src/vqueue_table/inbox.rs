// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::RoughTimestamp;
use restate_storage_api::vqueue_table::Stage;
use restate_types::identifiers::PartitionKey;
use restate_types::vqueues::{EntryId, Seq, VQueueId};

use crate::TableKind::VQueue;
use crate::keys::{EncodeTableKey, KeyKind, define_table_key};

use super::key_codec::HasLock;

// 'qi' | PKEY | STAGE | QID | HAS_LOCK(1B) | RUN_AT(8B) | SEQ(8B) | ENTRY_ID(17B)
define_table_key!(
    VQueue,
    KeyKind::VQueueInbox,
    InboxKey(
        partition_key: PartitionKey,
        stage: Stage,
        qid: VQueueId,
        has_lock: HasLock,
        run_at: RoughTimestamp,
        seq: Seq,
        entry_id: EntryId,
    )
);

static_assertions::const_assert_eq!(InboxKey::serialized_length_fixed(), 70);

impl InboxKey {
    // 70 bytes
    pub const fn serialized_length_fixed() -> usize {
        // 2 bytes for prefix
        KeyKind::SERIALIZED_LENGTH
            // 8 bytes
            + std::mem::size_of::<PartitionKey>()
            // 1 byte
            + Stage::serialized_length_fixed()
            // 25 bytes (we can trim the partition key from it but it's not worth the hassle)
            + VQueueId::serialized_length_fixed()
            // 1 byte
            + HasLock::serialized_length_fixed()
            // 8 bytes (run_at)
            + std::mem::size_of::<u64>()
            // 8 bytes (seq)
            + std::mem::size_of::<Seq>()
            // 17 bytes
            + EntryId::serialized_length_fixed()
    }

    pub const fn by_stage_in_qid_prefix_len() -> usize {
        KeyKind::SERIALIZED_LENGTH
            // 8 bytes
            + std::mem::size_of::<PartitionKey>()
            // 1 byte for stage
            + std::mem::size_of::<Stage>()
            // 25 bytes
            + VQueueId::serialized_length_fixed()
    }

    pub const fn by_stage_prefix_len() -> usize {
        KeyKind::SERIALIZED_LENGTH
            // 8 bytes
            + std::mem::size_of::<PartitionKey>()
            // 1 byte for stage
            + std::mem::size_of::<Stage>()
    }

    pub const fn offset_of_entry_key() -> usize {
        KeyKind::SERIALIZED_LENGTH
            // 8 bytes
            + std::mem::size_of::<PartitionKey>()
            // stage
            + std::mem::size_of::<Stage>()
            // vqueue id
            + VQueueId::serialized_length_fixed()
    }

    pub const fn offset_of_stage() -> usize {
        KeyKind::SERIALIZED_LENGTH
            // 8 bytes
            + std::mem::size_of::<PartitionKey>()
    }

    #[inline]
    pub fn to_bytes(&self) -> [u8; Self::serialized_length_fixed()] {
        let mut buf = [0u8; Self::serialized_length_fixed()];
        self.serialize_to(&mut buf.as_mut());
        buf
    }
}

// 'qa' | QID (QID is prefixed by PartitionKey internally)
define_table_key!(
    VQueue,
    KeyKind::VQueueActive,
    ActiveKey(
        qid: VQueueId,
    )
);

static_assertions::const_assert_eq!(ActiveKey::serialized_length_fixed(), 27);

impl ActiveKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH + VQueueId::serialized_length_fixed()
    }

    pub const fn by_partition_prefix_len() -> usize {
        KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<PartitionKey>()
    }
}
