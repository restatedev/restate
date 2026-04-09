// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BufMut};

use restate_storage_api::vqueue_table::{EntryKey, Stage};
use restate_types::identifiers::PartitionKey;
use restate_types::vqueue::VQueueId;

use crate::TableKind::VQueue;
use crate::keys::{EncodeTableKey, KeyDecode, KeyEncode, KeyKind, define_table_key};

// 'qi' | PKEY | QID | STAGE | HAS_LOCK | RUN_AT | SEQ
define_table_key!(
    VQueue,
    KeyKind::VQueueInbox,
    InboxKey(
        // todo:
        // split the partition key and the rest of the vqueue id (re-order such that the stage
        // is after the partition key) to leverarge efficient prefix scans.
        qid: VQueueId,
        stage: Stage,
        entry_key: EntryKey,
    )
);

static_assertions::const_assert_eq!(InboxKey::serialized_length_fixed(), 44);

impl KeyEncode for EntryKey {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put_slice(self.as_bytes());
    }

    fn serialized_length(&self) -> usize {
        EntryKey::serialized_length_fixed()
    }
}

impl KeyDecode for EntryKey {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let mut buf = [0u8; EntryKey::serialized_length_fixed()];
        source.copy_to_slice(&mut buf);
        Ok(Self::from_bytes(buf))
    }
}

impl InboxKey {
    // 44 bytes
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + VQueueId::serialized_length_fixed()
            // stage
            + std::mem::size_of::<Stage>()
            + EntryKey::serialized_length_fixed()
    }

    // 28 bytes
    pub const fn by_stage_prefix_len() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + VQueueId::serialized_length_fixed()
            // stage
            + std::mem::size_of::<Stage>()
    }

    pub const fn offset_of_entry_key() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + VQueueId::serialized_length_fixed()
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
