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
use restate_storage_api::vqueue_table::{EntryKey, Stage};
use restate_types::vqueues::{EntryId, Seq, VQueueId};

use crate::TableKind::VQueue;
use crate::keys::{KeyKind, define_table_key};

use super::key_codec::HasLock;

// The stage is the second character of the table kind.
// For instance:
// 'qI' | QID | HAS_LOCK(1B) | RUN_AT(8B) | SEQ(8B) | ENTRY_ID(17B)
// 'qR' | QID | HAS_LOCK(1B) | RUN_AT(8B) | SEQ(8B) | ENTRY_ID(17B)
// 'qS' | QID | HAS_LOCK(1B) | RUN_AT(8B) | SEQ(8B) | ENTRY_ID(17B)
// 'qP' | QID | HAS_LOCK(1B) | RUN_AT(8B) | SEQ(8B) | ENTRY_ID(17B)
// 'qF' | QID | HAS_LOCK(1B) | RUN_AT(8B) | SEQ(8B) | ENTRY_ID(17B)

// Inbox, Running, Suspended, Paused, and have the same key design.
macro_rules! define_stage_keys {
    ( $(Stage::$stage: ident => $key_name: ident),+ $(,)? ) => {
        paste::paste! {
            $(
                define_table_key!(
                    VQueue,
                    KeyKind::[<VQueue  $stage  Stage >],
                    $key_name(
                        qid: VQueueId,
                        has_lock: HasLock,
                        run_at: RoughTimestamp,
                        seq: Seq,
                        entry_id: EntryId,
                    )
                );

                static_assertions::const_assert_eq!($key_name::serialized_length_fixed(), 61);

                // ensure that the key has the same length as prefix + qid + entry-key
                static_assertions::const_assert_eq!($key_name::serialized_length_fixed(), 2 +
                    VQueueId::serialized_length_fixed() + EntryKey::serialized_length_fixed());

                impl $key_name {
                    // 61 bytes
                    pub const fn serialized_length_fixed() -> usize {
                        // 2 bytes for prefix
                        KeyKind::SERIALIZED_LENGTH
                            // 25 bytes
                            + VQueueId::serialized_length_fixed()
                            // 34 bytes
                            + EntryKey::serialized_length_fixed()
                    }
                    #[allow(dead_code)]
                    pub const fn by_qid_prefix_len() -> usize {
                        // 2 bytes for prefix
                        KeyKind::SERIALIZED_LENGTH
                            // 25 bytes
                            + VQueueId::serialized_length_fixed()
                    }
                    #[allow(dead_code)]
                    pub const fn offset_of_entry_key() -> usize {
                        Self::by_qid_prefix_len()
                    }
                }
            )+
        }
    };
}

// Inbox, Running, Suspended, Paused, and have the same key design.
define_stage_keys! {
    Stage::Inbox => InboxKey,
    Stage::Running => RunningKey,
    Stage::Suspended => SuspendedKey,
    Stage::Paused => PausedKey,
    Stage::Finished => FinishedKey,
}

pub(super) fn encode_stage_key(
    stage: Stage,
    qid: &VQueueId,
    entry_key: &EntryKey,
) -> [u8; InboxKey::serialized_length_fixed()] {
    let mut key_buffer = [0u8; InboxKey::serialized_length_fixed()];
    {
        let mut buf = &mut key_buffer[..];
        match stage {
            Stage::Unknown => unreachable!(),
            Stage::Inbox => {
                KeyKind::VQueueInboxStage.serialize(&mut buf);
            }
            Stage::Running => {
                KeyKind::VQueueRunningStage.serialize(&mut buf);
            }
            Stage::Suspended => {
                KeyKind::VQueueSuspendedStage.serialize(&mut buf);
            }
            Stage::Paused => {
                KeyKind::VQueuePausedStage.serialize(&mut buf);
            }
            Stage::Finished => {
                KeyKind::VQueueFinishedStage.serialize(&mut buf);
            }
        }
        crate::keys::serialize(qid, &mut buf);
        crate::keys::serialize(entry_key, &mut buf);
    }
    key_buffer
}
