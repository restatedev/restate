// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::vqueues::{EntryId, Seq, VQueueId};

use crate::TableKind::VQueue;
use crate::keys::{KeyKind, define_table_key};

// Input payloads stored for vqueue items.
// Vqueue items are stored under the qid they belong to and their creation order
// 'qi' | QID | SEQ | ENTRY_ID
define_table_key!(
    VQueue,
    KeyKind::VQueueInput,
    InputPayloadKey (
        qid: VQueueId,
        seq: Seq,
        id: EntryId,
    )
);

impl InputPayloadKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + VQueueId::serialized_length_fixed()
            + std::mem::size_of::<Seq>()
            + EntryId::serialized_length_fixed()
    }
}
