// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::EntryCard;
use restate_types::vqueue::VQueueId;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Cards {
    // todo(asoli): please remove me before usage in any released version.
    // we should not need this field if we pass the partition-key from the envelope's
    // header at rsm replay time.
    pub partition_key: u64,
    // doing so to avoid the need to implement serde for qid/entry-card types.
    pub parent: u32,
    pub instance: u32,
    // encoded entry cards
    pub entry_cards: Vec<Bytes>,
}

impl Cards {
    pub fn with_capacity(qid: &VQueueId, capacity: usize) -> Self {
        Self {
            partition_key: qid.partition_key,
            parent: qid.parent.as_u32(),
            instance: qid.instance.as_u32(),
            entry_cards: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, encoded_card: Bytes) {
        self.entry_cards.push(encoded_card);
    }

    pub fn decode_entry_cards(&self) -> impl Iterator<Item = Result<EntryCard, StorageError>> + '_ {
        self.entry_cards
            .iter()
            .map(|buf| EntryCard::decode(&mut buf.as_ref()))
    }
}
