// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bilrost::OwnedMessage;
use bytes::{Buf, Bytes};

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryCard, WaitStats};
use restate_types::vqueues::VQueueId;

#[derive(Debug, Clone, bilrost::Message)]
pub struct VQWaitingToRunning {
    #[bilrost(1)]
    pub assignment: Assignment,
    #[bilrost(2)]
    pub meta_updates: MetaUpdates,
}

impl VQWaitingToRunning {
    pub fn encode_to_bytes(&self) -> Bytes {
        bilrost::Message::encode_length_delimited_to_bytes(self)
    }

    pub fn decode<B: Buf>(buf: B) -> Result<Self, StorageError> {
        Ok(Self::decode_length_delimited(buf)?)
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct VQYieldRunning {
    #[bilrost(1)]
    pub assignment: Assignment,
}

impl VQYieldRunning {
    pub fn encode_to_bytes(&self) -> Bytes {
        bilrost::Message::encode_length_delimited_to_bytes(self)
    }

    pub fn decode<B: Buf>(buf: B) -> Result<Self, StorageError> {
        Ok(Self::decode_length_delimited(buf)?)
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct Assignment {
    #[bilrost(1)]
    pub qid: VQueueId,
    // encoded entry cards
    #[bilrost(4)]
    pub entries: Vec<Entry>,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct Entry {
    #[bilrost(1)]
    pub card: EntryCard,
    #[bilrost(2)]
    pub stats: WaitStats,
}

impl Assignment {
    pub fn with_capacity(qid: VQueueId, capacity: usize) -> Self {
        Self {
            qid,
            entries: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, item: EntryCard, stats: WaitStats) {
        self.entries.push(Entry { card: item, stats });
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct MetaUpdates {
    // todo: remove this if it's not needed for counters/lock tracking
}
