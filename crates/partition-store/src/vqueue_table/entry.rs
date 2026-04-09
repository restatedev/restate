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

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{
    AsEntryState, AsEntryStateHeader, EntryId, EntryKey, EntryKind, Stage,
};
use restate_types::identifiers::{InvocationId, PartitionKey, WithPartitionKey as _};
use restate_types::vqueue::VQueueId;

use crate::TableKind;
use crate::keys::{EncodeTableKey, KeyDecode, KeyEncode, KeyKind, define_table_key};

// todo: consider adding (validity timestamp, status, or tombstone flag)
// `qe` | PKEY | ENTRY_ID
define_table_key!(
    TableKind::VQueue,
    KeyKind::VQueueEntryState,
    EntryStateKey(
        partition_key: PartitionKey,
        id: EntryId,
    )
);

static_assertions::const_assert_eq!(EntryKind::serialized_length_fixed(), 1);

impl KeyEncode for EntryId {
    fn encode<B: BufMut>(&self, target: &mut B) {
        match self {
            EntryId::Unknown => panic!("cannot encode unknown entry id"),
            EntryId::Invocation(remainder) => {
                target.put_u8(EntryKind::Invocation as u8);
                target.put_slice(remainder);
            }
            EntryId::StateMutation(remainder) => {
                target.put_u8(EntryKind::StateMutation as u8);
                target.put_slice(remainder);
            }
        }
    }

    fn serialized_length(&self) -> usize {
        EntryKind::serialized_length_fixed()
            + match self {
                EntryId::Unknown => 0,
                EntryId::Invocation(uuid) => uuid.serialized_length(),
                EntryId::StateMutation(_) => std::mem::size_of::<u64>(),
            }
    }
}

impl KeyDecode for EntryId {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let raw_kind = source.get_u8();
        let entry_kind = EntryKind::from_repr(raw_kind).ok_or_else(|| {
            StorageError::Generic(anyhow::anyhow!("Unknown vqueue entry kind {raw_kind}"))
        })?;

        match entry_kind {
            EntryKind::Invocation => {
                let mut buf = [0u8; 16];
                source.copy_to_slice(&mut buf);
                Ok(EntryId::Invocation(buf))
            }
            EntryKind::StateMutation => {
                let mut buf = [0u8; 16];
                source.copy_to_slice(&mut buf);
                Ok(EntryId::StateMutation(buf))
            }
        }
    }
}

impl EntryStateKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + std::mem::size_of::<PartitionKey>()
            + std::mem::size_of::<EntryKind>()
            // entry id (e.g. invocation uuid)
            + std::mem::size_of::<EntryId>()
    }

    #[inline]
    pub fn to_bytes(&self) -> [u8; Self::serialized_length_fixed()] {
        let mut buf = [0u8; Self::serialized_length_fixed()];
        self.serialize_to(&mut buf.as_mut());
        buf
    }
}

impl From<&InvocationId> for EntryStateKey {
    #[inline]
    fn from(id: &InvocationId) -> Self {
        EntryStateKey {
            partition_key: id.partition_key(),
            id: EntryId::from(id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, bilrost::Message)]
pub struct EntryStateHeader {
    #[bilrost(1)]
    pub qid: VQueueId,
    /// Unknown is an invalid state, this will be set to None when the invocation
    /// leaves the queue.
    #[bilrost(2)]
    pub stage: Stage,
    // current entry card details
    #[bilrost(3)]
    pub current_entry_key: EntryKey,
}

impl AsEntryStateHeader for EntryStateHeader {
    fn vqueue_id(&self) -> &VQueueId {
        &self.qid
    }

    fn stage(&self) -> Stage {
        self.stage
    }

    fn current_entry_key(&self) -> &EntryKey {
        &self.current_entry_key
    }
}

pub struct OwnedEntryState<E> {
    pub(crate) header: EntryStateHeader,
    pub(crate) state: E,
}

impl<E> AsEntryStateHeader for OwnedEntryState<E> {
    fn stage(&self) -> Stage {
        self.header.stage()
    }

    fn vqueue_id(&self) -> &VQueueId {
        self.header.vqueue_id()
    }

    fn current_entry_key(&self) -> &EntryKey {
        self.header.current_entry_key()
    }
}

impl<E> AsEntryState for OwnedEntryState<E> {
    type State = E;

    fn state(&self) -> &Self::State {
        &self.state
    }
}
