// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::vqueue_table::{
    AsEntryState, AsEntryStateHeader, EntryCard, EntryId, EntryKind, Stage, VisibleAt,
};
use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::{InvocationId, PartitionKey, WithPartitionKey as _};
use restate_types::vqueue::{EffectivePriority, VQueueId, VQueueInstance, VQueueParent};

use crate::TableKind;
use crate::keys::{KeyKind, TableKey, define_table_key};

// `qe` | PKEY | KIND | ENTRY_ID
define_table_key!(
    TableKind::VQueue,
    KeyKind::VQueueEntryState,
    EntryStateKey(
        partition_key: PartitionKey,
        kind: EntryKind,
        id: EntryId,
    )
);

static_assertions::const_assert_eq!(EntryStateKey::serialized_length_fixed(), 27);

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
            kind: EntryKind::Invocation,
            id: EntryId::from(id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, bilrost::Message)]
pub struct EntryStateHeader {
    /// Unknown is an invalid state, this will be set to None when the invocation
    /// leaves the queue.
    #[bilrost(1)]
    pub stage: Stage,
    #[bilrost(2)]
    pub queue_parent: u32,
    #[bilrost(3)]
    pub queue_instance: u32,
    // current entry card details
    #[bilrost(4)]
    pub effective_priority: EffectivePriority,
    #[bilrost(5)]
    pub visible_at: VisibleAt,
    #[bilrost(6)]
    pub created_at: UniqueTimestamp,
}

pub struct OwnedHeader {
    pub(crate) partition_key: PartitionKey,
    pub(crate) kind: EntryKind,
    pub(crate) id: EntryId,

    pub(crate) inner: EntryStateHeader,
}

impl AsEntryStateHeader for OwnedHeader {
    fn kind(&self) -> EntryKind {
        self.kind
    }

    fn stage(&self) -> Stage {
        self.inner.stage
    }

    fn queue_parent(&self) -> VQueueParent {
        VQueueParent::from_raw(self.inner.queue_parent)
    }

    fn queue_instance(&self) -> VQueueInstance {
        VQueueInstance::from_raw(self.inner.queue_instance)
    }

    fn vqueue_id(&self) -> VQueueId {
        VQueueId::new(
            self.queue_parent(),
            self.partition_key,
            self.queue_instance(),
        )
    }

    fn current_entry_card(&self) -> EntryCard {
        EntryCard {
            priority: self.inner.effective_priority,
            visible_at: self.inner.visible_at,
            created_at: self.inner.created_at,
            kind: self.kind,
            id: self.id,
        }
    }
}

pub struct OwnedEntryState<E> {
    pub(crate) header: OwnedHeader,
    pub(crate) state: E,
}

impl<E> AsEntryStateHeader for OwnedEntryState<E> {
    fn kind(&self) -> EntryKind {
        self.header.kind()
    }

    fn stage(&self) -> Stage {
        self.header.stage()
    }

    fn queue_parent(&self) -> VQueueParent {
        self.header.queue_parent()
    }

    fn queue_instance(&self) -> VQueueInstance {
        self.header.queue_instance()
    }

    fn vqueue_id(&self) -> VQueueId {
        self.header.vqueue_id()
    }

    fn current_entry_card(&self) -> EntryCard {
        self.header.current_entry_card()
    }
}

impl<E> AsEntryState for OwnedEntryState<E> {
    type State = E;

    fn state(&self) -> &Self::State {
        &self.state
    }
}

// pub struct Borrowed<'a, E> {
//     pub(crate) partition_key: PartitionKey,
//     pub(crate) kind: EntryKind,
//     pub(crate) id: EntryId,
//
//     pub(crate) inner: State<E>,
//     // pins the underlying rocksdb slice as long as this struct is alive
//     pub(crate) _pinned: DBPinnableSlice<'a>,
// }
//
// impl<'a, E> AsEntryStateHeader for Borrowed<'a, E> {
//     fn kind(&self) -> EntryKind {
//         self.kind
//     }
//
//     fn stage(&self) -> Stage {
//         self.inner.stage
//     }
//
//     fn queue_parent(&self) -> VQueueParent {
//         VQueueParent::from_raw(self.inner.queue_parent)
//     }
//
//     fn queue_instance(&self) -> VQueueInstance {
//         VQueueInstance::from_raw(self.inner.queue_instance)
//     }
//
//     fn vqueue_id(&self) -> VQueueId {
//         VQueueId::new(
//             self.queue_parent(),
//             self.partition_key,
//             self.queue_instance(),
//         )
//     }
//
//     fn current_entry_card(&self) -> EntryCard {
//         EntryCard {
//             priority: self.inner.effective_priority,
//             visible_at: self.inner.visible_at,
//             created_at: self.inner.created_at,
//             kind: self.kind,
//             id: self.id,
//         }
//     }
// }
//
//
// impl<'a, E> AsEntryState for Borrowed<'a, E> {
//     type State = E;
//
//     fn state(&self) -> &Self::State {
//         &self.inner.extras
//     }
// }
//
// impl<'a> ReadVQueueEntryState for PartitionStoreTransaction<'a> {
//
//     async fn get_entry_state<E>(
//         &mut self,
//         partition_key: PartitionKey,
//         id: &EntryId,
//     ) -> Result<Option<impl AsEntryState + 'static>>
//     where
//         E: EntryStateKind + bilrost::OwnedMessage + Sized + 'static,
//         State<E>: bilrost::OwnedMessage + Sized + Send,
//     {
//         use super::super::invocation_table::MetaKey;
//         let mut key_buffer = [0u8; MetaKey::serialized_length_fixed()];
//         MetaKey {
//             partition_key,
//             invocation_uuid: *id,
//         }
//         .serialize_to(&mut key_buffer.as_mut());
//         let Some(raw_value) = self.get(MetaKey::TABLE, key_buffer)? else {
//             return Ok(None);
//         };
//
//         let slice = raw_value;
//         let decoded = State::<E>::decode(&mut slice.as_ref())?;
//         Ok(Some(Owned {
//             partition_key,
//             kind: EntryKind::Invocation,
//             id: *id,
//             inner: decoded,
//         }))
//     }
// }
//
// impl WriteVQueueEntryState for PartitionStoreTransaction<'_> {
//     fn create_vqueue_entry_state<E>(
//         &mut self,
//         at: UniqueTimestamp,
//         qid: &VQueueId,
//         card: &EntryCard,
//         stage: Stage,
//         // visible_at: VisibleAt,
//         // priority: EffectivePriority,
//         entry_state: E,
//     ) where
//         E: EntryStateKind + bilrost::Message + bilrost::encoding::RawMessage,
//         State<E>: bilrost::Message + bilrost::encoding::RawMessage,
//     {
//         use super::super::invocation_table::MetaKey;
//         let key_buffer = MetaKey {
//             partition_key: qid.partition_key,
//             invocation_uuid: card.id.clone(),
//         }
//         .to_bytes();
//
//         let entry = State {
//             stage: 1,
//             queue_parent: qid.parent.as_u16(),
//             queue_instance: qid.instance.as_u32(),
//             initial_visible_at: card.visible_at,
//             latest_visible_at: card.visible_at,
//             effective_priority: card.priority,
//             created_at: at,
//             entry_state,
//         };
//
//         let value_buf = {
//             let value_buf = self.cleared_value_buffer_mut(entry.encoded_len());
//             // unwrap is safe because we know the buffer is big enough.
//             entry.encode(value_buf).unwrap();
//             value_buf.split()
//         };
//
//         self.raw_put_cf(KeyKind::ResourceInvocation, key_buffer, value_buf);
//     }
// }
