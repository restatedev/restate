// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BufMut};

use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::InvocationId;
use restate_types::vqueue::{
    EffectivePriority, NewEntryPriority, VQueueId, VQueueInstance, VQueueParent,
};

use crate::StorageError;

use super::{Stage, VisibleAt};

thread_local! {
    // arbitrary seeds, safe to change since we don't use hashes in storage
    static HASHER: ahash::RandomState = const { ahash::RandomState::with_seeds(1232134512, 14, 82334, 988889) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, strum::FromRepr)]
#[repr(u8)]
pub enum EntryKind {
    Unknown = 0,
    Invocation = b'i',    // 0x69
    StateMutation = b's', // 0x73
}

// Using u128 would have added an extra unnecessary 8 bytes to due to alignment
// requirements (u128 is 0x10 aligned and it forces the struct to be 0x10 aligned)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntryId([u8; 16]);

impl EntryId {
    #[inline]
    pub const fn new(id: [u8; 16]) -> Self {
        Self(id)
    }

    #[inline]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    #[inline]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }

    #[inline]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

impl From<&InvocationId> for EntryId {
    #[inline]
    fn from(id: &InvocationId) -> Self {
        Self::from_bytes(id.invocation_uuid().to_bytes())
    }
}

impl From<InvocationId> for EntryId {
    #[inline]
    fn from(id: InvocationId) -> Self {
        Self::from_bytes(id.invocation_uuid().to_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntryCard {
    pub priority: EffectivePriority,
    pub visible_at: VisibleAt,
    /// The unique timestamp of the initial creation of the entry.
    pub created_at: UniqueTimestamp,
    pub kind: EntryKind,
    pub id: EntryId,
}

impl EntryCard {
    pub const fn serialized_length() -> usize {
        // priority
        std::mem::size_of::<EffectivePriority>()
        // visible at
        + std::mem::size_of::<VisibleAt>()
        // created_at
        + std::mem::size_of::<UniqueTimestamp>()
        // entry kind
        + std::mem::size_of::<EntryKind>()
        // entry id
        + std::mem::size_of::<EntryId>()
    }

    /// A unique hash of the entry card.
    ///
    /// Do not use this for any stored data as it changes across version/restarts.
    #[inline(always)]
    pub fn unique_hash(&self) -> u64 {
        HASHER.with(|hasher| hasher.hash_one(self))
    }

    pub fn new(
        priority: NewEntryPriority,
        visible_at: VisibleAt,
        created_at: UniqueTimestamp,
        kind: EntryKind,
        id: EntryId,
    ) -> Self {
        Self {
            priority: EffectivePriority::from(priority),
            visible_at,
            created_at,
            kind,
            id,
        }
    }

    fn from_raw(
        priority: EffectivePriority,
        visible_at: VisibleAt,
        created_at: UniqueTimestamp,
        kind: EntryKind,
        id: EntryId,
    ) -> Self {
        Self {
            priority,
            visible_at,
            created_at,
            kind,
            id,
        }
    }

    pub fn encode<B: BufMut>(&self, target: &mut B) {
        target.put_u8(self.priority as u8);
        target.put_u64(self.visible_at.as_u64());
        target.put_u64(self.created_at.as_u64());
        target.put_u8(self.kind as u8);
        target.put_slice(&self.id.0);
    }

    pub fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        if source.remaining() < Self::serialized_length() {
            return Err(StorageError::Generic(anyhow::anyhow!(
                "Not enough bytes to decode an EntryCard"
            )));
        }

        let p = source.get_u8();
        let priority = EffectivePriority::from_repr(p).ok_or_else(|| {
            StorageError::Conversion(anyhow::anyhow!("Wrong value for EffectivePriority: {p}"))
        })?;
        let visible_at = VisibleAt::from_raw(source.get_u64());
        let created_at = UniqueTimestamp::try_from(source.get_u64())
            .map_err(|e| StorageError::Conversion(e.into()))?;
        let k = source.get_u8();
        let kind = EntryKind::from_repr(k).ok_or_else(|| {
            StorageError::Generic(anyhow::anyhow!("Wrong value for EntryKind: {k}"))
        })?;

        let mut buf = [0u8; 16];
        source.copy_to_slice(&mut buf);
        let entry_id = EntryId::from_bytes(buf);

        Ok(Self {
            priority,
            visible_at,
            created_at,
            kind,
            id: entry_id,
        })
    }
}

pub trait AsEntryStateHeader {
    fn kind(&self) -> EntryKind;
    fn stage(&self) -> Stage;
    fn queue_parent(&self) -> VQueueParent;
    fn queue_instance(&self) -> VQueueInstance;
    fn vqueue_id(&self) -> VQueueId;
    fn current_entry_card(&self) -> EntryCard;
}

pub trait AsEntryState: AsEntryStateHeader {
    type State;

    fn state(&self) -> &Self::State;
}

pub trait EntryStateKind: Send {
    const KIND: EntryKind;
}

impl EntryStateKind for () {
    const KIND: EntryKind = EntryKind::Unknown;
}
