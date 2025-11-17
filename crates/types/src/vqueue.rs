// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZero;

use crate::identifiers::PartitionKey;

/// Queue parent identifies which configuration to use for a particular vqueue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct VQueueParent(u32);

impl VQueueParent {
    #[inline]
    pub const fn from_raw(raw: u32) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum VQueueInstance {
    /// The default instance is used when the queue is not sharded or when the shard
    /// is not defined.
    #[default]
    Default,
    Specific(NonZero<u32>),
}

impl VQueueInstance {
    // cannot be const because map_or is not const in stable rust yet.
    #[inline]
    pub fn from_raw(raw: u32) -> Self {
        NonZero::new(raw).map_or(VQueueInstance::Default, VQueueInstance::Specific)
    }

    #[inline]
    pub const fn as_u32(self) -> u32 {
        match self {
            Self::Default => 0,
            Self::Specific(n) => n.get(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VQueueId {
    // Identifies the configuration/parent of the vqueue
    pub parent: VQueueParent,
    // Key+Instance identify the individual queue.
    pub partition_key: PartitionKey,
    pub instance: VQueueInstance,
}

impl VQueueId {
    #[inline]
    pub fn new(
        parent: VQueueParent,
        partition_key: PartitionKey,
        instance: VQueueInstance,
    ) -> Self {
        Self {
            parent,
            partition_key,
            instance,
        }
    }
}

// needed when using hashbrown's entry_ref API to convert the key reference to a value
// lazily when inserting into the map.
impl From<&VQueueId> for VQueueId {
    fn from(value: &VQueueId) -> Self {
        *value
    }
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    Ord,
    PartialOrd,
    PartialEq,
    Eq,
    Hash,
    bilrost::Enumeration,
    strum::FromRepr,
)]
#[repr(u8)]
pub enum EffectivePriority {
    /// Exclusively for wake-ups that hold tokens already. All other wake-ups will
    /// continue to run with their original priority.
    ///
    /// This is crucial to ensure that when we release our token back to the pool that it gets
    /// picked up again by the scheduler and we can re-acquire it.
    TokenHeld = 0, // Resuming with held concurrency token
    /// High priority
    Started = 1, // Resuming (started before) with no concurrency token
    /// System high priority (new)
    System = 2,
    /// User-defined high-priority
    UserHigh = 3,
    /// User-defined low priority
    #[default]
    UserDefault = 4,
}

impl EffectivePriority {
    pub const NUM_PRIORITIES: usize = 5;

    /// Whether this entry has never been started or not
    pub fn is_new(&self) -> bool {
        *self >= EffectivePriority::System
    }

    pub fn token_held(&self) -> bool {
        *self == EffectivePriority::TokenHeld
    }

    pub fn has_started(&self) -> bool {
        *self <= EffectivePriority::Started
    }
}

/// Priorities for entries in the vqueue when inserting new entries
#[derive(Debug, Default, Clone, Copy, Ord, PartialOrd, PartialEq, Eq)]
#[repr(u8)]
pub enum NewEntryPriority {
    /// System high priority
    System = 2,
    /// Default priority
    UserHigh = 3,
    #[default]
    UserDefault = 4,
}

impl From<NewEntryPriority> for EffectivePriority {
    #[inline(always)]
    fn from(value: NewEntryPriority) -> Self {
        match value {
            NewEntryPriority::System => EffectivePriority::System,
            NewEntryPriority::UserHigh => EffectivePriority::UserHigh,
            NewEntryPriority::UserDefault => EffectivePriority::UserDefault,
        }
    }
}
