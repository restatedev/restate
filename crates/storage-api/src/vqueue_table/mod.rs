// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod entry;
pub mod metadata;
pub mod scheduler;
mod store;
mod tables;

pub use entry::*;
pub use restate_types::vqueues::{EntryId, EntryKind};
pub use store::*;
pub use tables::*;

/// A marker trait for types that can be used as entry state values.
pub trait EntryState {}

/// Some stats that are collected from the scheduler. Emitted along the item
/// at decision time.
#[derive(Debug, Clone, Default, bilrost::Message)]
pub struct WaitStats {
    /// Total milliseconds the item spent waiting on global invoker capacity
    #[bilrost(tag(1))]
    pub blocked_on_global_capacity_ms: u32,
    /// Total milliseconds the item was throttled on vqueue's "start" token bucket
    #[bilrost(tag(2))]
    pub vqueue_start_throttling_ms: u32,
    /// Total milliseconds the item was throttled on global "run" token bucket
    #[bilrost(tag(3))]
    pub global_invoker_throttling_ms: u32,
    /// Total milliseconds the item spent waiting on invoker memory pool
    #[bilrost(tag(4))]
    pub blocked_on_invoker_memory_ms: u32,
}
