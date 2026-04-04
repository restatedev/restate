// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use restate_clock::UniqueTimestamp;
use restate_types::identifiers::{InvocationId, PartitionKey};
use restate_types::{LockName, Scope};
use restate_util_string::ReString;

use crate::Result;

/// Used for introspection purposes
#[derive(Debug, Clone, bilrost::Message)]
pub struct LockState {
    #[bilrost(tag(1))]
    pub acquired_at: UniqueTimestamp,
    #[bilrost(tag(2))]
    pub acquired_by: AcquiredBy,
}

#[derive(Debug, Clone, bilrost::Oneof, bilrost::Message)]
pub enum AcquiredBy {
    #[bilrost(empty)]
    Empty,
    #[bilrost(tag(1))]
    Other(ReString),
    #[bilrost(tag(2))]
    InvocationId(InvocationId),
}

pub trait LoadLocks {
    /// Iterate over all locks without deserializing their state as we only care about
    /// their presence.
    fn scan_all_locked(&self, on_item: impl FnMut(Option<Scope>, LockName)) -> Result<()>;
}

pub trait ScanLocksTable {
    /// Used for data-fusion queries
    fn for_each_lock<
        F: FnMut((PartitionKey, Option<Scope>, LockName, LockState)) -> std::ops::ControlFlow<()>
            + Send
            + Sync
            + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}

/// The Lock table will fully replace the ServiceStatus table after a migration process
/// that will be triggered when vqueues are enabled.
///
/// The differences between Lock table and service status are:
/// - Supports scoped locks
/// - Removes the lock from the database when the lock is released instead of leaving a
///   sentinel value.
/// - Can allow future named locks that are not bound to service names.
/// - Fully managed by the vqueues' scheduler which keeps a cache of active locks in memory.
pub trait WriteLockTable {
    fn acquire_lock(&mut self, scope: &Option<Scope>, lock_name: &LockName, state: &LockState);
    fn release_lock(&mut self, scope: &Option<Scope>, lock_name: &LockName);
}
