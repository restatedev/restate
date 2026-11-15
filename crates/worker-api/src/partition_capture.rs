// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A **partition capture** is what one part of a query plan reads of one
//! partition: a storage snapshot and the processor's own live state (scheduler
//! and user-limit rows), taken by the processor between two of its committed
//! turns, and the invoker's status rows, each only if that part asks for it.
//!
//! The sources within one admitted part of the plan share its capture; other
//! parts of the same query get their own. For example, `sys_invocation` joins
//! stored invocation statuses with the invoker's status: within that join, both
//! come from one capture. The storage snapshot and the processor's live state
//! come from the same point between turns. The invoker's rows are read on their
//! own, after the snapshot: they aren't atomic with it, and can be from any
//! moment of the invoker.

use std::sync::Arc;

use restate_partition_store::ReadSnapshot;
use restate_types::identifiers::PartitionId;

use crate::invoker::InvocationStatusReport;
use crate::{SchedulerStatusEntry, UserLimitCounterEntry};

/// What a capture includes: the union of what the query's sources for one
/// partition need.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PartitionCaptureRequirements {
    /// A storage snapshot.
    pub storage: bool,
    /// The invoker's status rows.
    pub invoker: bool,
    /// The scheduler's rows.
    pub scheduler: bool,
    /// The user-limit counters.
    pub user_limits: bool,
}

impl PartitionCaptureRequirements {
    /// Adds what `other` needs.
    pub fn include(&mut self, other: Self) {
        self.storage |= other.storage;
        self.invoker |= other.invoker;
        self.scheduler |= other.scheduler;
        self.user_limits |= other.user_limits;
    }

    /// Whether any live state is needed, which only the partition's leader has.
    pub fn needs_live_state(self) -> bool {
        self.invoker || self.scheduler || self.user_limits
    }
}

/// The live state of a capture. `None` means not requested; `Some([])` means
/// requested, available, and empty. The invoker's rows aren't atomic with the
/// processor's: the invoker runs on its own.
#[derive(Debug, Default, Clone)]
pub struct CapturedPartitionState {
    pub invoker: Option<Arc<[InvocationStatusReport]>>,
    pub scheduler: Option<Arc<[SchedulerStatusEntry]>>,
    pub user_limits: Option<Arc<[UserLimitCounterEntry]>>,
}

impl CapturedPartitionState {
    /// Whether every requested live source is present.
    pub fn satisfies(&self, requirements: PartitionCaptureRequirements) -> bool {
        (!requirements.invoker || self.invoker.is_some())
            && (!requirements.scheduler || self.scheduler.is_some())
            && (!requirements.user_limits || self.user_limits.is_some())
    }

    /// The live state of a partition read offline, from a restored snapshot: no
    /// rows, but every requested source present.
    pub fn empty(requirements: PartitionCaptureRequirements) -> Self {
        Self {
            invoker: requirements.invoker.then(Arc::default),
            scheduler: requirements.scheduler.then(Arc::default),
            user_limits: requirements.user_limits.then(Arc::default),
        }
    }
}

/// One partition's capture, shared by the sources of one admitted part of a
/// query plan.
#[derive(Debug, Clone)]
pub struct CapturedPartitionRead {
    pub partition: PartitionId,
    pub storage: Option<ReadSnapshot>,
    pub live: CapturedPartitionState,
}
