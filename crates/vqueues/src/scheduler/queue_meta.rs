// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::counter;

use restate_limiter::LimitKey;
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_storage_api::vqueue_table::{EntryKey, EntryValue};
use restate_types::{LockName, Scope};
use restate_util_string::ReString;

use crate::metric_definitions::VQUEUE_ENQUEUE;

/// A smaller version of the vqueue metadata that only contains the information needed for the
/// scheduler to operate.
#[derive(Debug, Clone)]
pub struct VQueueMetaLite {
    /// if true, the vqueue is paused, we don't pop entries from it until it's resumed.
    queue_is_paused: bool,

    /// the length of the inbox stage in the vqueue
    num_waiting: usize,
    scope: Option<Scope>,
    limit_key: LimitKey<ReString>,
    lock_name: Option<LockName>,
}

impl VQueueMetaLite {
    pub fn new(original: &VQueueMeta) -> Self {
        Self {
            queue_is_paused: original.queue_is_paused(),
            num_waiting: original.total_waiting() as usize,
            scope: original.scope().clone(),
            limit_key: original.limit_key().clone(),
            lock_name: original.lock_name().cloned(),
        }
    }

    pub const fn new_empty(
        scope: Option<Scope>,
        limit_key: LimitKey<ReString>,
        lock_name: Option<LockName>,
    ) -> Self {
        Self {
            queue_is_paused: false,
            num_waiting: 0,
            scope,
            limit_key,
            lock_name,
        }
    }

    pub fn inbox_len(&self) -> usize {
        self.num_waiting
    }

    pub fn is_inbox_empty(&self) -> bool {
        self.num_waiting == 0
    }

    pub fn is_queue_paused(&self) -> bool {
        self.queue_is_paused
    }

    pub fn scope(&self) -> &Option<Scope> {
        &self.scope
    }

    pub fn lock_name(&self) -> Option<&LockName> {
        match self.lock_name {
            Some(ref lock_name) => Some(lock_name),
            _ => None,
        }
    }

    // pub fn service_name(&self) -> Option<&ServiceName> {
    //     match self.link {
    //         VQueueLink::Service(ref service_name) => Some(service_name),
    //         VQueueLink::Lock(ref lock) => Some(lock.service_name()),
    //         _ => None,
    //     }
    // }

    pub fn limit_key(&self) -> &LimitKey<ReString> {
        &self.limit_key
    }

    pub fn apply_update(&mut self, update: &MetaLiteUpdate) {
        match update {
            MetaLiteUpdate::QueuePaused => self.queue_is_paused = true,
            MetaLiteUpdate::QueueResumed { num_waiting } => {
                self.queue_is_paused = false;
                self.num_waiting = *num_waiting as usize;
            }
            MetaLiteUpdate::EnqueuedToInbox { .. } => {
                counter!(VQUEUE_ENQUEUE).increment(1);
                self.num_waiting += 1;
            }
            MetaLiteUpdate::RemovedFromInbox(_) => {
                self.num_waiting = self.num_waiting.saturating_sub(1)
            }
        }
    }
}

#[derive(Debug)]
pub enum MetaLiteUpdate {
    QueuePaused,
    QueueResumed { num_waiting: u64 },
    RemovedFromInbox(EntryKey),
    EnqueuedToInbox { key: EntryKey, value: EntryValue },
}
