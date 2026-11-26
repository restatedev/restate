// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwapOption;
use tokio::sync::Notify;

use crate::walk::{PublishedDriverState, WalkStats};

use super::PublishedRegistry;

/// The registry state of a physical partition that all its sessions and
/// handles share: the published registry, what the walk drivers did and why
/// they wait, and the request for the indexes over the vqueue entries. Lives as
/// long as the physical partition, across reopenings of it.
#[derive(Default)]
pub(crate) struct SharedRegistry {
    /// The published registry, with the indexes maintained on entry writes, as
    /// reconciled when the partition was opened and replaced whole by each
    /// commit that changes it. `None` until first installed.
    published: ArcSwapOption<PublishedRegistry>,
    walk_stats: WalkStats,
    driver_state: PublishedDriverState,
    /// Whether the indexes over the vqueue entries wait to be registered, as the
    /// processor asks once the partition has fully migrated to vqueues; and the
    /// wake-up of the walk driver that registers them.
    vqueue_indexes_requested: AtomicBool,
    vqueue_indexes_wakeup: Notify,
}

impl SharedRegistry {
    /// The published registry, once installed; see [`Self::publish`].
    pub(crate) fn published(&self) -> Option<Arc<PublishedRegistry>> {
        self.published.load_full()
    }

    /// Publishes `registry` to every session and handle of the partition.
    pub(crate) fn publish(&self, registry: Arc<PublishedRegistry>) {
        self.published.store(Some(registry));
    }

    /// What the background walk drivers of the partition did.
    pub(crate) fn walk_stats(&self) -> &WalkStats {
        &self.walk_stats
    }

    /// Why the walks of the partition's running driver aren't progressing.
    pub(crate) fn driver_state(&self) -> &PublishedDriverState {
        &self.driver_state
    }

    /// See [`crate::PartitionDb::request_vqueue_indexes`].
    pub(crate) fn request_vqueue_indexes(&self) {
        self.vqueue_indexes_requested.store(true, Ordering::Release);
        self.vqueue_indexes_wakeup.notify_one();
    }

    /// Whether the indexes over the vqueue entries were asked for and not yet
    /// taken.
    pub(crate) fn vqueue_indexes_requested(&self) -> bool {
        self.vqueue_indexes_requested.load(Ordering::Acquire)
    }

    /// Takes the request for the indexes over the vqueue entries, if any.
    pub(crate) fn take_vqueue_indexes_request(&self) -> bool {
        self.vqueue_indexes_requested.swap(false, Ordering::AcqRel)
    }

    /// Resolves once the indexes over the vqueue entries are asked for, or
    /// right away if they were asked for since the last wait. Check
    /// [`Self::vqueue_indexes_requested`] too: a wake-up may be stale.
    pub(crate) async fn vqueue_indexes_wakeup(&self) {
        self.vqueue_indexes_wakeup.notified().await;
    }
}
