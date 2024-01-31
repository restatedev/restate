// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tracing::debug;

use crate::metadata::{Chain, LogletConfig, Logs};
use crate::options::Options;
use crate::types::Version;
use crate::watchdog::{WatchdogCommand, WatchdogSender};
use crate::{Error, LogId};

/// Bifrost is Restate's durable interconnect system
///
/// Bifrost is a cheaply cloneable handle to access the system. You don't need
/// to wrap this in an Arc or a lock, pass it around by reference or by a clone.
#[derive(Clone)]
pub struct Bifrost {
    inner: Arc<BifrostInner>,
    watchdog: WatchdogSender,
}

impl Bifrost {
    pub(crate) fn new(inner: Arc<BifrostInner>, watchdog: WatchdogSender) -> Self {
        Self { inner, watchdog }
    }

    /// The version of the currently loaded metadata
    pub fn metadata_version(&self) -> Version {
        self.inner.logs.lock().unwrap().version
    }

    /// Asks bifrost to sync it's metadata whenever possible.
    pub fn schedule_metadata_update(&self) {
        self.watchdog
            .send(WatchdogCommand::ScheduleMetadataSync)
            .unwrap_or_else(|_| debug!("Didn't schedule metatdata update, shutting down"))
    }
}

// compile-time check
static_assertions::assert_impl_all!(Bifrost: Send, Sync, Clone);

// Locks in this data-structure are held for very short time and should never be
// held across an async boundary.
pub(crate) struct BifrostInner {
    opts: Options,
    num_partitions: u64,
    logs: Mutex<Logs>,
    shutting_down: AtomicBool,
}

impl BifrostInner {
    pub fn new(opts: Options, num_partitions: u64) -> Self {
        Self {
            opts,
            num_partitions,
            logs: Mutex::new(Logs::empty()),
            shutting_down: AtomicBool::new(false),
        }
    }

    /// Indicates that an ongoingn shutdown/drain is in progress. New writes and
    /// reads will be rejected during shutdown, but in-flight operations are
    /// allowed to complete.
    pub fn set_shutdown(&self) {
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Immediately fetch new metadata from metadata store and update the local copy
    ///
    /// The current version assumes as static metadata map until the metadata store
    /// is implemented.
    pub async fn sync_metadata(&self) -> Result<(), Error> {
        if self.shutting_down.load(Ordering::Relaxed) || self.logs.lock().unwrap().is_initialized()
        {
            // No-op.
            return Ok(());
        }
        // Get metadata from somewhere
        let mut log_chain: HashMap<LogId, Chain> =
            HashMap::with_capacity(self.num_partitions as usize);
        // fixed config (temporary)
        let config = LogletConfig::new(
            self.opts.default_loglet,
            self.opts.default_loglet_params.clone(),
        );

        // pre-fill with all possible logs up to `num_partitions`
        (0..self.num_partitions).for_each(|i| {
            log_chain.insert(LogId::from(i), Chain::new(config.clone()));
        });

        let logs = Logs::new(Version::from(1), log_chain);
        let mut guard = self.logs.lock().unwrap();
        *guard = logs;
        Ok(())
    }
}
