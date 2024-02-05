// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: Remove after fleshing the code out.
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::metadata::Logs;
use crate::options::Options;
use crate::types::Version;
use crate::watchdog::WatchdogSender;
use crate::{create_static_metadata, Error};

/// Bifrost is Restate's durable interconnect system
///
/// Bifrost is a mutable-friendly handle to access the system. You don't need
/// to wrap this in an Arc or a lock, pass it around by reference or by a clone.
/// Bifrost handle is relatively cheap to clone.
#[derive(Clone)]
pub struct Bifrost {
    inner: Arc<BifrostInner>,
}

impl Bifrost {
    pub(crate) fn new(inner: Arc<BifrostInner>) -> Self {
        Self { inner }
    }

    /// The version of the currently loaded metadata
    pub fn metadata_version(&self) -> Version {
        self.inner.log_metadata.lock().unwrap().version
    }

    #[cfg(test)]
    pub fn inner(&self) -> Arc<BifrostInner> {
        self.inner.clone()
    }
}

// compile-time check
static_assertions::assert_impl_all!(Bifrost: Send, Sync, Clone);

// Locks in this data-structure are held for very short time and should never be
// held across an async boundary.
pub struct BifrostInner {
    opts: Options,
    num_partitions: u64,
    watchdog: WatchdogSender,
    log_metadata: Mutex<Logs>,
    shutting_down: AtomicBool,
}

impl BifrostInner {
    pub fn new(opts: Options, watchdog: WatchdogSender, num_partitions: u64) -> Self {
        Self {
            opts,
            num_partitions,
            watchdog,
            log_metadata: Mutex::new(Logs::empty()),
            shutting_down: AtomicBool::new(false),
        }
    }

    /// Indicates that an ongoing shutdown/drain is in progress. New writes and
    /// reads will be rejected during shutdown, but in-flight operations are
    /// allowed to complete.
    pub fn set_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Relaxed);
    }

    #[inline]
    fn fail_if_shutting_down(&self) -> Result<(), Error> {
        if self.shutting_down.load(Ordering::Relaxed) {
            Err(Error::Shutdown)
        } else {
            Ok(())
        }
    }

    /// Immediately fetch new metadata from metadata store and update the local copy
    ///
    /// NOTE: The current version assumes static metadata map!
    pub async fn sync_metadata(&self) -> Result<(), Error> {
        self.fail_if_shutting_down()?;

        let logs = create_static_metadata(&self.opts, self.num_partitions);
        let mut guard = self.log_metadata.lock().unwrap();
        if logs.version > guard.version {
            *guard = logs;
        }
        Ok(())
    }
}
