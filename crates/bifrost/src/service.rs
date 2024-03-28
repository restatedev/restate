// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use anyhow::Context;
use restate_core::{task_center, TaskKind};

use crate::bifrost::BifrostInner;
use crate::options::Options;
use crate::watchdog::Watchdog;
use crate::Bifrost;

pub struct BifrostService {
    inner: Arc<BifrostInner>,
    bifrost: Bifrost,
    watchdog: Watchdog,
}

impl BifrostService {
    pub fn new(opts: Options) -> Self {
        let (watchdog_sender, watchdog_receiver) = tokio::sync::mpsc::unbounded_channel();
        let inner = Arc::new(BifrostInner::new(opts, watchdog_sender));
        let bifrost = Bifrost::new(inner.clone());
        let watchdog = Watchdog::new(inner.clone(), watchdog_receiver);
        Self {
            inner,
            bifrost,
            watchdog,
        }
    }

    pub fn handle(&self) -> Bifrost {
        self.bifrost.clone()
    }
    /// Runs initialization phase, then returns a handle to join on shutdown.
    /// In this phase the system should wait until this is completed before
    /// continuing. For instance, a worker mark itself as `STARTING_UP` and not
    /// accept any requests until this is completed.
    ///
    /// This requires to run within a task_center context.
    pub async fn start(self) -> anyhow::Result<()> {
        // Perform an initial metadata sync.
        self.inner
            .sync_metadata()
            .await
            .context("Initial bifrost metadata sync has failed!")?;

        // we spawn the watchdog as a task to ensure cancellation safety if the outer
        // future was dropped in the select loop.
        task_center().spawn(
            TaskKind::BifrostBackgroundHighPriority,
            "bifrost-watchdog",
            None,
            self.watchdog.run(),
        )?;

        // Bifrost started!
        Ok(())
    }
}
