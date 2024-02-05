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

use tokio::task::JoinHandle;
use tracing::info;

use crate::bifrost::BifrostInner;
use crate::options::Options;
use crate::watchdog::Watchdog;
use crate::{Bifrost, Error};

pub struct BifrostService {
    inner: Arc<BifrostInner>,
    bifrost: Bifrost,
    watchdog: Watchdog,
}

impl BifrostService {
    pub fn new(opts: Options, num_partitions: u64) -> Self {
        let (watchdog_sender, watchdog_receiver) = tokio::sync::mpsc::unbounded_channel();
        let inner = Arc::new(BifrostInner::new(opts, watchdog_sender, num_partitions));
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
    pub async fn start(self, drain: drain::Watch) -> Result<JoinHandle<Result<(), Error>>, Error> {
        // Perform an initial metadata sync.
        self.inner.sync_metadata().await?;

        // we spawn the watchdog as a task to ensure cancellation safety if the outer
        // future was dropped in the select loop.
        let join_handle = tokio::spawn({
            async {
                self.watchdog.run(drain).await?;
                info!("Bifrost watchdog shutdown complete");
                Ok(())
            }
        });
        Ok(join_handle)
    }
}
