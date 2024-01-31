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

use tracing::{debug, info};

use crate::bifrost::BifrostInner;
use crate::Error;

pub type WatchdogSender = tokio::sync::mpsc::UnboundedSender<WatchdogCommand>;
type WatchdogReceiver = tokio::sync::mpsc::UnboundedReceiver<WatchdogCommand>;

/// The watchdog is a task manager for background jobs that needs to run on bifrost.
/// tasks managed by the watchdogs are cooperative and should not be terminated abruptly.
///
/// Tasks are expected to check for the cancellation token when appropriate and finalize their
/// work before termination.
pub struct Watchdog {
    inner: Arc<BifrostInner>,
    inbound: WatchdogReceiver,
}

impl Watchdog {
    pub fn new(inner: Arc<BifrostInner>, inbound: WatchdogReceiver) -> Self {
        Self { inner, inbound }
    }

    fn handle_command(&mut self, cmd: WatchdogCommand) {
        match cmd {
            WatchdogCommand::ScheduleMetadataSync => {
                // TODO: Convert to a background task
                //let _ = self.inner.sync_metadata().await;
            }
        }
    }

    pub async fn run(mut self, drain: drain::Watch) -> Result<(), Error> {
        let shutdown = drain.signaled();
        tokio::pin!(shutdown);
        info!("Bifrost watchdog started");

        loop {
            tokio::select! {
            biased;
            _ = &mut shutdown => {
                info!("Bifrost watchdog shutdown started");
                // Stop accepting new commands
                self.inner.set_shutdown();
                self.inbound.close();
                debug!("Draining bifrost tasks");

                // Consume buffered commands
                let mut i = 0;
                while let Some(cmd) = self.inbound.recv().await {
                    i += 1;
                    self.handle_command(cmd)
                }
                debug!("Bifrost drained {i} commands due to an ongoing shutdown");
                // Ask all tasks to shutdown
                // TODO
                break;
            }
            Some(cmd) = self.inbound.recv() => {
                self.handle_command(cmd)
            }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum WatchdogCommand {
    /// Request to sync metadata if the client believes that it's outdated.
    #[allow(dead_code)]
    ScheduleMetadataSync,
}
