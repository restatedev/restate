// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Weak;
use std::time::Duration;

use tokio::time::Instant;
use tracing::{debug, trace};

use restate_core::network::TransportConnect;
use restate_types::replicated_loglet::ReplicatedLogletId;

use crate::loglet::{Loglet, OperationError};
use crate::providers::replicated_loglet::loglet::ReplicatedLoglet;

pub struct PeriodicTailChecker {}

impl PeriodicTailChecker {
    pub async fn run<T: TransportConnect>(
        loglet_id: ReplicatedLogletId,
        loglet: Weak<ReplicatedLoglet<T>>,
        duration: Duration,
    ) -> anyhow::Result<()> {
        debug!(
            %loglet_id,
            "Starting a background periodic tail checker for this loglet",
        );
        // Optimization. Don't run the check if the tail/seal has been updated recently.
        // Unfortunately this requires a litte bit more setup in the TailOffsetWatch so we don't do
        // it.
        loop {
            let Some(loglet) = loglet.upgrade() else {
                trace!(
                    %loglet_id,
                    "Loglet has been dropped, stopping periodic tail checker",
                );
                return Ok(());
            };
            trace!(
                %loglet_id,
                is_sequencer = ?loglet.is_sequencer_local(),
                "Checking tail status for loglet",
            );
            if loglet.known_global_tail().is_sealed() {
                // stop the task. we are sealed already.
                trace!(
                    %loglet_id,
                    is_sequencer = ?loglet.is_sequencer_local(),
                    "Loglet has been sealed, stopping the periodic tail checker",
                );
                return Ok(());
            }
            tokio::time::sleep(duration).await;
            let start = Instant::now();
            match loglet.find_tail().await {
                Ok(tail) => {
                    // todo: maybe remove this.
                    trace!(
                        %loglet_id,
                        known_global_tail = %tail.offset(),
                        is_sequencer = ?loglet.is_sequencer_local(),
                        is_sealed = ?tail.is_sealed(),
                        "Successfully determined the tail status of the loglet, took={:?}",
                        start.elapsed(),
                    );
                }
                Err(OperationError::Shutdown(_)) => {
                    trace!(
                        %loglet_id,
                        is_sequencer = ?loglet.is_sequencer_local(),
                        "System is shutting down, stopping period tail checker",
                    );
                    return Ok(());
                }
                Err(OperationError::Other(e)) => {
                    trace!(
                        ?e,
                        is_sequencer = ?loglet.is_sequencer_local(),
                        %loglet_id,
                        "Couldn't determine the tail status of the loglet. Will retry in the next period",
                    );
                }
            }
        }
    }
}
