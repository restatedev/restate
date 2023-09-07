// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::leadership::ActuatorOutput;
use super::{AckCommand, Command};
use crate::util::IdentitySender;

/// Responsible for proposing [ActuatorOutput].
pub(super) struct ActuatorOutputHandler {
    proposal_tx: IdentitySender<AckCommand>,
}

impl ActuatorOutputHandler {
    pub(super) fn new(proposal_tx: IdentitySender<AckCommand>) -> Self {
        Self { proposal_tx }
    }

    pub(super) async fn handle(&self, actuator_output: ActuatorOutput) {
        match actuator_output {
            ActuatorOutput::Invoker(invoker_output) => {
                // Err only if the consensus module is shutting down
                let _ = self
                    .proposal_tx
                    .send(AckCommand::no_ack(Command::Invoker(invoker_output)))
                    .await;
            }
            ActuatorOutput::Shuffle(outbox_truncation) => {
                // Err only if the consensus module is shutting down
                let _ = self
                    .proposal_tx
                    .send(AckCommand::no_ack(Command::OutboxTruncation(
                        outbox_truncation.index(),
                    )))
                    .await;
            }
            ActuatorOutput::Timer(timer) => {
                // Err only if the consensus module is shutting down
                let _ = self
                    .proposal_tx
                    .send(AckCommand::no_ack(Command::Timer(timer)))
                    .await;
            }
            ActuatorOutput::BuiltInInvoker(invoker_output) => {
                // Err only if the consensus module is shutting down
                let _ = self
                    .proposal_tx
                    .send(AckCommand::no_ack(Command::BuiltInInvoker(invoker_output)))
                    .await;
            }
        };
    }
}
