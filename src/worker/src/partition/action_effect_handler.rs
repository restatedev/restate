// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::leadership::ActionEffect;
use crate::partition::services::non_deterministic::Effects as NBISEffects;
use crate::partition::state_machine::{AckCommand, Command};
use crate::util::IdentitySender;

/// Responsible for proposing [ActionEffect].
pub(super) struct ActionEffectHandler {
    proposal_tx: IdentitySender<AckCommand>,
}

impl ActionEffectHandler {
    pub(super) fn new(proposal_tx: IdentitySender<AckCommand>) -> Self {
        Self { proposal_tx }
    }

    pub(super) async fn handle(&self, actuator_output: ActionEffect) {
        match actuator_output {
            ActionEffect::Invoker(invoker_output) => {
                // Err only if the consensus module is shutting down
                let _ = self
                    .proposal_tx
                    .send(AckCommand::no_ack(Command::Invoker(invoker_output)))
                    .await;
            }
            ActionEffect::Shuffle(outbox_truncation) => {
                // Err only if the consensus module is shutting down
                let _ = self
                    .proposal_tx
                    .send(AckCommand::no_ack(Command::OutboxTruncation(
                        outbox_truncation.index(),
                    )))
                    .await;
            }
            ActionEffect::Timer(timer) => {
                // Err only if the consensus module is shutting down
                let _ = self
                    .proposal_tx
                    .send(AckCommand::no_ack(Command::Timer(timer)))
                    .await;
            }
            ActionEffect::BuiltInInvoker(invoker_output) => {
                // TODO Super BAD code to mitigate https://github.com/restatedev/restate/issues/851
                //  until we properly fix it.
                //  By proposing the effects one by one we avoid the read your own writes issue,
                //  because for each proposal the state machine goes through a transaction commit,
                //  to make sure the next command can see the effects of the previous one.
                //  A problematic example case is a sequence of CreateVirtualJournal and AppendJournalEntry:
                //  to append a journal entry we must have stored the JournalMetadata first.
                let (fid, effects) = invoker_output.into_inner();
                for effect in effects {
                    // Err only if the consensus module is shutting down
                    let _ = self
                        .proposal_tx
                        .send(AckCommand::no_ack(Command::BuiltInInvoker(
                            NBISEffects::new(fid.clone(), vec![effect]),
                        )))
                        .await;
                }
            }
        };
    }
}
