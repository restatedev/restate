use super::leadership::ActuatorOutput;
use super::{AckCommand, Command};
use crate::util::IdentitySender;

/// Responsible for enriching and then proposing [`ActuatorOutput`].
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
        };
    }
}
