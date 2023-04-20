use crate::partition::effects::Effects;
use crate::partition::state_machine::{Error, StateMachine};
use crate::partition::storage::Transaction;
use crate::partition::AckableCommand;
use restate_common::types::{ServiceInvocationId, SpanRelation};
use restate_journal::raw::RawEntryCodec;

#[derive(Debug)]
pub(crate) struct DedupLayer<Codec> {
    state_machine: StateMachine<Codec>,
}

impl<Codec> DedupLayer<Codec>
where
    Codec: RawEntryCodec,
{
    pub(super) fn new(state_machine: StateMachine<Codec>) -> Self {
        DedupLayer { state_machine }
    }

    pub(super) async fn on_apply<TransactionType: restate_storage_api::Transaction>(
        &mut self,
        command: AckableCommand,
        effects: &mut Effects,
        transaction: &mut Transaction<TransactionType>,
    ) -> Result<(Option<ServiceInvocationId>, SpanRelation), Error> {
        let (fsm_command, ack_target) = command.into_inner();

        if let Some(ack_target) = ack_target {
            effects.send_ack_response(ack_target.acknowledge());
        }

        self.state_machine
            .on_apply(fsm_command, effects, transaction)
            .await
    }
}
