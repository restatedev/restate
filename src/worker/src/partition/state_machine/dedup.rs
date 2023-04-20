use crate::partition::ack::AckMode;
use crate::partition::effects::Effects;
use crate::partition::state_machine::{Error, StateMachine};
use crate::partition::storage::Transaction;
use crate::partition::AckCommand;
use restate_common::types::{ServiceInvocationId, SpanRelation};
use restate_journal::raw::RawEntryCodec;

#[derive(Debug)]
pub(crate) struct DeduplicatingStateMachine<Codec> {
    state_machine: StateMachine<Codec>,
}

impl<Codec> DeduplicatingStateMachine<Codec> {
    pub(crate) fn new(state_machine: StateMachine<Codec>) -> Self {
        DeduplicatingStateMachine { state_machine }
    }
}

impl<Codec> DeduplicatingStateMachine<Codec>
where
    Codec: RawEntryCodec,
{
    pub(crate) async fn on_apply<TransactionType: restate_storage_api::Transaction>(
        &mut self,
        command: AckCommand,
        effects: &mut Effects,
        transaction: &mut Transaction<TransactionType>,
    ) -> Result<(Option<ServiceInvocationId>, SpanRelation), Error> {
        let (fsm_command, ack_mode) = command.into_inner();

        match ack_mode {
            AckMode::Ack(ack_target) => {
                effects.send_ack_response(ack_target.acknowledge());
            }
            AckMode::Dedup {
                producer_id,
                ack_target,
            } => {
                if let Some(dedup_seq_number) =
                    transaction.load_dedup_seq_number(producer_id).await?
                {
                    if ack_target.dedup_seq_number() <= dedup_seq_number {
                        effects.send_ack_response(ack_target.duplicate(dedup_seq_number));
                        return Ok((None, SpanRelation::None));
                    }
                }

                transaction
                    .store_dedup_seq_number(producer_id, ack_target.dedup_seq_number())
                    .await;
                effects.send_ack_response(ack_target.acknowledge());
            }
            AckMode::None => {}
        }

        self.state_machine
            .on_apply(fsm_command, effects, transaction)
            .await
    }
}
