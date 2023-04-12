use crate::partition::leadership::ActuatorOutput;
use crate::partition::types::{
    EnrichedEntryHeader, EnrichedRawEntry, InvokerEffect, InvokerEffectKind, TimerValue,
};
use crate::partition::{AckableCommand, Command, TimerOutput};
use crate::util::IdentitySender;
use assert2::let_assert;
use bytes::Bytes;
use common::types::{
    InvocationId, RawEntry, ResolutionResult, ServiceInvocationSpanContext, SpanRelation,
};
use journal::raw::{PlainRawEntry, RawEntryCodec, RawEntryHeader};
use journal::InvokeRequest;
use journal::{BackgroundInvokeEntry, CompletionResult, Entry, InvokeEntry};
use opentelemetry_api::trace::SpanContext;
use std::marker::PhantomData;
use std::sync::Arc;

/// Responsible for enriching and then proposing [`ActuatorOutput`].
pub(super) struct ActuatorOutputHandler<KeyExtractor, Codec> {
    proposal_tx: IdentitySender<AckableCommand>,
    key_extractor: KeyExtractor,

    _codec: PhantomData<Codec>,
}

impl<KeyExtractor, Codec> ActuatorOutputHandler<KeyExtractor, Codec>
where
    KeyExtractor: service_key_extractor::KeyExtractor,
    Codec: RawEntryCodec,
{
    pub(super) fn new(
        proposal_tx: IdentitySender<AckableCommand>,
        key_extractor: KeyExtractor,
    ) -> Self {
        Self {
            proposal_tx,
            key_extractor,
            _codec: Default::default(),
        }
    }

    pub(super) async fn handle(&self, actuator_output: ActuatorOutput) {
        match actuator_output {
            ActuatorOutput::Invoker(invoker_output) => {
                let invoker_effect = self.map_invoker_output_into_effect(invoker_output);

                // Err only if the consensus module is shutting down
                let _ = self
                    .proposal_tx
                    .send(AckableCommand::no_ack(Command::Invoker(invoker_effect)))
                    .await;
            }
            ActuatorOutput::Shuffle(outbox_truncation) => {
                // Err only if the consensus module is shutting down
                let _ = self
                    .proposal_tx
                    .send(AckableCommand::no_ack(Command::OutboxTruncation(
                        outbox_truncation.index(),
                    )))
                    .await;
            }
            ActuatorOutput::Timer(timer_output) => {
                let timer_effect = self.map_timer_output_into_effect(timer_output);

                // Err only if the consensus module is shutting down
                let _ = self
                    .proposal_tx
                    .send(AckableCommand::no_ack(Command::Timer(timer_effect)))
                    .await;
            }
        };
    }

    fn map_invoker_output_into_effect(
        &self,
        invoker_output: invoker::OutputEffect,
    ) -> InvokerEffect {
        let invoker::OutputEffect {
            service_invocation_id,
            kind,
        } = invoker_output;

        InvokerEffect::new(service_invocation_id, self.map_kind_into_effect_kind(kind))
    }

    fn map_kind_into_effect_kind(&self, kind: invoker::Kind) -> InvokerEffectKind {
        match kind {
            invoker::Kind::JournalEntry {
                entry_index,
                entry,
                parent_span_context,
            } => InvokerEffectKind::JournalEntry {
                entry_index,
                entry: self.enrich_journal_entry(entry, parent_span_context),
            },
            invoker::Kind::Suspended {
                waiting_for_completed_entries,
            } => InvokerEffectKind::Suspended {
                waiting_for_completed_entries,
            },
            invoker::Kind::End => InvokerEffectKind::End,
            invoker::Kind::Failed { error_code, error } => {
                InvokerEffectKind::Failed { error_code, error }
            }
        }
    }

    fn enrich_journal_entry(
        &self,
        mut raw_entry: PlainRawEntry,
        parent_span_context: Arc<SpanContext>,
    ) -> EnrichedRawEntry {
        let enriched_header = match raw_entry.header {
            RawEntryHeader::PollInputStream { is_completed } => {
                EnrichedEntryHeader::PollInputStream { is_completed }
            }
            RawEntryHeader::OutputStream => EnrichedEntryHeader::OutputStream,
            RawEntryHeader::GetState { is_completed } => {
                EnrichedEntryHeader::GetState { is_completed }
            }
            RawEntryHeader::SetState => EnrichedEntryHeader::SetState,
            RawEntryHeader::ClearState => EnrichedEntryHeader::ClearState,
            RawEntryHeader::Sleep { is_completed } => EnrichedEntryHeader::Sleep { is_completed },
            RawEntryHeader::Invoke { is_completed } => {
                if !is_completed {
                    let resolution_result = self.resolve_service_invocation_target(
                        &raw_entry,
                        |entry| {
                            let_assert!(Entry::Invoke(InvokeEntry { request, .. }) = entry);
                            request
                        },
                        SpanRelation::Parent(parent_span_context.as_ref().clone()),
                    );

                    // complete journal entry in case that we could not resolve the service invocation target
                    let is_completed = if let ResolutionResult::Failure {
                        error_code,
                        ref error,
                    } = resolution_result
                    {
                        Codec::write_completion(
                            &mut raw_entry,
                            CompletionResult::Failure(error_code, error.clone()),
                        ).expect("Failed completing a journal entry. This is a bug. Please contact the Restate developers.");
                        true
                    } else {
                        is_completed
                    };

                    EnrichedEntryHeader::Invoke {
                        is_completed,
                        resolution_result: Some(resolution_result),
                    }
                } else {
                    // No need to service resolution if the entry was completed by the service endpoint
                    EnrichedEntryHeader::Invoke {
                        is_completed,
                        resolution_result: None,
                    }
                }
            }
            RawEntryHeader::BackgroundInvoke => {
                let resolution_result = self.resolve_service_invocation_target(
                    &raw_entry,
                    |entry| {
                        let_assert!(
                            Entry::BackgroundInvoke(BackgroundInvokeEntry(request)) = entry
                        );
                        request
                    },
                    SpanRelation::CausedBy(parent_span_context.as_ref().clone()),
                );

                EnrichedEntryHeader::BackgroundInvoke { resolution_result }
            }
            RawEntryHeader::Awakeable { is_completed } => {
                EnrichedEntryHeader::Awakeable { is_completed }
            }
            RawEntryHeader::CompleteAwakeable => EnrichedEntryHeader::CompleteAwakeable,
            RawEntryHeader::Custom { code, requires_ack } => {
                EnrichedEntryHeader::Custom { code, requires_ack }
            }
        };

        RawEntry::new(enriched_header, raw_entry.entry)
    }

    fn resolve_service_invocation_target(
        &self,
        raw_entry: &PlainRawEntry,
        request_extractor: impl Fn(Entry) -> InvokeRequest,
        span_relation: SpanRelation,
    ) -> ResolutionResult {
        let entry = Codec::deserialize(raw_entry);

        if let Err(err) = entry {
            return ResolutionResult::Failure {
                error_code: 13,
                error: err.to_string().into(),
            };
        }

        let entry = entry.unwrap();

        let request = request_extractor(entry);

        let service_key = self.extract_service_key(
            &request.service_name,
            &request.method_name,
            request.parameter,
        );

        match service_key {
            Ok(service_key) => {
                let invocation_id = InvocationId::now_v7();

                // Create the span context
                let (span_context, span) = ServiceInvocationSpanContext::start(
                    &request.service_name,
                    &request.method_name,
                    &service_key,
                    invocation_id,
                    span_relation,
                );

                // Enter the span to commit it
                let _ = span.enter();

                ResolutionResult::Success {
                    invocation_id,
                    service_key,
                    span_context,
                }
            }
            Err(service_key_extractor::Error::NotFound) => ResolutionResult::Failure {
                error_code: 5,
                error: format!(
                    "{}/{} not found",
                    request.service_name, &request.method_name
                )
                .into(),
            },
            Err(err) => ResolutionResult::Failure {
                error_code: 13,
                error: err.to_string().into(),
            },
        }
    }

    fn extract_service_key(
        &self,
        service_name: impl AsRef<str>,
        service_method: impl AsRef<str>,
        payload: Bytes,
    ) -> Result<Bytes, service_key_extractor::Error> {
        self.key_extractor
            .extract(service_name, service_method, payload)
    }

    fn map_timer_output_into_effect(&self, timer_output: TimerOutput) -> TimerValue {
        match timer_output {
            TimerOutput::TimerFired(timer) => timer,
        }
    }
}
