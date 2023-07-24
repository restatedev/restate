use assert2::let_assert;
use restate_types::identifiers::{InvocationId, ServiceInvocationId};
use restate_types::invocation::{ServiceInvocationSpanContext, SpanRelation};
use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry, ResolutionResult};
use restate_types::journal::raw::{PlainRawEntry, RawEntry, RawEntryCodec, RawEntryHeader};
use restate_types::journal::InvokeRequest;
use restate_types::journal::{BackgroundInvokeEntry, Entry, InvokeEntry};
use std::marker::PhantomData;

#[derive(Debug, Clone)]
pub(super) struct EntryEnricher<KeyExtractor, Codec> {
    key_extractor: KeyExtractor,

    _codec: PhantomData<Codec>,
}

impl<KeyExtractor, Codec> EntryEnricher<KeyExtractor, Codec> {
    pub(super) fn new(key_extractor: KeyExtractor) -> Self {
        Self {
            key_extractor,
            _codec: Default::default(),
        }
    }
}

impl<KeyExtractor, Codec> EntryEnricher<KeyExtractor, Codec>
where
    KeyExtractor: restate_schema_api::key::KeyExtractor,
    Codec: RawEntryCodec,
{
    fn resolve_service_invocation_target(
        &self,
        raw_entry: &PlainRawEntry,
        request_extractor: impl Fn(Entry) -> InvokeRequest,
        span_relation: SpanRelation,
    ) -> Result<ResolutionResult, anyhow::Error> {
        let entry = Codec::deserialize(raw_entry)?;
        let request = request_extractor(entry);

        let service_key = self.key_extractor.extract(
            &request.service_name,
            &request.method_name,
            request.parameter,
        )?;

        let invocation_id = InvocationId::now_v7();

        // Create the span context
        let span_context = ServiceInvocationSpanContext::start(
            &ServiceInvocationId::new(request.service_name, service_key.clone(), invocation_id),
            span_relation,
        );

        Ok(ResolutionResult {
            invocation_id,
            service_key,
            span_context,
        })
    }
}

impl<KeyExtractor, Codec> restate_invoker_api::EntryEnricher for EntryEnricher<KeyExtractor, Codec>
where
    KeyExtractor: restate_schema_api::key::KeyExtractor,
    Codec: RawEntryCodec,
{
    fn enrich_entry(
        &self,
        raw_entry: PlainRawEntry,
        invocation_span_context: &ServiceInvocationSpanContext,
    ) -> Result<EnrichedRawEntry, anyhow::Error> {
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
                        invocation_span_context.as_parent(),
                    )?;

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
                            Entry::BackgroundInvoke(BackgroundInvokeEntry { request, .. }) = entry
                        );
                        request
                    },
                    invocation_span_context.as_linked(),
                )?;

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

        Ok(RawEntry::new(enriched_header, raw_entry.entry))
    }
}
