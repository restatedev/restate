// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use assert2::let_assert;
use bytes::Bytes;
use restate_schema_api::key::extraction;
use restate_service_protocol::awakeable_id::AwakeableIdentifier;
use restate_types::errors::{InvocationError, UserErrorCode};
use restate_types::identifiers::{FullInvocationId, InvocationUuid};
use restate_types::invocation::{ServiceInvocationSpanContext, SpanRelation};
use restate_types::journal::enriched::{
    AwakeableEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry, InvokeEnrichmentResult,
};
use restate_types::journal::raw::{PlainEntryHeader, PlainRawEntry, RawEntry, RawEntryCodec};
use restate_types::journal::{BackgroundInvokeEntry, CompleteAwakeableEntry, Entry, InvokeEntry};
use restate_types::journal::{EntryType, InvokeRequest};
use std::marker::PhantomData;
use std::str::FromStr;

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
        entry_type: EntryType,
        serialized_entry: &Bytes,
        request_extractor: impl Fn(Entry) -> InvokeRequest,
        span_relation: SpanRelation,
    ) -> Result<InvokeEnrichmentResult, InvocationError> {
        let entry = Codec::deserialize(entry_type, serialized_entry.clone())
            .map_err(InvocationError::internal)?;
        let request = request_extractor(entry);

        let service_key = match self.key_extractor.extract(
            &request.service_name,
            &request.method_name,
            request.parameter,
        ) {
            Ok(k) => k,
            Err(extraction::Error::NotFound) => {
                return Err(InvocationError::service_method_not_found(
                    &request.service_name,
                    &request.method_name,
                ))
            }
            Err(e) => return Err(InvocationError::internal(e)),
        };

        let invocation_id = InvocationUuid::new();

        // Create the span context
        let span_context = ServiceInvocationSpanContext::start(
            &FullInvocationId::new(
                request.service_name.clone(),
                service_key.clone(),
                invocation_id,
            ),
            span_relation,
        );

        Ok(InvokeEnrichmentResult {
            invocation_uuid: invocation_id,
            service_key,
            service_name: request.service_name,
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
    ) -> Result<EnrichedRawEntry, InvocationError> {
        let (header, serialized_entry) = raw_entry.into_inner();
        let enriched_header = match header {
            PlainEntryHeader::PollInputStream { is_completed } => {
                EnrichedEntryHeader::PollInputStream { is_completed }
            }
            PlainEntryHeader::OutputStream {} => EnrichedEntryHeader::OutputStream {},
            PlainEntryHeader::GetState { is_completed } => {
                EnrichedEntryHeader::GetState { is_completed }
            }
            PlainEntryHeader::SetState {} => EnrichedEntryHeader::SetState {},
            PlainEntryHeader::ClearState {} => EnrichedEntryHeader::ClearState {},
            PlainEntryHeader::ClearAllState => EnrichedEntryHeader::ClearAllState {},
            PlainEntryHeader::Sleep { is_completed } => EnrichedEntryHeader::Sleep { is_completed },
            PlainEntryHeader::Invoke { is_completed, .. } => {
                if !is_completed {
                    let enrichment_result = self.resolve_service_invocation_target(
                        header.as_entry_type(),
                        &serialized_entry,
                        |entry| {
                            let_assert!(Entry::Invoke(InvokeEntry { request, .. }) = entry);
                            request
                        },
                        invocation_span_context.as_parent(),
                    )?;

                    EnrichedEntryHeader::Invoke {
                        is_completed,
                        enrichment_result: Some(enrichment_result),
                    }
                } else {
                    // No need to service resolution if the entry was completed by the deployment
                    EnrichedEntryHeader::Invoke {
                        is_completed,
                        enrichment_result: None,
                    }
                }
            }
            PlainEntryHeader::BackgroundInvoke { .. } => {
                let enrichment_result = self.resolve_service_invocation_target(
                    header.as_entry_type(),
                    &serialized_entry,
                    |entry| {
                        let_assert!(
                            Entry::BackgroundInvoke(BackgroundInvokeEntry { request, .. }) = entry
                        );
                        request
                    },
                    invocation_span_context.as_linked(),
                )?;

                EnrichedEntryHeader::BackgroundInvoke { enrichment_result }
            }
            PlainEntryHeader::Awakeable { is_completed } => {
                EnrichedEntryHeader::Awakeable { is_completed }
            }
            PlainEntryHeader::CompleteAwakeable { .. } => {
                let entry =
                    Codec::deserialize(EntryType::CompleteAwakeable, serialized_entry.clone())
                        .map_err(InvocationError::internal)?;
                let_assert!(Entry::CompleteAwakeable(CompleteAwakeableEntry { id, .. }) = entry);

                let (invocation_id, entry_index) = AwakeableIdentifier::from_str(&id)
                    .map_err(|e| {
                        InvocationError::new(
                            UserErrorCode::InvalidArgument,
                            format!("Invalid awakeable identifier: {}", e),
                        )
                    })?
                    .into_inner();

                EnrichedEntryHeader::CompleteAwakeable {
                    enrichment_result: AwakeableEnrichmentResult {
                        invocation_id,
                        entry_index,
                    },
                }
            }
            PlainEntryHeader::Custom { code } => EnrichedEntryHeader::Custom { code },
        };

        Ok(RawEntry::new(enriched_header, serialized_entry))
    }
}
