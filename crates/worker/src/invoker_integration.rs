// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use assert2::let_assert;
use bytes::Bytes;
use bytestring::ByteString;
use restate_service_protocol::awakeable_id::AwakeableIdentifier;
use restate_types::errors::{codes, InvocationError};
use restate_types::identifiers::{InvocationId, InvocationUuid, ServiceId, WithPartitionKey};
use restate_types::invocation::{
    ComponentType, InvocationTarget, ServiceInvocationSpanContext, SpanRelation,
};
use restate_types::journal::enriched::{
    AwakeableEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry, InvokeEnrichmentResult,
};
use restate_types::journal::raw::{PlainEntryHeader, PlainRawEntry, RawEntry, RawEntryCodec};
use restate_types::journal::{BackgroundInvokeEntry, CompleteAwakeableEntry, Entry, InvokeEntry};
use restate_types::journal::{EntryType, InvokeRequest};
use std::marker::PhantomData;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub(super) struct EntryEnricher<Schemas, Codec> {
    schemas: Schemas,

    _codec: PhantomData<Codec>,
}

impl<Schemas, Codec> EntryEnricher<Schemas, Codec> {
    pub(super) fn new(schemas: Schemas) -> Self {
        Self {
            schemas,
            _codec: Default::default(),
        }
    }
}

impl<Schemas, Codec> EntryEnricher<Schemas, Codec>
where
    Schemas: restate_schema_api::invocation_target::InvocationTargetResolver,
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

        let (service_id, invocation_target) = match self
            .schemas
            .resolve_latest_invocation_target(&request.service_name, &request.method_name)
        {
            Some(meta) => match meta.component_ty {
                ComponentType::Service => (
                    ServiceId::unkeyed(request.service_name.clone()),
                    InvocationTarget::service(request.service_name, request.method_name),
                ),
                ComponentType::VirtualObject => (
                    ServiceId::new(request.service_name.clone(), request.key.as_bytes().clone()),
                    InvocationTarget::virtual_object(
                        request.service_name.clone(),
                        ByteString::try_from(request.key.clone().into_bytes()).map_err(|e| {
                            InvocationError::from(anyhow!(
                                "The request key is not a valid UTF-8 string: {e}"
                            ))
                        })?,
                        request.method_name,
                        meta.handler_ty,
                    ),
                ),
            },
            None => {
                return Err(InvocationError::component_handler_not_found(
                    &request.service_name,
                    &request.method_name,
                ))
            }
        };

        let invocation_uuid = InvocationUuid::new();
        let invocation_id = InvocationId::from_parts(service_id.partition_key(), invocation_uuid);

        // Create the span context
        let span_context = ServiceInvocationSpanContext::start(&invocation_id, span_relation);

        Ok(InvokeEnrichmentResult {
            invocation_id,
            invocation_target,
            service_key: service_id.key,
            span_context,
        })
    }
}

impl<Schemas, Codec> restate_invoker_api::EntryEnricher for EntryEnricher<Schemas, Codec>
where
    Schemas: restate_schema_api::invocation_target::InvocationTargetResolver,
    Codec: RawEntryCodec,
{
    fn enrich_entry(
        &self,
        raw_entry: PlainRawEntry,
        invocation_span_context: &ServiceInvocationSpanContext,
    ) -> Result<EnrichedRawEntry, InvocationError> {
        let (header, serialized_entry) = raw_entry.into_inner();
        let enriched_header = match header {
            PlainEntryHeader::Input {} => EnrichedEntryHeader::Input {},
            PlainEntryHeader::Output {} => EnrichedEntryHeader::Output {},
            PlainEntryHeader::GetState { is_completed } => {
                EnrichedEntryHeader::GetState { is_completed }
            }
            PlainEntryHeader::SetState {} => EnrichedEntryHeader::SetState {},
            PlainEntryHeader::ClearState {} => EnrichedEntryHeader::ClearState {},
            PlainEntryHeader::GetStateKeys { is_completed } => {
                EnrichedEntryHeader::GetStateKeys { is_completed }
            }
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
                            codes::BAD_REQUEST,
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
