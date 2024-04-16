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
use restate_types::identifiers::InvocationId;
use restate_types::invocation::{
    ComponentType, HandlerType, InvocationTarget, ServiceInvocationSpanContext, SpanRelation,
};
use restate_types::journal::enriched::{
    AwakeableEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry, InvokeEnrichmentResult,
};
use restate_types::journal::raw::{
    EntryHeader, PlainEntryHeader, PlainRawEntry, RawEntry, RawEntryCodec,
};
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

        let invocation_target = match self
            .schemas
            .resolve_latest_invocation_target(&request.service_name, &request.method_name)
        {
            Some(meta) => match meta.component_ty {
                ComponentType::Service => {
                    InvocationTarget::service(request.service_name, request.method_name)
                }
                ComponentType::VirtualObject => InvocationTarget::virtual_object(
                    request.service_name.clone(),
                    ByteString::try_from(request.key.clone().into_bytes()).map_err(|e| {
                        InvocationError::from(anyhow!(
                            "The request key is not a valid UTF-8 string: {e}"
                        ))
                    })?,
                    request.method_name,
                    meta.handler_ty,
                ),
            },
            None => {
                return Err(InvocationError::component_handler_not_found(
                    &request.service_name,
                    &request.method_name,
                ))
            }
        };

        let invocation_id = InvocationId::generate(&invocation_target);

        // Create the span context
        let span_context = ServiceInvocationSpanContext::start(&invocation_id, span_relation);

        Ok(InvokeEnrichmentResult {
            invocation_id,
            invocation_target,
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
        entry: PlainRawEntry,
        current_invocation_target: &InvocationTarget,
        current_invocation_span_context: &ServiceInvocationSpanContext,
    ) -> Result<EnrichedRawEntry, InvocationError> {
        let (header, serialized_entry) = entry.into_inner();

        let enriched_header = match header {
            PlainEntryHeader::Input {} => EnrichedEntryHeader::Input {},
            PlainEntryHeader::Output {} => EnrichedEntryHeader::Output {},
            PlainEntryHeader::GetState { is_completed } => {
                can_read_state(
                    &header.as_entry_type(),
                    &current_invocation_target.service_ty(),
                )?;
                EnrichedEntryHeader::GetState { is_completed }
            }
            PlainEntryHeader::SetState {} => {
                can_write_state(
                    &header.as_entry_type(),
                    &current_invocation_target.service_ty(),
                    current_invocation_target.handler_ty(),
                )?;
                EnrichedEntryHeader::SetState {}
            }
            PlainEntryHeader::ClearState {} => {
                can_write_state(
                    &header.as_entry_type(),
                    &current_invocation_target.service_ty(),
                    current_invocation_target.handler_ty(),
                )?;
                EnrichedEntryHeader::ClearState {}
            }
            PlainEntryHeader::GetStateKeys { is_completed } => {
                can_read_state(
                    &header.as_entry_type(),
                    &current_invocation_target.service_ty(),
                )?;
                EnrichedEntryHeader::GetStateKeys { is_completed }
            }
            PlainEntryHeader::ClearAllState => {
                can_write_state(
                    &header.as_entry_type(),
                    &current_invocation_target.service_ty(),
                    current_invocation_target.handler_ty(),
                )?;
                EnrichedEntryHeader::ClearAllState {}
            }
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
                        current_invocation_span_context.as_parent(),
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
                    current_invocation_span_context.as_linked(),
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
            EntryHeader::SideEffect { .. } => EnrichedEntryHeader::SideEffect {},
            PlainEntryHeader::Custom { code } => EnrichedEntryHeader::Custom { code },
        };

        Ok(RawEntry::new(enriched_header, serialized_entry))
    }
}

#[inline]
fn can_read_state(
    entry_type: &EntryType,
    service_type: &ComponentType,
) -> Result<(), InvocationError> {
    if !service_type.has_state() {
        return Err(InvocationError::new(
            codes::BAD_REQUEST,
            format!(
                "The service type {} does not have state and, therefore, does not support the entry type {}",
                service_type, entry_type
            ),
        ));
    }
    Ok(())
}

#[inline]
fn can_write_state(
    entry_type: &EntryType,
    service_type: &ComponentType,
    handler_type: Option<HandlerType>,
) -> Result<(), InvocationError> {
    can_read_state(entry_type, service_type)?;
    if handler_type != Some(HandlerType::Exclusive) {
        return Err(InvocationError::new(
            codes::BAD_REQUEST,
            format!(
                "The service type {} with handler type {:?} has no exclusive state access and, therefore, does not support the entry type {}",
                service_type, handler_type, entry_type
            ),
        ));
    }
    Ok(())
}
