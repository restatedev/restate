// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::ops::Deref;
use std::str::FromStr;

use anyhow::anyhow;
use assert2::let_assert;
use bytes::Bytes;
use bytestring::ByteString;

use restate_types::errors::{InvocationError, codes};
use restate_types::identifiers::{AwakeableIdentifier, ExternalSignalIdentifier, InvocationId};
use restate_types::invocation::{
    InvocationTarget, InvocationTargetType, ServiceInvocationSpanContext, ServiceType, SpanRelation,
};
use restate_types::journal::enriched::{
    AwakeableEnrichmentResult, CallEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry,
};
use restate_types::journal::raw::{PlainEntryHeader, PlainRawEntry, RawEntry, RawEntryCodec};
use restate_types::journal::{
    AttachInvocationEntry, AttachInvocationTarget, CancelInvocationEntry, CancelInvocationTarget,
    CompleteAwakeableEntry, Entry, GetInvocationOutputEntry, InvokeEntry, OneWayCallEntry,
};
use restate_types::journal::{EntryType, InvokeRequest};
use restate_types::journal_v2::SignalId;
use restate_types::live::Live;
use restate_types::schema::invocation_target::{DeploymentStatus, InvocationTargetResolver};

#[derive(Clone)]
pub(super) struct EntryEnricher<Schemas, Codec> {
    schemas: Live<Schemas>,

    _codec: PhantomData<Codec>,
}

impl<Schemas, Codec> EntryEnricher<Schemas, Codec> {
    pub(super) fn new(schemas: Live<Schemas>) -> Self {
        Self {
            schemas,
            _codec: Default::default(),
        }
    }
}

impl<Schemas, Codec> EntryEnricher<Schemas, Codec>
where
    Schemas: InvocationTargetResolver,
    Codec: RawEntryCodec,
{
    fn resolve_service_invocation_target(
        &mut self,
        entry_type: EntryType,
        serialized_entry: &Bytes,
        request_extractor: impl Fn(Entry) -> InvokeRequest,
        span_relation: SpanRelation,
    ) -> Result<CallEnrichmentResult, InvocationError> {
        let entry = Codec::deserialize(entry_type, serialized_entry.clone())
            .map_err(|e| InvocationError::internal(e.to_string()))?;
        let request = request_extractor(entry);

        let meta = self
            .schemas
            .live_load()
            .resolve_latest_invocation_target(&request.service_name, &request.handler_name)
            .ok_or_else(|| {
                InvocationError::service_handler_not_found(
                    &request.service_name,
                    &request.handler_name,
                )
            })?;
        if let DeploymentStatus::Deprecated(dp_id) = meta.deployment_status {
            return Err(InvocationError::new(
                codes::INTERNAL,
                format!(
                    "The service {} is exposed by the deprecated deployment {dp_id}. Upgrade the SDK used by {} and register a new deployment.",
                    request.service_name, request.service_name
                ),
            ));
        }

        let invocation_target = match meta.target_ty {
            InvocationTargetType::Service => {
                InvocationTarget::service(request.service_name, request.handler_name)
            }
            InvocationTargetType::VirtualObject(h_ty) => InvocationTarget::virtual_object(
                request.service_name.clone(),
                ByteString::try_from(request.key.clone().into_bytes()).map_err(|e| {
                    InvocationError::from(anyhow!(
                        "The request key is not a valid UTF-8 string: {e}"
                    ))
                })?,
                request.handler_name,
                h_ty,
            ),
            InvocationTargetType::Workflow(h_ty) => InvocationTarget::workflow(
                request.service_name.clone(),
                ByteString::try_from(request.key.clone().into_bytes()).map_err(|e| {
                    InvocationError::from(anyhow!(
                        "The request key is not a valid UTF-8 string: {e}"
                    ))
                })?,
                request.handler_name,
                h_ty,
            ),
        };

        let idempotency_key = if let Some(idempotency_key) = &request.idempotency_key {
            if idempotency_key.is_empty() {
                return Err(InvocationError::from(anyhow!(
                    "The provided idempotency key is empty"
                )));
            }
            Some(idempotency_key.deref())
        } else {
            None
        };
        let invocation_id = InvocationId::generate(&invocation_target, idempotency_key);

        // Create the span context
        let span_context = ServiceInvocationSpanContext::start(&invocation_id, span_relation);

        let completion_retention_duration = meta
            .compute_retention(idempotency_key.is_some())
            .completion_retention;

        Ok(CallEnrichmentResult {
            invocation_id,
            invocation_target,
            completion_retention_time: if completion_retention_duration.is_zero() {
                None
            } else {
                Some(completion_retention_duration)
            },
            span_context,
        })
    }
}

impl<Schemas, Codec> restate_invoker_api::EntryEnricher for EntryEnricher<Schemas, Codec>
where
    Schemas: InvocationTargetResolver,
    Codec: RawEntryCodec,
{
    fn enrich_entry(
        &mut self,
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
                    &current_invocation_target.invocation_target_ty(),
                )?;
                EnrichedEntryHeader::GetState { is_completed }
            }
            PlainEntryHeader::SetState {} => {
                can_write_state(
                    &header.as_entry_type(),
                    &current_invocation_target.invocation_target_ty(),
                )?;
                EnrichedEntryHeader::SetState {}
            }
            PlainEntryHeader::ClearState {} => {
                can_write_state(
                    &header.as_entry_type(),
                    &current_invocation_target.invocation_target_ty(),
                )?;
                EnrichedEntryHeader::ClearState {}
            }
            PlainEntryHeader::GetStateKeys { is_completed } => {
                can_read_state(
                    &header.as_entry_type(),
                    &current_invocation_target.invocation_target_ty(),
                )?;
                EnrichedEntryHeader::GetStateKeys { is_completed }
            }
            PlainEntryHeader::ClearAllState => {
                can_write_state(
                    &header.as_entry_type(),
                    &current_invocation_target.invocation_target_ty(),
                )?;
                EnrichedEntryHeader::ClearAllState {}
            }
            PlainEntryHeader::GetPromise { is_completed } => {
                check_workflow_type(
                    &header.as_entry_type(),
                    &current_invocation_target.service_ty(),
                )?;
                EnrichedEntryHeader::GetPromise { is_completed }
            }
            PlainEntryHeader::PeekPromise { is_completed } => {
                check_workflow_type(
                    &header.as_entry_type(),
                    &current_invocation_target.service_ty(),
                )?;
                EnrichedEntryHeader::PeekPromise { is_completed }
            }
            PlainEntryHeader::CompletePromise { is_completed } => {
                check_workflow_type(
                    &header.as_entry_type(),
                    &current_invocation_target.service_ty(),
                )?;
                EnrichedEntryHeader::CompletePromise { is_completed }
            }
            PlainEntryHeader::Sleep { is_completed } => EnrichedEntryHeader::Sleep { is_completed },
            PlainEntryHeader::Call { is_completed, .. } => {
                if !is_completed {
                    let enrichment_result = self.resolve_service_invocation_target(
                        header.as_entry_type(),
                        &serialized_entry,
                        |entry| {
                            let_assert!(Entry::Call(InvokeEntry { request, .. }) = entry);
                            request
                        },
                        current_invocation_span_context.as_parent(),
                    )?;

                    EnrichedEntryHeader::Call {
                        is_completed,
                        enrichment_result: Some(enrichment_result),
                    }
                } else {
                    // No need to service resolution if the entry was completed by the deployment
                    EnrichedEntryHeader::Call {
                        is_completed,
                        enrichment_result: None,
                    }
                }
            }
            PlainEntryHeader::OneWayCall { .. } => {
                let enrichment_result = self.resolve_service_invocation_target(
                    header.as_entry_type(),
                    &serialized_entry,
                    |entry| {
                        let_assert!(Entry::OneWayCall(OneWayCallEntry { request, .. }) = entry);
                        request
                    },
                    current_invocation_span_context.as_linked(),
                )?;

                EnrichedEntryHeader::OneWayCall { enrichment_result }
            }
            PlainEntryHeader::Awakeable { is_completed } => {
                EnrichedEntryHeader::Awakeable { is_completed }
            }
            PlainEntryHeader::CompleteAwakeable { .. } => {
                let entry =
                    Codec::deserialize(EntryType::CompleteAwakeable, serialized_entry.clone())
                        .map_err(|e| InvocationError::internal(e.to_string()))?;
                let_assert!(Entry::CompleteAwakeable(CompleteAwakeableEntry { id, .. }) = entry);

                let (invocation_id, entry_index) = if let Ok(old_awk_id) =
                    AwakeableIdentifier::from_str(&id)
                {
                    old_awk_id.into_inner()
                } else if let Ok(new_awk_id) = ExternalSignalIdentifier::from_str(&id) {
                    let (invocation_id, signal_id) = new_awk_id.into_inner();
                    if let SignalId::Index(idx) = signal_id {
                        (invocation_id, idx)
                    } else {
                        return Err(InvocationError::new(
                            codes::BAD_REQUEST,
                            "Unsupported awakeable signal identifier. Only signals with auto generated id can be completed using service protocol <= v3.".to_string(),
                        ));
                    }
                } else {
                    return Err(InvocationError::new(
                        codes::BAD_REQUEST,
                        "Invalid awakeable identifier. The identifier doesn't start with `awk_1`, neither with `sign_1`".to_string(),
                    ));
                };

                EnrichedEntryHeader::CompleteAwakeable {
                    enrichment_result: AwakeableEnrichmentResult {
                        invocation_id,
                        entry_index,
                    },
                }
            }
            PlainEntryHeader::Run { .. } => EnrichedEntryHeader::Run {},
            PlainEntryHeader::CancelInvocation { .. } => {
                // Validate the invocation id is valid
                let entry =
                    Codec::deserialize(EntryType::CancelInvocation, serialized_entry.clone())
                        .map_err(|e| InvocationError::internal(e.to_string()))?;
                let_assert!(Entry::CancelInvocation(CancelInvocationEntry { target }) = entry);
                if let CancelInvocationTarget::InvocationId(id) = target
                    && let Err(e) = id.parse::<InvocationId>()
                {
                    return Err(InvocationError::new(
                        codes::BAD_REQUEST,
                        format!("The given invocation id '{id}' to cancel is invalid: {e}"),
                    ));
                }

                EnrichedEntryHeader::CancelInvocation {}
            }
            PlainEntryHeader::AttachInvocation { is_completed } => {
                // Validate the invocation id is valid
                let entry =
                    Codec::deserialize(EntryType::AttachInvocation, serialized_entry.clone())
                        .map_err(|e| InvocationError::internal(e.to_string()))?;
                let_assert!(Entry::AttachInvocation(AttachInvocationEntry { target, .. }) = entry);
                if let AttachInvocationTarget::InvocationId(id) = target
                    && let Err(e) = id.parse::<InvocationId>()
                {
                    return Err(InvocationError::new(
                        codes::BAD_REQUEST,
                        format!("The given invocation id '{id}' to attach is invalid: {e}"),
                    ));
                }

                EnrichedEntryHeader::AttachInvocation { is_completed }
            }
            PlainEntryHeader::GetInvocationOutput { is_completed } => {
                // Validate the invocation id is valid
                let entry =
                    Codec::deserialize(EntryType::GetInvocationOutput, serialized_entry.clone())
                        .map_err(|e| InvocationError::internal(e.to_string()))?;
                let_assert!(
                    Entry::GetInvocationOutput(GetInvocationOutputEntry { target, .. }) = entry
                );
                if let AttachInvocationTarget::InvocationId(id) = target
                    && let Err(e) = id.parse::<InvocationId>()
                {
                    return Err(InvocationError::new(
                        codes::BAD_REQUEST,
                        format!("The given invocation id '{id}' to get output is invalid: {e}"),
                    ));
                }

                EnrichedEntryHeader::GetInvocationOutput { is_completed }
            }
            PlainEntryHeader::GetCallInvocationId { is_completed } => {
                EnrichedEntryHeader::GetCallInvocationId { is_completed }
            }
            PlainEntryHeader::Custom { code } => EnrichedEntryHeader::Custom { code },
        };

        Ok(RawEntry::new(enriched_header, serialized_entry))
    }
}

#[inline]
fn check_workflow_type(
    entry_type: &EntryType,
    service_type: &ServiceType,
) -> Result<(), InvocationError> {
    if *service_type != ServiceType::Workflow {
        return Err(InvocationError::new(
            codes::BAD_REQUEST,
            format!(
                "The service type {service_type} does not support the entry type {entry_type}, only Workflow supports it"
            ),
        ));
    }
    Ok(())
}

#[inline]
fn can_read_state(
    entry_type: &EntryType,
    invocation_target_type: &InvocationTargetType,
) -> Result<(), InvocationError> {
    if !invocation_target_type.can_read_state() {
        return Err(InvocationError::new(
            codes::BAD_REQUEST,
            format!(
                "The service/handler type {invocation_target_type} does not have state and, therefore, does not support the entry type {entry_type}"
            ),
        ));
    }
    Ok(())
}

#[inline]
fn can_write_state(
    entry_type: &EntryType,
    invocation_target_type: &InvocationTargetType,
) -> Result<(), InvocationError> {
    can_read_state(entry_type, invocation_target_type)?;
    if !invocation_target_type.can_write_state() {
        return Err(InvocationError::new(
            codes::BAD_REQUEST,
            format!(
                "The service/handler type {invocation_target_type} has no exclusive state access and, therefore, does not support the entry type {entry_type}"
            ),
        ));
    }
    Ok(())
}
