// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod actions;
mod tracing;

use crate::metric_definitions::PARTITION_APPLY_COMMAND;
use crate::partition::state_machine::tracing::StateMachineSpanExt;
use crate::partition::types::InvocationIdAndTarget;
use crate::partition::types::{InvokerEffect, InvokerEffectKind, OutboxMessageExt};
use ::tracing::{debug, trace, warn, Instrument, Span};
use assert2::let_assert;
use bytes::Bytes;
use bytestring::ByteString;
use futures::{StreamExt, TryStreamExt};
use metrics::{histogram, Histogram};
use opentelemetry::trace::SpanId;
use restate_invoker_api::InvokeInputJournal;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_api::idempotency_table::IdempotencyMetadata;
use restate_storage_api::idempotency_table::{IdempotencyTable, ReadOnlyIdempotencyTable};
use restate_storage_api::inbox_table::{InboxEntry, InboxTable};
use restate_storage_api::invocation_status_table;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, InboxedInvocation, InvocationStatusTable,
    PreFlightInvocationMetadata, ReadOnlyInvocationStatusTable,
};
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ScheduledInvocation, SourceTable,
};
use restate_storage_api::journal_table::ReadOnlyJournalTable;
use restate_storage_api::journal_table::{JournalEntry, JournalTable};
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_storage_api::promise_table::{Promise, PromiseState, PromiseTable};
use restate_storage_api::service_status_table::{
    ReadOnlyVirtualObjectStatusTable, VirtualObjectStatus, VirtualObjectStatusTable,
};
use restate_storage_api::timer_table::TimerKey;
use restate_storage_api::timer_table::{Timer, TimerTable};
use restate_storage_api::Result as StorageResult;
use restate_types::deployment::PinnedDeployment;
use restate_types::errors::{
    InvocationError, InvocationErrorCode, ALREADY_COMPLETED_INVOCATION_ERROR,
    ATTACH_NOT_SUPPORTED_INVOCATION_ERROR, CANCELED_INVOCATION_ERROR, GONE_INVOCATION_ERROR,
    KILLED_INVOCATION_ERROR, NOT_FOUND_INVOCATION_ERROR, WORKFLOW_ALREADY_INVOKED_INVOCATION_ERROR,
};
use restate_types::identifiers::{EntryIndex, InvocationId, PartitionKey, ServiceId};
use restate_types::identifiers::{
    IdempotencyId, JournalEntryId, WithInvocationId, WithPartitionKey,
};
use restate_types::ingress;
use restate_types::ingress::{IngressResponseEnvelope, IngressResponseResult};
use restate_types::invocation::InvocationInput;
use restate_types::invocation::{
    AttachInvocationRequest, InvocationQuery, InvocationResponse, InvocationTarget,
    InvocationTargetType, InvocationTermination, ResponseResult, ServiceInvocation,
    ServiceInvocationResponseSink, ServiceInvocationSpanContext, Source, SpanRelationCause,
    SubmitNotificationSink, TerminationFlavor, VirtualObjectHandlerType, WorkflowHandlerType,
};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::enriched::{
    AwakeableEnrichmentResult, CallEnrichmentResult, EnrichedEntryHeader,
};
use restate_types::journal::raw::{RawEntryCodec, RawEntryCodecError};
use restate_types::journal::Completion;
use restate_types::journal::CompletionResult;
use restate_types::journal::EntryType;
use restate_types::journal::*;
use restate_types::message::MessageIndex;
use restate_types::state_mut::ExternalStateMutation;
use restate_types::state_mut::StateMutationVersion;
use restate_types::time::MillisSinceEpoch;
use restate_wal_protocol::timer::TimerKeyDisplay;
use restate_wal_protocol::timer::TimerKeyValue;
use restate_wal_protocol::Command;
use std::collections::HashSet;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::iter;
use std::marker::PhantomData;
use std::ops::RangeInclusive;
use std::time::Duration;
use std::time::Instant;

pub use actions::{Action, ActionCollector};
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::state_table::StateTable;

pub struct StateMachine<Codec> {
    // initialized from persistent storage
    inbox_seq_number: MessageIndex,
    /// First outbox message index.
    outbox_head_seq_number: Option<MessageIndex>,
    /// Sequence number of the next outbox message to be appended.
    outbox_seq_number: MessageIndex,
    partition_key_range: RangeInclusive<PartitionKey>,
    latency: Histogram,

    /// This is used to establish whether for new invocations we should use the NeoInvocationStatus or not.
    /// The neo table is enabled via [restate_types::config::WorkerOptions::experimental_feature_new_invocation_status_table].
    default_invocation_status_source_table: SourceTable,

    _codec: PhantomData<Codec>,
}

impl<Codec> Debug for StateMachine<Codec> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateMachine")
            .field("inbox_seq_number", &self.inbox_seq_number)
            .field("outbox_head_seq_number", &self.outbox_head_seq_number)
            .field("outbox_seq_number", &self.outbox_seq_number)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to deserialize entry: {0}")]
    Codec(#[from] RawEntryCodecError),
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
}

macro_rules! debug_if_leader {
    ($i_am_leader:expr, $($args:tt)*) => {{
        use ::tracing::Level;
        if $i_am_leader {
            ::tracing::event!(Level::DEBUG, $($args)*)
        } else {
            ::tracing::event!(Level::TRACE, $($args)*)
        }
    }};
}

macro_rules! span_if_leader {
    ($level:expr, $i_am_leader:expr, $sampled:expr, $span_relation:expr, $($args:tt)*) => {{
        if $i_am_leader && $sampled {
            let span = ::tracing::span!($level, $($args)*);
            $span_relation
                .attach_to_span(&span);
            let _ = span.enter();
        }
    }};
}

macro_rules! info_span_if_leader {
    ($i_am_leader:expr, $sampled:expr, $span_relation:expr, $($args:tt)*) => {{
        use ::tracing::Level;
        span_if_leader!(Level::INFO, $i_am_leader, $sampled, $span_relation, $($args)*)
    }};
}

impl<Codec> StateMachine<Codec> {
    pub fn new(
        inbox_seq_number: MessageIndex,
        outbox_seq_number: MessageIndex,
        outbox_head_seq_number: Option<MessageIndex>,
        partition_key_range: RangeInclusive<PartitionKey>,
        default_invocation_status_source_table: invocation_status_table::SourceTable,
    ) -> Self {
        let latency =
            histogram!(crate::metric_definitions::PARTITION_HANDLE_INVOKER_EFFECT_COMMAND);
        Self {
            inbox_seq_number,
            outbox_seq_number,
            outbox_head_seq_number,
            partition_key_range,
            latency,
            default_invocation_status_source_table,
            _codec: PhantomData,
        }
    }
}

pub(crate) struct StateMachineApplyContext<'a, S> {
    storage: &'a mut S,
    action_collector: &'a mut ActionCollector,
    is_leader: bool,
}

impl<'a, S> StateMachineApplyContext<'a, S> {
    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<InvocationStatus, Error>
    where
        S: ReadOnlyInvocationStatusTable,
    {
        Span::record_invocation_id(invocation_id);
        let status = self.storage.get_invocation_status(invocation_id).await?;
        if let Some(invocation_target) = status.invocation_target() {
            Span::record_invocation_target(invocation_target);
        }
        if let Some(journal_metadata) = status.get_journal_metadata() {
            journal_metadata
                .span_context
                .as_parent()
                .attach_to_span(&Span::current());
        }
        Ok(status)
    }
}

impl<Codec: RawEntryCodec> StateMachine<Codec> {
    pub async fn apply<TransactionType: restate_storage_api::Transaction + Send>(
        &mut self,
        command: Command,
        transaction: &mut TransactionType,
        action_collector: &mut ActionCollector,
        is_leader: bool,
    ) -> Result<(), Error> {
        let span = tracing::state_machine_apply_command_span(is_leader, &command);
        async {
            let start = Instant::now();
            // Apply the command
            let command_type = command.name();
            let res = self
                .on_apply(
                    StateMachineApplyContext {
                        storage: transaction,
                        action_collector,
                        is_leader,
                    },
                    command,
                )
                .await;
            histogram!(PARTITION_APPLY_COMMAND, "command" => command_type).record(start.elapsed());
            res
        }
        .instrument(span)
        .await
    }

    async fn on_apply<
        State: IdempotencyTable
            + PromiseTable
            + JournalTable
            + InvocationStatusTable
            + OutboxTable
            + FsmTable
            + TimerTable
            + VirtualObjectStatusTable
            + InboxTable
            + StateTable,
    >(
        &mut self,
        mut ctx: StateMachineApplyContext<'_, State>,
        command: Command,
    ) -> Result<(), Error> {
        match command {
            Command::Invoke(service_invocation) => {
                self.on_service_invocation(&mut ctx, service_invocation)
                    .await
            }
            Command::InvocationResponse(InvocationResponse {
                id,
                entry_index,
                result,
            }) => {
                let completion = Completion {
                    entry_index,
                    result: result.into(),
                };

                Self::handle_completion(&mut ctx, id, completion).await
            }
            Command::ProxyThrough(service_invocation) => {
                self.handle_outgoing_message(
                    &mut ctx,
                    OutboxMessage::ServiceInvocation(service_invocation),
                )
                .await?;
                Ok(())
            }
            Command::AttachInvocation(attach_invocation_request) => {
                self.handle_attach_invocation_request(&mut ctx, attach_invocation_request)
                    .await
            }
            Command::InvokerEffect(effect) => self.try_invoker_effect(&mut ctx, effect).await,
            Command::TruncateOutbox(index) => {
                Self::do_truncate_outbox(
                    &mut ctx,
                    RangeInclusive::new(self.outbox_head_seq_number.unwrap_or(index), index),
                )
                .await?;
                self.outbox_head_seq_number = Some(index + 1);
                Ok(())
            }
            Command::Timer(timer) => self.on_timer(&mut ctx, timer).await,
            Command::TerminateInvocation(invocation_termination) => {
                self.try_terminate_invocation(&mut ctx, invocation_termination)
                    .await
            }
            Command::PurgeInvocation(purge_invocation_request) => {
                self.try_purge_invocation(&mut ctx, purge_invocation_request.invocation_id)
                    .await
            }
            Command::PatchState(mutation) => {
                self.handle_external_state_mutation(&mut ctx, mutation)
                    .await
            }
            Command::AnnounceLeader(_) => {
                // no-op :-)
                Ok(())
            }
            Command::ScheduleTimer(timer) => {
                Self::register_timer(&mut ctx, timer, Default::default()).await?;
                Ok(())
            }
        }
    }

    async fn on_service_invocation<
        State: IdempotencyTable
            + InvocationStatusTable
            + OutboxTable
            + FsmTable
            + VirtualObjectStatusTable
            + TimerTable
            + InboxTable
            + FsmTable
            + JournalTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        service_invocation: ServiceInvocation,
    ) -> Result<(), Error> {
        let invocation_id = service_invocation.invocation_id;
        debug_assert!(
            self.partition_key_range.contains(&service_invocation.partition_key()),
            "Service invocation with partition key '{}' has been delivered to a partition processor with key range '{:?}'. This indicates a bug.",
            service_invocation.partition_key(),
            self.partition_key_range);

        Span::record_invocation_id(&invocation_id);
        Span::record_invocation_target(&service_invocation.invocation_target);
        service_invocation
            .span_context
            .as_parent()
            .attach_to_span(&Span::current());

        // Phases of an invocation
        // 1. Try deduplicate it first
        // 1.1. Deduplicate using idempotency id
        // 1.2. Deduplicate for "run once" workflow semantics (only for workflow handlers of workflows services)
        // 2. Check if we need to schedule it
        // 3. Check if we need to inbox it (only for exclusive handlers of virtual objects services)
        // 4. Execute it

        // 1.1. Handle deduplication for idempotency id
        let Some(service_invocation) = self
            .handle_service_invocation_idempotency_id(ctx, service_invocation)
            .await?
        else {
            // Invocation was deduplicated, nothing else to do here
            return Ok(());
        };

        // 1.2. Handle deduplication for workflows
        let Some(mut service_invocation) = self
            .handle_service_invocation_workflow_run(ctx, service_invocation)
            .await?
        else {
            // Invocation was deduplicated, nothing else to do here
            return Ok(());
        };

        // Prepare PreFlightInvocationMetadata structure
        let submit_notification_sink = service_invocation.submit_notification_sink.take();
        let pre_flight_invocation_metadata = PreFlightInvocationMetadata::from_service_invocation(
            service_invocation,
            self.default_invocation_status_source_table,
        );

        // 2. Check if we need to schedule it
        let Some(pre_flight_invocation_metadata) = self
            .handle_service_invocation_execution_time(
                ctx,
                invocation_id,
                pre_flight_invocation_metadata,
            )
            .await?
        else {
            // Invocation was scheduled, send back the ingress attach notification and return
            Self::send_submit_notification_if_needed(
                ctx,
                invocation_id,
                None,
                submit_notification_sink,
            );
            return Ok(());
        };

        // 3. Check if we need to inbox it (only for exclusive methods of virtual objects)
        let Some(pre_flight_invocation_metadata) = self
            .handle_service_invocation_exclusive_handler(
                ctx,
                invocation_id,
                pre_flight_invocation_metadata,
            )
            .await?
        else {
            // Invocation was inboxed, send back the ingress attach notification and return
            Self::send_submit_notification_if_needed(
                ctx,
                invocation_id,
                None,
                submit_notification_sink,
            );
            // Invocation was inboxed, nothing else to do here
            return Ok(());
        };

        // 4. Execute it
        Self::send_submit_notification_if_needed(
            ctx,
            invocation_id,
            None,
            submit_notification_sink,
        );

        let (in_flight_invocation_metadata, invocation_input) =
            InFlightInvocationMetadata::from_pre_flight_invocation_metadata(
                pre_flight_invocation_metadata,
            );

        Self::init_journal_and_invoke(
            ctx,
            invocation_id,
            in_flight_invocation_metadata,
            invocation_input,
        )
        .await
    }

    /// Returns the invocation in case the invocation is not a duplicate
    async fn handle_service_invocation_idempotency_id<
        State: IdempotencyTable + InvocationStatusTable + OutboxTable + FsmTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        service_invocation: ServiceInvocation,
    ) -> Result<Option<ServiceInvocation>, Error> {
        if let Some(idempotency_id) = service_invocation.compute_idempotency_id() {
            if service_invocation.invocation_target.invocation_target_ty()
                == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
            {
                warn!("The idempotency key for workflow methods is ignored!");
            } else {
                if let Some(attached_invocation_id) = self
                    .try_resolve_idempotent_request(
                        ctx,
                        &idempotency_id,
                        &service_invocation.invocation_id,
                        service_invocation.response_sink.as_ref(),
                    )
                    .await?
                {
                    Self::send_submit_notification_if_needed(
                        ctx,
                        service_invocation.invocation_id,
                        Some(attached_invocation_id),
                        service_invocation.submit_notification_sink,
                    );
                    debug_if_leader!(
                        ctx.is_leader,
                        restate.idempotency.id = ?idempotency_id,
                        "Invocation is a duplicate"
                    );

                    // Invocation was either resolved, or the sink was enqueued. Nothing else to do here.
                    return Ok(None);
                }

                debug_if_leader!(
                        ctx.is_leader,
                        restate.idempotency.id = ?idempotency_id,
                        "First time we see this idempotency id, invocation will be processed");

                // Idempotent invocation needs to be processed for the first time, let's roll!
                Self::do_store_idempotency_id(
                    ctx,
                    idempotency_id,
                    service_invocation.invocation_id,
                )
                .await?;
            }
        }
        Ok(Some(service_invocation))
    }

    /// Returns the invocation in case the invocation is not a duplicate
    async fn handle_service_invocation_workflow_run<
        State: VirtualObjectStatusTable + OutboxTable + FsmTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        mut service_invocation: ServiceInvocation,
    ) -> Result<Option<ServiceInvocation>, Error> {
        if service_invocation.invocation_target.invocation_target_ty()
            == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
        {
            let keyed_service_id = service_invocation
                .invocation_target
                .as_keyed_service_id()
                .expect("When the handler type is Workflow, the invocation target must have a key");

            let service_status = ctx
                .storage
                .get_virtual_object_status(&keyed_service_id)
                .await?;

            // If locked, then we check the original invocation
            if let VirtualObjectStatus::Locked(original_invocation_id) = service_status {
                if let Some(response_sink) = service_invocation.response_sink {
                    // --- ATTACH business logic below, this is currently disabled due to the pending discussion about equality check.
                    //     We instead simply fail the invocation with CONFLICT status code
                    //
                    // let invocation_status =
                    //     ctx.storage.get_invocation_status(&original_invocation_id).await?;
                    //
                    // match invocation_status {
                    //     InvocationStatus::Completed(
                    //         CompletedInvocation { response_result, .. }) => {
                    //         self.send_response_to_sinks(
                    //             effects,
                    //             iter::once(response_sink),
                    //             response_result,
                    //             Some(service_invocation.invocation_id),
                    //             Some(&service_invocation.invocation_target),
                    //         );
                    //     }
                    //     InvocationStatus::Free => panic!("Unexpected state, the InvocationStatus cannot be Free for invocation {} given it's in locked status", original_invocation_id),
                    //     is => Self::do_append_response_sink(ctx,
                    //         original_invocation_id,
                    //         is,
                    //         response_sink
                    //     )
                    // }

                    self.send_response_to_sinks(
                        ctx,
                        iter::once(response_sink),
                        ResponseResult::Failure(WORKFLOW_ALREADY_INVOKED_INVOCATION_ERROR),
                        Some(original_invocation_id),
                        Some(&service_invocation.invocation_target),
                    )
                    .await?;
                }

                Self::send_submit_notification_if_needed(
                    ctx,
                    service_invocation.invocation_id,
                    Some(original_invocation_id),
                    service_invocation.submit_notification_sink.take(),
                );

                debug_if_leader!(
                    ctx.is_leader,
                    "Invocation to workflow method is a duplicate"
                );

                return Ok(None);
            }
            debug_if_leader!(
                ctx.is_leader,
                "First time we see this workflow id, invocation will be processed"
            );

            ctx.storage
                .put_virtual_object_status(
                    &keyed_service_id,
                    &VirtualObjectStatus::Locked(service_invocation.invocation_id),
                )
                .await;
        }
        Ok(Some(service_invocation))
    }

    /// Returns the invocation in case the invocation should run immediately
    async fn handle_service_invocation_execution_time<State: TimerTable + InvocationStatusTable>(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        metadata: PreFlightInvocationMetadata,
    ) -> Result<Option<PreFlightInvocationMetadata>, Error> {
        if let Some(execution_time) = metadata.execution_time {
            let span_context = metadata.span_context.clone();
            match self.default_invocation_status_source_table {
                SourceTable::Old => {
                    // TODO remove this code once we remove the Old table
                    Self::register_timer(
                        ctx,
                        TimerKeyValue::invoke(
                            execution_time,
                            ServiceInvocation {
                                invocation_id,
                                invocation_target: metadata.invocation_target,
                                argument: metadata.argument,
                                source: metadata.source,
                                span_context: span_context.clone(),
                                headers: metadata.headers,
                                execution_time: metadata.execution_time,
                                completion_retention_duration: Some(
                                    metadata.completion_retention_duration,
                                ),
                                idempotency_key: metadata.idempotency_key,
                                response_sink: metadata.response_sinks.into_iter().next(),
                                submit_notification_sink: None,
                            },
                        ),
                        span_context,
                    )
                    .await?;
                }
                SourceTable::New => {
                    debug_if_leader!(ctx.is_leader, "Store scheduled invocation");

                    Self::register_timer(
                        ctx,
                        TimerKeyValue::neo_invoke(execution_time, invocation_id),
                        span_context,
                    )
                    .await?;

                    ctx.storage
                        .put_invocation_status(
                            &invocation_id.clone(),
                            &InvocationStatus::Scheduled(
                                ScheduledInvocation::from_pre_flight_invocation_metadata(metadata),
                            ),
                        )
                        .await;
                }
            }
            // The span will be created later on invocation
            return Ok(None);
        }

        Ok(Some(metadata))
    }

    /// Returns the invocation in case the invocation was not inboxed
    async fn handle_service_invocation_exclusive_handler<
        State: VirtualObjectStatusTable + InvocationStatusTable + InboxTable + FsmTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        metadata: PreFlightInvocationMetadata,
    ) -> Result<Option<PreFlightInvocationMetadata>, Error> {
        if metadata.invocation_target.invocation_target_ty()
            == InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive)
        {
            let keyed_service_id = metadata.invocation_target.as_keyed_service_id().expect(
                "When the handler type is Exclusive, the invocation target must have a key",
            );

            let service_status = ctx
                .storage
                .get_virtual_object_status(&keyed_service_id)
                .await?;

            if let VirtualObjectStatus::Locked(_) = service_status {
                // If locked, enqueue in inbox and be done with it
                let inbox_seq_number = self
                    .enqueue_into_inbox(
                        ctx,
                        InboxEntry::Invocation(keyed_service_id, invocation_id),
                    )
                    .await?;

                debug_if_leader!(
                    ctx.is_leader,
                    restate.outbox.seq = inbox_seq_number,
                    "Store inboxed invocation"
                );
                ctx.storage
                    .put_invocation_status(
                        &invocation_id,
                        &InvocationStatus::Inboxed(
                            InboxedInvocation::from_pre_flight_invocation_metadata(
                                metadata,
                                inbox_seq_number,
                            ),
                        ),
                    )
                    .await;

                return Ok(None);
            } else {
                // If unlocked, lock it
                debug_if_leader!(
                    ctx.is_leader,
                    restate.service.id = %keyed_service_id,
                    "Locking service"
                );

                ctx.storage
                    .put_virtual_object_status(
                        &keyed_service_id,
                        &VirtualObjectStatus::Locked(invocation_id),
                    )
                    .await;
            }
        }
        Ok(Some(metadata))
    }

    async fn init_journal_and_invoke<State: JournalTable + InvocationStatusTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        mut in_flight_invocation_metadata: InFlightInvocationMetadata,
        invocation_input: InvocationInput,
    ) -> Result<(), Error> {
        let invoke_input_journal = Self::init_journal(
            ctx,
            invocation_id,
            &mut in_flight_invocation_metadata,
            invocation_input,
        )
        .await?;

        Self::invoke(
            ctx,
            invocation_id,
            in_flight_invocation_metadata,
            invoke_input_journal,
        )
        .await
    }

    async fn init_journal<State: JournalTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        in_flight_invocation_metadata: &mut InFlightInvocationMetadata,
        invocation_input: InvocationInput,
    ) -> Result<InvokeInputJournal, Error> {
        debug_if_leader!(ctx.is_leader, "Init journal with input entry");

        // In our current data model, ServiceInvocation has always an input, so initial length is 1
        in_flight_invocation_metadata.journal_metadata.length = 1;

        let input_entry = JournalEntry::Entry(Codec::serialize_as_input_entry(
            invocation_input.headers,
            invocation_input.argument,
        ));

        ctx.storage
            .put_journal_entry(&invocation_id, 0, &input_entry)
            .await;

        let_assert!(JournalEntry::Entry(input_entry) = input_entry);

        Ok(InvokeInputJournal::CachedJournal(
            restate_invoker_api::JournalMetadata::new(
                in_flight_invocation_metadata.journal_metadata.length,
                in_flight_invocation_metadata
                    .journal_metadata
                    .span_context
                    .clone(),
                None,
                // This is safe to do as only the leader will execute the invoker command
                MillisSinceEpoch::now(),
            ),
            vec![input_entry.erase_enrichment()],
        ))
    }

    async fn invoke<State: InvocationStatusTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        in_flight_invocation_metadata: InFlightInvocationMetadata,
        invoke_input_journal: InvokeInputJournal,
    ) -> Result<(), Error> {
        debug_if_leader!(ctx.is_leader, "Invoke");

        ctx.action_collector.push(Action::Invoke {
            invocation_id,
            invocation_target: in_flight_invocation_metadata.invocation_target.clone(),
            invoke_input_journal,
        });
        ctx.storage
            .put_invocation_status(
                &invocation_id,
                &InvocationStatus::Invoked(in_flight_invocation_metadata),
            )
            .await;

        Ok(())
    }

    async fn enqueue_into_inbox<State: InboxTable + FsmTable>(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        inbox_entry: InboxEntry,
    ) -> Result<MessageIndex, Error> {
        let seq_number = self.inbox_seq_number;
        debug_if_leader!(
            ctx.is_leader,
            restate.inbox.seq = seq_number,
            "Enqueue inbox entry"
        );

        ctx.storage.put_inbox_entry(seq_number, &inbox_entry).await;
        // need to store the next inbox sequence number
        ctx.storage.put_inbox_seq_number(seq_number + 1).await;
        self.inbox_seq_number += 1;
        Ok(seq_number)
    }

    /// If an invocation id is returned, the request has been resolved and no further processing is needed
    async fn try_resolve_idempotent_request<
        State: ReadOnlyIdempotencyTable + InvocationStatusTable + OutboxTable + FsmTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        idempotency_id: &IdempotencyId,
        caller_id: &InvocationId,
        response_sink: Option<&ServiceInvocationResponseSink>,
    ) -> Result<Option<InvocationId>, Error> {
        if let Some(idempotency_meta) = ctx.storage.get_idempotency_metadata(idempotency_id).await?
        {
            let original_invocation_id = idempotency_meta.invocation_id;
            match ctx
                .storage
                .get_invocation_status(&original_invocation_id)
                .await?
            {
                is @ InvocationStatus::Invoked { .. }
                | is @ InvocationStatus::Suspended { .. }
                | is @ InvocationStatus::Inboxed { .. }
                | is @ InvocationStatus::Scheduled { .. } => {
                    if let Some(response_sink) = response_sink {
                        if !is
                            .get_response_sinks()
                            .expect("response sink must be present")
                            .contains(response_sink)
                        {
                            Self::do_append_response_sink(
                                ctx,
                                original_invocation_id,
                                is,
                                response_sink.clone(),
                            )
                            .await?
                        }
                    }
                }
                InvocationStatus::Completed(completed) => {
                    self.send_response_to_sinks(
                        ctx,
                        response_sink.cloned(),
                        completed.response_result,
                        Some(original_invocation_id),
                        Some(&completed.invocation_target),
                    )
                    .await?;
                }
                InvocationStatus::Free => {
                    self.send_response_to_sinks(
                        ctx,
                        response_sink.cloned(),
                        GONE_INVOCATION_ERROR,
                        Some(*caller_id),
                        None,
                    )
                    .await?
                }
            }
            Ok(Some(original_invocation_id))
        } else {
            Ok(None)
        }
    }

    async fn handle_external_state_mutation<
        State: StateTable + InboxTable + FsmTable + VirtualObjectStatusTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        mutation: ExternalStateMutation,
    ) -> Result<(), Error> {
        let service_status = ctx
            .storage
            .get_virtual_object_status(&mutation.service_id)
            .await?;

        match service_status {
            VirtualObjectStatus::Locked(_) => {
                self.enqueue_into_inbox(ctx, InboxEntry::StateMutation(mutation))
                    .await?;
            }
            VirtualObjectStatus::Unlocked => Self::do_mutate_state(ctx, mutation).await?,
        }

        Ok(())
    }

    async fn try_terminate_invocation<
        State: VirtualObjectStatusTable
            + InvocationStatusTable
            + InboxTable
            + FsmTable
            + StateTable
            + JournalTable
            + OutboxTable
            + TimerTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        InvocationTermination {
            invocation_id,
            flavor: termination_flavor,
        }: InvocationTermination,
    ) -> Result<(), Error> {
        match termination_flavor {
            TerminationFlavor::Kill => self.try_kill_invocation(ctx, invocation_id).await,
            TerminationFlavor::Cancel => self.try_cancel_invocation(ctx, invocation_id).await,
        }
    }

    async fn try_kill_invocation<
        State: VirtualObjectStatusTable
            + InvocationStatusTable
            + InboxTable
            + FsmTable
            + StateTable
            + JournalTable
            + OutboxTable
            + FsmTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
    ) -> Result<(), Error> {
        let status = ctx.get_invocation_status(&invocation_id).await?;

        match status {
            InvocationStatus::Invoked(metadata) | InvocationStatus::Suspended { metadata, .. } => {
                self.kill_invocation(ctx, invocation_id, metadata).await?;
            }
            InvocationStatus::Inboxed(inboxed) => {
                self.terminate_inboxed_invocation(
                    ctx,
                    TerminationFlavor::Kill,
                    invocation_id,
                    inboxed,
                )
                .await?
            }
            _ => {
                trace!("Received kill command for unknown invocation with id '{invocation_id}'.");
                // We still try to send the abort signal to the invoker,
                // as it might be the case that previously the user sent an abort signal
                // but some message was still between the invoker/PP queues.
                // This can happen because the invoke/resume and the abort invoker messages end up in different queues,
                // and the abort message can overtake the invoke/resume.
                // Consequently the invoker might have not received the abort and the user tried to send it again.
                Self::do_send_abort_invocation_to_invoker(ctx, invocation_id);
            }
        };

        Ok(())
    }

    async fn try_cancel_invocation<
        State: VirtualObjectStatusTable
            + InvocationStatusTable
            + InboxTable
            + FsmTable
            + StateTable
            + JournalTable
            + OutboxTable
            + TimerTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
    ) -> Result<(), Error> {
        let status = ctx.get_invocation_status(&invocation_id).await?;

        match status {
            InvocationStatus::Invoked(metadata) => {
                self.cancel_journal_leaves(
                    ctx,
                    invocation_id,
                    InvocationStatusProjection::Invoked,
                    metadata.journal_metadata.length,
                )
                .await?;
            }
            InvocationStatus::Suspended {
                metadata,
                waiting_for_completed_entries,
            } => {
                if self
                    .cancel_journal_leaves(
                        ctx,
                        invocation_id,
                        InvocationStatusProjection::Suspended(waiting_for_completed_entries),
                        metadata.journal_metadata.length,
                    )
                    .await?
                {
                    Self::do_resume_service(ctx, invocation_id, metadata).await?;
                }
            }
            InvocationStatus::Inboxed(inboxed) => {
                self.terminate_inboxed_invocation(
                    ctx,
                    TerminationFlavor::Cancel,
                    invocation_id,
                    inboxed,
                )
                .await?
            }
            _ => {
                trace!("Received cancel command for unknown invocation with id '{invocation_id}'.");
                // We still try to send the abort signal to the invoker,
                // as it might be the case that previously the user sent an abort signal
                // but some message was still between the invoker/PP queues.
                // This can happen because the invoke/resume and the abort invoker messages end up in different queues,
                // and the abort message can overtake the invoke/resume.
                // Consequently the invoker might have not received the abort and the user tried to send it again.
                Self::do_send_abort_invocation_to_invoker(ctx, invocation_id);
            }
        };

        Ok(())
    }

    async fn terminate_inboxed_invocation<
        State: InvocationStatusTable + InboxTable + OutboxTable + FsmTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        termination_flavor: TerminationFlavor,
        invocation_id: InvocationId,
        inboxed_invocation: InboxedInvocation,
    ) -> Result<(), Error> {
        let error = match termination_flavor {
            TerminationFlavor::Kill => KILLED_INVOCATION_ERROR,
            TerminationFlavor::Cancel => CANCELED_INVOCATION_ERROR,
        };

        let InboxedInvocation {
            inbox_sequence_number,
            metadata:
                PreFlightInvocationMetadata {
                    response_sinks,
                    span_context,
                    invocation_target,
                    ..
                },
        } = inboxed_invocation;

        // Reply back to callers with error, and publish end trace
        self.send_response_to_sinks(
            ctx,
            response_sinks,
            &error,
            Some(invocation_id),
            Some(&invocation_target),
        )
        .await?;

        // Delete inbox entry and invocation status.
        Self::do_delete_inbox_entry(
            ctx,
            invocation_target
                .as_keyed_service_id()
                .expect("Because the invocation is inboxed, it must have a keyed service id"),
            inbox_sequence_number,
        )
        .await?;
        Self::do_free_invocation(ctx, invocation_id).await;

        self.notify_invocation_result(
            ctx,
            invocation_id,
            invocation_target,
            span_context,
            MillisSinceEpoch::now(),
            Err((error.code(), error.to_string())),
        );

        Ok(())
    }

    async fn kill_invocation<
        State: InboxTable
            + VirtualObjectStatusTable
            + InvocationStatusTable
            + VirtualObjectStatusTable
            + StateTable
            + JournalTable
            + OutboxTable
            + FsmTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error> {
        self.kill_child_invocations(ctx, &invocation_id, metadata.journal_metadata.length)
            .await?;

        self.fail_invocation(ctx, invocation_id, metadata, KILLED_INVOCATION_ERROR)
            .await?;
        Self::do_send_abort_invocation_to_invoker(ctx, invocation_id);
        Ok(())
    }

    async fn kill_child_invocations<State: OutboxTable + FsmTable + ReadOnlyJournalTable>(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> Result<(), Error> {
        let invocation_ids_to_kill: Vec<InvocationId> = ctx
            .storage
            .get_journal(invocation_id, journal_length)
            .try_filter_map(|(_, journal_entry)| async {
                if let JournalEntry::Entry(enriched_entry) = journal_entry {
                    let (h, _) = enriched_entry.into_inner();
                    match h {
                        // we only need to kill child invocations if they are not completed and the target was resolved
                        EnrichedEntryHeader::Call {
                            is_completed,
                            enrichment_result: Some(enrichment_result),
                        } if !is_completed => return Ok(Some(enrichment_result.invocation_id)),
                        // we neither kill background calls nor delayed calls since we are considering them detached from this
                        // call tree. In the future we want to support a mode which also kills these calls (causally related).
                        // See https://github.com/restatedev/restate/issues/979
                        _ => {}
                    }
                }

                Ok(None)
            })
            .try_collect()
            .await?;

        for id in invocation_ids_to_kill {
            self.handle_outgoing_message(
                ctx,
                OutboxMessage::InvocationTermination(InvocationTermination::kill(id)),
            )
            .await?;
        }

        Ok(())
    }

    async fn cancel_journal_leaves<State: JournalTable + OutboxTable + FsmTable + TimerTable>(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        invocation_status: InvocationStatusProjection,
        journal_length: EntryIndex,
    ) -> Result<bool, Error> {
        let journal_entries_to_cancel: Vec<(EntryIndex, EnrichedRawEntry)> = ctx
            .storage
            .get_journal(&invocation_id, journal_length)
            .try_filter_map(|(journal_index, journal_entry)| async move {
                if let JournalEntry::Entry(journal_entry) = journal_entry {
                    match journal_entry.header() {
                        EnrichedEntryHeader::Call { is_completed, .. } if !is_completed => {
                            return Ok(Some((journal_index, journal_entry)))
                        }
                        EnrichedEntryHeader::Awakeable { is_completed }
                        | EnrichedEntryHeader::GetState { is_completed }
                            if !is_completed =>
                        {
                            return Ok(Some((journal_index, journal_entry)))
                        }
                        EnrichedEntryHeader::Sleep { is_completed } if !is_completed => {
                            return Ok(Some((journal_index, journal_entry)))
                        }
                        header => {
                            assert!(
                                header.is_completed().unwrap_or(true),
                                "All non canceled journal entries must be completed."
                            );
                        }
                    }
                }

                Ok(None)
            })
            .try_collect()
            .await?;

        let canceled_result = CompletionResult::from(&CANCELED_INVOCATION_ERROR);

        let mut resume_invocation = false;

        for (journal_index, journal_entry) in journal_entries_to_cancel {
            let (header, entry) = journal_entry.into_inner();
            match header {
                // cancel uncompleted invocations
                EnrichedEntryHeader::Call {
                    enrichment_result: Some(enrichment_result),
                    ..
                } => {
                    self.handle_outgoing_message(
                        ctx,
                        OutboxMessage::InvocationTermination(InvocationTermination::cancel(
                            enrichment_result.invocation_id,
                        )),
                    )
                    .await?;
                }
                EnrichedEntryHeader::Sleep { is_completed } if !is_completed => {
                    resume_invocation |= Self::cancel_journal_entry_with(
                        ctx,
                        invocation_id,
                        &invocation_status,
                        journal_index,
                        canceled_result.clone(),
                    )
                    .await?;

                    // For the sleep, we also delete the associated timer
                    let_assert!(
                        Entry::Sleep(SleepEntry { wake_up_time, .. }) =
                            ProtobufRawEntryCodec::deserialize(EntryType::Sleep, entry)?
                    );

                    let (timer_key, _) =
                        Timer::complete_journal_entry(wake_up_time, invocation_id, journal_index);

                    Self::do_delete_timer(ctx, timer_key).await?;
                }
                _ => {
                    resume_invocation |= Self::cancel_journal_entry_with(
                        ctx,
                        invocation_id,
                        &invocation_status,
                        journal_index,
                        canceled_result.clone(),
                    )
                    .await?;
                }
            }
        }

        Ok(resume_invocation)
    }

    async fn cancel_journal_entry_with<State: JournalTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        invocation_status: &InvocationStatusProjection,
        journal_index: EntryIndex,
        canceled_result: CompletionResult,
    ) -> Result<bool, Error> {
        match invocation_status {
            InvocationStatusProjection::Invoked => {
                Self::handle_completion_for_invoked(
                    ctx,
                    invocation_id,
                    Completion::new(journal_index, canceled_result),
                )
                .await?;
                Ok(false)
            }
            InvocationStatusProjection::Suspended(waiting_for_completed_entry) => {
                Self::handle_completion_for_suspended(
                    ctx,
                    invocation_id,
                    Completion::new(journal_index, canceled_result),
                    waiting_for_completed_entry,
                )
                .await
            }
        }
    }

    async fn try_purge_invocation<
        State: InvocationStatusTable
            + IdempotencyTable
            + VirtualObjectStatusTable
            + StateTable
            + PromiseTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
    ) -> Result<(), Error> {
        match ctx.get_invocation_status(&invocation_id).await? {
            InvocationStatus::Completed(CompletedInvocation {
                invocation_target,
                idempotency_key,
                ..
            }) => {
                Self::do_free_invocation(ctx, invocation_id).await;

                // Also cleanup the associated idempotency key if any
                if let Some(idempotency_key) = idempotency_key {
                    Self::do_delete_idempotency_id(
                        ctx,
                        IdempotencyId::combine(invocation_id, &invocation_target, idempotency_key),
                    )
                    .await?;
                }

                // For workflow, we should also clean up the service lock, associated state and promises.
                if invocation_target.invocation_target_ty()
                    == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
                {
                    let service_id = invocation_target
                        .as_keyed_service_id()
                        .expect("Workflow methods must have keyed service id");

                    Self::do_unlock_service(ctx, service_id.clone()).await?;
                    Self::do_clear_all_state(
                        ctx,
                        service_id.clone(),
                        invocation_id,
                        ServiceInvocationSpanContext::empty(),
                    )
                    .await?;
                    Self::do_clear_all_promises(ctx, service_id).await?;
                }
            }
            InvocationStatus::Free => {
                trace!("Received purge command for unknown invocation with id '{invocation_id}'.");
                // Nothing to do
            }
            _ => {
                trace!(
                    "Ignoring purge command as the invocation '{invocation_id}' is still ongoing."
                );
            }
        };

        Ok(())
    }

    async fn on_timer<
        State: IdempotencyTable
            + InvocationStatusTable
            + OutboxTable
            + FsmTable
            + VirtualObjectStatusTable
            + TimerTable
            + InboxTable
            + FsmTable
            + JournalTable
            + TimerTable
            + PromiseTable
            + StateTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        timer_value: TimerKeyValue,
    ) -> Result<(), Error> {
        let (key, value) = timer_value.into_inner();
        Self::do_delete_timer(ctx, key).await?;

        match value {
            Timer::CompleteJournalEntry(invocation_id, entry_index) => {
                Self::handle_completion(
                    ctx,
                    invocation_id,
                    Completion {
                        entry_index,
                        result: CompletionResult::Empty,
                    },
                )
                .await
            }
            Timer::Invoke(mut service_invocation) => {
                // Remove the execution time from the service invocation request
                service_invocation.execution_time = None;

                // ServiceInvocations scheduled with a timer are always owned by the same partition processor
                // where the invocation should be executed
                self.on_service_invocation(ctx, service_invocation).await
            }
            Timer::CleanInvocationStatus(invocation_id) => {
                self.try_purge_invocation(ctx, invocation_id).await
            }
            Timer::NeoInvoke(invocation_id) => self.on_neo_invoke_timer(ctx, invocation_id).await,
        }
    }

    async fn on_neo_invoke_timer<
        State: VirtualObjectStatusTable + InvocationStatusTable + InboxTable + FsmTable + JournalTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            "Handle scheduled invocation timer with invocation id {invocation_id}"
        );
        let invocation_status = ctx.get_invocation_status(&invocation_id).await?;

        if let InvocationStatus::Free = &invocation_status {
            warn!("Fired a timer for an unknown invocation. The invocation might have been deleted/purged previously.");
            return Ok(());
        }

        let_assert!(
            InvocationStatus::Scheduled(scheduled_invocation) = invocation_status,
            "Invocation {} should be in scheduled status",
            invocation_id
        );

        // Scheduled invocations have been deduplicated already in on_service_invocation, and they already sent back the submit notification.

        // 3. Check if we need to inbox it (only for exclusive methods of virtual objects)
        let Some(pre_flight_invocation_metadata) = self
            .handle_service_invocation_exclusive_handler(
                ctx,
                invocation_id,
                scheduled_invocation.metadata,
            )
            .await?
        else {
            // Invocation was inboxed, nothing else to do here
            return Ok(());
        };

        // 4. Execute it
        let (in_flight_invocation_metadata, invocation_input) =
            InFlightInvocationMetadata::from_pre_flight_invocation_metadata(
                pre_flight_invocation_metadata,
            );

        Self::init_journal_and_invoke(
            ctx,
            invocation_id,
            in_flight_invocation_metadata,
            invocation_input,
        )
        .await
    }

    async fn try_invoker_effect<
        State: InvocationStatusTable
            + JournalTable
            + StateTable
            + PromiseTable
            + OutboxTable
            + FsmTable
            + TimerTable
            + InboxTable
            + VirtualObjectStatusTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invoker_effect: InvokerEffect,
    ) -> Result<(), Error> {
        let start = Instant::now();
        let status = ctx
            .get_invocation_status(&invoker_effect.invocation_id)
            .await?;

        match status {
            InvocationStatus::Invoked(invocation_metadata) => {
                self.on_invoker_effect(ctx, invoker_effect, invocation_metadata)
                    .await?
            }
            _ => {
                trace!("Received invoker effect for unknown service invocation. Ignoring the effect and aborting.");
                Self::do_send_abort_invocation_to_invoker(ctx, invoker_effect.invocation_id);
            }
        };
        self.latency.record(start.elapsed());

        Ok(())
    }

    async fn on_invoker_effect<
        State: InvocationStatusTable
            + JournalTable
            + StateTable
            + PromiseTable
            + OutboxTable
            + FsmTable
            + TimerTable
            + InboxTable
            + VirtualObjectStatusTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        InvokerEffect {
            invocation_id,
            kind,
        }: InvokerEffect,
        invocation_metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error> {
        match kind {
            InvokerEffectKind::PinnedDeployment(pinned_deployment) => {
                Self::do_store_pinned_deployment(
                    ctx,
                    invocation_id,
                    pinned_deployment,
                    invocation_metadata,
                )
                .await;
            }
            InvokerEffectKind::JournalEntry { entry_index, entry } => {
                self.handle_journal_entry(
                    ctx,
                    invocation_id,
                    entry_index,
                    entry,
                    invocation_metadata,
                )
                .await?;
            }
            InvokerEffectKind::Suspended {
                waiting_for_completed_entries,
            } => {
                debug_assert!(
                    !waiting_for_completed_entries.is_empty(),
                    "Expecting at least one entry on which the invocation {invocation_id} is waiting."
                );
                let mut any_completed = false;
                for entry_index in &waiting_for_completed_entries {
                    if ctx
                        .storage
                        .get_journal_entry(&invocation_id, *entry_index)
                        .await?
                        .map(|entry| entry.is_resumable())
                        .unwrap_or_default()
                    {
                        trace!(
                            rpc.service = %invocation_metadata.invocation_target.service_name(),
                            rectx.storage.invocation.id = %invocation_id,
                            "Resuming instead of suspending service because an awaited entry is completed/acked.");
                        any_completed = true;
                        break;
                    }
                }
                if any_completed {
                    Self::do_resume_service(ctx, invocation_id, invocation_metadata).await?;
                } else {
                    Self::do_suspend_service(
                        ctx,
                        invocation_id,
                        invocation_metadata,
                        waiting_for_completed_entries,
                    )
                    .await;
                }
            }
            InvokerEffectKind::End => {
                self.end_invocation(ctx, invocation_id, invocation_metadata)
                    .await?;
            }
            InvokerEffectKind::Failed(e) => {
                self.fail_invocation(ctx, invocation_id, invocation_metadata, e)
                    .await?;
            }
        }

        Ok(())
    }

    async fn end_invocation<
        State: InboxTable
            + VirtualObjectStatusTable
            + JournalTable
            + OutboxTable
            + FsmTable
            + InvocationStatusTable
            + StateTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        invocation_metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error> {
        let journal_length = invocation_metadata.journal_metadata.length;
        let completion_retention_time = invocation_metadata.completion_retention_duration;

        self.notify_invocation_result(
            ctx,
            invocation_id,
            invocation_metadata.invocation_target.clone(),
            invocation_metadata.journal_metadata.span_context.clone(),
            unsafe { invocation_metadata.timestamps.creation_time() },
            Ok(()),
        );

        // Pop from inbox
        Self::consume_inbox(ctx, &invocation_metadata.invocation_target).await?;

        // If there are any response sinks, or we need to store back the completed status,
        //  we need to find the latest output entry
        if !invocation_metadata.response_sinks.is_empty() || !completion_retention_time.is_zero() {
            let result = if let Some(output_entry) = self
                .read_last_output_entry(ctx, &invocation_id, journal_length)
                .await?
            {
                ResponseResult::from(output_entry.result)
            } else {
                // We don't panic on this, although it indicates a bug at the moment.
                warn!("Invocation completed without an output entry. This is not supported yet.");
                return Ok(());
            };

            // Send responses out
            self.send_response_to_sinks(
                ctx,
                invocation_metadata.response_sinks.clone(),
                result.clone(),
                Some(invocation_id),
                Some(&invocation_metadata.invocation_target),
            )
            .await?;

            // Store the completed status, if needed
            if !completion_retention_time.is_zero() {
                let (completed_invocation, completion_retention_time) =
                    CompletedInvocation::from_in_flight_invocation_metadata(
                        invocation_metadata,
                        result,
                    );
                Self::do_store_completed_invocation(
                    ctx,
                    invocation_id,
                    completion_retention_time,
                    completed_invocation,
                )
                .await;
            }
        }

        // If no retention, immediately cleanup the invocation status
        if completion_retention_time.is_zero() {
            Self::do_free_invocation(ctx, invocation_id).await;
        }
        Self::do_drop_journal(ctx, invocation_id, journal_length).await;

        Ok(())
    }

    async fn fail_invocation<
        State: InboxTable
            + VirtualObjectStatusTable
            + InvocationStatusTable
            + VirtualObjectStatusTable
            + StateTable
            + JournalTable
            + OutboxTable
            + FsmTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        invocation_metadata: InFlightInvocationMetadata,
        error: InvocationError,
    ) -> Result<(), Error> {
        let journal_length = invocation_metadata.journal_metadata.length;

        self.notify_invocation_result(
            ctx,
            invocation_id,
            invocation_metadata.invocation_target.clone(),
            invocation_metadata.journal_metadata.span_context.clone(),
            unsafe { invocation_metadata.timestamps.creation_time() },
            Err((error.code(), error.to_string())),
        );

        let response_result = ResponseResult::from(error);

        // Send responses out
        self.send_response_to_sinks(
            ctx,
            invocation_metadata.response_sinks.clone(),
            response_result.clone(),
            Some(invocation_id),
            Some(&invocation_metadata.invocation_target),
        )
        .await?;

        // Pop from inbox
        Self::consume_inbox(ctx, &invocation_metadata.invocation_target).await?;

        // Store the completed status or free it
        if !invocation_metadata.completion_retention_duration.is_zero() {
            let (completed_invocation, completion_retention_time) =
                CompletedInvocation::from_in_flight_invocation_metadata(
                    invocation_metadata,
                    response_result,
                );
            Self::do_store_completed_invocation(
                ctx,
                invocation_id,
                completion_retention_time,
                completed_invocation,
            )
            .await;
        } else {
            Self::do_free_invocation(ctx, invocation_id).await;
        }

        Self::do_drop_journal(ctx, invocation_id, journal_length).await;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_response_to_sinks<State: OutboxTable + FsmTable>(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        response_sinks: impl IntoIterator<Item = ServiceInvocationResponseSink>,
        res: impl Into<ResponseResult>,
        invocation_id: Option<InvocationId>,
        invocation_target: Option<&InvocationTarget>,
    ) -> Result<(), Error> {
        let result = res.into();
        for response_sink in response_sinks {
            match response_sink {
                ServiceInvocationResponseSink::PartitionProcessor {
                    entry_index,
                    caller,
                } => self.handle_outgoing_message(ctx, OutboxMessage::ServiceResponse(InvocationResponse {
                    id: caller,
                    entry_index,
                    result: result.clone(),
                })).await?,
                ServiceInvocationResponseSink::Ingress { node_id, request_id } => {
                    Self::send_ingress_response(ctx, IngressResponseEnvelope{ target_node: node_id, inner: ingress::InvocationResponse {
                        request_id,
                        invocation_id,
                        response: match result.clone() {
                            ResponseResult::Success(res) => {
                                IngressResponseResult::Success(invocation_target.expect("For success responses, there must be an invocation target!").clone(), res)
                            }
                            ResponseResult::Failure(err) => {
                                IngressResponseResult::Failure(err)
                            }
                        },
                    } })
                }
            }
        }
        Ok(())
    }

    async fn consume_inbox<
        State: InboxTable
            + VirtualObjectStatusTable
            + InvocationStatusTable
            + VirtualObjectStatusTable
            + StateTable
            + JournalTable,
    >(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_target: &InvocationTarget,
    ) -> Result<(), Error> {
        // Inbox exists only for virtual object exclusive handler cases
        if invocation_target.invocation_target_ty()
            == InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive)
        {
            let keyed_service_id = invocation_target.as_keyed_service_id().expect(
                "When the handler type is Exclusive, the invocation target must have a key",
            );

            debug_if_leader!(
                ctx.is_leader,
                rpc.service = %keyed_service_id,
                "Consume inbox"
            );

            // Pop until we find the first inbox entry.
            // Note: the inbox seq numbers can have gaps.
            while let Some(inbox_entry) = ctx.storage.pop_inbox(&keyed_service_id).await? {
                match inbox_entry.inbox_entry {
                    InboxEntry::Invocation(_, invocation_id) => {
                        let inboxed_status = ctx.get_invocation_status(&invocation_id).await?;

                        let_assert!(
                            InvocationStatus::Inboxed(inboxed_invocation) = inboxed_status,
                            "InvocationStatus must contain an Inboxed invocation for the id {}",
                            invocation_id
                        );

                        debug_if_leader!(
                            ctx.is_leader,
                            rpc.service = %keyed_service_id,
                            "Invoke inboxed"
                        );

                        // Lock the service
                        ctx.storage
                            .put_virtual_object_status(
                                &keyed_service_id,
                                &VirtualObjectStatus::Locked(invocation_id),
                            )
                            .await;

                        let (in_flight_invocation_meta, invocation_input) =
                            InFlightInvocationMetadata::from_inboxed_invocation(inboxed_invocation);
                        Self::init_journal_and_invoke(
                            ctx,
                            invocation_id,
                            in_flight_invocation_meta,
                            invocation_input,
                        )
                        .await?;

                        // Started a new invocation
                        return Ok(());
                    }
                    InboxEntry::StateMutation(state_mutation) => {
                        Self::mutate_state(ctx.storage, state_mutation).await?;
                    }
                }
            }

            // We consumed the inbox, nothing else to do here
            ctx.storage
                .put_virtual_object_status(&keyed_service_id, &VirtualObjectStatus::Unlocked)
                .await;
        }

        Ok(())
    }

    async fn handle_journal_entry<
        State: StateTable
            + PromiseTable
            + OutboxTable
            + FsmTable
            + TimerTable
            + JournalTable
            + InvocationStatusTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        mut journal_entry: EnrichedRawEntry,
        invocation_metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error> {
        debug_assert_eq!(
            entry_index, invocation_metadata.journal_metadata.length,
            "Expect to receive next journal entry for {invocation_id}"
        );

        match journal_entry.header() {
            // nothing to do
            EnrichedEntryHeader::Input { .. } => {}
            EnrichedEntryHeader::Output { .. } => {
                // Just store it, on End we send back the responses
            }
            EnrichedEntryHeader::GetState { is_completed, .. } => {
                if !is_completed {
                    let_assert!(
                        Entry::GetState(GetStateEntry { key, .. }) =
                            journal_entry.deserialize_entry_ref::<Codec>()?
                    );

                    if let Some(service_id) =
                        invocation_metadata.invocation_target.as_keyed_service_id()
                    {
                        // Load state and write completion
                        let value = ctx.storage.get_user_state(&service_id, &key).await?;
                        let completion_result = value
                            .map(CompletionResult::Success)
                            .unwrap_or(CompletionResult::Empty);
                        Codec::write_completion(&mut journal_entry, completion_result.clone())?;

                        Self::do_forward_completion(
                            ctx,
                            invocation_id,
                            Completion::new(entry_index, completion_result),
                        );
                    } else {
                        warn!(
                            "Trying to process entry {} for a target that has no state",
                            journal_entry.header().as_entry_type()
                        );
                        Self::do_forward_completion(
                            ctx,
                            invocation_id,
                            Completion::new(entry_index, CompletionResult::Empty),
                        );
                    }
                }
            }
            EnrichedEntryHeader::SetState { .. } => {
                let_assert!(
                    Entry::SetState(SetStateEntry { key, value }) =
                        journal_entry.deserialize_entry_ref::<Codec>()?
                );

                if let Some(service_id) =
                    invocation_metadata.invocation_target.as_keyed_service_id()
                {
                    Self::do_set_state(
                        ctx,
                        service_id,
                        invocation_id,
                        invocation_metadata.journal_metadata.span_context.clone(),
                        key,
                        value,
                    )
                    .await;
                } else {
                    warn!(
                        "Trying to process entry {} for a target that has no state",
                        journal_entry.header().as_entry_type()
                    );
                }
            }
            EnrichedEntryHeader::ClearState { .. } => {
                let_assert!(
                    Entry::ClearState(ClearStateEntry { key }) =
                        journal_entry.deserialize_entry_ref::<Codec>()?
                );

                if let Some(service_id) =
                    invocation_metadata.invocation_target.as_keyed_service_id()
                {
                    Self::do_clear_state(
                        ctx,
                        service_id,
                        invocation_id,
                        invocation_metadata.journal_metadata.span_context.clone(),
                        key,
                    )
                    .await;
                } else {
                    warn!(
                        "Trying to process entry {} for a target that has no state",
                        journal_entry.header().as_entry_type()
                    );
                }
            }
            EnrichedEntryHeader::ClearAllState { .. } => {
                if let Some(service_id) =
                    invocation_metadata.invocation_target.as_keyed_service_id()
                {
                    Self::do_clear_all_state(
                        ctx,
                        service_id,
                        invocation_id,
                        invocation_metadata.journal_metadata.span_context.clone(),
                    )
                    .await?;
                } else {
                    warn!(
                        "Trying to process entry {} for a target that has no state",
                        journal_entry.header().as_entry_type()
                    );
                }
            }
            EnrichedEntryHeader::GetStateKeys { is_completed, .. } => {
                if !is_completed {
                    // Load state and write completion
                    let value = if let Some(service_id) =
                        invocation_metadata.invocation_target.as_keyed_service_id()
                    {
                        ctx.storage
                            .get_all_user_states_for_service(&service_id)
                            .map(|res| res.map(|v| v.0))
                            .try_collect()
                            .await?
                    } else {
                        warn!(
                            "Trying to process entry {} for a target that has no state",
                            journal_entry.header().as_entry_type()
                        );
                        vec![]
                    };

                    let completion_result = Codec::serialize_get_state_keys_completion(value);
                    Codec::write_completion(&mut journal_entry, completion_result.clone())?;

                    // We can already forward the completion
                    Self::do_forward_completion(
                        ctx,
                        invocation_id,
                        Completion::new(entry_index, completion_result),
                    );
                }
            }
            EnrichedEntryHeader::GetPromise { is_completed, .. } => {
                if !is_completed {
                    let_assert!(
                        Entry::GetPromise(GetPromiseEntry { key, .. }) =
                            journal_entry.deserialize_entry_ref::<Codec>()?
                    );

                    if let Some(service_id) =
                        invocation_metadata.invocation_target.as_keyed_service_id()
                    {
                        // Load state and write completion
                        let promise_metadata = ctx.storage.get_promise(&service_id, &key).await?;

                        match promise_metadata {
                            Some(Promise {
                                state: PromiseState::Completed(result),
                            }) => {
                                // Result is already available
                                let completion_result: CompletionResult = result.into();
                                Codec::write_completion(
                                    &mut journal_entry,
                                    completion_result.clone(),
                                )?;

                                // Forward completion
                                Self::do_forward_completion(
                                    ctx,
                                    invocation_id,
                                    Completion::new(entry_index, completion_result),
                                );
                            }
                            Some(Promise {
                                state: PromiseState::NotCompleted(mut v),
                            }) => {
                                v.push(JournalEntryId::from_parts(invocation_id, entry_index));
                                Self::do_put_promise(
                                    ctx,
                                    service_id,
                                    key,
                                    Promise {
                                        state: PromiseState::NotCompleted(v),
                                    },
                                )
                                .await;
                            }
                            None => {
                                Self::do_put_promise(
                                    ctx,
                                    service_id,
                                    key,
                                    Promise {
                                        state: PromiseState::NotCompleted(vec![
                                            JournalEntryId::from_parts(invocation_id, entry_index),
                                        ]),
                                    },
                                )
                                .await
                            }
                        }
                    } else {
                        warn!(
                            "Trying to process entry {} for a target that has no promises",
                            journal_entry.header().as_entry_type()
                        );
                        Self::do_forward_completion(
                            ctx,
                            invocation_id,
                            Completion::new(entry_index, CompletionResult::Success(Bytes::new())),
                        );
                    }
                }
            }
            EnrichedEntryHeader::PeekPromise { is_completed, .. } => {
                if !is_completed {
                    let_assert!(
                        Entry::PeekPromise(PeekPromiseEntry { key, .. }) =
                            journal_entry.deserialize_entry_ref::<Codec>()?
                    );

                    if let Some(service_id) =
                        invocation_metadata.invocation_target.as_keyed_service_id()
                    {
                        // Load state and write completion
                        let promise_metadata = ctx.storage.get_promise(&service_id, &key).await?;

                        let completion_result = match promise_metadata {
                            Some(Promise {
                                state: PromiseState::Completed(result),
                            }) => result.into(),
                            _ => CompletionResult::Empty,
                        };

                        Codec::write_completion(&mut journal_entry, completion_result.clone())?;

                        // Forward completion
                        Self::do_forward_completion(
                            ctx,
                            invocation_id,
                            Completion::new(entry_index, completion_result),
                        );
                    } else {
                        warn!(
                            "Trying to process entry {} for a target that has no promises",
                            journal_entry.header().as_entry_type()
                        );
                        Self::do_forward_completion(
                            ctx,
                            invocation_id,
                            Completion::new(entry_index, CompletionResult::Empty),
                        );
                    }
                }
            }
            EnrichedEntryHeader::CompletePromise { is_completed, .. } => {
                if !is_completed {
                    let_assert!(
                        Entry::CompletePromise(CompletePromiseEntry {
                            key,
                            completion,
                            ..
                        }) = journal_entry.deserialize_entry_ref::<Codec>()?
                    );

                    if let Some(service_id) =
                        invocation_metadata.invocation_target.as_keyed_service_id()
                    {
                        // Load state and write completion
                        let promise_metadata = ctx.storage.get_promise(&service_id, &key).await?;

                        let completion_result = match promise_metadata {
                            None => {
                                // Just register the promise completion
                                Self::do_put_promise(
                                    ctx,
                                    service_id,
                                    key,
                                    Promise {
                                        state: PromiseState::Completed(completion),
                                    },
                                )
                                .await;
                                CompletionResult::Empty
                            }
                            Some(Promise {
                                state: PromiseState::NotCompleted(listeners),
                            }) => {
                                // Send response to listeners
                                for listener in listeners {
                                    self.handle_outgoing_message(
                                        ctx,
                                        OutboxMessage::ServiceResponse(InvocationResponse {
                                            id: listener.invocation_id(),
                                            entry_index: listener.journal_index(),
                                            result: completion.clone().into(),
                                        }),
                                    )
                                    .await?;
                                }

                                // Now register the promise completion
                                Self::do_put_promise(
                                    ctx,
                                    service_id,
                                    key,
                                    Promise {
                                        state: PromiseState::Completed(completion),
                                    },
                                )
                                .await;
                                CompletionResult::Empty
                            }
                            Some(Promise {
                                state: PromiseState::Completed(_),
                            }) => {
                                // Conflict!
                                (&ALREADY_COMPLETED_INVOCATION_ERROR).into()
                            }
                        };

                        Codec::write_completion(&mut journal_entry, completion_result.clone())?;

                        // Forward completion
                        Self::do_forward_completion(
                            ctx,
                            invocation_id,
                            Completion::new(entry_index, completion_result),
                        );
                    } else {
                        warn!(
                            "Trying to process entry {} for a target that has no promises",
                            journal_entry.header().as_entry_type()
                        );
                        Self::do_forward_completion(
                            ctx,
                            invocation_id,
                            Completion::new(entry_index, CompletionResult::Empty),
                        );
                    }
                }
            }
            EnrichedEntryHeader::Sleep { is_completed, .. } => {
                debug_assert!(!is_completed, "Sleep entry must not be completed.");
                let_assert!(
                    Entry::Sleep(SleepEntry { wake_up_time, .. }) =
                        journal_entry.deserialize_entry_ref::<Codec>()?
                );
                Self::register_timer(
                    ctx,
                    TimerKeyValue::complete_journal_entry(
                        MillisSinceEpoch::new(wake_up_time),
                        invocation_id,
                        entry_index,
                    ),
                    invocation_metadata.journal_metadata.span_context.clone(),
                )
                .await?;
            }
            EnrichedEntryHeader::Call {
                enrichment_result, ..
            } => {
                if let Some(CallEnrichmentResult {
                    span_context,
                    invocation_id: callee_invocation_id,
                    invocation_target: callee_invocation_target,
                    completion_retention_time,
                }) = enrichment_result
                {
                    let_assert!(
                        Entry::Call(InvokeEntry { request, .. }) =
                            journal_entry.deserialize_entry_ref::<Codec>()?
                    );

                    let service_invocation = ServiceInvocation {
                        invocation_id: *callee_invocation_id,
                        invocation_target: callee_invocation_target.clone(),
                        argument: request.parameter,
                        source: Source::Service(
                            invocation_id,
                            invocation_metadata.invocation_target.clone(),
                        ),
                        response_sink: Some(ServiceInvocationResponseSink::partition_processor(
                            invocation_id,
                            entry_index,
                        )),
                        span_context: span_context.clone(),
                        headers: request.headers,
                        execution_time: None,
                        completion_retention_duration: *completion_retention_time,
                        idempotency_key: None,
                        submit_notification_sink: None,
                    };

                    self.handle_outgoing_message(
                        ctx,
                        OutboxMessage::ServiceInvocation(service_invocation),
                    )
                    .await?;
                } else {
                    // no action needed for an invoke entry that has been completed by the deployment
                }
            }
            EnrichedEntryHeader::OneWayCall {
                enrichment_result, ..
            } => {
                let CallEnrichmentResult {
                    invocation_id: callee_invocation_id,
                    invocation_target: callee_invocation_target,
                    span_context,
                    completion_retention_time,
                } = enrichment_result;

                let_assert!(
                    Entry::OneWayCall(OneWayCallEntry {
                        request,
                        invoke_time
                    }) = journal_entry.deserialize_entry_ref::<Codec>()?
                );

                // 0 is equal to not set, meaning execute now
                let delay = if invoke_time == 0 {
                    None
                } else {
                    Some(MillisSinceEpoch::new(invoke_time))
                };

                let service_invocation = ServiceInvocation {
                    invocation_id: *callee_invocation_id,
                    invocation_target: callee_invocation_target.clone(),
                    argument: request.parameter,
                    source: Source::Service(
                        invocation_id,
                        invocation_metadata.invocation_target.clone(),
                    ),
                    response_sink: None,
                    span_context: span_context.clone(),
                    headers: request.headers,
                    execution_time: delay,
                    completion_retention_duration: *completion_retention_time,
                    idempotency_key: None,
                    submit_notification_sink: None,
                };

                let pointer_span_id = match span_context.span_cause() {
                    Some(SpanRelationCause::Linked(_, span_id)) => Some(*span_id),
                    _ => None,
                };

                Self::do_trace_background_invoke(
                    ctx,
                    (*callee_invocation_id, callee_invocation_target.clone()),
                    invocation_metadata.journal_metadata.span_context.clone(),
                    pointer_span_id,
                );

                self.handle_outgoing_message(
                    ctx,
                    OutboxMessage::ServiceInvocation(service_invocation),
                )
                .await?;
            }
            EnrichedEntryHeader::Awakeable { is_completed, .. } => {
                debug_assert!(!is_completed, "Awakeable entry must not be completed.");
                // Check the awakeable_completion_received_before_entry test in state_machine/server for more details

                // If completion is already here, let's merge it and forward it.
                if let Some(completion_result) = ctx
                    .storage
                    .get_journal_entry(&invocation_id, entry_index)
                    .await?
                    .and_then(|journal_entry| match journal_entry {
                        JournalEntry::Entry(_) => None,
                        JournalEntry::Completion(completion_result) => Some(completion_result),
                    })
                {
                    Codec::write_completion(&mut journal_entry, completion_result.clone())?;

                    Self::do_forward_completion(
                        ctx,
                        invocation_id,
                        Completion::new(entry_index, completion_result),
                    );
                }
            }
            EnrichedEntryHeader::CompleteAwakeable {
                enrichment_result:
                    AwakeableEnrichmentResult {
                        invocation_id,
                        entry_index,
                    },
                ..
            } => {
                let_assert!(
                    Entry::CompleteAwakeable(entry) =
                        journal_entry.deserialize_entry_ref::<Codec>()?
                );

                self.handle_outgoing_message(
                    ctx,
                    OutboxMessage::from_awakeable_completion(
                        *invocation_id,
                        *entry_index,
                        entry.result.into(),
                    ),
                )
                .await?;
            }
            EnrichedEntryHeader::Run { .. } | EnrichedEntryHeader::Custom { .. } => {
                // We just store it
            }
        }

        Self::append_journal_entry(
            ctx,
            invocation_id,
            InvocationStatus::Invoked(invocation_metadata),
            entry_index,
            &JournalEntry::Entry(journal_entry),
        )
        .await;
        ctx.action_collector.push(Action::AckStoredEntry {
            invocation_id,
            entry_index,
        });

        Ok(())
    }

    async fn handle_completion<State: JournalTable + InvocationStatusTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        completion: Completion,
    ) -> Result<(), Error> {
        let status = ctx.get_invocation_status(&invocation_id).await?;

        match status {
            InvocationStatus::Invoked(_) => {
                Self::handle_completion_for_invoked(ctx, invocation_id, completion).await?;
            }
            InvocationStatus::Suspended {
                metadata,
                waiting_for_completed_entries,
            } => {
                if Self::handle_completion_for_suspended(
                    ctx,
                    invocation_id,
                    completion,
                    &waiting_for_completed_entries,
                )
                .await?
                {
                    Self::do_resume_service(ctx, invocation_id, metadata).await?;
                }
            }
            _ => {
                debug!(
                    rectx.storage.invocation.id = %invocation_id,
                    ?completion,
                    "Ignoring completion for invocation that is no longer running."
                )
            }
        }

        Ok(())
    }

    async fn handle_completion_for_suspended<State: JournalTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        completion: Completion,
        waiting_for_completed_entries: &HashSet<EntryIndex>,
    ) -> Result<bool, Error> {
        let resume_invocation = waiting_for_completed_entries.contains(&completion.entry_index);
        Self::do_store_completion(ctx, invocation_id, completion).await?;

        Ok(resume_invocation)
    }

    async fn handle_completion_for_invoked<State: JournalTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        completion: Completion,
    ) -> Result<(), Error> {
        Self::do_store_completion(ctx, invocation_id, completion.clone()).await?;
        Self::do_forward_completion(ctx, invocation_id, completion);
        Ok(())
    }

    async fn read_last_output_entry<State: ReadOnlyJournalTable>(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> Result<Option<OutputEntry>, Error> {
        // Find last output entry
        let mut output_entry = None;
        for i in (0..journal_length).rev() {
            if let JournalEntry::Entry(e) = ctx
                .storage
                .get_journal_entry(invocation_id, i)
                .await?
                .unwrap_or_else(|| panic!("There should be a journal entry at index {}", i))
            {
                if e.ty() == EntryType::Output {
                    output_entry = Some(e);
                    break;
                }
            }
        }

        output_entry
            .map(|enriched_entry| {
                let_assert!(Entry::Output(e) = enriched_entry.deserialize_entry_ref::<Codec>()?);
                Ok(e)
            })
            .transpose()
    }

    fn notify_invocation_result<State>(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        span_context: ServiceInvocationSpanContext,
        creation_time: MillisSinceEpoch,
        result: Result<(), (InvocationErrorCode, String)>,
    ) {
        let (result, error) = match result {
            Ok(_) => ("Success", false),
            Err(_) => ("Failure", true),
        };

        info_span_if_leader!(
            ctx.is_leader,
            span_context.is_sampled(),
            span_context.causing_span_relation(),
            "invoke",
            otel.name = format!("invoke {invocation_target}"),
            rpc.service = %invocation_target.service_name(),
            rpc.method = %invocation_target.handler_name(),
            restate.invocation.id = %invocation_id,
            restate.invocation.target = %invocation_target,
            restate.invocation.result = result,
            error = error, // jaeger uses this tag to show an error icon
            // without converting to i64 this field will encode as a string
            // however, overflowing i64 seems unlikely
            restate.internal.start_time = i64::try_from(creation_time.as_u64()).expect("creation time should fit into i64"),
            restate.internal.span_id = %span_context.span_context().span_id(),
            restate.internal.trace_id = %span_context.span_context().trace_id()
        );
    }

    async fn handle_outgoing_message<State: OutboxTable + FsmTable>(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        message: OutboxMessage,
    ) -> Result<(), Error> {
        // TODO Here we could add an optimization to immediately execute outbox message command
        //  for partition_key within the range of this PP, but this is problematic due to how we tie
        //  the effects buffer with tracing. Once we solve that, we could implement that by roughly uncommenting this code :)
        //  if self.partition_key_range.contains(&message.partition_key()) {
        //             // We can process this now!
        //             let command = message.to_command();
        //             return self.on_apply(
        //                 command,
        //                 effects,
        //                 state
        //             ).await
        //         }

        Self::do_enqueue_into_outbox(ctx, self.outbox_seq_number, message).await?;
        self.outbox_seq_number += 1;
        Ok(())
    }

    async fn handle_attach_invocation_request<
        State: ReadOnlyIdempotencyTable
            + InvocationStatusTable
            + ReadOnlyVirtualObjectStatusTable
            + OutboxTable
            + FsmTable,
    >(
        &mut self,
        ctx: &mut StateMachineApplyContext<'_, State>,
        attach_invocation_request: AttachInvocationRequest,
    ) -> Result<(), Error> {
        debug_assert!(
            self.partition_key_range.contains(&attach_invocation_request.partition_key()),
            "Attach invocation request with partition key '{}' has been delivered to a partition processor with key range '{:?}'. This indicates a bug.",
            attach_invocation_request.partition_key(),
            self.partition_key_range);

        let invocation_id = match attach_invocation_request.invocation_query {
            InvocationQuery::Invocation(iid) => iid,
            InvocationQuery::Workflow(sid) => {
                match ctx.storage.get_virtual_object_status(&sid).await? {
                    VirtualObjectStatus::Locked(iid) => iid,
                    VirtualObjectStatus::Unlocked => {
                        self.send_response_to_sinks(
                            ctx,
                            vec![attach_invocation_request.response_sink],
                            NOT_FOUND_INVOCATION_ERROR,
                            None,
                            None,
                        )
                        .await?;
                        return Ok(());
                    }
                }
            }
            InvocationQuery::IdempotencyId(iid) => {
                match ctx.storage.get_idempotency_metadata(&iid).await? {
                    Some(idempotency_metadata) => idempotency_metadata.invocation_id,
                    None => {
                        self.send_response_to_sinks(
                            ctx,
                            vec![attach_invocation_request.response_sink],
                            NOT_FOUND_INVOCATION_ERROR,
                            None,
                            None,
                        )
                        .await?;
                        return Ok(());
                    }
                }
            }
        };
        match ctx.get_invocation_status(&invocation_id).await? {
            InvocationStatus::Free => {
                self.send_response_to_sinks(
                    ctx,
                    vec![attach_invocation_request.response_sink],
                    NOT_FOUND_INVOCATION_ERROR,
                    Some(invocation_id),
                    None,
                )
                .await?
            }
            is if is.idempotency_key().is_none()
                && is
                    .invocation_target()
                    .map(InvocationTarget::invocation_target_ty)
                    != Some(InvocationTargetType::Workflow(
                        WorkflowHandlerType::Workflow,
                    )) =>
            {
                self.send_response_to_sinks(
                    ctx,
                    vec![attach_invocation_request.response_sink],
                    ATTACH_NOT_SUPPORTED_INVOCATION_ERROR,
                    Some(invocation_id),
                    None,
                )
                .await?
            }
            is @ InvocationStatus::Invoked(_)
            | is @ InvocationStatus::Suspended { .. }
            | is @ InvocationStatus::Inboxed(_)
            | is @ InvocationStatus::Scheduled(_) => {
                Self::do_append_response_sink(
                    ctx,
                    invocation_id,
                    is,
                    attach_invocation_request.response_sink,
                )
                .await?;
            }
            InvocationStatus::Completed(completed) => {
                self.send_response_to_sinks(
                    ctx,
                    vec![attach_invocation_request.response_sink],
                    completed.response_result,
                    Some(invocation_id),
                    Some(&completed.invocation_target),
                )
                .await?;
            }
        }

        Ok(())
    }

    fn send_ingress_response<State>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        ingress_response: IngressResponseEnvelope<ingress::InvocationResponse>,
    ) {
        match &ingress_response.inner {
            ingress::InvocationResponse {
                response: IngressResponseResult::Success(_, _),
                request_id,
                ..
            } => debug_if_leader!(
                ctx.is_leader,
                "Send response to ingress with request id '{:?}': Success",
                request_id
            ),
            ingress::InvocationResponse {
                response: IngressResponseResult::Failure(e),
                request_id,
                ..
            } => debug_if_leader!(
                ctx.is_leader,
                "Send response to ingress with request id '{:?}': Failure({})",
                request_id,
                e
            ),
        };

        ctx.action_collector
            .push(Action::IngressResponse(ingress_response));
    }

    fn send_submit_notification_if_needed<State>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        original_invocation_id: InvocationId,
        attached_invocation_id: Option<InvocationId>,
        submit_notification_sink: Option<SubmitNotificationSink>,
    ) {
        // Notify the ingress, if needed, of the chosen invocation_id
        if let Some(SubmitNotificationSink::Ingress {
            node_id,
            request_id,
        }) = submit_notification_sink
        {
            let attached_invocation_id = attached_invocation_id.unwrap_or(original_invocation_id);

            debug_if_leader!(
                ctx.is_leader,
                "Sending ingress attach invocation {} to {}",
                original_invocation_id,
                attached_invocation_id,
            );

            ctx.action_collector
                .push(Action::IngressSubmitNotification(IngressResponseEnvelope {
                    target_node: node_id,
                    inner: ingress::SubmittedInvocationNotification {
                        request_id,
                        original_invocation_id,
                        attached_invocation_id,
                    },
                }));
        }
    }

    async fn do_resume_service<State: InvocationStatusTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        mut metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.length = metadata.journal_metadata.length,
            "Effect: Resume service"
        );

        metadata.timestamps.update();
        let invocation_target = metadata.invocation_target.clone();
        ctx.storage
            .put_invocation_status(&invocation_id, &InvocationStatus::Invoked(metadata))
            .await;

        ctx.action_collector.push(Action::Invoke {
            invocation_id,
            invocation_target,
            invoke_input_journal: InvokeInputJournal::NoCachedJournal,
        });

        Ok(())
    }

    async fn do_suspend_service<State: InvocationStatusTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        mut metadata: InFlightInvocationMetadata,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    ) {
        info_span_if_leader!(
            ctx.is_leader,
            metadata.journal_metadata.span_context.is_sampled(),
            metadata.journal_metadata.span_context.as_parent(),
            "suspend",
            restate.journal.length = metadata.journal_metadata.length,
            restate.invocation.id = %invocation_id,
        );
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.length = metadata.journal_metadata.length,
            "Effect: Suspend service waiting on entries {:?}",
            waiting_for_completed_entries
        );

        metadata.timestamps.update();
        ctx.storage
            .put_invocation_status(
                &invocation_id,
                &InvocationStatus::Suspended {
                    metadata,
                    waiting_for_completed_entries,
                },
            )
            .await;
    }

    async fn do_store_completed_invocation<State: InvocationStatusTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        retention: Duration,
        completed_invocation: CompletedInvocation,
    ) {
        debug_if_leader!(
            ctx.is_leader,
            restate.invocation.id = %invocation_id,
            "Effect: Store completed invocation"
        );

        // New table invocations are cleaned using the Cleaner task.
        if let SourceTable::Old = completed_invocation.source_table {
            ctx.action_collector
                .push(Action::ScheduleInvocationStatusCleanup {
                    invocation_id,
                    retention,
                });
        }

        ctx.storage
            .put_invocation_status(
                &invocation_id,
                &InvocationStatus::Completed(completed_invocation),
            )
            .await;
    }

    async fn do_free_invocation<State: InvocationStatusTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
    ) {
        debug_if_leader!(
            ctx.is_leader,
            restate.invocation.id = %invocation_id,
            "Effect: Free invocation"
        );

        ctx.storage
            .put_invocation_status(&invocation_id, &InvocationStatus::Free)
            .await;
    }

    async fn do_delete_inbox_entry<State: InboxTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        service_id: ServiceId,
        sequence_number: MessageIndex,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            rpc.service = %service_id.service_name,
            restate.inbox.seq = sequence_number,
            "Effect: Delete inbox entry",
        );

        ctx.storage
            .delete_inbox_entry(&service_id, sequence_number)
            .await;

        Ok(())
    }

    async fn do_enqueue_into_outbox<State: OutboxTable + FsmTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        seq_number: MessageIndex,
        message: OutboxMessage,
    ) -> Result<(), Error> {
        match &message {
            OutboxMessage::ServiceInvocation(service_invocation) => debug_if_leader!(
                ctx.is_leader,
                rpc.service = %service_invocation.invocation_target.service_name(),
                rpc.method = %service_invocation.invocation_target.handler_name(),
                restate.invocation.id = %service_invocation.invocation_id,
                restate.invocation.target = %service_invocation.invocation_target,
                restate.outbox.seq = seq_number,
                "Effect: Send service invocation to partition processor"
            ),
            OutboxMessage::ServiceResponse(InvocationResponse {
                result: ResponseResult::Success(_),
                entry_index,
                id,
            }) => {
                debug_if_leader!(
                    ctx.is_leader,
                    restate.invocation.id = %id,
                    restate.outbox.seq = seq_number,
                    "Effect: Send success response to another invocation, completing entry index {}",
                    entry_index
                )
            }
            OutboxMessage::InvocationTermination(invocation_termination) => debug_if_leader!(
                ctx.is_leader,
                restate.invocation.id = %invocation_termination.invocation_id,
                restate.outbox.seq = seq_number,
                "Effect: Send invocation termination command '{:?}' to partition processor",
                invocation_termination.flavor
            ),
            OutboxMessage::ServiceResponse(InvocationResponse {
                result: ResponseResult::Failure(e),
                entry_index,
                id,
            }) => debug_if_leader!(
                ctx.is_leader,
                restate.invocation.id = %id,
                restate.outbox.seq = seq_number,
                "Effect: Send failure '{}' response to another invocation, completing entry index {}",
                e,
                entry_index
            ),
        };

        ctx.storage.put_outbox_message(seq_number, &message).await;
        // need to store the next outbox sequence number
        ctx.storage.put_outbox_seq_number(seq_number + 1).await;

        ctx.action_collector.push(Action::NewOutboxMessage {
            seq_number,
            message,
        });

        Ok(())
    }

    async fn do_unlock_service<State: VirtualObjectStatusTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        service_id: ServiceId,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            rpc.service = %service_id.service_name,
            "Effect: Unlock service id",
        );

        ctx.storage
            .put_virtual_object_status(&service_id, &VirtualObjectStatus::Unlocked)
            .await;

        Ok(())
    }

    async fn do_set_state<State: StateTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        service_id: ServiceId,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        key: Bytes,
        value: Bytes,
    ) {
        info_span_if_leader!(
            ctx.is_leader,
            span_context.is_sampled(),
            span_context.as_parent(),
            "set_state",
            otel.name = format!("set_state {key:?}"),
            restate.state.key = ?key,
            rpc.service = %service_id.service_name,
            restate.invocation.id = %invocation_id,
        );

        debug_if_leader!(
            ctx.is_leader,
            restate.state.key = ?key,
            "Effect: Set state"
        );

        ctx.storage.put_user_state(&service_id, key, value).await;
    }

    async fn do_clear_state<State: StateTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        service_id: ServiceId,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        key: Bytes,
    ) {
        info_span_if_leader!(
            ctx.is_leader,
            span_context.is_sampled(),
            span_context.as_parent(),
            "clear_state",
            otel.name = format!("clear_state {key:?}"),
            restate.state.key = ?key,
            rpc.service = %service_id.service_name,
            restate.invocation.id = %invocation_id,
        );

        debug_if_leader!(
            ctx.is_leader,
            restate.state.key = ?key,
            "Effect: Clear state"
        );

        ctx.storage.delete_user_state(&service_id, &key).await;
    }

    async fn do_clear_all_state<State: StateTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        service_id: ServiceId,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
    ) -> Result<(), Error> {
        info_span_if_leader!(
            ctx.is_leader,
            span_context.is_sampled(),
            span_context.as_parent(),
            "clear_all_state",
            rpc.service = %service_id.service_name,
            restate.invocation.id = %invocation_id,
        );

        debug_if_leader!(ctx.is_leader, "Effect: Clear all state");

        ctx.storage.delete_all_user_state(&service_id).await?;

        Ok(())
    }

    async fn register_timer<State: TimerTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        timer_value: TimerKeyValue,
        span_context: ServiceInvocationSpanContext,
    ) -> Result<(), Error> {
        match timer_value.value() {
            Timer::CompleteJournalEntry(_, entry_index) => {
                info_span_if_leader!(
                    ctx.is_leader,
                    span_context.is_sampled(),
                    span_context.as_parent(),
                    "sleep",
                    restate.journal.index = entry_index,
                    restate.timer.wake_up_time = %timer_value.wake_up_time(),
                    restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                    // without converting to i64 this field will encode as a string
                    // however, overflowing i64 seems unlikely
                    restate.internal.end_time = i64::try_from(timer_value.wake_up_time().as_u64()).expect("wake up time should fit into i64"),
                );

                debug_if_leader!(
                    ctx.is_leader,
                    restate.journal.index = entry_index,
                    restate.timer.wake_up_time = %timer_value.wake_up_time(),
                    restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                    "Register Sleep timer"
                )
            }
            Timer::Invoke(service_invocation) => {
                // no span necessary; there will already be a background_invoke span
                debug_if_leader!(
                    ctx.is_leader,
                    rpc.service = %service_invocation.invocation_target.service_name(),
                    rpc.method = %service_invocation.invocation_target.handler_name(),
                    restate.invocation.target = %service_invocation.invocation_target,
                    restate.timer.wake_up_time = %timer_value.wake_up_time(),
                    restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                    "Register background invoke timer"
                )
            }
            Timer::NeoInvoke(invocation_id) => {
                // no span necessary; there will already be a background_invoke span
                debug_if_leader!(
                    ctx.is_leader,
                    restate.invocation.id = %invocation_id,
                    restate.timer.wake_up_time = %timer_value.wake_up_time(),
                    restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                    "Register background invoke timer"
                )
            }
            Timer::CleanInvocationStatus(_) => {
                debug_if_leader!(
                    ctx.is_leader,
                    restate.timer.wake_up_time = %timer_value.wake_up_time(),
                    restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                    "Register cleanup invocation status timer"
                )
            }
        };

        ctx.storage
            .put_timer(timer_value.key(), timer_value.value())
            .await;

        ctx.action_collector
            .push(Action::RegisterTimer { timer_value });

        Ok(())
    }

    async fn do_delete_timer<State: TimerTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        timer_key: TimerKey,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.timer.key = %TimerKeyDisplay(&timer_key),
            "Effect: Delete timer"
        );

        ctx.storage.delete_timer(&timer_key).await;
        ctx.action_collector.push(Action::DeleteTimer { timer_key });

        Ok(())
    }

    async fn do_store_pinned_deployment<State: InvocationStatusTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        pinned_deployment: PinnedDeployment,
        mut metadata: InFlightInvocationMetadata,
    ) {
        debug_if_leader!(
            ctx.is_leader,
            restate.deployment.id = %pinned_deployment.deployment_id,
            restate.deployment.service_protocol_version = %pinned_deployment.service_protocol_version.as_repr(),
            "Effect: Store chosen deployment to storage"
        );

        metadata.set_pinned_deployment(pinned_deployment);

        // We recreate the InvocationStatus in Invoked state as the invoker can notify the
        // chosen deployment_id only when the invocation is in-flight.
        ctx.storage
            .put_invocation_status(&invocation_id, &InvocationStatus::Invoked(metadata))
            .await;
    }

    async fn append_journal_entry<State: JournalTable + InvocationStatusTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        // We pass around the invocation_status here to avoid an additional read.
        // We could in theory get rid of this here (and in other places, such as StoreDeploymentId),
        // by using a merge operator in rocksdb.
        mut previous_invocation_status: InvocationStatus,
        entry_index: EntryIndex,
        journal_entry: &JournalEntry,
    ) {
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.index = entry_index,
            restate.invocation.id = %invocation_id,
            "Write journal entry {:?} to storage",
            journal_entry.entry_type()
        );

        // Store journal entry
        ctx.storage
            .put_journal_entry(&invocation_id, entry_index, journal_entry)
            .await;

        // update the journal metadata length
        let journal_meta = previous_invocation_status
            .get_journal_metadata_mut()
            .expect("At this point there must be a journal");
        debug_assert_eq!(
            journal_meta.length, entry_index,
            "journal should not have gaps"
        );
        journal_meta.length = entry_index + 1;

        // Update timestamps
        if let Some(timestamps) = previous_invocation_status.get_timestamps_mut() {
            timestamps.update();
        }

        // Store invocation status
        ctx.storage
            .put_invocation_status(&invocation_id, &previous_invocation_status)
            .await;
    }

    async fn do_drop_journal<State: JournalTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) {
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.length = journal_length,
            "Effect: Drop journal"
        );

        // TODO: Only drop journals if the inbox is empty; this requires that keep track of the max journal length: https://github.com/restatedev/restate/issues/272
        ctx.storage
            .delete_journal(&invocation_id, journal_length)
            .await;
    }

    async fn do_truncate_outbox<State: OutboxTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        range: RangeInclusive<MessageIndex>,
    ) -> Result<(), Error> {
        trace!(
            restate.outbox.seq_from = range.start(),
            restate.outbox.seq_to = range.end(),
            "Effect: Truncate outbox"
        );

        ctx.storage.truncate_outbox(range).await;

        Ok(())
    }

    async fn do_store_completion<State: JournalTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        Completion {
            entry_index,
            result,
        }: Completion,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.index = entry_index,
            "Effect: Store completion {}",
            CompletionResultFmt(&result)
        );

        Self::store_completion(ctx.storage, &invocation_id, entry_index, result).await?;

        Ok(())
    }

    fn do_forward_completion<State>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        completion: Completion,
    ) {
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.index = completion.entry_index,
            "Effect: Forward completion {} to deployment",
            CompletionResultFmt(&completion.result)
        );

        ctx.action_collector.push(Action::ForwardCompletion {
            invocation_id,
            completion,
        });
    }

    async fn do_append_response_sink<State: InvocationStatusTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
        // We pass around the invocation_status here to avoid an additional read.
        // We could in theory get rid of this here (and in other places, such as StoreDeploymentId),
        // by using a merge operator in rocksdb.
        mut previous_invocation_status: InvocationStatus,
        additional_response_sink: ServiceInvocationResponseSink,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.invocation.id = %invocation_id,
            "Effect: Store additional response sink {:?}",
            additional_response_sink
        );

        previous_invocation_status
            .get_response_sinks_mut()
            .expect("No response sinks available")
            .insert(additional_response_sink);
        if let Some(timestamps) = previous_invocation_status.get_timestamps_mut() {
            timestamps.update();
        }

        ctx.storage
            .put_invocation_status(&invocation_id, &previous_invocation_status)
            .await;

        Ok(())
    }

    async fn do_store_idempotency_id<State: IdempotencyTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        idempotency_id: IdempotencyId,
        invocation_id: InvocationId,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.invocation.id = %invocation_id,
            "Effect: Store idempotency id {:?}",
            idempotency_id
        );

        ctx.storage
            .put_idempotency_metadata(&idempotency_id, &IdempotencyMetadata { invocation_id })
            .await;

        Ok(())
    }

    async fn do_delete_idempotency_id<State: IdempotencyTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        idempotency_id: IdempotencyId,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            "Effect: Delete idempotency id {:?}",
            idempotency_id
        );

        ctx.storage
            .delete_idempotency_metadata(&idempotency_id)
            .await;

        Ok(())
    }

    fn do_trace_background_invoke<State>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        (invocation_id, invocation_target): InvocationIdAndTarget,
        span_context: ServiceInvocationSpanContext,
        pointer_span_id: Option<SpanId>,
    ) {
        // create an instantaneous 'pointer span' which lives in the calling trace at the
        // time of background call, and primarily exists to be linked to by the new trace
        // that will be created for the background invocation, but even if that trace wasn't
        // sampled for some reason, it's still worth producing this span

        if let Some(pointer_span_id) = pointer_span_id {
            info_span_if_leader!(
                ctx.is_leader,
                span_context.is_sampled(),
                span_context.as_parent(),
                "background_invoke",
                otel.name = format!("background_invoke {invocation_target}"),
                rpc.service = %invocation_target.service_name(),
                rpc.method = %invocation_target.handler_name(),
                restate.invocation.id = %invocation_id,
                restate.invocation.target = %invocation_target,
                restate.internal.span_id = %pointer_span_id,
            );
        } else {
            info_span_if_leader!(
                ctx.is_leader,
                span_context.is_sampled(),
                span_context.as_parent(),
                "background_invoke",
                otel.name = format!("background_invoke {invocation_target}"),
                rpc.service = %invocation_target.service_name(),
                rpc.method = %invocation_target.handler_name(),
                restate.invocation.id = %invocation_id,
                restate.invocation.target = %invocation_target,
            );
        }
    }

    fn do_send_abort_invocation_to_invoker<State>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        invocation_id: InvocationId,
    ) {
        debug_if_leader!(ctx.is_leader, restate.invocation.id = %invocation_id, "Effect: Send abort command to invoker");

        ctx.action_collector
            .push(Action::AbortInvocation(invocation_id));
    }

    async fn do_mutate_state<State: StateTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        state_mutation: ExternalStateMutation,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            "Effect: Mutate state for service id '{:?}'",
            &state_mutation.service_id
        );

        Self::mutate_state(ctx.storage, state_mutation).await?;

        Ok(())
    }

    async fn do_put_promise<State: PromiseTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        service_id: ServiceId,
        key: ByteString,
        promise: Promise,
    ) {
        debug_if_leader!(ctx.is_leader, rpc.service = %service_id.service_name, "Effect: Put promise {} in non completed state", key);

        ctx.storage.put_promise(&service_id, &key, &promise).await;
    }

    async fn do_clear_all_promises<State: PromiseTable>(
        ctx: &mut StateMachineApplyContext<'_, State>,
        service_id: ServiceId,
    ) -> Result<(), Error> {
        debug_if_leader!(
                    ctx.is_leader,
                    rpc.service = %service_id.service_name,
                    "Effect: Clear all promises");

        ctx.storage.delete_all_promises(&service_id).await;

        Ok(())
    }

    async fn mutate_state<State: StateTable>(
        state_storage: &mut State,
        state_mutation: ExternalStateMutation,
    ) -> StorageResult<()> {
        let ExternalStateMutation {
            service_id,
            version,
            state,
        } = state_mutation;

        // overwrite all existing key value pairs with the provided ones; delete all entries that
        // are not contained in state
        let all_user_states: Vec<(Bytes, Bytes)> = state_storage
            .get_all_user_states_for_service(&service_id)
            .try_collect()
            .await?;

        if let Some(expected_version) = version {
            let expected = StateMutationVersion::from_raw(expected_version);
            let actual = StateMutationVersion::from_user_state(&all_user_states);

            if actual != expected {
                debug!("Ignore state mutation for service id '{:?}' because the expected version '{}' is not matching the actual version '{}'", &service_id, expected, actual);
                return Ok(());
            }
        }

        for (key, _) in &all_user_states {
            if !state.contains_key(key) {
                state_storage.delete_user_state(&service_id, key).await;
            }
        }

        // overwrite existing key value pairs
        for (key, value) in state {
            state_storage.put_user_state(&service_id, key, value).await
        }

        Ok(())
    }

    /// Stores the given completion. Returns `true` if an [`RawEntry`] was completed.
    async fn store_completion<State: JournalTable>(
        state_storage: &mut State,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
        completion_result: CompletionResult,
    ) -> Result<bool, Error> {
        if let Some(mut journal_entry) = state_storage
            .get_journal_entry(invocation_id, entry_index)
            .await?
            .and_then(|journal_entry| match journal_entry {
                JournalEntry::Entry(entry) => Some(entry),
                JournalEntry::Completion(_) => None,
            })
        {
            if journal_entry.ty() == EntryType::Awakeable
                && journal_entry.header().is_completed() == Some(true)
            {
                // We can ignore when we get an awakeable completion twice as they might be a result of
                // some request being retried from the ingress to complete the awakeable.
                // We'll use only the first completion, because changing the awakeable result
                // after it has been completed for the first time can cause non-deterministic execution.
                warn!(
                    restate.invocation.id = %invocation_id,
                    restate.journal.index = entry_index,
                    "Trying to complete an awakeable already completed. Ignoring this completion");
                debug!("Discarded awakeable completion: {:?}", completion_result);
                return Ok(false);
            }
            Codec::write_completion(&mut journal_entry, completion_result)?;
            state_storage
                .put_journal_entry(
                    invocation_id,
                    entry_index,
                    &JournalEntry::Entry(journal_entry),
                )
                .await;
            Ok(true)
        } else {
            // In case we don't have the journal entry (only awakeables case),
            // we'll send the completion afterward once we receive the entry.
            state_storage
                .put_journal_entry(
                    invocation_id,
                    entry_index,
                    &JournalEntry::Completion(completion_result),
                )
                .await;
            Ok(false)
        }
    }
}

// To write completions in the effects log
struct CompletionResultFmt<'a>(&'a CompletionResult);

impl<'a> fmt::Display for CompletionResultFmt<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            CompletionResult::Empty => write!(f, "Empty"),
            CompletionResult::Success(_) => write!(f, "Success"),
            CompletionResult::Failure(code, reason) => write!(f, "Failure({}, {})", code, reason),
        }
    }
}

/// Projected [`InvocationStatus`] for cancellation purposes.
enum InvocationStatusProjection {
    Invoked,
    Suspended(HashSet<EntryIndex>),
}

#[cfg(test)]
mod tests;
