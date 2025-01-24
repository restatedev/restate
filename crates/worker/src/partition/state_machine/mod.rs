// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod actions;
mod entries;
mod lifecycle;
mod utils;

use crate::metric_definitions::PARTITION_APPLY_COMMAND;
use crate::partition::state_machine::lifecycle::OnCancelCommand;
use crate::partition::types::{InvokerEffect, InvokerEffectKind, OutboxMessageExt};
use ::tracing::{debug, trace, warn, Instrument, Span};
pub use actions::{Action, ActionCollector};
use assert2::let_assert;
use bytes::Bytes;
use bytestring::ByteString;
use enumset::EnumSet;
use futures::{StreamExt, TryStreamExt};
use metrics::{histogram, Histogram};
use restate_invoker_api::InvokeInputJournal;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::idempotency_table::IdempotencyMetadata;
use restate_storage_api::idempotency_table::{IdempotencyTable, ReadOnlyIdempotencyTable};
use restate_storage_api::inbox_table::{InboxEntry, InboxTable};
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, InboxedInvocation, InvocationStatusTable,
    PreFlightInvocationMetadata, ReadOnlyInvocationStatusTable,
};
use restate_storage_api::invocation_status_table::{InvocationStatus, ScheduledInvocation};
use restate_storage_api::journal_table::ReadOnlyJournalTable;
use restate_storage_api::journal_table::{JournalEntry, JournalTable};
use restate_storage_api::journal_table_v2;
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_storage_api::promise_table::{Promise, PromiseState, PromiseTable};
use restate_storage_api::service_status_table::{
    ReadOnlyVirtualObjectStatusTable, VirtualObjectStatus, VirtualObjectStatusTable,
};
use restate_storage_api::state_table::StateTable;
use restate_storage_api::timer_table::TimerKey;
use restate_storage_api::timer_table::{Timer, TimerTable};
use restate_storage_api::Result as StorageResult;
use restate_tracing_instrumentation as instrumentation;
use restate_types::errors::{
    GenericError, InvocationErrorCode, ALREADY_COMPLETED_INVOCATION_ERROR,
    ATTACH_NOT_SUPPORTED_INVOCATION_ERROR, CANCELED_INVOCATION_ERROR, KILLED_INVOCATION_ERROR,
    NOT_FOUND_INVOCATION_ERROR, NOT_READY_INVOCATION_ERROR,
    WORKFLOW_ALREADY_INVOKED_INVOCATION_ERROR,
};
use restate_types::identifiers::{
    AwakeableIdentifier, EntryIndex, ExternalSignalIdentifier, InvocationId, PartitionKey,
    PartitionProcessorRpcRequestId, ServiceId,
};
use restate_types::identifiers::{
    IdempotencyId, JournalEntryId, WithInvocationId, WithPartitionKey,
};
use restate_types::invocation::{
    AttachInvocationRequest, InvocationQuery, InvocationResponse, InvocationTarget,
    InvocationTargetType, InvocationTermination, NotifySignalRequest, ResponseResult,
    ServiceInvocation, ServiceInvocationResponseSink, ServiceInvocationSpanContext, Source,
    SubmitNotificationSink, TerminationFlavor, VirtualObjectHandlerType, WorkflowHandlerType,
};
use restate_types::invocation::{InvocationInput, SpanRelation};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::enriched::{
    AwakeableEnrichmentResult, CallEnrichmentResult, EnrichedEntryHeader,
};
use restate_types::journal::raw::{EntryHeader, RawEntryCodec, RawEntryCodecError};
use restate_types::journal::Completion;
use restate_types::journal::CompletionResult;
use restate_types::journal::EntryType;
use restate_types::journal::*;
use restate_types::journal_v2;
use restate_types::journal_v2::command::{OutputCommand, OutputResult};
use restate_types::journal_v2::raw::RawNotification;
use restate_types::journal_v2::{
    AttachInvocationCompletion, AttachInvocationResult, CallCompletion, CallResult, CommandType,
    CompletionId, EntryMetadata, GetInvocationOutputCompletion, GetInvocationOutputResult,
    GetPromiseCompletion, GetPromiseResult, NotificationId, Signal, SignalResult, SleepCompletion,
};
use restate_types::message::MessageIndex;
use restate_types::net::partition_processor::IngressResponseResult;
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::state_mut::ExternalStateMutation;
use restate_types::state_mut::StateMutationVersion;
use restate_types::time::MillisSinceEpoch;
use restate_wal_protocol::timer::TimerKeyDisplay;
use restate_wal_protocol::timer::TimerKeyValue;
use restate_wal_protocol::Command;
use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::time::Instant;
use tracing::{error, info};
use utils::SpanExt;

#[derive(Debug, Hash, enumset::EnumSetType, strum::Display)]
pub enum ExperimentalFeature {
    /// This is used to disable writing to idempotency table/virtual object status table for idempotent invocations/workflow invocations.
    /// From Restate 1.2 invocation ids are generated deterministically, so this additional index is not needed.
    DisableIdempotencyTable,
    /// If true, kill should wait for end signal from invoker, in order to implement the restart functionality.
    /// This is enabled by experimental_feature_kill_and_restart.
    InvocationStatusKilled,
}

pub struct StateMachine {
    // initialized from persistent storage
    inbox_seq_number: MessageIndex,
    /// First outbox message index.
    outbox_head_seq_number: Option<MessageIndex>,
    /// Sequence number of the next outbox message to be appended.
    outbox_seq_number: MessageIndex,
    partition_key_range: RangeInclusive<PartitionKey>,
    invoker_apply_latency: Histogram,

    /// Enabled experimental features.
    experimental_features: EnumSet<ExperimentalFeature>,
}

impl Debug for StateMachine {
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
    #[error("expecting entry type {0:?}, but wasn't. This indicates data corruption.")]
    BadEntryVariant(journal_v2::EntryType),
    #[error("error when trying to apply command effect for entry {0:?}. Reason: {1}")]
    ApplyCommandEffect(journal_v2::EntryType, GenericError),
    #[error(transparent)]
    EntryEncoding(#[from] journal_v2::encoding::DecodingError),
    #[error("failed to deserialize entry: {0}")]
    EntryDecoding(#[from] journal_v2::raw::RawEntryError),
    #[error("error when trying to apply invocation response with completion id {1}, the entry type {0} is not expected to be completed through InvocationResponse command")]
    BadCommandTypeForInvocationResponse(journal_v2::CommandType, CompletionId),
    #[error("error when trying to apply invocation response with completion id {0}, because no command was found for given completion id")]
    MissingCommandForInvocationResponse(CompletionId),
    #[error("error when trying to apply invocation response with completion id {1}, the entry type {0} doesn't expect variant {2}")]
    BadCompletionVariantForInvocationResponse(journal_v2::CommandType, CompletionId, &'static str),
}

#[macro_export]
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

#[macro_export]
macro_rules! span_if_leader {
    ($level:expr, $i_am_leader:expr, $sampled:expr, $span_relation:expr, $($args:tt)*) => {{
        if $i_am_leader && $sampled {
            let span = ::tracing::span!($level, $($args)*);
            // span.set_relation($span_relation);
            let _ = span.enter();
        }
    }};
}

// creates and inter an info span if both i_am_leader and sampled are true
#[macro_export]
macro_rules! info_span_if_leader {
    ($i_am_leader:expr, $sampled:expr, $span_relation:expr, $($args:tt)*) => {{
        use ::tracing::Level;
        span_if_leader!(Level::INFO, $i_am_leader, $sampled, $span_relation, $($args)*)
    }};
}

impl StateMachine {
    pub fn new(
        inbox_seq_number: MessageIndex,
        outbox_seq_number: MessageIndex,
        outbox_head_seq_number: Option<MessageIndex>,
        partition_key_range: RangeInclusive<PartitionKey>,
        experimental_features: EnumSet<ExperimentalFeature>,
    ) -> Self {
        let invoker_apply_latency =
            histogram!(crate::metric_definitions::PARTITION_HANDLE_INVOKER_EFFECT_COMMAND);
        Self {
            inbox_seq_number,
            outbox_seq_number,
            outbox_head_seq_number,
            partition_key_range,
            invoker_apply_latency,
            experimental_features,
        }
    }
}

pub(crate) struct StateMachineApplyContext<'a, S> {
    storage: &'a mut S,
    action_collector: &'a mut ActionCollector,
    inbox_seq_number: &'a mut MessageIndex,
    outbox_seq_number: &'a mut MessageIndex,
    outbox_head_seq_number: &'a mut Option<MessageIndex>,
    partition_key_range: RangeInclusive<PartitionKey>,
    invoker_apply_latency: &'a Histogram,
    experimental_features: &'a EnumSet<ExperimentalFeature>,
    is_leader: bool,
}

trait CommandHandler<CTX> {
    async fn apply(self, ctx: CTX) -> Result<(), Error>;
}

impl StateMachine {
    pub async fn apply<TransactionType: restate_storage_api::Transaction + Send>(
        &mut self,
        command: Command,
        transaction: &mut TransactionType,
        action_collector: &mut ActionCollector,
        is_leader: bool,
    ) -> Result<(), Error> {
        let span = utils::state_machine_apply_command_span(is_leader, &command);
        async {
            let start = Instant::now();
            // Apply the command
            let command_type = command.name();
            let res = StateMachineApplyContext {
                storage: transaction,
                action_collector,
                inbox_seq_number: &mut self.inbox_seq_number,
                outbox_seq_number: &mut self.outbox_seq_number,
                outbox_head_seq_number: &mut self.outbox_head_seq_number,
                partition_key_range: self.partition_key_range.clone(),
                invoker_apply_latency: &self.invoker_apply_latency,
                experimental_features: &self.experimental_features,
                is_leader,
            }
            .on_apply(command)
            .await;
            histogram!(PARTITION_APPLY_COMMAND, "command" => command_type).record(start.elapsed());
            res
        }
        .instrument(span)
        .await
    }
}

impl<'a, S> StateMachineApplyContext<'a, S> {
    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<InvocationStatus, Error>
    where
        S: ReadOnlyInvocationStatusTable,
    {
        Span::current().record_invocation_id(invocation_id);
        let status = self.storage.get_invocation_status(invocation_id).await?;

        if let Some(invocation_traget) = status.invocation_target() {
            Span::current().record_invocation_target(invocation_traget);
        }
        Ok(status)
    }

    async fn register_timer(
        &mut self,
        timer_value: TimerKeyValue,
        span_context: ServiceInvocationSpanContext,
    ) -> Result<(), Error>
    where
        S: TimerTable,
    {
        match timer_value.value() {
            Timer::CompleteJournalEntry(_, entry_index) => {
                info_span_if_leader!(
                    self.is_leader,
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
                    self.is_leader,
                    restate.journal.index = entry_index,
                    restate.timer.wake_up_time = %timer_value.wake_up_time(),
                    restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                    "Register Sleep timer"
                )
            }
            Timer::Invoke(service_invocation) => {
                // no span necessary; there will already be a background_invoke span
                debug_if_leader!(
                    self.is_leader,
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
                    self.is_leader,
                    restate.invocation.id = %invocation_id,
                    restate.timer.wake_up_time = %timer_value.wake_up_time(),
                    restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                    "Register background invoke timer"
                )
            }
            Timer::CleanInvocationStatus(_) => {
                debug_if_leader!(
                    self.is_leader,
                    restate.timer.wake_up_time = %timer_value.wake_up_time(),
                    restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                    "Register cleanup invocation status timer"
                )
            }
        };

        self.storage
            .put_timer(timer_value.key(), timer_value.value())
            .await;

        self.action_collector
            .push(Action::RegisterTimer { timer_value });

        Ok(())
    }

    fn forward_notification(&mut self, invocation_id: InvocationId, notification: RawNotification) {
        debug_if_leader!(
            self.is_leader,
            restate.notification.id = %notification.id(),
            "Forward notification to deployment",
        );

        self.action_collector.push(Action::ForwardNotification {
            invocation_id,
            notification,
        });
    }

    fn send_abort_invocation_to_invoker(&mut self, invocation_id: InvocationId, acknowledge: bool) {
        debug_if_leader!(
            self.is_leader,
            restate.invocation.id = %invocation_id,
            "Send abort command to invoker"
        );

        self.action_collector.push(Action::AbortInvocation {
            invocation_id,
            acknowledge,
        });
    }

    async fn on_apply(&mut self, command: Command) -> Result<(), Error>
    where
        S: IdempotencyTable
            + PromiseTable
            + JournalTable
            + InvocationStatusTable
            + OutboxTable
            + FsmTable
            + TimerTable
            + VirtualObjectStatusTable
            + InboxTable
            + StateTable
            + journal_table_v2::JournalTable,
    {
        match command {
            Command::Invoke(service_invocation) => {
                self.on_service_invocation(service_invocation).await
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
                let status = self.get_invocation_status(&id).await?;

                if should_use_journal_table_v2(&status) {
                    self.handle_invocation_response_for_journal_v2(id, status, completion)
                        .await?;
                    return Ok(());
                }

                self.handle_completion(id, status, completion).await
            }
            Command::ProxyThrough(service_invocation) => {
                self.handle_outgoing_message(OutboxMessage::ServiceInvocation(service_invocation))
                    .await?;
                Ok(())
            }
            Command::AttachInvocation(attach_invocation_request) => {
                self.handle_attach_invocation_request(attach_invocation_request)
                    .await
            }
            Command::InvokerEffect(effect) => self.try_invoker_effect(effect).await,
            Command::TruncateOutbox(index) => {
                self.do_truncate_outbox(RangeInclusive::new(
                    (*self.outbox_head_seq_number).unwrap_or(index),
                    index,
                ))
                .await?;
                *self.outbox_head_seq_number = Some(index + 1);
                Ok(())
            }
            Command::Timer(timer) => self.on_timer(timer).await,
            Command::TerminateInvocation(invocation_termination) => {
                self.on_terminate_invocation(invocation_termination).await
            }
            Command::PurgeInvocation(purge_invocation_request) => {
                self.on_purge_invocation(purge_invocation_request.invocation_id)
                    .await
            }
            Command::PatchState(mutation) => self.handle_external_state_mutation(mutation).await,
            Command::AnnounceLeader(_) => {
                // no-op :-)
                Ok(())
            }
            Command::ScheduleTimer(timer) => {
                self.register_timer(timer, Default::default()).await?;
                Ok(())
            }
            Command::NotifySignal(notify_signal_request) => {
                entries::OnJournalEntryCommand::from_entry(
                    notify_signal_request.invocation_id,
                    self.get_invocation_status(&notify_signal_request.invocation_id)
                        .await?,
                    notify_signal_request.signal.into(),
                )
                .apply(self)
                .await?;
                Ok(())
            }
            Command::NotifyGetInvocationOutputResponse(get_invocation_output_response) => {
                entries::OnJournalEntryCommand::from_entry(
                    get_invocation_output_response.caller_id,
                    self.get_invocation_status(&get_invocation_output_response.caller_id)
                        .await?,
                    GetInvocationOutputCompletion {
                        completion_id: get_invocation_output_response.completion_id,
                        result: get_invocation_output_response.result,
                    }
                    .into(),
                )
                .apply(self)
                .await?;
                Ok(())
            }
        }
    }

    async fn on_service_invocation(
        &mut self,
        service_invocation: ServiceInvocation,
    ) -> Result<(), Error>
    where
        S: IdempotencyTable
            + InvocationStatusTable
            + OutboxTable
            + FsmTable
            + VirtualObjectStatusTable
            + TimerTable
            + InboxTable
            + FsmTable
            + JournalTable,
    {
        let invocation_id = service_invocation.invocation_id;
        debug_assert!(
            self.partition_key_range.contains(&service_invocation.partition_key()),
            "Service invocation with partition key '{}' has been delivered to a partition processor with key range '{:?}'. This indicates a bug.",
            service_invocation.partition_key(),
            self.partition_key_range);

        Span::current().record_invocation_id(&invocation_id);
        Span::current().record_invocation_target(&service_invocation.invocation_target);
        // Phases of an invocation
        // 1. Try deduplicate it first
        // 2. Check if we need to schedule it
        // 3. Check if we need to inbox it (only for exclusive handlers of virtual objects services)
        // 4. Execute it

        // 1. Try deduplicate it first
        let Some(mut service_invocation) =
            self.handle_duplicated_requests(service_invocation).await?
        else {
            // Invocation was deduplicated, nothing else to do here
            return Ok(());
        };

        // Prepare PreFlightInvocationMetadata structure
        let submit_notification_sink = service_invocation.submit_notification_sink.take();
        let pre_flight_invocation_metadata =
            PreFlightInvocationMetadata::from_service_invocation(service_invocation);

        // 2. Check if we need to schedule it
        let Some(pre_flight_invocation_metadata) = self
            .handle_service_invocation_execution_time(invocation_id, pre_flight_invocation_metadata)
            .await?
        else {
            // Invocation was scheduled, send back the ingress attach notification and return
            self.send_submit_notification_if_needed(invocation_id, true, submit_notification_sink);
            return Ok(());
        };

        // 3. Check if we need to inbox it (only for exclusive methods of virtual objects)
        let Some(pre_flight_invocation_metadata) = self
            .handle_service_invocation_exclusive_handler(
                invocation_id,
                pre_flight_invocation_metadata,
            )
            .await?
        else {
            // Invocation was inboxed, send back the ingress attach notification and return
            self.send_submit_notification_if_needed(invocation_id, true, submit_notification_sink);
            // Invocation was inboxed, nothing else to do here
            return Ok(());
        };

        // 4. Execute it
        Self::send_submit_notification_if_needed(
            self,
            invocation_id,
            true,
            submit_notification_sink,
        );

        let (in_flight_invocation_metadata, invocation_input) =
            InFlightInvocationMetadata::from_pre_flight_invocation_metadata(
                pre_flight_invocation_metadata,
            );

        self.init_journal_and_invoke(
            invocation_id,
            in_flight_invocation_metadata,
            invocation_input,
        )
        .await
    }

    /// Returns the invocation in case the invocation is not a duplicate
    async fn handle_duplicated_requests(
        &mut self,
        mut service_invocation: ServiceInvocation,
    ) -> Result<Option<ServiceInvocation>, Error>
    where
        S: IdempotencyTable
            + InvocationStatusTable
            + VirtualObjectStatusTable
            + OutboxTable
            + FsmTable,
    {
        let invocation_id = service_invocation.invocation_id;
        let is_workflow_run = service_invocation.invocation_target.invocation_target_ty()
            == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow);
        let mut has_idempotency_key = service_invocation.idempotency_key.is_some();

        if is_workflow_run && has_idempotency_key {
            warn!("The idempotency key for workflow methods is ignored!");
            has_idempotency_key = false;
        }

        let previous_invocation_status = async {
            let mut invocation_status = self.get_invocation_status(&invocation_id).await?;
            if invocation_status != InvocationStatus::Free {
                // Deduplicated invocation with the new deterministic invocation id
              Ok::<_, Error>(invocation_status)
            } else {
                // We might still need to deduplicate based on the idempotency table for old invocation ids
                // TODO get rid of this code when we remove the idempotency table
                if has_idempotency_key {
                    let idempotency_id = service_invocation
                        .compute_idempotency_id()
                        .expect("Idempotency key must be present");

                    if let Some(idempotency_metadata) = self.storage.get_idempotency_metadata(&idempotency_id).await? {
                        invocation_status = self.get_invocation_status(&idempotency_metadata.invocation_id).await?;
                    }
                }
                // Or on lock status for workflow runs with old invocation ids
                // TODO get rid of this code when we remove the usage of the virtual object table for workflows
                if is_workflow_run {
                    let keyed_service_id = service_invocation
                        .invocation_target
                        .as_keyed_service_id()
                        .expect("When the handler type is Workflow, the invocation target must have a key");

                    if let VirtualObjectStatus::Locked(locked_invocation_id) = self
                        .storage
                        .get_virtual_object_status(&keyed_service_id)
                        .await? {
                        invocation_status = self.get_invocation_status(&locked_invocation_id).await?;
                    }
                }
                Ok(invocation_status)
            }

        }.await?;

        if previous_invocation_status == InvocationStatus::Free {
            // --- New invocation
            debug_if_leader!(
                self.is_leader,
                "First time we see this invocation id, invocation will be processed"
            );

            // Store the invocation id mapping if we have to and continue the processing
            // TODO get rid of this code when we remove the usage of the virtual object table for workflows
            if is_workflow_run
                && !self
                    .experimental_features
                    .contains(ExperimentalFeature::DisableIdempotencyTable)
            {
                self.storage
                        .put_virtual_object_status(
                            &service_invocation
                                .invocation_target
                                .as_keyed_service_id()
                                .expect("When the handler type is Workflow, the invocation target must have a key"),
                            &VirtualObjectStatus::Locked(invocation_id),
                        )
                        .await;
            }
            // TODO get rid of this code when we remove the idempotency table
            if has_idempotency_key
                && !self
                    .experimental_features
                    .contains(ExperimentalFeature::DisableIdempotencyTable)
            {
                self.do_store_idempotency_id(
                    service_invocation
                        .compute_idempotency_id()
                        .expect("Idempotency key must be present"),
                    service_invocation.invocation_id,
                )
                .await?;
            }
            return Ok(Some(service_invocation));
        }

        // --- Invocation already exists

        // Send submit notification
        self.send_submit_notification_if_needed(
            service_invocation.invocation_id,
            // is_new_invocation is true if the RPC ingress request is a duplicate.
            service_invocation.source
                == *previous_invocation_status
                    .source()
                    .expect("source must be present when InvocationStatus is not Free"),
            service_invocation.submit_notification_sink,
        );

        // For workflow run, we don't append the response sink, but we send a failure instead.
        // This is a special handling we do only for workflows.
        if is_workflow_run {
            debug_if_leader!(
                self.is_leader,
                "Invocation to workflow method is a duplicate"
            );
            self.send_response_to_sinks(
                service_invocation.response_sink.take().into_iter(),
                ResponseResult::Failure(WORKFLOW_ALREADY_INVOKED_INVOCATION_ERROR),
                Some(invocation_id),
                None,
                Some(&service_invocation.invocation_target),
            )
            .await?;
        }

        // For all the other type of duplicate requests, append the response sink or return back the original result
        if has_idempotency_key {
            debug_if_leader!(
                self.is_leader,
                restate.idempotency.key = ?service_invocation.idempotency_key.unwrap(),
                "Invocation with idempotency key is a duplicate"
            );
        }

        match previous_invocation_status {
            is @ InvocationStatus::Invoked { .. }
            | is @ InvocationStatus::Suspended { .. }
            | is @ InvocationStatus::Inboxed { .. }
            | is @ InvocationStatus::Scheduled { .. } => {
                if let Some(ref response_sink) = service_invocation.response_sink {
                    if !is
                        .get_response_sinks()
                        .expect("response sink must be present")
                        .contains(response_sink)
                    {
                        self.do_append_response_sink(invocation_id, is, response_sink.clone())
                            .await?
                    }
                }
            }
            InvocationStatus::Killed(metadata) => {
                self.send_response_to_sinks(
                    service_invocation.response_sink.take().into_iter(),
                    KILLED_INVOCATION_ERROR,
                    Some(invocation_id),
                    None,
                    Some(&metadata.invocation_target),
                )
                .await?;
            }
            InvocationStatus::Completed(completed) => {
                // SAFETY: We use this field to send back the notification to ingress, and not as part of the PP deterministic logic.
                let completion_expiry_time = unsafe { completed.completion_expiry_time() };
                self.send_response_to_sinks(
                    service_invocation.response_sink.take().into_iter(),
                    completed.response_result,
                    Some(invocation_id),
                    completion_expiry_time,
                    Some(&completed.invocation_target),
                )
                .await?;
            }
            InvocationStatus::Free => {
                unreachable!("This was checked before!")
            }
        }

        Ok(None)
    }

    /// Returns the invocation in case the invocation should run immediately
    async fn handle_service_invocation_execution_time(
        &mut self,
        invocation_id: InvocationId,
        metadata: PreFlightInvocationMetadata,
    ) -> Result<Option<PreFlightInvocationMetadata>, Error>
    where
        S: TimerTable + InvocationStatusTable,
    {
        if let Some(execution_time) = metadata.execution_time {
            let span_context = metadata.span_context.clone();
            debug_if_leader!(self.is_leader, "Store scheduled invocation");

            self.register_timer(
                TimerKeyValue::neo_invoke(execution_time, invocation_id),
                span_context,
            )
            .await?;

            self.storage
                .put_invocation_status(
                    &invocation_id.clone(),
                    &InvocationStatus::Scheduled(
                        ScheduledInvocation::from_pre_flight_invocation_metadata(metadata),
                    ),
                )
                .await;
            // The span will be created later on invocation
            return Ok(None);
        }

        Ok(Some(metadata))
    }

    /// Returns the invocation in case the invocation was not inboxed
    async fn handle_service_invocation_exclusive_handler(
        &mut self,
        invocation_id: InvocationId,
        metadata: PreFlightInvocationMetadata,
    ) -> Result<Option<PreFlightInvocationMetadata>, Error>
    where
        S: VirtualObjectStatusTable + InvocationStatusTable + InboxTable + FsmTable,
    {
        if metadata.invocation_target.invocation_target_ty()
            == InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive)
        {
            let keyed_service_id = metadata.invocation_target.as_keyed_service_id().expect(
                "When the handler type is Exclusive, the invocation target must have a key",
            );

            let service_status = self
                .storage
                .get_virtual_object_status(&keyed_service_id)
                .await?;

            if let VirtualObjectStatus::Locked(_) = service_status {
                // If locked, enqueue in inbox and be done with it
                let inbox_seq_number = self
                    .enqueue_into_inbox(InboxEntry::Invocation(keyed_service_id, invocation_id))
                    .await?;

                debug_if_leader!(
                    self.is_leader,
                    restate.outbox.seq = inbox_seq_number,
                    "Store inboxed invocation"
                );
                self.storage
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
                    self.is_leader,
                    restate.service.id = %keyed_service_id,
                    "Locking service"
                );

                self.storage
                    .put_virtual_object_status(
                        &keyed_service_id,
                        &VirtualObjectStatus::Locked(invocation_id),
                    )
                    .await;
            }
        }
        Ok(Some(metadata))
    }

    async fn init_journal_and_invoke(
        &mut self,
        invocation_id: InvocationId,
        mut in_flight_invocation_metadata: InFlightInvocationMetadata,
        invocation_input: InvocationInput,
    ) -> Result<(), Error>
    where
        S: JournalTable + InvocationStatusTable,
    {
        let invoke_input_journal = self
            .init_journal(
                invocation_id,
                &mut in_flight_invocation_metadata,
                invocation_input,
            )
            .await?;

        self.invoke(
            invocation_id,
            in_flight_invocation_metadata,
            invoke_input_journal,
        )
        .await
    }

    async fn init_journal(
        &mut self,
        invocation_id: InvocationId,
        in_flight_invocation_metadata: &mut InFlightInvocationMetadata,
        invocation_input: InvocationInput,
    ) -> Result<InvokeInputJournal, Error>
    where
        S: JournalTable,
    {
        debug_if_leader!(self.is_leader, "Init journal with input entry");

        // In our current data model, ServiceInvocation has always an input, so initial length is 1
        in_flight_invocation_metadata.journal_metadata.length = 1;

        // We store the entry in the JournalTable V1.
        // When pinning the deployment version we figure the concrete protocol version
        // * If <= V3, we keep everything in JournalTable V1
        // * If >= V4, we migrate the JournalTable to V2
        let input_entry = JournalEntry::Entry(ProtobufRawEntryCodec::serialize_as_input_entry(
            invocation_input.headers,
            invocation_input.argument,
        ));
        self.storage
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
            vec![
                restate_invoker_api::journal_reader::JournalEntry::JournalV1(
                    input_entry.erase_enrichment(),
                ),
            ],
        ))
    }

    async fn invoke(
        &mut self,
        invocation_id: InvocationId,
        in_flight_invocation_metadata: InFlightInvocationMetadata,
        invoke_input_journal: InvokeInputJournal,
    ) -> Result<(), Error>
    where
        S: InvocationStatusTable,
    {
        debug_if_leader!(self.is_leader, "Invoke");

        self.action_collector.push(Action::Invoke {
            invocation_id,
            invocation_target: in_flight_invocation_metadata.invocation_target.clone(),
            invoke_input_journal,
        });
        self.storage
            .put_invocation_status(
                &invocation_id,
                &InvocationStatus::Invoked(in_flight_invocation_metadata),
            )
            .await;

        Ok(())
    }

    async fn enqueue_into_inbox(&mut self, inbox_entry: InboxEntry) -> Result<MessageIndex, Error>
    where
        S: InboxTable + FsmTable,
    {
        let seq_number = *self.inbox_seq_number;
        debug_if_leader!(
            self.is_leader,
            restate.inbox.seq = seq_number,
            "Enqueue inbox entry"
        );

        self.storage.put_inbox_entry(seq_number, &inbox_entry).await;
        // need to store the next inbox sequence number
        self.storage.put_inbox_seq_number(seq_number + 1).await;
        *self.inbox_seq_number += 1;
        Ok(seq_number)
    }

    async fn handle_external_state_mutation(
        &mut self,
        mutation: ExternalStateMutation,
    ) -> Result<(), Error>
    where
        S: StateTable + InboxTable + FsmTable + VirtualObjectStatusTable,
    {
        let service_status = self
            .storage
            .get_virtual_object_status(&mutation.service_id)
            .await?;

        match service_status {
            VirtualObjectStatus::Locked(_) => {
                self.enqueue_into_inbox(InboxEntry::StateMutation(mutation))
                    .await?;
            }
            VirtualObjectStatus::Unlocked => Self::do_mutate_state(self, mutation).await?,
        }

        Ok(())
    }

    async fn on_terminate_invocation(
        &mut self,
        InvocationTermination {
            invocation_id,
            flavor: termination_flavor,
        }: InvocationTermination,
    ) -> Result<(), Error>
    where
        S: VirtualObjectStatusTable
            + InvocationStatusTable
            + InboxTable
            + FsmTable
            + StateTable
            + JournalTable
            + OutboxTable
            + journal_table_v2::JournalTable
            + TimerTable
            + PromiseTable,
    {
        match termination_flavor {
            TerminationFlavor::Kill => self.on_kill_invocation(invocation_id).await,
            TerminationFlavor::Cancel => self.on_cancel_invocation(invocation_id).await,
        }
    }

    async fn on_kill_invocation(&mut self, invocation_id: InvocationId) -> Result<(), Error>
    where
        S: VirtualObjectStatusTable
            + InvocationStatusTable
            + InboxTable
            + FsmTable
            + StateTable
            + JournalTable
            + OutboxTable
            + TimerTable
            + FsmTable
            + journal_table_v2::JournalTable,
    {
        let status = self.get_invocation_status(&invocation_id).await?;

        match status {
            InvocationStatus::Invoked(metadata) => {
                self.kill_invoked_invocation(invocation_id, metadata)
                    .await?;
            }
            InvocationStatus::Suspended { metadata, .. } => {
                self.kill_suspended_invocation(invocation_id, metadata)
                    .await?;
            }
            InvocationStatus::Inboxed(inboxed) => {
                self.terminate_inboxed_invocation(TerminationFlavor::Kill, invocation_id, inboxed)
                    .await?
            }
            InvocationStatus::Scheduled(scheduled) => {
                self.terminate_scheduled_invocation(
                    TerminationFlavor::Kill,
                    invocation_id,
                    scheduled,
                )
                .await?
            }
            InvocationStatus::Killed(_) => {
                trace!("Received kill command for an already killed invocation with id '{invocation_id}'.");
                // Nothing to do here really, let's send again the abort signal to the invoker just in case
                self.do_send_abort_invocation_to_invoker(invocation_id, true);
            }
            InvocationStatus::Completed(_) => {
                debug!("Received kill command for completed invocation '{invocation_id}'. To cleanup the invocation after it's been completed, use the purge invocation command.");
            }
            InvocationStatus::Free => {
                trace!("Received kill command for unknown invocation with id '{invocation_id}'.");
                // We still try to send the abort signal to the invoker,
                // as it might be the case that previously the user sent an abort signal
                // but some message was still between the invoker/PP queues.
                // This can happen because the invoke/resume and the abort invoker messages end up in different queues,
                // and the abort message can overtake the invoke/resume.
                // Consequently the invoker might have not received the abort and the user tried to send it again.
                self.do_send_abort_invocation_to_invoker(invocation_id, false);
            }
        };

        Ok(())
    }

    async fn on_cancel_invocation(&mut self, invocation_id: InvocationId) -> Result<(), Error>
    where
        S: VirtualObjectStatusTable
            + InvocationStatusTable
            + InboxTable
            + FsmTable
            + StateTable
            + JournalTable
            + OutboxTable
            + journal_table_v2::JournalTable
            + PromiseTable
            + TimerTable,
    {
        let status = self.get_invocation_status(&invocation_id).await?;

        match status.get_invocation_metadata().and_then(|meta| {
            meta.pinned_deployment
                .as_ref()
                .map(|pd| pd.service_protocol_version)
        }) {
            Some(sp_version) if sp_version >= ServiceProtocolVersion::V4 => {
                OnCancelCommand {
                    invocation_id,
                    invocation_status: status,
                }
                .apply(self)
                .await?;
                return Ok(());
            }
            None if matches!(
                status,
                InvocationStatus::Invoked(_) | InvocationStatus::Suspended { .. }
            ) =>
            {
                // We ignore the cancel when we haven't a pinned deployment yet,
                // because we don't know what we should do (cancellation works differently between journal table v1 and v2).
                // In any case, this would have no visible effect to the user
                info!("Ignoring cancellation because the invocation made no progress so far!");
                return Ok(());
            }
            _ => {
                // Continue below
            }
        };

        match status {
            InvocationStatus::Invoked(metadata) => {
                self.cancel_journal_leaves(
                    invocation_id,
                    InvocationStatusProjection::Invoked,
                    metadata.journal_metadata.length,
                )
                .await?;
            }
            InvocationStatus::Suspended {
                metadata,
                waiting_for_notifications,
            } => {
                if self
                    .cancel_journal_leaves(
                        invocation_id,
                        InvocationStatusProjection::Suspended(
                            waiting_for_notifications
                                .into_iter()
                                .map(|n| match n {
                                    NotificationId::CompletionId(idx) => idx,
                                    _ => panic!("When using Service Protocol <= 3, an invocation cannot be suspended on a named notification")
                                }).collect()

                        ),
                        metadata.journal_metadata.length,
                    )
                    .await?
                {
                    self.do_resume_service( invocation_id, metadata).await?;
                }
            }
            InvocationStatus::Inboxed(inboxed) => {
                self.terminate_inboxed_invocation(
                    TerminationFlavor::Cancel,
                    invocation_id,
                    inboxed,
                )
                .await?
            }
            InvocationStatus::Scheduled(scheduled) => {
                self.terminate_scheduled_invocation(
                    TerminationFlavor::Cancel,
                    invocation_id,
                    scheduled,
                )
                .await?
            }
            InvocationStatus::Killed(_) => {
                trace!(
                    "Received cancel command for an already killed invocation '{invocation_id}'."
                );
                // Nothing to do here really, let's send again the abort signal to the invoker just in case
                self.do_send_abort_invocation_to_invoker( invocation_id, true);
            }
            InvocationStatus::Completed(_) => {
                debug!("Received cancel command for completed invocation '{invocation_id}'. To cleanup the invocation after it's been completed, use the purge invocation command.");
            }
            InvocationStatus::Free => {
                trace!("Received cancel command for unknown invocation with id '{invocation_id}'.");
                // We still try to send the abort signal to the invoker,
                // as it might be the case that previously the user sent an abort signal
                // but some message was still between the invoker/PP queues.
                // This can happen because the invoke/resume and the abort invoker messages end up in different queues,
                // and the abort message can overtake the invoke/resume.
                // Consequently the invoker might have not received the abort and the user tried to send it again.
                self.do_send_abort_invocation_to_invoker(invocation_id, false);
            }
        };

        Ok(())
    }

    async fn terminate_inboxed_invocation(
        &mut self,
        termination_flavor: TerminationFlavor,
        invocation_id: InvocationId,
        inboxed_invocation: InboxedInvocation,
    ) -> Result<(), Error>
    where
        S: InvocationStatusTable + InboxTable + OutboxTable + FsmTable,
    {
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
            response_sinks,
            &error,
            Some(invocation_id),
            None,
            Some(&invocation_target),
        )
        .await?;

        // Delete inbox entry and invocation status.
        self.do_delete_inbox_entry(
            invocation_target
                .as_keyed_service_id()
                .expect("Because the invocation is inboxed, it must have a keyed service id"),
            inbox_sequence_number,
        )
        .await?;
        self.do_free_invocation(invocation_id).await;

        self.notify_invocation_result(
            invocation_id,
            invocation_target,
            span_context,
            MillisSinceEpoch::now(),
            Err((error.code(), error.to_string())),
        );

        Ok(())
    }

    async fn terminate_scheduled_invocation(
        &mut self,
        termination_flavor: TerminationFlavor,
        invocation_id: InvocationId,
        scheduled_invocation: ScheduledInvocation,
    ) -> Result<(), Error>
    where
        S: InvocationStatusTable + TimerTable + OutboxTable + FsmTable,
    {
        let error = match termination_flavor {
            TerminationFlavor::Kill => KILLED_INVOCATION_ERROR,
            TerminationFlavor::Cancel => CANCELED_INVOCATION_ERROR,
        };

        let ScheduledInvocation {
            metadata:
                PreFlightInvocationMetadata {
                    response_sinks,
                    span_context,
                    invocation_target,
                    execution_time,
                    ..
                },
        } = scheduled_invocation;

        // Reply back to callers with error, and publish end trace
        self.send_response_to_sinks(
            response_sinks,
            &error,
            Some(invocation_id),
            None,
            Some(&invocation_target),
        )
        .await?;

        // Delete timer
        if let Some(execution_time) = execution_time {
            self.do_delete_timer(TimerKey::neo_invoke(
                execution_time.as_u64(),
                invocation_id.invocation_uuid(),
            ))
            .await?;
        } else {
            warn!("Scheduled invocations must always have an execution time.");
        }
        self.do_free_invocation(invocation_id).await;

        self.notify_invocation_result(
            invocation_id,
            invocation_target,
            span_context,
            MillisSinceEpoch::now(),
            Err((error.code(), error.to_string())),
        );

        Ok(())
    }

    async fn kill_invoked_invocation(
        &mut self,
        invocation_id: InvocationId,
        metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error>
    where
        S: InboxTable
            + VirtualObjectStatusTable
            + InvocationStatusTable
            + VirtualObjectStatusTable
            + StateTable
            + JournalTable
            + OutboxTable
            + FsmTable
            + journal_table_v2::JournalTable,
    {
        self.kill_child_invocations(&invocation_id, metadata.journal_metadata.length, &metadata)
            .await?;

        if self
            .experimental_features
            .contains(ExperimentalFeature::InvocationStatusKilled)
        {
            debug_if_leader!(
                self.is_leader,
                restate.invocation.id = %invocation_id,
                "Effect: Store killed invocation"
            );

            self.storage
                .put_invocation_status(&invocation_id, &InvocationStatus::Killed(metadata))
                .await;
            self.do_send_abort_invocation_to_invoker(invocation_id, true);
        } else {
            self.end_invocation(
                invocation_id,
                metadata,
                Some(ResponseResult::Failure(KILLED_INVOCATION_ERROR)),
            )
            .await?;
            self.do_send_abort_invocation_to_invoker(invocation_id, false);
        }
        Ok(())
    }

    async fn kill_suspended_invocation(
        &mut self,
        invocation_id: InvocationId,
        metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error>
    where
        S: InboxTable
            + VirtualObjectStatusTable
            + InvocationStatusTable
            + VirtualObjectStatusTable
            + StateTable
            + JournalTable
            + OutboxTable
            + FsmTable
            + journal_table_v2::JournalTable,
    {
        self.kill_child_invocations(&invocation_id, metadata.journal_metadata.length, &metadata)
            .await?;

        // No need to go through the Killed state when we're suspended,
        // because it means we already got a terminal state from the invoker.
        self.end_invocation(
            invocation_id,
            metadata,
            Some(ResponseResult::Failure(KILLED_INVOCATION_ERROR)),
        )
        .await?;
        self.do_send_abort_invocation_to_invoker(invocation_id, false);
        Ok(())
    }

    async fn kill_child_invocations(
        &mut self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
        metadata: &InFlightInvocationMetadata,
    ) -> Result<(), Error>
    where
        S: OutboxTable + FsmTable + ReadOnlyJournalTable + journal_table_v2::ReadOnlyJournalTable,
    {
        let invocation_ids_to_kill: Vec<InvocationId> = if metadata
            .pinned_deployment
            .as_ref()
            .is_some_and(|pd| pd.service_protocol_version >= ServiceProtocolVersion::V4)
        {
            journal_table_v2::ReadOnlyJournalTable::get_journal(
                self.storage,
                *invocation_id,
                journal_length,
            )
            .try_filter_map(|(_, journal_entry)| async {
                if let Some(cmd) = journal_entry.inner.try_as_command() {
                    if let journal_v2::raw::RawCommandSpecificMetadata::CallOrSend(
                        call_or_send_metadata,
                    ) = cmd.command_specific_metadata()
                    {
                        return Ok(Some(call_or_send_metadata.invocation_id));
                    }
                }

                Ok(None)
            })
            .try_collect()
            .await?
        } else {
            ReadOnlyJournalTable::get_journal(self.storage, invocation_id, journal_length)
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
                .await?
        };

        for id in invocation_ids_to_kill {
            self.handle_outgoing_message(OutboxMessage::InvocationTermination(
                InvocationTermination::kill(id),
            ))
            .await?;
        }

        Ok(())
    }

    async fn cancel_journal_leaves(
        &mut self,
        invocation_id: InvocationId,
        invocation_status: InvocationStatusProjection,
        journal_length: EntryIndex,
    ) -> Result<bool, Error>
    where
        S: JournalTable + OutboxTable + FsmTable + TimerTable,
    {
        let journal_entries_to_cancel: Vec<(EntryIndex, EnrichedRawEntry)> = self
            .storage
            .get_journal(&invocation_id, journal_length)
            .try_filter_map(|(journal_index, journal_entry)| async move {
                if let JournalEntry::Entry(journal_entry) = journal_entry {
                    if let Some(is_completed) = journal_entry.header().is_completed() {
                        if !is_completed {
                            // Every completable journal entry that hasn't been completed yet should be cancelled
                            return Ok(Some((journal_index, journal_entry)));
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
                EnrichedEntryHeader::Call {
                    enrichment_result: Some(enrichment_result),
                    ..
                } => {
                    // For calls, we don't immediately complete the call entry with cancelled,
                    // but we let the cancellation result propagate from the callee.
                    self.handle_outgoing_message(OutboxMessage::InvocationTermination(
                        InvocationTermination::cancel(enrichment_result.invocation_id),
                    ))
                    .await?;
                }
                EnrichedEntryHeader::Sleep { is_completed } if !is_completed => {
                    resume_invocation |= self
                        .cancel_journal_entry_with(
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

                    self.do_delete_timer(timer_key).await?;
                }
                _ => {
                    resume_invocation |= self
                        .cancel_journal_entry_with(
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

    /// Cancels a generic completable journal entry
    async fn cancel_journal_entry_with(
        &mut self,
        invocation_id: InvocationId,
        invocation_status: &InvocationStatusProjection,
        journal_index: EntryIndex,
        canceled_result: CompletionResult,
    ) -> Result<bool, Error>
    where
        S: JournalTable,
    {
        match invocation_status {
            InvocationStatusProjection::Invoked => {
                self.handle_completion_for_invoked(
                    invocation_id,
                    Completion::new(journal_index, canceled_result),
                )
                .await?;
                Ok(false)
            }
            InvocationStatusProjection::Suspended(waiting_for_completed_entry) => {
                self.handle_completion_for_suspended(
                    invocation_id,
                    Completion::new(journal_index, canceled_result),
                    waiting_for_completed_entry,
                )
                .await
            }
        }
    }

    async fn on_purge_invocation(&mut self, invocation_id: InvocationId) -> Result<(), Error>
    where
        S: InvocationStatusTable
            + IdempotencyTable
            + VirtualObjectStatusTable
            + StateTable
            + PromiseTable,
    {
        match self.get_invocation_status(&invocation_id).await? {
            InvocationStatus::Completed(CompletedInvocation {
                invocation_target,
                idempotency_key,
                ..
            }) => {
                self.do_free_invocation(invocation_id).await;

                // Also cleanup the associated idempotency key if any
                if let Some(idempotency_key) = idempotency_key {
                    self.do_delete_idempotency_id(IdempotencyId::combine(
                        invocation_id,
                        &invocation_target,
                        idempotency_key,
                    ))
                    .await?;
                }

                // For workflow, we should also clean up the service lock, associated state and promises.
                if invocation_target.invocation_target_ty()
                    == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
                {
                    let service_id = invocation_target
                        .as_keyed_service_id()
                        .expect("Workflow methods must have keyed service id");

                    self.do_unlock_service(service_id.clone()).await?;
                    self.do_clear_all_state(service_id.clone(), invocation_id)
                        .await?;
                    self.do_clear_all_promises(service_id).await?;
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

    async fn on_timer(&mut self, timer_value: TimerKeyValue) -> Result<(), Error>
    where
        S: IdempotencyTable
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
            + StateTable
            + journal_table_v2::JournalTable,
    {
        let (key, value) = timer_value.into_inner();
        self.do_delete_timer(key).await?;

        match value {
            Timer::CompleteJournalEntry(invocation_id, entry_index) => {
                let status = self.get_invocation_status(&invocation_id).await?;
                if should_use_journal_table_v2(&status) {
                    // We just apply the journal entry
                    entries::OnJournalEntryCommand::from_entry(
                        invocation_id,
                        status,
                        SleepCompletion {
                            completion_id: entry_index,
                        }
                        .into(),
                    )
                    .apply(self)
                    .await?;
                    return Ok(());
                }

                self.handle_completion(
                    invocation_id,
                    status,
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
                self.on_service_invocation(service_invocation).await
            }
            Timer::CleanInvocationStatus(invocation_id) => {
                self.on_purge_invocation(invocation_id).await
            }
            Timer::NeoInvoke(invocation_id) => self.on_neo_invoke_timer(invocation_id).await,
        }
    }

    async fn on_neo_invoke_timer(&mut self, invocation_id: InvocationId) -> Result<(), Error>
    where
        S: VirtualObjectStatusTable + InvocationStatusTable + InboxTable + FsmTable + JournalTable,
    {
        debug_if_leader!(
            self.is_leader,
            "Handle scheduled invocation timer with invocation id {invocation_id}"
        );
        let invocation_status = self.get_invocation_status(&invocation_id).await?;

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

        self.init_journal_and_invoke(
            invocation_id,
            in_flight_invocation_metadata,
            invocation_input,
        )
        .await
    }

    async fn try_invoker_effect(&mut self, invoker_effect: InvokerEffect) -> Result<(), Error>
    where
        S: InvocationStatusTable
            + JournalTable
            + StateTable
            + PromiseTable
            + OutboxTable
            + FsmTable
            + TimerTable
            + InboxTable
            + VirtualObjectStatusTable
            + journal_table_v2::JournalTable,
    {
        let start = Instant::now();
        let status = self
            .get_invocation_status(&invoker_effect.invocation_id)
            .await?;
        self.on_invoker_effect(invoker_effect, status).await?;
        self.invoker_apply_latency.record(start.elapsed());

        Ok(())
    }

    async fn on_invoker_effect(
        &mut self,
        InvokerEffect {
            invocation_id,
            kind,
        }: InvokerEffect,
        invocation_status: InvocationStatus,
    ) -> Result<(), Error>
    where
        S: InvocationStatusTable
            + JournalTable
            + StateTable
            + PromiseTable
            + OutboxTable
            + FsmTable
            + TimerTable
            + InboxTable
            + VirtualObjectStatusTable
            + journal_table_v2::JournalTable,
    {
        let is_status_invoked = matches!(invocation_status, InvocationStatus::Invoked(_));
        let is_status_killed = matches!(invocation_status, InvocationStatus::Killed(_));

        if !is_status_invoked && !is_status_killed {
            trace!("Received invoker effect for invocation not in invoked nor killed status. Ignoring the effect.");
            self.do_send_abort_invocation_to_invoker(invocation_id, false);
            return Ok(());
        }
        if is_status_killed
            && !matches!(kind, InvokerEffectKind::Failed(_) | InvokerEffectKind::End)
        {
            warn!(
                "Received non terminal invoker effect for killed invocation. Ignoring the effect."
            );
            return Ok(());
        }

        match kind {
            InvokerEffectKind::PinnedDeployment(pinned_deployment) => {
                lifecycle::OnPinnedDeploymentCommand {
                    invocation_id,
                    invocation_status,
                    pinned_deployment,
                }
                .apply(self)
                .await?;
            }
            InvokerEffectKind::JournalEntry { entry_index, entry } => {
                self.handle_journal_entry(
                    invocation_id,
                    entry_index,
                    entry,
                    invocation_status
                        .into_invocation_metadata()
                        .expect("Must be present if status is killed or invoked"),
                )
                .await?;
            }
            InvokerEffectKind::JournalEntryV2 {
                command_index_to_ack,
                entry,
            } => {
                entries::OnJournalEntryCommand::from_raw_entry(
                    invocation_id,
                    invocation_status,
                    entry,
                )
                .apply(self)
                .await?;
                if let Some(command_index_to_ack) = command_index_to_ack {
                    self.action_collector.push(Action::AckStoredCommand {
                        invocation_id,
                        command_index: command_index_to_ack,
                    });
                }
            }
            InvokerEffectKind::Suspended {
                waiting_for_completed_entries,
            } => {
                let invocation_metadata = invocation_status
                    .into_invocation_metadata()
                    .expect("Must be present if status is killed or invoked");
                debug_assert!(
                    !waiting_for_completed_entries.is_empty(),
                    "Expecting at least one entry on which the invocation {invocation_id} is waiting."
                );
                let mut any_completed = false;
                for entry_index in &waiting_for_completed_entries {
                    if ReadOnlyJournalTable::get_journal_entry(
                        self.storage,
                        &invocation_id,
                        *entry_index,
                    )
                    .await?
                    .map(|entry| entry.is_resumable())
                    .unwrap_or_default()
                    {
                        trace!(
                            rpc.service = %invocation_metadata.invocation_target.service_name(),
                            reself.storage.invocation.id = %invocation_id,
                            "Resuming instead of suspending service because an awaited entry is completed/acked.");
                        any_completed = true;
                        break;
                    }
                }
                if any_completed {
                    self.do_resume_service(invocation_id, invocation_metadata)
                        .await?;
                } else {
                    self.do_suspend_service(
                        invocation_id,
                        invocation_metadata,
                        waiting_for_completed_entries,
                    )
                    .await;
                }
            }
            InvokerEffectKind::SuspendedV2 {
                waiting_for_notifications,
            } => {
                lifecycle::OnSuspendCommand {
                    invocation_id,
                    invocation_status,
                    waiting_for_notifications,
                }
                .apply(self)
                .await?;
            }
            InvokerEffectKind::End => {
                self.end_invocation(
                    invocation_id,
                    invocation_status
                        .into_invocation_metadata()
                        .expect("Must be present if status is killed or invoked"),
                    if is_status_killed {
                        // It doesn't matter that the invocation successfully completed, we return failed anyway in this case.
                        Some(ResponseResult::Failure(KILLED_INVOCATION_ERROR))
                    } else {
                        None
                    },
                )
                .await?;
            }
            InvokerEffectKind::Failed(e) => {
                self.end_invocation(
                    invocation_id,
                    invocation_status
                        .into_invocation_metadata()
                        .expect("Must be present if status is killed or invoked"),
                    Some(ResponseResult::Failure(e)),
                )
                .await?;
            }
        }

        Ok(())
    }

    /// TODO(slinkydeveloper) move this to lifecycle command
    async fn end_invocation(
        &mut self,
        invocation_id: InvocationId,
        invocation_metadata: InFlightInvocationMetadata,
        // If given, this will override any Output Entry available in the journal table
        response_result_override: Option<ResponseResult>,
    ) -> Result<(), Error>
    where
        S: InboxTable
            + VirtualObjectStatusTable
            + JournalTable
            + OutboxTable
            + FsmTable
            + InvocationStatusTable
            + StateTable
            + journal_table_v2::JournalTable,
    {
        let invocation_target = invocation_metadata.invocation_target.clone();
        let journal_length = invocation_metadata.journal_metadata.length;
        let completion_retention_time = invocation_metadata.completion_retention_duration;

        // If there are any response sinks, or we need to store back the completed status,
        //  we need to find the latest output entry
        if !invocation_metadata.response_sinks.is_empty() || !completion_retention_time.is_zero() {
            let response_result = if let Some(response_result) = response_result_override {
                response_result
            } else if let Some(response_result) = self
                .read_last_output_entry_result(
                    &invocation_id,
                    journal_length,
                    invocation_metadata
                        .pinned_deployment
                        .as_ref()
                        .map(|pd| pd.service_protocol_version)
                        .unwrap_or_default(),
                )
                .await?
            {
                response_result
            } else {
                // We don't panic on this, although it indicates a bug at the moment.
                warn!("Invocation completed without an output entry. This is not supported yet.");
                return Ok(());
            };

            // Send responses out
            self.send_response_to_sinks(
                invocation_metadata.response_sinks.clone(),
                response_result.clone(),
                Some(invocation_id),
                None,
                Some(&invocation_metadata.invocation_target),
            )
            .await?;

            // Notify invocation result
            self.notify_invocation_result(
                invocation_id,
                invocation_metadata.invocation_target.clone(),
                invocation_metadata.journal_metadata.span_context.clone(),
                // SAFETY: We use this field to send back the notification to ingress, and not as part of the PP deterministic logic.
                unsafe { invocation_metadata.timestamps.creation_time() },
                match &response_result {
                    ResponseResult::Success(_) => Ok(()),
                    ResponseResult::Failure(err) => Err((err.code(), err.message().to_owned())),
                },
            );

            // Store the completed status, if needed
            if !completion_retention_time.is_zero() {
                let completed_invocation = CompletedInvocation::from_in_flight_invocation_metadata(
                    invocation_metadata,
                    response_result,
                );
                self.do_store_completed_invocation(invocation_id, completed_invocation)
                    .await;
            }
        } else {
            // Just notify Ok, no need to read the output entry
            self.notify_invocation_result(
                invocation_id,
                invocation_target.clone(),
                invocation_metadata.journal_metadata.span_context.clone(),
                // SAFETY: We use this field to send back the notification to ingress, and not as part of the PP deterministic logic.
                unsafe { invocation_metadata.timestamps.creation_time() },
                Ok(()),
            );
        }

        // If no retention, immediately cleanup the invocation status
        if completion_retention_time.is_zero() {
            self.do_free_invocation(invocation_id).await;
        }
        self.do_drop_journal(invocation_id, journal_length).await?;

        // Consume inbox and move on
        self.consume_inbox(&invocation_target).await?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_response_to_sinks(
        &mut self,
        response_sinks: impl IntoIterator<Item = ServiceInvocationResponseSink>,
        res: impl Into<ResponseResult>,
        invocation_id: Option<InvocationId>,
        completion_expiry_time: Option<MillisSinceEpoch>,
        invocation_target: Option<&InvocationTarget>,
    ) -> Result<(), Error>
    where
        S: OutboxTable + FsmTable,
    {
        let result = res.into();
        for response_sink in response_sinks {
            match response_sink {
                ServiceInvocationResponseSink::PartitionProcessor {
                    entry_index,
                    caller,
                } => {
                    self.handle_outgoing_message(OutboxMessage::ServiceResponse(
                        InvocationResponse {
                            id: caller,
                            entry_index,
                            result: result.clone(),
                        },
                    ))
                    .await?
                }
                ServiceInvocationResponseSink::Ingress { request_id } => self
                    .send_ingress_response(
                    request_id,
                    invocation_id,
                    completion_expiry_time,
                    match result.clone() {
                        ResponseResult::Success(res) => IngressResponseResult::Success(
                            invocation_target
                                .expect(
                                    "For success responses, there must be an invocation target!",
                                )
                                .clone(),
                            res,
                        ),
                        ResponseResult::Failure(err) => IngressResponseResult::Failure(err),
                    },
                ),
            }
        }
        Ok(())
    }

    async fn consume_inbox(&mut self, invocation_target: &InvocationTarget) -> Result<(), Error>
    where
        S: InboxTable
            + VirtualObjectStatusTable
            + InvocationStatusTable
            + VirtualObjectStatusTable
            + StateTable
            + JournalTable,
    {
        // Inbox exists only for virtual object exclusive handler cases
        if invocation_target.invocation_target_ty()
            == InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive)
        {
            let keyed_service_id = invocation_target.as_keyed_service_id().expect(
                "When the handler type is Exclusive, the invocation target must have a key",
            );

            debug_if_leader!(
                self.is_leader,
                rpc.service = %keyed_service_id,
                "Consume inbox"
            );

            // Pop until we find the first inbox entry.
            // Note: the inbox seq numbers can have gaps.
            while let Some(inbox_entry) = self.storage.pop_inbox(&keyed_service_id).await? {
                match inbox_entry.inbox_entry {
                    InboxEntry::Invocation(_, invocation_id) => {
                        let inboxed_status = self.get_invocation_status(&invocation_id).await?;

                        let_assert!(
                            InvocationStatus::Inboxed(inboxed_invocation) = inboxed_status,
                            "InvocationStatus must contain an Inboxed invocation for the id {}",
                            invocation_id
                        );

                        debug_if_leader!(
                            self.is_leader,
                            rpc.service = %keyed_service_id,
                            "Invoke inboxed"
                        );

                        // Lock the service
                        self.storage
                            .put_virtual_object_status(
                                &keyed_service_id,
                                &VirtualObjectStatus::Locked(invocation_id),
                            )
                            .await;

                        let (in_flight_invocation_meta, invocation_input) =
                            InFlightInvocationMetadata::from_inboxed_invocation(inboxed_invocation);
                        self.init_journal_and_invoke(
                            invocation_id,
                            in_flight_invocation_meta,
                            invocation_input,
                        )
                        .await?;

                        // Started a new invocation
                        return Ok(());
                    }
                    InboxEntry::StateMutation(state_mutation) => {
                        self.mutate_state(state_mutation).await?;
                    }
                }
            }

            // We consumed the inbox, nothing else to do here
            self.storage
                .put_virtual_object_status(&keyed_service_id, &VirtualObjectStatus::Unlocked)
                .await;
        }

        Ok(())
    }

    async fn handle_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        mut journal_entry: EnrichedRawEntry,
        invocation_metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error>
    where
        S: StateTable
            + PromiseTable
            + OutboxTable
            + FsmTable
            + TimerTable
            + JournalTable
            + InvocationStatusTable,
    {
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
                            journal_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                    );

                    if let Some(service_id) =
                        invocation_metadata.invocation_target.as_keyed_service_id()
                    {
                        // Load state and write completion
                        let value = self.storage.get_user_state(&service_id, &key).await?;
                        let completion_result = value
                            .map(CompletionResult::Success)
                            .unwrap_or(CompletionResult::Empty);
                        ProtobufRawEntryCodec::write_completion(
                            &mut journal_entry,
                            completion_result.clone(),
                        )?;

                        self.forward_completion(
                            invocation_id,
                            Completion::new(entry_index, completion_result),
                        );
                    } else {
                        warn!(
                            "Trying to process entry {} for a target that has no state",
                            journal_entry.header().as_entry_type()
                        );
                        self.forward_completion(
                            invocation_id,
                            Completion::new(entry_index, CompletionResult::Empty),
                        );
                    }
                }
            }
            EnrichedEntryHeader::SetState { .. } => {
                let_assert!(
                    Entry::SetState(SetStateEntry { key, value }) =
                        journal_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                );

                let _span = instrumentation::info_invocation_span!(
                    relation = invocation_metadata
                        .journal_metadata
                        .span_context
                        .as_parent(),
                    id = invocation_id,
                    name = format!("set-state {key:?}"),
                    tags = (rpc.service = invocation_metadata
                        .invocation_target
                        .service_name()
                        .to_string())
                );

                if let Some(service_id) =
                    invocation_metadata.invocation_target.as_keyed_service_id()
                {
                    self.do_set_state(service_id, invocation_id, key, value)
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
                        journal_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                );

                let _span = instrumentation::info_invocation_span!(
                    relation = invocation_metadata
                        .journal_metadata
                        .span_context
                        .as_parent(),
                    id = invocation_id,
                    name = "clear-state",
                    tags = (rpc.service = invocation_metadata
                        .invocation_target
                        .service_name()
                        .to_string())
                );

                if let Some(service_id) =
                    invocation_metadata.invocation_target.as_keyed_service_id()
                {
                    self.do_clear_state(service_id, invocation_id, key).await;
                } else {
                    warn!(
                        "Trying to process entry {} for a target that has no state",
                        journal_entry.header().as_entry_type()
                    );
                }
            }
            EnrichedEntryHeader::ClearAllState { .. } => {
                let _span = instrumentation::info_invocation_span!(
                    relation = invocation_metadata
                        .journal_metadata
                        .span_context
                        .as_parent(),
                    id = invocation_id,
                    name = "clear-all-state",
                    tags = (rpc.service = invocation_metadata
                        .invocation_target
                        .service_name()
                        .to_string())
                );

                if let Some(service_id) =
                    invocation_metadata.invocation_target.as_keyed_service_id()
                {
                    self.do_clear_all_state(service_id, invocation_id).await?;
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
                        self.storage
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

                    let completion_result =
                        ProtobufRawEntryCodec::serialize_get_state_keys_completion(value);
                    ProtobufRawEntryCodec::write_completion(
                        &mut journal_entry,
                        completion_result.clone(),
                    )?;

                    // We can already forward the completion
                    self.forward_completion(
                        invocation_id,
                        Completion::new(entry_index, completion_result),
                    );
                }
            }
            EnrichedEntryHeader::GetPromise { is_completed, .. } => {
                if !is_completed {
                    let_assert!(
                        Entry::GetPromise(GetPromiseEntry { key, .. }) =
                            journal_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                    );

                    if let Some(service_id) =
                        invocation_metadata.invocation_target.as_keyed_service_id()
                    {
                        // Load state and write completion
                        let promise_metadata = self.storage.get_promise(&service_id, &key).await?;

                        match promise_metadata {
                            Some(Promise {
                                state: PromiseState::Completed(result),
                            }) => {
                                // Result is already available
                                let completion_result: CompletionResult = result.into();
                                ProtobufRawEntryCodec::write_completion(
                                    &mut journal_entry,
                                    completion_result.clone(),
                                )?;

                                // Forward completion
                                self.forward_completion(
                                    invocation_id,
                                    Completion::new(entry_index, completion_result),
                                );
                            }
                            Some(Promise {
                                state: PromiseState::NotCompleted(mut v),
                            }) => {
                                v.push(JournalEntryId::from_parts(invocation_id, entry_index));
                                self.do_put_promise(
                                    service_id,
                                    key,
                                    Promise {
                                        state: PromiseState::NotCompleted(v),
                                    },
                                )
                                .await;
                            }
                            None => {
                                self.do_put_promise(
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
                        self.forward_completion(
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
                            journal_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                    );

                    if let Some(service_id) =
                        invocation_metadata.invocation_target.as_keyed_service_id()
                    {
                        // Load state and write completion
                        let promise_metadata = self.storage.get_promise(&service_id, &key).await?;

                        let completion_result = match promise_metadata {
                            Some(Promise {
                                state: PromiseState::Completed(result),
                            }) => result.into(),
                            _ => CompletionResult::Empty,
                        };

                        ProtobufRawEntryCodec::write_completion(
                            &mut journal_entry,
                            completion_result.clone(),
                        )?;

                        // Forward completion
                        self.forward_completion(
                            invocation_id,
                            Completion::new(entry_index, completion_result),
                        );
                    } else {
                        warn!(
                            "Trying to process entry {} for a target that has no promises",
                            journal_entry.header().as_entry_type()
                        );
                        self.forward_completion(
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
                        }) = journal_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                    );

                    if let Some(service_id) =
                        invocation_metadata.invocation_target.as_keyed_service_id()
                    {
                        // Load state and write completion
                        let promise_metadata = self.storage.get_promise(&service_id, &key).await?;

                        let completion_result = match promise_metadata {
                            None => {
                                // Just register the promise completion
                                self.do_put_promise(
                                    service_id,
                                    key,
                                    Promise {
                                        state: PromiseState::Completed(completion.into()),
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
                                    self.handle_outgoing_message(OutboxMessage::ServiceResponse(
                                        InvocationResponse {
                                            id: listener.invocation_id(),
                                            entry_index: listener.journal_index(),
                                            result: completion.clone().into(),
                                        },
                                    ))
                                    .await?;
                                }

                                // Now register the promise completion
                                self.do_put_promise(
                                    service_id,
                                    key,
                                    Promise {
                                        state: PromiseState::Completed(completion.into()),
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

                        ProtobufRawEntryCodec::write_completion(
                            &mut journal_entry,
                            completion_result.clone(),
                        )?;

                        // Forward completion
                        self.forward_completion(
                            invocation_id,
                            Completion::new(entry_index, completion_result),
                        );
                    } else {
                        warn!(
                            "Trying to process entry {} for a target that has no promises",
                            journal_entry.header().as_entry_type()
                        );
                        self.forward_completion(
                            invocation_id,
                            Completion::new(entry_index, CompletionResult::Empty),
                        );
                    }
                }
            }
            EnrichedEntryHeader::Sleep { is_completed, .. } => {
                let _span = instrumentation::info_invocation_span!(
                    relation = invocation_metadata
                        .journal_metadata
                        .span_context
                        .as_parent(),
                    id = invocation_id,
                    name = "sleep",
                    tags = (rpc.service = invocation_metadata
                        .invocation_target
                        .service_name()
                        .to_string())
                );

                debug_assert!(!is_completed, "Sleep entry must not be completed.");
                let_assert!(
                    Entry::Sleep(SleepEntry { wake_up_time, .. }) =
                        journal_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                );
                self.register_timer(
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
                            journal_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
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
                        idempotency_key: request.idempotency_key,
                        submit_notification_sink: None,
                    };

                    self.handle_outgoing_message(OutboxMessage::ServiceInvocation(
                        service_invocation,
                    ))
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
                    }) = journal_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                );

                // 0 is equal to not set, meaning execute now
                let delay = if invoke_time == 0 {
                    None
                } else {
                    Some(MillisSinceEpoch::new(invoke_time))
                };

                use opentelemetry::trace::Span;
                let mut span = instrumentation::info_invocation_span!(
                    relation = invocation_metadata
                        .journal_metadata
                        .span_context
                        .as_parent(),
                    prefix = "oneway-call",
                    id = callee_invocation_id,
                    target = callee_invocation_target,
                    tags = ()
                );

                if let SpanRelation::Linked(ctx) = span_context.causing_span_relation() {
                    span.add_link(ctx, Vec::default());
                }

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
                    idempotency_key: request.idempotency_key,
                    submit_notification_sink: None,
                };

                self.handle_outgoing_message(OutboxMessage::ServiceInvocation(service_invocation))
                    .await?;
            }
            EnrichedEntryHeader::Awakeable { is_completed, .. } => {
                debug_assert!(!is_completed, "Awakeable entry must not be completed.");
                // Check the awakeable_completion_received_before_entry test in state_machine/server for more details

                // If completion is already here, let's merge it and forward it.
                if let Some(completion_result) = self
                    .storage
                    .get_journal_entry(&invocation_id, entry_index)
                    .await?
                    .and_then(|journal_entry| match journal_entry {
                        JournalEntry::Entry(_) => None,
                        JournalEntry::Completion(completion_result) => Some(completion_result),
                    })
                {
                    ProtobufRawEntryCodec::write_completion(
                        &mut journal_entry,
                        completion_result.clone(),
                    )?;

                    self.forward_completion(
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
                        journal_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                );

                // Check is this is old or new awakeable id
                if AwakeableIdentifier::from_str(&entry.id).is_ok() {
                    self.handle_outgoing_message(OutboxMessage::from_awakeable_completion(
                        *invocation_id,
                        *entry_index,
                        entry.result.into(),
                    ))
                    .await?;
                } else if let Ok(new_awk_id) = ExternalSignalIdentifier::from_str(&entry.id) {
                    let (invocation_id, signal_id) = new_awk_id.into_inner();
                    self.handle_outgoing_message(OutboxMessage::NotifySignal(
                        NotifySignalRequest {
                            invocation_id,
                            signal: Signal::new(
                                signal_id,
                                match entry.result {
                                    EntryResult::Success(s) => SignalResult::Success(s),
                                    EntryResult::Failure(code, message) => {
                                        SignalResult::Failure(journal_v2::Failure { code, message })
                                    }
                                },
                            ),
                        },
                    ))
                    .await?;
                } else {
                    warn!("Invalid awakeable identifier {}. The identifier doesn't start with `awk_1`, neither with `sign_1`", entry.id);
                };
            }
            EnrichedEntryHeader::Run { .. } => {
                let _span = instrumentation::info_invocation_span!(
                    relation = invocation_metadata
                        .journal_metadata
                        .span_context
                        .as_parent(),
                    id = invocation_id,
                    name = match journal_entry
                        .deserialize_name::<ProtobufRawEntryCodec>()?
                        .as_deref()
                    {
                        None | Some("") => Cow::Borrowed("run"),
                        Some(name) => Cow::Owned(format!("run {name}")),
                    },
                    tags = (rpc.service = invocation_metadata
                        .invocation_target
                        .service_name()
                        .to_string())
                );

                // We just store it
            }
            EnrichedEntryHeader::Custom { .. } => {
                // We just store it
            }
            EntryHeader::CancelInvocation => {
                let_assert!(
                    Entry::CancelInvocation(entry) =
                        journal_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                );
                self.apply_cancel_invocation_journal_entry_action(&invocation_id, entry)
                    .await?;
            }
            EntryHeader::GetCallInvocationId { is_completed } => {
                if !is_completed {
                    let_assert!(
                        Entry::GetCallInvocationId(entry) =
                            journal_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                    );
                    let callee_invocation_id = self
                        .get_journal_entry_callee_invocation_id(
                            &invocation_id,
                            entry.call_entry_index,
                        )
                        .await?;

                    if let Some(callee_invocation_id) = callee_invocation_id {
                        let completion_result = CompletionResult::Success(Bytes::from(
                            callee_invocation_id.to_string(),
                        ));

                        ProtobufRawEntryCodec::write_completion(
                            &mut journal_entry,
                            completion_result.clone(),
                        )?;
                        self.forward_completion(
                            invocation_id,
                            Completion::new(entry_index, completion_result),
                        );
                    } else {
                        // Nothing we can do here, just forward an empty completion (which is invalid for this entry).
                        self.forward_completion(
                            invocation_id,
                            Completion::new(entry_index, CompletionResult::Empty),
                        );
                    }
                }
            }
            EnrichedEntryHeader::AttachInvocation { is_completed } => {
                if !is_completed {
                    let_assert!(
                        Entry::AttachInvocation(entry) =
                            journal_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                    );

                    if let Some(invocation_query) = self
                        .get_invocation_query_from_attach_invocation_target(
                            &invocation_id,
                            entry.target,
                        )
                        .await?
                    {
                        self.handle_outgoing_message(OutboxMessage::AttachInvocation(
                            AttachInvocationRequest {
                                invocation_query,
                                block_on_inflight: true,
                                response_sink: ServiceInvocationResponseSink::partition_processor(
                                    invocation_id,
                                    entry_index,
                                ),
                            },
                        ))
                        .await?;
                    }
                }
            }
            EnrichedEntryHeader::GetInvocationOutput { is_completed } => {
                if !is_completed {
                    let_assert!(
                        Entry::GetInvocationOutput(entry) =
                            journal_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                    );

                    if let Some(invocation_query) = self
                        .get_invocation_query_from_attach_invocation_target(
                            &invocation_id,
                            entry.target,
                        )
                        .await?
                    {
                        self.handle_outgoing_message(OutboxMessage::AttachInvocation(
                            AttachInvocationRequest {
                                invocation_query,
                                block_on_inflight: false,
                                response_sink: ServiceInvocationResponseSink::partition_processor(
                                    invocation_id,
                                    entry_index,
                                ),
                            },
                        ))
                        .await?;
                    }
                }
            }
        }

        self.append_journal_entry(
            invocation_id,
            InvocationStatus::Invoked(invocation_metadata),
            entry_index,
            &JournalEntry::Entry(journal_entry),
        )
        .await;
        // In the old journal world, command_index == entry_index
        self.action_collector.push(Action::AckStoredCommand {
            invocation_id,
            command_index: entry_index,
        });

        Ok(())
    }

    async fn apply_cancel_invocation_journal_entry_action(
        &mut self,
        invocation_id: &InvocationId,
        entry: CancelInvocationEntry,
    ) -> Result<(), Error>
    where
        S: OutboxTable + FsmTable + ReadOnlyJournalTable,
    {
        let target_invocation_id = match entry.target {
            CancelInvocationTarget::InvocationId(id) => {
                if let Ok(id) = id.parse::<InvocationId>() {
                    Some(id)
                } else {
                    warn!(
                        "Error when trying to parse the invocation id '{}' of CancelInvocation. \
                                This should have been previously checked by the invoker.",
                        id
                    );
                    None
                }
            }
            CancelInvocationTarget::CallEntryIndex(call_entry_index) => {
                // Look for the given entry index, then resolve the invocation id.
                self.get_journal_entry_callee_invocation_id(invocation_id, call_entry_index)
                    .await?
            }
        };

        if let Some(target_invocation_id) = target_invocation_id {
            self.handle_outgoing_message(OutboxMessage::InvocationTermination(
                InvocationTermination {
                    invocation_id: target_invocation_id,
                    flavor: TerminationFlavor::Cancel,
                },
            ))
            .await?;
        }
        Ok(())
    }

    async fn get_invocation_query_from_attach_invocation_target(
        &mut self,
        invocation_id: &InvocationId,
        target: AttachInvocationTarget,
    ) -> Result<Option<InvocationQuery>, Error>
    where
        S: ReadOnlyJournalTable,
    {
        Ok(match target {
            AttachInvocationTarget::InvocationId(id) => {
                if let Ok(id) = id.parse::<InvocationId>() {
                    Some(InvocationQuery::Invocation(id))
                } else {
                    warn!(
                        "Error when trying to parse the invocation id '{}' of attach/get output. \
                                This should have been previously checked by the invoker.",
                        id
                    );
                    None
                }
            }
            AttachInvocationTarget::CallEntryIndex(call_entry_index) => {
                // Look for the given entry index, then resolve the invocation id.
                self.get_journal_entry_callee_invocation_id(invocation_id, call_entry_index)
                    .await?
                    .map(InvocationQuery::Invocation)
            }
            AttachInvocationTarget::IdempotentRequest(idempotency_id) => {
                Some(InvocationQuery::IdempotencyId(idempotency_id))
            }
            AttachInvocationTarget::Workflow(service_id) => {
                Some(InvocationQuery::Workflow(service_id))
            }
        })
    }

    async fn get_journal_entry_callee_invocation_id(
        &mut self,
        invocation_id: &InvocationId,
        call_entry_index: EntryIndex,
    ) -> Result<Option<InvocationId>, Error>
    where
        S: ReadOnlyJournalTable,
    {
        Ok(
            match self
                .storage
                .get_journal_entry(invocation_id, call_entry_index)
                .await?
            {
                Some(JournalEntry::Entry(e)) => {
                    match e.header() {
                        EnrichedEntryHeader::Call {
                            enrichment_result: Some(CallEnrichmentResult { invocation_id, .. }),
                            ..
                        }
                        | EnrichedEntryHeader::OneWayCall {
                            enrichment_result: CallEnrichmentResult { invocation_id, .. },
                            ..
                        } => Some(*invocation_id),
                        // This is the corner case when there is no enrichment result due to
                        // the invocation being already completed from the SDK. Nothing to do here.
                        EnrichedEntryHeader::Call {
                            enrichment_result: None,
                            ..
                        } => None,
                        _ => {
                            warn!(
                            "The given journal entry index '{}' is not a Call/OneWayCall entry.",
                            call_entry_index
                        );
                            None
                        }
                    }
                }
                _ => {
                    warn!(
                        "The given journal entry index '{}' does not exist.",
                        call_entry_index
                    );
                    None
                }
            },
        )
    }

    async fn handle_invocation_response_for_journal_v2(
        &mut self,
        invocation_id: InvocationId,
        status: InvocationStatus,
        completion: Completion,
    ) -> Result<(), Error>
    where
        S: JournalTable
            + journal_table_v2::JournalTable
            + InvocationStatusTable
            + TimerTable
            + FsmTable
            + OutboxTable
            + PromiseTable
            + StateTable,
    {
        // We need this code until we remove Service Protocol <= V3, because of the InvocationResponse WAL command.
        // When we get rid of this WAL command, this code should handle just Call and AttachInvocation commands.

        let command = self
            .storage
            .get_command_by_completion_id(invocation_id, completion.entry_index)
            .await?;

        if let Some(cmd) = command {
            let entry: journal_v2::Entry = match cmd.command_type() {
                CommandType::GetPromise => GetPromiseCompletion {
                    completion_id: completion.entry_index,
                    result: match completion.result {
                        CompletionResult::Success(s) => GetPromiseResult::Success(s),
                        CompletionResult::Failure(code, message) => {
                            GetPromiseResult::Failure(journal_v2::Failure { code, message })
                        }
                        CompletionResult::Empty => {
                            return Err(Error::BadCompletionVariantForInvocationResponse(
                                CommandType::GetPromise,
                                completion.entry_index,
                                "Empty",
                            ))
                        }
                    },
                }
                .into(),
                CommandType::Sleep => SleepCompletion {
                    completion_id: completion.entry_index,
                }
                .into(),
                CommandType::Call => CallCompletion {
                    completion_id: completion.entry_index,
                    result: match completion.result {
                        CompletionResult::Success(s) => CallResult::Success(s),
                        CompletionResult::Failure(code, message) => {
                            CallResult::Failure(journal_v2::Failure { code, message })
                        }
                        CompletionResult::Empty => {
                            return Err(Error::BadCompletionVariantForInvocationResponse(
                                CommandType::Call,
                                completion.entry_index,
                                "Empty",
                            ))
                        }
                    },
                }
                .into(),
                CommandType::AttachInvocation => AttachInvocationCompletion {
                    completion_id: completion.entry_index,
                    result: match completion.result {
                        CompletionResult::Success(s) => AttachInvocationResult::Success(s),
                        CompletionResult::Failure(code, message) => {
                            AttachInvocationResult::Failure(journal_v2::Failure { code, message })
                        }
                        CompletionResult::Empty => {
                            return Err(Error::BadCompletionVariantForInvocationResponse(
                                CommandType::AttachInvocation,
                                completion.entry_index,
                                "Empty",
                            ))
                        }
                    },
                }
                .into(),
                CommandType::GetInvocationOutput => {
                    GetInvocationOutputCompletion {
                        completion_id: completion.entry_index,
                        result: match completion.result {
                            CompletionResult::Success(s) => GetInvocationOutputResult::Success(s),
                            failure @ CompletionResult::Failure(_, _)
                                if failure
                                    == CompletionResult::from(&NOT_READY_INVOCATION_ERROR) =>
                            {
                                // Corner case with old journal/state machine
                                GetInvocationOutputResult::Void
                            }
                            CompletionResult::Failure(code, message) => {
                                GetInvocationOutputResult::Failure(journal_v2::Failure {
                                    code,
                                    message,
                                })
                            }
                            CompletionResult::Empty => GetInvocationOutputResult::Void,
                        },
                    }
                    .into()
                }
                cmd_ty => {
                    error!(
                        "Got an invocation response, the command type {cmd_ty} is unexpected for completion index {}. This indicates storage corruption.",
                        completion.entry_index
                    );
                    return Err(Error::BadCommandTypeForInvocationResponse(
                        cmd_ty,
                        completion.entry_index,
                    ));
                }
            };

            entries::OnJournalEntryCommand::from_entry(invocation_id, status, entry)
                .apply(self)
                .await?;
        } else {
            error!(
                "Got an invocation response, but there is no corresponding command in the journal for completion index {}. This indicates storage corruption.",
                completion.entry_index
            );
            return Err(Error::MissingCommandForInvocationResponse(
                completion.entry_index,
            ));
        }

        Ok(())
    }

    async fn handle_completion(
        &mut self,
        invocation_id: InvocationId,
        status: InvocationStatus,
        completion: Completion,
    ) -> Result<(), Error>
    where
        S: JournalTable
            + journal_table_v2::JournalTable
            + InvocationStatusTable
            + TimerTable
            + FsmTable
            + OutboxTable,
    {
        match status {
            InvocationStatus::Invoked(_) => {
                self.handle_completion_for_invoked( invocation_id, completion).await?;
            }
            InvocationStatus::Suspended {
                metadata,
                waiting_for_notifications,
            } => {
                if self.handle_completion_for_suspended(
                    invocation_id,
                    completion,
                    &waiting_for_notifications
                        .into_iter()
                        .map(|n| match n {
                            NotificationId::CompletionId(idx) => idx,
                            _ => panic!("When using Service Protocol <= 3, an invocation cannot be suspended on a named notification")
                        }).collect()
                    ,
                )
                .await?
                {
            self.do_resume_service( invocation_id, metadata).await?;
                }
            }
            _ => {
                debug!(
                    reself.storage.invocation.id = %invocation_id,
                    ?completion,
                    "Ignoring completion for invocation that is no longer running."
                )
            }
        }

        Ok(())
    }

    async fn handle_completion_for_suspended(
        &mut self,
        invocation_id: InvocationId,
        completion: Completion,
        waiting_for_completed_entries: &HashSet<EntryIndex>,
    ) -> Result<bool, Error>
    where
        S: JournalTable,
    {
        let resume_invocation = waiting_for_completed_entries.contains(&completion.entry_index);
        self.store_completion(invocation_id, completion).await?;

        Ok(resume_invocation)
    }

    async fn handle_completion_for_invoked(
        &mut self,
        invocation_id: InvocationId,
        completion: Completion,
    ) -> Result<(), Error>
    where
        S: JournalTable,
    {
        if let Some(completion) = self.store_completion(invocation_id, completion).await? {
            self.forward_completion(invocation_id, completion);
        }
        Ok(())
    }

    async fn read_last_output_entry_result(
        &mut self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
        service_protocol_version: ServiceProtocolVersion,
    ) -> Result<Option<ResponseResult>, Error>
    where
        S: ReadOnlyJournalTable + journal_table_v2::ReadOnlyJournalTable,
    {
        if service_protocol_version >= ServiceProtocolVersion::V4 {
            // Find last output entry
            for i in (0..journal_length).rev() {
                let entry = journal_table_v2::ReadOnlyJournalTable::get_journal_entry(
                    self.storage,
                    *invocation_id,
                    i,
                )
                .await?
                .unwrap_or_else(|| panic!("There should be a journal entry at index {i}"));
                if entry.ty() == journal_v2::EntryType::Command(CommandType::Output) {
                    let cmd = entry.decode::<ServiceProtocolV4Codec, OutputCommand>()?;
                    return Ok(Some(match cmd.result {
                        OutputResult::Success(s) => ResponseResult::Success(s),
                        OutputResult::Failure(f) => ResponseResult::Failure(f.into()),
                    }));
                }
            }
            Ok(None)
        } else {
            // Find last output entry
            let mut output_entry = None;
            for i in (0..journal_length).rev() {
                if let JournalEntry::Entry(e) =
                    ReadOnlyJournalTable::get_journal_entry(self.storage, invocation_id, i)
                        .await?
                        .unwrap_or_else(|| panic!("There should be a journal entry at index {i}"))
                {
                    if e.ty() == EntryType::Output {
                        output_entry = Some(e);
                        break;
                    }
                }
            }

            output_entry
                .map(|enriched_entry| {
                    let_assert!(
                        Entry::Output(e) =
                            enriched_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                    );
                    Ok(e.result.into())
                })
                .transpose()
        }
    }

    fn notify_invocation_result(
        &mut self,
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

        if self.is_leader && span_context.is_sampled() {
            instrumentation::info_invocation_span!(
                relation = span_context.causing_span_relation(),
                prefix = "invoke",
                id = invocation_id,
                target = invocation_target,
                tags = (
                    restate.invocation.result = result,
                    error = error,
                    restate.span.context = format!("{:?}", span_context)
                ),
                fields = (
                    with_start_time = creation_time,
                    with_trace_id = span_context.span_context().trace_id(),
                    with_span_id = span_context.span_context().span_id()
                )
            );
        }
    }

    async fn handle_outgoing_message(&mut self, message: OutboxMessage) -> Result<(), Error>
    where
        S: OutboxTable + FsmTable,
    {
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

        self.do_enqueue_into_outbox(*self.outbox_seq_number, message)
            .await?;
        *self.outbox_seq_number += 1;
        Ok(())
    }

    async fn handle_attach_invocation_request(
        &mut self,
        attach_invocation_request: AttachInvocationRequest,
    ) -> Result<(), Error>
    where
        S: ReadOnlyIdempotencyTable
            + InvocationStatusTable
            + ReadOnlyVirtualObjectStatusTable
            + OutboxTable
            + FsmTable,
    {
        debug_assert!(
            self.partition_key_range.contains(&attach_invocation_request.partition_key()),
            "Attach invocation request with partition key '{}' has been delivered to a partition processor with key range '{:?}'. This indicates a bug.",
            attach_invocation_request.partition_key(),
            self.partition_key_range);

        let invocation_id = match attach_invocation_request.invocation_query {
            InvocationQuery::Invocation(iid) => iid,
            ref q @ InvocationQuery::Workflow(ref sid) => {
                match self.storage.get_virtual_object_status(sid).await? {
                    VirtualObjectStatus::Locked(iid) => iid,
                    VirtualObjectStatus::Unlocked => {
                        // Try the deterministic id
                        q.to_invocation_id()
                    }
                }
            }
            ref q @ InvocationQuery::IdempotencyId(ref iid) => {
                match self.storage.get_idempotency_metadata(iid).await? {
                    Some(idempotency_metadata) => idempotency_metadata.invocation_id,
                    None => {
                        // Try the deterministic id
                        q.to_invocation_id()
                    }
                }
            }
        };
        match self.get_invocation_status(&invocation_id).await? {
            InvocationStatus::Free => {
                self.send_response_to_sinks(
                    vec![attach_invocation_request.response_sink],
                    NOT_FOUND_INVOCATION_ERROR,
                    Some(invocation_id),
                    None,
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
                    vec![attach_invocation_request.response_sink],
                    ATTACH_NOT_SUPPORTED_INVOCATION_ERROR,
                    Some(invocation_id),
                    None,
                    None,
                )
                .await?
            }
            is @ InvocationStatus::Invoked(_)
            | is @ InvocationStatus::Suspended { .. }
            | is @ InvocationStatus::Inboxed(_)
            | is @ InvocationStatus::Scheduled(_) => {
                if attach_invocation_request.block_on_inflight {
                    self.do_append_response_sink(
                        invocation_id,
                        is,
                        attach_invocation_request.response_sink,
                    )
                    .await?;
                } else {
                    self.send_response_to_sinks(
                        vec![attach_invocation_request.response_sink],
                        NOT_READY_INVOCATION_ERROR,
                        Some(invocation_id),
                        None,
                        is.invocation_target(),
                    )
                    .await?;
                }
            }
            InvocationStatus::Killed(metadata) => {
                self.send_response_to_sinks(
                    vec![attach_invocation_request.response_sink],
                    KILLED_INVOCATION_ERROR,
                    Some(invocation_id),
                    None,
                    Some(&metadata.invocation_target),
                )
                .await?;
            }
            InvocationStatus::Completed(completed) => {
                // SAFETY: We use this field to send back the notification to ingress, and not as part of the PP deterministic logic.
                let completion_expiry_time = unsafe { completed.completion_expiry_time() };
                self.send_response_to_sinks(
                    vec![attach_invocation_request.response_sink],
                    completed.response_result,
                    Some(invocation_id),
                    completion_expiry_time,
                    Some(&completed.invocation_target),
                )
                .await?;
            }
        }

        Ok(())
    }

    fn send_ingress_response(
        &mut self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: Option<InvocationId>,
        completion_expiry_time: Option<MillisSinceEpoch>,
        response: IngressResponseResult,
    ) {
        match &response {
            IngressResponseResult::Success(_, _) => {
                debug_if_leader!(
                    self.is_leader,
                    "Send response to ingress with request id '{:?}': Success",
                    request_id
                )
            }
            IngressResponseResult::Failure(e) => {
                debug_if_leader!(
                    self.is_leader,
                    "Send response to ingress with request id '{:?}': Failure({})",
                    request_id,
                    e
                )
            }
        };

        self.action_collector.push(Action::IngressResponse {
            request_id,
            invocation_id,
            completion_expiry_time,
            response,
        });
    }

    fn send_submit_notification_if_needed(
        &mut self,
        invocation_id: InvocationId,
        is_new_invocation: bool,
        submit_notification_sink: Option<SubmitNotificationSink>,
    ) {
        // Notify the ingress, if needed, of the chosen invocation_id
        if let Some(SubmitNotificationSink::Ingress { request_id }) = submit_notification_sink {
            debug_if_leader!(
                self.is_leader,
                "Sending ingress attach invocation for {}",
                invocation_id,
            );

            self.action_collector
                .push(Action::IngressSubmitNotification {
                    request_id,
                    is_new_invocation,
                });
        }
    }

    async fn do_resume_service(
        &mut self,
        invocation_id: InvocationId,
        mut metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error>
    where
        S: InvocationStatusTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.journal.length = metadata.journal_metadata.length,
            "Effect: Resume service"
        );

        metadata.timestamps.update();
        let invocation_target = metadata.invocation_target.clone();
        self.storage
            .put_invocation_status(&invocation_id, &InvocationStatus::Invoked(metadata))
            .await;

        self.action_collector.push(Action::Invoke {
            invocation_id,
            invocation_target,
            invoke_input_journal: InvokeInputJournal::NoCachedJournal,
        });

        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        level="info",
        name="suspend",
        fields(
            metadata.journal.length = metadata.journal_metadata.length,
            restate.invocation.id = %invocation_id)
        )
    ]
    async fn do_suspend_service(
        &mut self,
        invocation_id: InvocationId,
        mut metadata: InFlightInvocationMetadata,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    ) where
        S: InvocationStatusTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.journal.length = metadata.journal_metadata.length,
            "Effect: Suspend service waiting on entries {:?}",
            waiting_for_completed_entries
        );

        metadata.timestamps.update();
        self.storage
            .put_invocation_status(
                &invocation_id,
                &InvocationStatus::Suspended {
                    metadata,
                    waiting_for_notifications: waiting_for_completed_entries
                        .into_iter()
                        .map(NotificationId::CompletionId)
                        .collect(),
                },
            )
            .await;
    }

    async fn do_store_completed_invocation(
        &mut self,
        invocation_id: InvocationId,
        completed_invocation: CompletedInvocation,
    ) where
        S: InvocationStatusTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.invocation.id = %invocation_id,
            "Effect: Store completed invocation"
        );

        self.storage
            .put_invocation_status(
                &invocation_id,
                &InvocationStatus::Completed(completed_invocation),
            )
            .await;
    }

    async fn do_free_invocation(&mut self, invocation_id: InvocationId)
    where
        S: InvocationStatusTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.invocation.id = %invocation_id,
            "Effect: Free invocation"
        );

        self.storage
            .put_invocation_status(&invocation_id, &InvocationStatus::Free)
            .await;
    }

    async fn do_delete_inbox_entry(
        &mut self,
        service_id: ServiceId,
        sequence_number: MessageIndex,
    ) -> Result<(), Error>
    where
        S: InboxTable,
    {
        debug_if_leader!(
            self.is_leader,
            rpc.service = %service_id.service_name,
            restate.inbox.seq = sequence_number,
            "Effect: Delete inbox entry",
        );

        self.storage
            .delete_inbox_entry(&service_id, sequence_number)
            .await;

        Ok(())
    }

    async fn do_enqueue_into_outbox(
        &mut self,
        seq_number: MessageIndex,
        message: OutboxMessage,
    ) -> Result<(), Error>
    where
        S: OutboxTable + FsmTable,
    {
        match &message {
            OutboxMessage::ServiceInvocation(service_invocation) => {
                debug_if_leader!(
                    self.is_leader,
                    rpc.service = %service_invocation.invocation_target.service_name(),
                    rpc.method = %service_invocation.invocation_target.handler_name(),
                    restate.invocation.id = %service_invocation.invocation_id,
                    restate.invocation.target = %service_invocation.invocation_target,
                    restate.outbox.seq = seq_number,
                    "Effect: Send service invocation to partition processor"
                )
            }
            OutboxMessage::ServiceResponse(InvocationResponse {
                result: ResponseResult::Success(_),
                entry_index,
                id,
            }) => {
                debug_if_leader!(
                    self.is_leader,
                    restate.invocation.id = %id,
                    restate.outbox.seq = seq_number,
                    "Effect: Send success response to another invocation, completing entry index {}",
                    entry_index
                )
            }
            OutboxMessage::InvocationTermination(invocation_termination) => {
                debug_if_leader!(
                    self.is_leader,
                    restate.invocation.id = %invocation_termination.invocation_id,
                    restate.outbox.seq = seq_number,
                    "Effect: Send invocation termination command '{:?}' to partition processor",
                    invocation_termination.flavor
                )
            }
            OutboxMessage::ServiceResponse(InvocationResponse {
                result: ResponseResult::Failure(e),
                entry_index,
                id,
            }) => {
                debug_if_leader!(
                    self.is_leader,
                    restate.invocation.id = %id,
                    restate.outbox.seq = seq_number,
                    "Effect: Send failure '{}' response to another invocation, completing entry index {}",
                    e,
                    entry_index
                )
            }
            OutboxMessage::AttachInvocation(AttachInvocationRequest {
                invocation_query, ..
            }) => {
                debug_if_leader!(
                    self.is_leader,
                    restate.outbox.seq = seq_number,
                    "Effect: Enqueuing attach invocation request to '{:?}'",
                    invocation_query,
                )
            }
            OutboxMessage::NotifySignal(NotifySignalRequest {
                invocation_id,
                signal,
            }) => {
                debug_if_leader!(
                    self.is_leader,
                    restate.outbox.seq = seq_number,
                    "Notifying signal to {invocation_id} with signal id {:?}",
                    signal.id,
                )
            }
        };

        self.storage.put_outbox_message(seq_number, &message).await;
        // need to store the next outbox sequence number
        self.storage.put_outbox_seq_number(seq_number + 1).await;

        self.action_collector.push(Action::NewOutboxMessage {
            seq_number,
            message,
        });

        Ok(())
    }

    async fn do_unlock_service(&mut self, service_id: ServiceId) -> Result<(), Error>
    where
        S: VirtualObjectStatusTable,
    {
        debug_if_leader!(
            self.is_leader,
            rpc.service = %service_id.service_name,
            "Effect: Unlock service id",
        );

        self.storage
            .put_virtual_object_status(&service_id, &VirtualObjectStatus::Unlocked)
            .await;

        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        level="info",
        name="set_state",
        fields(
            restate.invocation.id = %invocation_id,
            restate.state.key = ?key,
            rpc.service = %service_id.service_name
        )
    )]
    async fn do_set_state(
        &mut self,
        service_id: ServiceId,
        invocation_id: InvocationId,
        key: Bytes,
        value: Bytes,
    ) where
        S: StateTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.state.key = ?key,
            "Effect: Set state"
        );

        self.storage.put_user_state(&service_id, key, value).await;
    }

    #[tracing::instrument(
        skip_all,
        level="info",
        name="clear_state",
        fields(
            restate.invocation.id = %invocation_id,
            restate.state.key = ?key,
            rpc.service = %service_id.service_name
        )
    )]
    async fn do_clear_state(
        &mut self,
        service_id: ServiceId,
        invocation_id: InvocationId,
        key: Bytes,
    ) where
        S: StateTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.state.key = ?key,
            "Effect: Clear state"
        );

        self.storage.delete_user_state(&service_id, &key).await;
    }

    #[tracing::instrument(
        skip_all,
        level="info",
        name="clear_all_state",
        fields(
            restate.invocation.id = %invocation_id,
            rpc.service = %service_id.service_name
        )
    )]
    async fn do_clear_all_state(
        &mut self,
        service_id: ServiceId,
        invocation_id: InvocationId,
    ) -> Result<(), Error>
    where
        S: StateTable,
    {
        debug_if_leader!(self.is_leader, "Effect: Clear all state");

        self.storage.delete_all_user_state(&service_id).await?;

        Ok(())
    }

    async fn do_delete_timer(&mut self, timer_key: TimerKey) -> Result<(), Error>
    where
        S: TimerTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.timer.key = %TimerKeyDisplay(&timer_key),
            "Effect: Delete timer"
        );

        self.storage.delete_timer(&timer_key).await;
        self.action_collector
            .push(Action::DeleteTimer { timer_key });

        Ok(())
    }

    async fn append_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        // We pass around the invocation_status here to avoid an additional read.
        // We could in theory get rid of this here (and in other places, such as StoreDeploymentId),
        // by using a merge operator in rocksdb.
        mut previous_invocation_status: InvocationStatus,
        entry_index: EntryIndex,
        journal_entry: &JournalEntry,
    ) where
        S: JournalTable + InvocationStatusTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.journal.index = entry_index,
            restate.invocation.id = %invocation_id,
            "Write journal entry {:?} to storage",
            journal_entry.entry_type()
        );

        // Store journal entry
        self.storage
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
        self.storage
            .put_invocation_status(&invocation_id, &previous_invocation_status)
            .await;
    }

    async fn do_drop_journal(
        &mut self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) -> Result<(), Error>
    where
        S: JournalTable + journal_table_v2::JournalTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.journal.length = journal_length,
            "Effect: Drop journal"
        );

        // TODO: Only drop journals if the inbox is empty; this requires that keep track of the max journal length: https://github.com/restatedev/restate/issues/272
        JournalTable::delete_journal(self.storage, &invocation_id, journal_length).await;
        journal_table_v2::JournalTable::delete_journal(self.storage, invocation_id, journal_length)
            .await?;
        Ok(())
    }

    async fn do_truncate_outbox(&mut self, range: RangeInclusive<MessageIndex>) -> Result<(), Error>
    where
        S: OutboxTable,
    {
        trace!(
            restate.outbox.seq_from = range.start(),
            restate.outbox.seq_to = range.end(),
            "Effect: Truncate outbox"
        );

        self.storage.truncate_outbox(range).await;

        Ok(())
    }

    /// Returns the completion if it should be forwarded.
    async fn store_completion(
        &mut self,
        invocation_id: InvocationId,
        mut completion: Completion,
    ) -> Result<Option<Completion>, Error>
    where
        S: JournalTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.journal.index = completion.entry_index,
            "Store completion {}",
            CompletionResultFmt(&completion.result)
        );

        if let Some(mut journal_entry) = self
            .storage
            .get_journal_entry(&invocation_id, completion.entry_index)
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
                    restate.journal.index = completion.entry_index,
                    "Trying to complete an awakeable already completed. Ignoring this completion");
                debug!("Discarded awakeable completion: {:?}", completion.result);
                return Ok(None);
            }
            if journal_entry.header().is_completed() == Some(true) {
                // We use error level here as this can happen only in case there is some bug
                // in the Partition Processor/Invoker.
                error!(
                    restate.invocation.id = %invocation_id,
                    restate.journal.index = completion.entry_index,
                    "Trying to complete the entry {:?}, but it's already completed. This is a bug.",
                    journal_entry.ty());
                return Ok(None);
            }
            if journal_entry.ty() == EntryType::GetInvocationOutput
                && completion.result == CompletionResult::from(&NOT_READY_INVOCATION_ERROR)
            {
                // For GetInvocationOutput, we convert the not ready error in an empty response.
                // This is a byproduct of the fact that for now we keep simple the invocation response contract.
                // This should be re-evaluated when we'll rework the journal with the immutable log
                completion.result = CompletionResult::Empty;
            }

            ProtobufRawEntryCodec::write_completion(&mut journal_entry, completion.result.clone())?;
            self.storage
                .put_journal_entry(
                    &invocation_id,
                    completion.entry_index,
                    &JournalEntry::Entry(journal_entry),
                )
                .await;
            Ok(Some(completion))
        } else {
            // In case we don't have the journal entry (only awakeables case),
            // we'll send the completion afterward once we receive the entry.
            self.storage
                .put_journal_entry(
                    &invocation_id,
                    completion.entry_index,
                    &JournalEntry::Completion(completion.result),
                )
                .await;
            Ok(None)
        }
    }

    fn forward_completion(&mut self, invocation_id: InvocationId, completion: Completion) {
        debug_if_leader!(
            self.is_leader,
            restate.journal.index = completion.entry_index,
            "Forward completion {} to deployment",
            CompletionResultFmt(&completion.result)
        );

        self.action_collector.push(Action::ForwardCompletion {
            invocation_id,
            completion,
        });
    }

    async fn do_append_response_sink(
        &mut self,
        invocation_id: InvocationId,
        // We pass around the invocation_status here to avoid an additional read.
        // We could in theory get rid of this here (and in other places, such as StoreDeploymentId),
        // by using a merge operator in rocksdb.
        mut previous_invocation_status: InvocationStatus,
        additional_response_sink: ServiceInvocationResponseSink,
    ) -> Result<(), Error>
    where
        S: InvocationStatusTable,
    {
        debug_if_leader!(
            self.is_leader,
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

        self.storage
            .put_invocation_status(&invocation_id, &previous_invocation_status)
            .await;

        Ok(())
    }

    async fn do_store_idempotency_id(
        &mut self,
        idempotency_id: IdempotencyId,
        invocation_id: InvocationId,
    ) -> Result<(), Error>
    where
        S: IdempotencyTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.invocation.id = %invocation_id,
            "Effect: Store idempotency id {:?}",
            idempotency_id
        );

        self.storage
            .put_idempotency_metadata(&idempotency_id, &IdempotencyMetadata { invocation_id })
            .await;

        Ok(())
    }

    async fn do_delete_idempotency_id(&mut self, idempotency_id: IdempotencyId) -> Result<(), Error>
    where
        S: IdempotencyTable,
    {
        debug_if_leader!(
            self.is_leader,
            "Effect: Delete idempotency id {:?}",
            idempotency_id
        );

        self.storage
            .delete_idempotency_metadata(&idempotency_id)
            .await;

        Ok(())
    }

    fn do_send_abort_invocation_to_invoker(
        &mut self,
        invocation_id: InvocationId,
        acknowledge: bool,
    ) {
        debug_if_leader!(self.is_leader, restate.invocation.id = %invocation_id, "Send abort command to invoker");

        self.action_collector.push(Action::AbortInvocation {
            invocation_id,
            acknowledge,
        });
    }

    async fn do_mutate_state(&mut self, state_mutation: ExternalStateMutation) -> Result<(), Error>
    where
        S: StateTable,
    {
        debug_if_leader!(
            self.is_leader,
            "Effect: Mutate state for service id '{:?}'",
            &state_mutation.service_id
        );

        self.mutate_state(state_mutation).await?;

        Ok(())
    }

    async fn do_put_promise(&mut self, service_id: ServiceId, key: ByteString, promise: Promise)
    where
        S: PromiseTable,
    {
        debug_if_leader!(self.is_leader, rpc.service = %service_id.service_name, "Effect: Put promise {} in non completed state", key);

        self.storage.put_promise(&service_id, &key, &promise).await;
    }

    async fn do_clear_all_promises(&mut self, service_id: ServiceId) -> Result<(), Error>
    where
        S: PromiseTable,
    {
        debug_if_leader!(
            self.is_leader,
            rpc.service = %service_id.service_name,
            "Effect: Clear all promises"
        );

        self.storage.delete_all_promises(&service_id).await;

        Ok(())
    }

    async fn mutate_state(&mut self, state_mutation: ExternalStateMutation) -> StorageResult<()>
    where
        S: StateTable,
    {
        let ExternalStateMutation {
            service_id,
            version,
            state,
        } = state_mutation;

        // overwrite all existing key value pairs with the provided ones; delete all entries that
        // are not contained in state
        let all_user_states: Vec<(Bytes, Bytes)> = self
            .storage
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
                self.storage.delete_user_state(&service_id, key).await;
            }
        }

        // overwrite existing key value pairs
        for (key, value) in state {
            self.storage.put_user_state(&service_id, key, value).await
        }

        Ok(())
    }
}

// To write completions in the effects log
struct CompletionResultFmt<'a>(&'a CompletionResult);

impl<'a> fmt::Display for CompletionResultFmt<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            CompletionResult::Empty => write!(f, "Empty"),
            CompletionResult::Success(_) => write!(f, "Success"),
            CompletionResult::Failure(code, reason) => write!(f, "Failure({code}, {reason})"),
        }
    }
}

/// Projected [`InvocationStatus`] for cancellation purposes.
enum InvocationStatusProjection {
    Invoked,
    Suspended(HashSet<EntryIndex>),
}

fn should_use_journal_table_v2(status: &InvocationStatus) -> bool {
    status
        .get_invocation_metadata()
        .and_then(|im| im.pinned_deployment.as_ref())
        .is_some_and(|pinned_deployment| {
            pinned_deployment.service_protocol_version >= ServiceProtocolVersion::V4
        })
}

#[cfg(test)]
mod tests;
