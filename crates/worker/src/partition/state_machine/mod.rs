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

pub use actions::{Action, ActionCollector};

use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::time::Instant;

use assert2::let_assert;
use bytes::Bytes;
use bytestring::ByteString;
use enumset::EnumSet;
use futures::{StreamExt, TryStreamExt};
use metrics::{counter, histogram};
use tracing::{Instrument, Span, debug, error, info, trace, warn};

use restate_invoker_api::InvokeInputJournal;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::Result as StorageResult;
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::idempotency_table::{IdempotencyTable, ReadOnlyIdempotencyTable};
use restate_storage_api::inbox_table::{InboxEntry, WriteInboxTable};
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, InboxedInvocation, JournalRetentionPolicy,
    PreFlightInvocationArgument, PreFlightInvocationJournal, PreFlightInvocationMetadata,
    ReadInvocationStatusTable, WriteInvocationStatusTable,
};
use restate_storage_api::invocation_status_table::{InvocationStatus, ScheduledInvocation};
use restate_storage_api::journal_events::WriteJournalEventsTable;
use restate_storage_api::journal_table::ReadJournalTable;
use restate_storage_api::journal_table::{JournalEntry, WriteJournalTable};
use restate_storage_api::journal_table_v2;
use restate_storage_api::outbox_table::{OutboxMessage, WriteOutboxTable};
use restate_storage_api::promise_table::{
    Promise, PromiseState, ReadPromiseTable, WritePromiseTable,
};
use restate_storage_api::service_status_table::{
    ReadVirtualObjectStatusTable, VirtualObjectStatus, WriteVirtualObjectStatusTable,
};
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_storage_api::timer_table::TimerKey;
use restate_storage_api::timer_table::{Timer, WriteTimerTable};
use restate_storage_api::vqueue_table::{self, EntryId, EntryKind, Stage, WaitStats};
use restate_storage_api::vqueue_table::{
    AsEntryStateHeader, EntryCard, ReadVQueueTable, VisibleAt, WriteVQueueTable,
};
use restate_tracing_instrumentation as instrumentation;
use restate_types::clock::UniqueTimestamp;
use restate_types::config::Configuration;
use restate_types::errors::{
    ALREADY_COMPLETED_INVOCATION_ERROR, CANCELED_INVOCATION_ERROR, GenericError,
    InvocationErrorCode, KILLED_INVOCATION_ERROR, NOT_FOUND_INVOCATION_ERROR,
    NOT_READY_INVOCATION_ERROR, WORKFLOW_ALREADY_INVOKED_INVOCATION_ERROR,
};
use restate_types::identifiers::{
    AwakeableIdentifier, EntryIndex, ExternalSignalIdentifier, InvocationId, InvocationUuid,
    PartitionKey, PartitionProcessorRpcRequestId, ServiceId,
};
use restate_types::identifiers::{IdempotencyId, WithPartitionKey};
use restate_types::invocation::client::{
    CancelInvocationResponse, InvocationOutputResponse, KillInvocationResponse,
    PurgeInvocationResponse, ResumeInvocationResponse,
};
use restate_types::invocation::{
    AttachInvocationRequest, IngressInvocationResponseSink, InvocationMutationResponseSink,
    InvocationQuery, InvocationResponse, InvocationTarget, InvocationTargetType,
    InvocationTermination, JournalCompletionTarget, NotifySignalRequest, ResponseResult,
    ServiceInvocation, ServiceInvocationResponseSink, ServiceInvocationSpanContext, ServiceType,
    Source, SubmitNotificationSink, TerminationFlavor, VirtualObjectHandlerType,
    WorkflowHandlerType,
};
use restate_types::invocation::{InvocationInput, SpanRelation};
use restate_types::journal::Completion;
use restate_types::journal::CompletionResult;
use restate_types::journal::EntryType;
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::enriched::{
    AwakeableEnrichmentResult, CallEnrichmentResult, EnrichedEntryHeader,
};
use restate_types::journal::raw::{EntryHeader, RawEntryCodec, RawEntryCodecError};
use restate_types::journal_v2;
use restate_types::journal_v2::command::{OutputCommand, OutputResult};
use restate_types::journal_v2::raw::{RawEntry, RawNotification};
use restate_types::journal_v2::{
    CommandIndex, CommandType, CompletionId, EntryMetadata, NotificationId, Signal, SignalResult,
};
use restate_types::logs::Lsn;
use restate_types::message::MessageIndex;
use restate_types::schema::Schema;
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::state_mut::ExternalStateMutation;
use restate_types::state_mut::StateMutationVersion;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueue::{NewEntryPriority, VQueueId, VQueueInstance, VQueueParent};
use restate_types::{RestateVersion, SemanticRestateVersion};
use restate_types::{Versioned, journal::*};
use restate_vqueues::{VQueues, VQueuesMetaMut};
use restate_wal_protocol::timer::TimerKeyDisplay;
use restate_wal_protocol::timer::TimerKeyValue;
use restate_wal_protocol::{Command, vqueues};

use self::utils::SpanExt;
use crate::metric_definitions::{PARTITION_APPLY_COMMAND, USAGE_LEADER_JOURNAL_ENTRY_COUNT};
use crate::partition::state_machine::lifecycle::OnCancelCommand;
use crate::partition::types::{InvokerEffect, InvokerEffectKind, OutboxMessageExt};

#[derive(Debug, Hash, enumset::EnumSetType, strum::Display)]
pub enum ExperimentalFeature {}

pub struct StateMachine {
    // initialized from persistent storage
    pub(crate) inbox_seq_number: MessageIndex,
    /// First outbox message index.
    pub(crate) outbox_head_seq_number: Option<MessageIndex>,
    /// The minimum version of restate server that we currently support
    pub(crate) min_restate_version: SemanticRestateVersion,
    /// Sequence number of the next outbox message to be appended.
    pub(crate) outbox_seq_number: MessageIndex,
    /// Consistent schema
    pub(crate) schema: Option<Schema>,

    pub(crate) partition_key_range: RangeInclusive<PartitionKey>,

    /// Enabled experimental features.
    pub(crate) experimental_features: EnumSet<ExperimentalFeature>,
}

impl Debug for StateMachine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateMachine")
            .field("inbox_seq_number", &self.inbox_seq_number)
            .field("outbox_head_seq_number", &self.outbox_head_seq_number)
            .field("outbox_seq_number", &self.outbox_seq_number)
            .field("min_restate_version", &self.min_restate_version)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(
        "partition is blocked; requires and upgrade to restate-server version \
        {required_min_version} or higher; reason='{barrier_reason}'"
    )]
    VersionBarrier {
        required_min_version: SemanticRestateVersion,
        barrier_reason: String,
    },
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
    #[error(
        "error when trying to apply invocation response with completion id {1}, the entry type {0} is not expected to be completed through InvocationResponse command"
    )]
    BadCommandTypeForInvocationResponse(journal_v2::CommandType, CompletionId),
    #[error(
        "error when trying to apply invocation response with completion id {0}, because no command was found for given completion id"
    )]
    MissingCommandForInvocationResponse(CompletionId),
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
        min_restate_version: SemanticRestateVersion,
        experimental_features: EnumSet<ExperimentalFeature>,
        schema: Option<Schema>,
    ) -> Self {
        Self {
            inbox_seq_number,
            outbox_seq_number,
            outbox_head_seq_number,
            partition_key_range,
            min_restate_version,
            experimental_features,
            schema,
        }
    }
}

pub(crate) struct StateMachineApplyContext<'a, S> {
    storage: &'a mut S,
    record_created_at: MillisSinceEpoch,
    record_lsn: Lsn,
    action_collector: &'a mut ActionCollector,
    vqueues_cache: &'a mut VQueuesMetaMut,
    inbox_seq_number: &'a mut MessageIndex,
    outbox_seq_number: &'a mut MessageIndex,
    outbox_head_seq_number: &'a mut Option<MessageIndex>,
    min_restate_version: &'a mut SemanticRestateVersion,
    schema: &'a mut Option<Schema>,
    partition_key_range: RangeInclusive<PartitionKey>,
    #[allow(dead_code)]
    experimental_features: &'a EnumSet<ExperimentalFeature>,
    is_leader: bool,
}

trait CommandHandler<CTX> {
    async fn apply(self, ctx: CTX) -> Result<(), Error>;
}

impl StateMachine {
    // todo(soli):
    // - This should accept `LsnEnvelope` instead of `Command` to get access to created_at, header,
    // and lsn.
    // - Accept `LsnEnvelope` by reference.
    #[allow(clippy::too_many_arguments)]
    pub async fn apply<TransactionType: restate_storage_api::Transaction + Send>(
        &mut self,
        command: Command,
        record_created_at: MillisSinceEpoch,
        record_lsn: Lsn,
        transaction: &mut TransactionType,
        action_collector: &mut ActionCollector,
        vqueues_cache: &mut VQueuesMetaMut,
        is_leader: bool,
    ) -> Result<(), Error> {
        let span = utils::state_machine_apply_command_span(is_leader, &command);
        async {
            let start = Instant::now();
            // Apply the command
            let command_type = command.name();
            let res = StateMachineApplyContext {
                storage: transaction,
                record_created_at,
                record_lsn,
                action_collector,
                inbox_seq_number: &mut self.inbox_seq_number,
                outbox_seq_number: &mut self.outbox_seq_number,
                outbox_head_seq_number: &mut self.outbox_head_seq_number,
                min_restate_version: &mut self.min_restate_version,
                vqueues_cache,
                schema: &mut self.schema,
                partition_key_range: self.partition_key_range.clone(),
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

impl<S> StateMachineApplyContext<'_, S> {
    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<InvocationStatus, Error>
    where
        S: ReadInvocationStatusTable,
    {
        Span::current().record_invocation_id(invocation_id);
        let status = self.storage.get_invocation_status(invocation_id).await?;

        if let Some(invocation_target) = status.invocation_target() {
            Span::current().record_invocation_target(invocation_target);
        }
        Ok(status)
    }

    fn register_timer(
        &mut self,
        timer_value: TimerKeyValue,
        span_context: ServiceInvocationSpanContext,
    ) -> Result<(), Error>
    where
        S: WriteTimerTable,
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
            .map_err(Error::Storage)?;

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

    fn send_abort_invocation_to_invoker(&mut self, invocation_id: InvocationId) {
        debug_if_leader!(
            self.is_leader,
            restate.invocation.id = %invocation_id,
            "Send abort command to invoker"
        );

        self.action_collector
            .push(Action::AbortInvocation { invocation_id });
    }

    async fn on_apply(&mut self, command: Command) -> Result<(), Error>
    where
        S: IdempotencyTable
            + ReadPromiseTable
            + WritePromiseTable
            + ReadJournalTable
            + WriteJournalTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + WriteOutboxTable
            + WriteFsmTable
            + WriteTimerTable
            + ReadVirtualObjectStatusTable
            + WriteVirtualObjectStatusTable
            + WriteInboxTable
            + ReadStateTable
            + WriteStateTable
            + WriteVQueueTable
            + ReadVQueueTable
            + journal_table_v2::WriteJournalTable
            + journal_table_v2::ReadJournalTable
            + WriteJournalEventsTable,
    {
        match command {
            Command::VQWaitingToRunning(encoded_cmd) => {
                // move the entry from inbox and notify the scheduler that it has started
                // also, ship to invoker.
                let command = vqueues::VQWaitingToRunning::decode(encoded_cmd)?;
                self.attempt_to_run(command).await?;
                Ok(())
            }
            // perhaps consolidate with the one above.
            Command::VQYieldRunning(encoded_cmd) => {
                // move the entry from inbox and notify the scheduler that it has started
                // also, ship to invoker.
                let cmd = vqueues::VQYieldRunning::decode(encoded_cmd)?;
                tracing::info!(
                    "Entry in qid_parent={}, instance={} should be placed back to the waiting queue",
                    cmd.assignment.parent,
                    cmd.assignment.instance
                );
                let qid = VQueueId::new(
                    VQueueParent::from_raw(cmd.assignment.parent),
                    cmd.assignment.partition_key,
                    VQueueInstance::from_raw(cmd.assignment.instance),
                );
                let mut inbox = VQueues::new(
                    qid,
                    self.storage,
                    self.vqueues_cache,
                    self.is_leader.then_some(self.action_collector),
                );

                let record_unique_ts =
                    UniqueTimestamp::from_unix_millis(self.record_created_at).unwrap();
                for entry in cmd.assignment.entries {
                    inbox.yield_running(record_unique_ts, entry.card).await?;
                }
                Ok(())
            }
            Command::UpdatePartitionDurability(_) => {
                // no-op :-)
                //
                // This is a partition-level command that doesn't impact the state machine.
                // Handling of this command should have happened without entering the state machine
                // on_apply() method.
                Ok(())
            }
            Command::VersionBarrier(barrier) => {
                // We have versions in play:
                // - Our binary's version (this process)
                // - `min_restate_version` coming from the FSM
                // - `barrier.version` from bifrost.
                //
                // If we can process this command, we update the FSM.
                //
                // We can process this command if our own version is at or higher than the barrier
                // version as indicated by the message. We'll apply the change to the FSM only
                // if the new barrier version is higher than what the FSM already has.
                //
                // If we can't, then what?
                //
                // In v1.4 we crash the PP but tell a good message. This is not the best solution
                // but it'll make clear what's going on. The issue with this approach is that we
                // will probably continue restarting PP on the same node leading to unavailability.
                //
                // [todo] What's the ideal scenario?
                // - Ideal scenario is that we inform the operator (flare).
                // - We mark this node *generational* as a bad candidate (not to take leadership
                //   or run follower again).
                // - Through gossip, this node broadcasts its partition block-list so it won't be
                //   considered for leadership until a new generation pops up.
                //   Noting that the blocklist for a generational node can only increase/grow until
                //   the daemon is restarted (higher generation).
                // - Controller attempts to reconfigure or selects a different leader
                //   that's not blocking this partition if such replacement exists.
                // - Peers will not pick this node as leader candidate when performing
                //   adhoc failovers.
                if SemanticRestateVersion::current().is_equal_or_newer_than(&barrier.version) {
                    // Feels amazing to be running a new version of restate!
                    lifecycle::OnVersionBarrierCommand { barrier }
                        .apply(self)
                        .await?;
                    Ok(())
                } else {
                    Err(Error::VersionBarrier {
                        required_min_version: barrier.version,
                        barrier_reason: barrier.human_reason.unwrap_or_default(),
                    })
                }
            }
            Command::Invoke(service_invocation) => {
                self.on_service_invocation(service_invocation).await
            }
            Command::InvocationResponse(InvocationResponse { target, result }) => {
                let status = self.get_invocation_status(&target.caller_id).await?;

                if should_use_journal_table_v2(&status) {
                    lifecycle::OnNotifyInvocationResponse {
                        invocation_id: target.caller_id,
                        status,
                        caller_completion_id: target.caller_completion_id,
                        result,
                    }
                    .apply(self)
                    .await?;
                    return Ok(());
                }

                let completion = Completion {
                    entry_index: target.caller_completion_id,
                    result: result.into(),
                };
                self.handle_completion(target.caller_id, status, completion)
                    .await
            }
            Command::ProxyThrough(service_invocation) => {
                self.handle_outgoing_message(OutboxMessage::ServiceInvocation(service_invocation))?;
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
                lifecycle::OnPurgeCommand {
                    invocation_id: purge_invocation_request.invocation_id,
                    response_sink: purge_invocation_request.response_sink,
                }
                .apply(self)
                .await?;
                Ok(())
            }
            Command::PurgeJournal(purge_invocation_request) => {
                lifecycle::OnPurgeJournalCommand {
                    invocation_id: purge_invocation_request.invocation_id,
                    response_sink: purge_invocation_request.response_sink,
                }
                .apply(self)
                .await?;
                Ok(())
            }
            Command::ResumeInvocation(resume_invocation_request) => {
                lifecycle::OnManualResumeCommand {
                    invocation_id: resume_invocation_request.invocation_id,
                    update_pinned_deployment_id: resume_invocation_request
                        .update_pinned_deployment_id,
                    response_sink: resume_invocation_request.response_sink,
                }
                .apply(self)
                .await?;
                Ok(())
            }
            Command::RestartAsNewInvocation(restart_as_new_invocation_request) => {
                lifecycle::OnRestartAsNewInvocationCommand {
                    invocation_id: restart_as_new_invocation_request.invocation_id,
                    new_invocation_id: restart_as_new_invocation_request.new_invocation_id,
                    copy_prefix_up_to_index_included: restart_as_new_invocation_request
                        .copy_prefix_up_to_index_included,
                    response_sink: restart_as_new_invocation_request.response_sink,
                    patch_deployment_id: restart_as_new_invocation_request.patch_deployment_id,
                }
                .apply(self)
                .await?;
                Ok(())
            }
            Command::PatchState(mutation) => self.handle_external_state_mutation(mutation).await,
            Command::AnnounceLeader(_) => {
                // no-op :-)
                Ok(())
            }
            Command::ScheduleTimer(timer) => {
                self.register_timer(timer, Default::default())?;
                Ok(())
            }
            Command::NotifySignal(notify_signal_request) => {
                lifecycle::OnNotifySignalCommand {
                    invocation_id: notify_signal_request.invocation_id,
                    invocation_status: self
                        .get_invocation_status(&notify_signal_request.invocation_id)
                        .await?,
                    signal: notify_signal_request.signal,
                }
                .apply(self)
                .await?;
                Ok(())
            }
            Command::NotifyGetInvocationOutputResponse(get_invocation_output_response) => {
                lifecycle::OnNotifyGetInvocationOutputResponse(get_invocation_output_response)
                    .apply(self)
                    .await?;
                Ok(())
            }
            Command::UpsertSchema(upsert) => {
                trace!(
                    "Upsert schema record to version '{}'",
                    upsert.schema.version()
                );
                if self
                    .schema
                    .as_ref()
                    .map(|current| current.version() < upsert.schema.version())
                    .unwrap_or(true)
                {
                    // only update if schema is none or has a smaller version
                    debug!("Schema updated to version '{}'", upsert.schema.version());
                    *self.schema = Some(upsert.schema);
                }

                Ok(())
            }
        }
    }

    async fn on_service_invocation(
        &mut self,
        service_invocation: Box<ServiceInvocation>,
    ) -> Result<(), Error>
    where
        S: IdempotencyTable
            + WriteOutboxTable
            + WriteFsmTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + ReadVirtualObjectStatusTable
            + WriteVirtualObjectStatusTable
            + WriteTimerTable
            + WriteInboxTable
            + WriteFsmTable
            + WriteVQueueTable
            + ReadVQueueTable
            + WriteJournalTable,
    {
        let invocation_id = service_invocation.invocation_id;
        debug_assert!(
            self.partition_key_range
                .contains(&service_invocation.partition_key()),
            "Service invocation with partition key '{}' has been delivered to a partition processor with key range '{:?}'. This indicates a bug.",
            service_invocation.partition_key(),
            self.partition_key_range
        );

        let invocation_span = Span::current();
        invocation_span.record_invocation_id(&invocation_id);
        invocation_span.record_invocation_target(&service_invocation.invocation_target);
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
        let pre_flight_invocation_metadata = PreFlightInvocationMetadata::from_service_invocation(
            self.record_created_at,
            *service_invocation,
        );

        self.on_pre_flight_invocation(
            invocation_id,
            pre_flight_invocation_metadata,
            submit_notification_sink,
        )
        .await
    }

    async fn on_pre_flight_invocation(
        &mut self,
        invocation_id: InvocationId,
        pre_flight_invocation_metadata: PreFlightInvocationMetadata,
        submit_notification_sink: Option<SubmitNotificationSink>,
    ) -> Result<(), Error>
    where
        S: IdempotencyTable
            + WriteInvocationStatusTable
            + WriteFsmTable
            + ReadVirtualObjectStatusTable
            + WriteVirtualObjectStatusTable
            + WriteTimerTable
            + WriteInboxTable
            + WriteFsmTable
            + WriteVQueueTable
            + ReadVQueueTable
            + WriteJournalTable,
    {
        if Configuration::pinned().common.experimental_enable_vqueues {
            // skips the rest of this logic and jumps straight to vqueues' implementation
            return self
                .vqueue_enqueue(
                    invocation_id,
                    pre_flight_invocation_metadata,
                    submit_notification_sink,
                )
                .await;
        }

        // A pre-flight invocation has been already deduplicated

        // 1. Check if we need to schedule it
        let execution_time = pre_flight_invocation_metadata.execution_time;
        let Some(pre_flight_invocation_metadata) = self.handle_service_invocation_execution_time(
            invocation_id,
            pre_flight_invocation_metadata,
        )?
        else {
            // Invocation was scheduled, send back the ingress attach notification and return
            self.send_submit_notification_if_needed(
                invocation_id,
                execution_time,
                true,
                submit_notification_sink,
            );
            return Ok(());
        };

        // 2. Check if we need to inbox it (only for exclusive methods of virtual objects)
        let Some(pre_flight_invocation_metadata) = self
            .handle_service_invocation_exclusive_handler(
                invocation_id,
                pre_flight_invocation_metadata,
            )
            .await?
        else {
            // Invocation was inboxed, send back the ingress attach notification and return
            self.send_submit_notification_if_needed(
                invocation_id,
                execution_time,
                true,
                submit_notification_sink,
            );
            // Invocation was inboxed, nothing else to do here
            return Ok(());
        };

        // 3. Execute it
        self.send_submit_notification_if_needed(
            invocation_id,
            pre_flight_invocation_metadata.execution_time,
            true,
            submit_notification_sink,
        );

        let (in_flight_invocation_metadata, invocation_input) =
            InFlightInvocationMetadata::from_pre_flight_invocation_metadata(
                pre_flight_invocation_metadata,
                self.record_created_at,
            );

        self.init_journal_and_invoke(
            invocation_id,
            in_flight_invocation_metadata,
            invocation_input,
        )
    }

    // Uses vqueues, replaces on_pre_flight_invocation
    async fn vqueue_enqueue(
        &mut self,
        invocation_id: InvocationId,
        metadata: PreFlightInvocationMetadata,
        submit_notification_sink: Option<SubmitNotificationSink>,
    ) -> Result<(), Error>
    where
        S: IdempotencyTable
            + WriteInvocationStatusTable
            + WriteFsmTable
            + ReadVirtualObjectStatusTable
            + WriteVirtualObjectStatusTable
            + WriteTimerTable
            + WriteInboxTable
            + WriteFsmTable
            + WriteVQueueTable
            + ReadVQueueTable
            + WriteJournalTable,
    {
        // todo(asoli): temporary until we move this to the invocation id creation site.
        let qid = Self::vqueue_id_from_invocation(&invocation_id, &metadata.invocation_target);

        let record_unique_ts = UniqueTimestamp::from_unix_millis(self.record_created_at).unwrap();
        let visible_at = if let Some(execution_time) = metadata.execution_time {
            VisibleAt::from_unix_millis(execution_time)
        } else {
            VisibleAt::At(record_unique_ts)
        };

        VQueues::new(
            qid,
            self.storage,
            self.vqueues_cache,
            self.is_leader.then_some(self.action_collector),
        )
        .enqueue_new(
            record_unique_ts,
            visible_at,
            NewEntryPriority::default(),
            vqueue_table::EntryKind::Invocation,
            vqueue_table::EntryId::from(invocation_id),
            None::<()>,
        )
        .await?;

        // 1. Check if we need to schedule it
        // only schedule the invocation if it's actually in the future
        let invocation_status = if visible_at > record_unique_ts {
            InvocationStatus::Scheduled(ScheduledInvocation::from_pre_flight_invocation_metadata(
                metadata,
                self.record_created_at,
            ))
        } else {
            InvocationStatus::Inboxed(InboxedInvocation::from_pre_flight_invocation_metadata(
                metadata,
                // todo: what do we do with this sequence number?
                1,
                self.record_created_at,
            ))
        };

        self.storage
            .put_invocation_status(&invocation_id, &invocation_status)
            .map_err(Error::Storage)?;

        // Invocation was scheduled, send back the ingress attach notification and return
        // Notify the ingress, if needed, of the chosen invocation_id
        if self.is_leader
            && let Some(SubmitNotificationSink::Ingress { request_id }) = submit_notification_sink
        {
            let execution_time = invocation_status.execution_time();
            debug!(
                "Sending ingress attach invocation for {invocation_id}, will run at: {execution_time:?}"
            );

            self.action_collector
                .push(Action::IngressSubmitNotification {
                    request_id,
                    execution_time,
                    is_new_invocation: true,
                });
        }

        Ok(())
    }

    /// Returns the invocation in case the invocation is not a duplicate
    async fn handle_duplicated_requests(
        &mut self,
        mut service_invocation: Box<ServiceInvocation>,
    ) -> Result<Option<Box<ServiceInvocation>>, Error>
    where
        S: IdempotencyTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + ReadVirtualObjectStatusTable
            + WriteVirtualObjectStatusTable
            + WriteOutboxTable
            + WriteFsmTable,
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
            return Ok(Some(service_invocation));
        }

        // --- Invocation already exists

        // Send submit notification
        self.send_submit_notification_if_needed(
            service_invocation.invocation_id,
            previous_invocation_status.execution_time(),
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
            )?;
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
            | is @ InvocationStatus::Paused { .. }
            | is @ InvocationStatus::Inboxed { .. }
            | is @ InvocationStatus::Scheduled { .. } => {
                if let Some(ref response_sink) = service_invocation.response_sink
                    && !is
                        .get_response_sinks()
                        .expect("response sink must be present")
                        .contains(response_sink)
                {
                    self.do_append_response_sink(invocation_id, is, response_sink.clone())?
                }
            }
            InvocationStatus::Completed(completed) => {
                let completion_expiry_time = completed.completion_expiry_time();
                self.send_response_to_sinks(
                    service_invocation.response_sink.take().into_iter(),
                    completed.response_result,
                    Some(invocation_id),
                    completion_expiry_time,
                    Some(&completed.invocation_target),
                )?;
            }
            InvocationStatus::Free => {
                unreachable!("This was checked before!")
            }
        }

        Ok(None)
    }

    /// Returns the invocation in case the invocation should run immediately
    fn handle_service_invocation_execution_time(
        &mut self,
        invocation_id: InvocationId,
        metadata: PreFlightInvocationMetadata,
    ) -> Result<Option<PreFlightInvocationMetadata>, Error>
    where
        S: WriteTimerTable + WriteInvocationStatusTable,
    {
        if let Some(execution_time) = metadata.execution_time {
            let span_context = metadata.span_context().clone();
            debug_if_leader!(self.is_leader, "Store scheduled invocation");

            self.register_timer(
                TimerKeyValue::neo_invoke(execution_time, invocation_id),
                span_context,
            )?;

            self.storage
                .put_invocation_status(
                    &invocation_id,
                    &InvocationStatus::Scheduled(
                        ScheduledInvocation::from_pre_flight_invocation_metadata(
                            metadata,
                            self.record_created_at,
                        ),
                    ),
                )
                .map_err(Error::Storage)?;
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
        S: ReadVirtualObjectStatusTable
            + WriteVirtualObjectStatusTable
            + WriteInvocationStatusTable
            + WriteInboxTable
            + WriteFsmTable,
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
                                self.record_created_at,
                            ),
                        ),
                    )
                    .map_err(Error::Storage)?;

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
                    .map_err(Error::Storage)?;
            }
        }
        Ok(Some(metadata))
    }

    fn init_journal_and_vqueue_invoke(
        &mut self,
        qid: VQueueId,
        item_hash: u64,
        invocation_id: InvocationId,
        mut in_flight_invocation_metadata: InFlightInvocationMetadata,
        invocation_input: Option<InvocationInput>,
    ) -> Result<(), Error>
    where
        S: WriteJournalTable + WriteInvocationStatusTable,
    {
        // Usage metering for "actions" should include the Input journal entry
        // type, but it gets filtered out before reaching the state machine.
        // Therefore we count it here, as a special case.
        if self.is_leader {
            counter!(
                USAGE_LEADER_JOURNAL_ENTRY_COUNT,
                "entry" => "Command/Input",
            )
            .increment(1);
        }

        let invoke_input_journal = if let Some(invocation_input) = invocation_input {
            self.init_journal(
                invocation_id,
                &mut in_flight_invocation_metadata,
                invocation_input,
            )?
        } else {
            InvokeInputJournal::NoCachedJournal
        };

        self.vqueue_invoke(
            qid,
            item_hash,
            invocation_id,
            in_flight_invocation_metadata,
            invoke_input_journal,
        )
    }

    fn init_journal_and_invoke(
        &mut self,
        invocation_id: InvocationId,
        mut in_flight_invocation_metadata: InFlightInvocationMetadata,
        invocation_input: Option<InvocationInput>,
    ) -> Result<(), Error>
    where
        S: WriteJournalTable + WriteInvocationStatusTable,
    {
        // Usage metering for "actions" should include the Input journal entry
        // type, but it gets filtered out before reaching the state machine.
        // Therefore we count it here, as a special case.
        if self.is_leader {
            counter!(
                USAGE_LEADER_JOURNAL_ENTRY_COUNT,
                "entry" => "Command/Input",
            )
            .increment(1);
        }

        let invoke_input_journal = if let Some(invocation_input) = invocation_input {
            self.init_journal(
                invocation_id,
                &mut in_flight_invocation_metadata,
                invocation_input,
            )?
        } else {
            InvokeInputJournal::NoCachedJournal
        };

        self.invoke(
            invocation_id,
            in_flight_invocation_metadata,
            invoke_input_journal,
        )
    }

    fn init_journal(
        &mut self,
        invocation_id: InvocationId,
        in_flight_invocation_metadata: &mut InFlightInvocationMetadata,
        invocation_input: InvocationInput,
    ) -> Result<InvokeInputJournal, Error>
    where
        S: WriteJournalTable,
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
            .map_err(Error::Storage)?;

        let_assert!(JournalEntry::Entry(input_entry) = input_entry);

        Ok(InvokeInputJournal::CachedJournal(
            restate_invoker_api::JournalMetadata::new(
                in_flight_invocation_metadata.journal_metadata.length,
                in_flight_invocation_metadata
                    .journal_metadata
                    .span_context
                    .clone(),
                None,
                self.record_created_at,
                in_flight_invocation_metadata
                    .random_seed
                    .unwrap_or_else(|| invocation_id.to_random_seed()),
            ),
            vec![
                restate_invoker_api::invocation_reader::JournalEntry::JournalV1(
                    input_entry.erase_enrichment(),
                ),
            ],
        ))
    }

    fn vqueue_invoke(
        &mut self,
        qid: VQueueId,
        item_hash: u64,
        invocation_id: InvocationId,
        in_flight_invocation_metadata: InFlightInvocationMetadata,
        invoke_input_journal: InvokeInputJournal,
    ) -> Result<(), Error>
    where
        S: WriteInvocationStatusTable,
    {
        debug_if_leader!(self.is_leader, "Invoke");

        let status = InvocationStatus::Invoked(in_flight_invocation_metadata);

        self.storage
            .put_invocation_status(&invocation_id, &status)
            .map_err(Error::Storage)?;

        if self.is_leader {
            let invocation_target = status.into_invocation_metadata().unwrap().invocation_target;
            self.action_collector.push(Action::VQInvoke {
                qid,
                item_hash,
                invocation_id,
                invocation_target,
                invoke_input_journal,
            });
        }

        Ok(())
    }

    fn invoke(
        &mut self,
        invocation_id: InvocationId,
        in_flight_invocation_metadata: InFlightInvocationMetadata,
        invoke_input_journal: InvokeInputJournal,
    ) -> Result<(), Error>
    where
        S: WriteInvocationStatusTable,
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
            .map_err(Error::Storage)?;

        Ok(())
    }

    async fn enqueue_into_inbox(&mut self, inbox_entry: InboxEntry) -> Result<MessageIndex, Error>
    where
        S: WriteInboxTable + WriteFsmTable,
    {
        let seq_number = *self.inbox_seq_number;
        debug_if_leader!(
            self.is_leader,
            restate.inbox.seq = seq_number,
            "Enqueue inbox entry"
        );

        self.storage
            .put_inbox_entry(seq_number, &inbox_entry)
            .map_err(Error::Storage)?;
        // need to store the next inbox sequence number
        self.storage
            .put_inbox_seq_number(seq_number + 1)
            .map_err(Error::Storage)?;
        *self.inbox_seq_number += 1;
        Ok(seq_number)
    }

    async fn handle_external_state_mutation(
        &mut self,
        mutation: ExternalStateMutation,
    ) -> Result<(), Error>
    where
        S: ReadStateTable
            + WriteStateTable
            + WriteInboxTable
            + WriteFsmTable
            + ReadVirtualObjectStatusTable
            + WriteVirtualObjectStatusTable
            + WriteVQueueTable
            + ReadVQueueTable,
    {
        if Configuration::pinned().common.experimental_enable_vqueues {
            self.vqueue_enqueue_state_mutation(mutation).await?;
        } else {
            let service_status = self
                .storage
                .get_virtual_object_status(&mutation.service_id)
                .await?;

            match service_status {
                VirtualObjectStatus::Locked(_) => {
                    self.enqueue_into_inbox(InboxEntry::StateMutation(mutation))
                        .await?;
                }
                VirtualObjectStatus::Unlocked => Self::do_mutate_state(self, &mutation).await?,
            }
        }

        Ok(())
    }

    async fn on_terminate_invocation(
        &mut self,
        InvocationTermination {
            invocation_id,
            flavor: termination_flavor,
            response_sink,
        }: InvocationTermination,
    ) -> Result<(), Error>
    where
        S: WriteVirtualObjectStatusTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + WriteInboxTable
            + WriteFsmTable
            + ReadStateTable
            + WriteStateTable
            + ReadJournalTable
            + WriteJournalTable
            + WriteOutboxTable
            + journal_table_v2::WriteJournalTable
            + journal_table_v2::ReadJournalTable
            + WriteTimerTable
            + ReadPromiseTable
            + WritePromiseTable
            + ReadVQueueTable
            + WriteVQueueTable
            + WriteJournalEventsTable,
    {
        match termination_flavor {
            TerminationFlavor::Kill => self.on_kill_invocation(invocation_id, response_sink).await,
            TerminationFlavor::Cancel => {
                self.on_cancel_invocation(invocation_id, response_sink)
                    .await
            }
        }
    }

    async fn on_kill_invocation(
        &mut self,
        invocation_id: InvocationId,
        response_sink: Option<InvocationMutationResponseSink>,
    ) -> Result<(), Error>
    where
        S: WriteVirtualObjectStatusTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + WriteInboxTable
            + WriteFsmTable
            + ReadStateTable
            + WriteStateTable
            + ReadJournalTable
            + WriteJournalTable
            + WriteOutboxTable
            + WriteTimerTable
            + WriteFsmTable
            + journal_table_v2::WriteJournalTable
            + journal_table_v2::ReadJournalTable
            + ReadVQueueTable
            + WriteVQueueTable
            + WriteJournalEventsTable,
    {
        let status = self.get_invocation_status(&invocation_id).await?;

        match status {
            InvocationStatus::Invoked(metadata) => {
                self.kill_invoked_invocation(invocation_id, metadata)
                    .await?;
                self.reply_to_kill(response_sink, KillInvocationResponse::Ok);
            }
            InvocationStatus::Suspended { metadata, .. } | InvocationStatus::Paused(metadata) => {
                self.kill_suspended_or_paused_invocation(invocation_id, metadata)
                    .await?;
                self.reply_to_kill(response_sink, KillInvocationResponse::Ok);
            }
            InvocationStatus::Inboxed(inboxed) => {
                self.terminate_inboxed_invocation(TerminationFlavor::Kill, invocation_id, inboxed)
                    .await?;
                self.reply_to_kill(response_sink, KillInvocationResponse::Ok);
            }
            InvocationStatus::Scheduled(scheduled) => {
                self.terminate_scheduled_invocation(
                    TerminationFlavor::Kill,
                    invocation_id,
                    scheduled,
                )
                .await?;
                self.reply_to_kill(response_sink, KillInvocationResponse::Ok);
            }
            InvocationStatus::Completed(_) => {
                debug!(
                    "Received kill command for completed invocation '{invocation_id}'. To cleanup the invocation after it's been completed, use the purge invocation command."
                );
                self.reply_to_kill(response_sink, KillInvocationResponse::AlreadyCompleted);
            }
            InvocationStatus::Free => {
                trace!("Received kill command for unknown invocation with id '{invocation_id}'.");
                // We still try to send the abort signal to the invoker,
                // as it might be the case that previously the user sent an abort signal
                // but some message was still between the invoker/PP queues.
                // This can happen because the invoke/resume and the abort invoker messages end up in different queues,
                // and the abort message can overtake the invoke/resume.
                // Consequently the invoker might have not received the abort and the user tried to send it again.
                self.do_send_abort_invocation_to_invoker(invocation_id);
                self.reply_to_kill(response_sink, KillInvocationResponse::NotFound);
            }
        };

        Ok(())
    }

    async fn on_cancel_invocation(
        &mut self,
        invocation_id: InvocationId,
        response_sink: Option<InvocationMutationResponseSink>,
    ) -> Result<(), Error>
    where
        S: WriteVirtualObjectStatusTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + WriteInboxTable
            + WriteFsmTable
            + ReadStateTable
            + WriteStateTable
            + WriteJournalTable
            + ReadJournalTable
            + WriteOutboxTable
            + journal_table_v2::WriteJournalTable
            + journal_table_v2::ReadJournalTable
            + WriteJournalEventsTable
            + ReadPromiseTable
            + WritePromiseTable
            + ReadVQueueTable
            + WriteVQueueTable
            + WriteTimerTable,
    {
        let mut status = self.get_invocation_status(&invocation_id).await?;

        match status.get_invocation_metadata().and_then(|meta| {
            meta.pinned_deployment
                .as_ref()
                .map(|pd| pd.service_protocol_version)
        }) {
            Some(sp_version) if sp_version >= ServiceProtocolVersion::V4 => {
                OnCancelCommand {
                    invocation_id,
                    invocation_status: status,
                    response_sink,
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
                // We need to apply a corner case fix here.
                // We don't know yet what's the protocol version being used, but we know the status is either invoker or suspended.
                // To sort this out, we write a field in invocation status to make sure that after pinning the deployment, we run the cancellation.
                // See OnPinnedDeploymentCommand for more info.
                trace!(
                    "Storing hotfix for cancellation when invocation doesn't have a pinned service protocol, but is invoked/suspended"
                );

                match &mut status {
                    InvocationStatus::Invoked(metadata)
                    | InvocationStatus::Suspended { metadata, .. } => {
                        metadata.hotfix_apply_cancellation_after_deployment_is_pinned = true;
                    }
                    _ => {
                        unreachable!("It's checked above")
                    }
                };

                self.storage
                    .put_invocation_status(&invocation_id, &status)?;
                self.reply_to_cancel(response_sink, CancelInvocationResponse::Appended);
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
                self.reply_to_cancel(response_sink, CancelInvocationResponse::Appended);
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
                self.reply_to_cancel(response_sink, CancelInvocationResponse::Appended);
            }
            InvocationStatus::Paused(metadata) => {
                self.cancel_journal_leaves(
                    invocation_id,
                    InvocationStatusProjection::Paused,
                    metadata.journal_metadata.length,
                )
                .await?;
                self.do_resume_service(invocation_id, metadata).await?;
                self.reply_to_cancel(response_sink, CancelInvocationResponse::Appended);
            }
            InvocationStatus::Inboxed(inboxed) => {
                self.terminate_inboxed_invocation(
                    TerminationFlavor::Cancel,
                    invocation_id,
                    inboxed,
                )
                .await?;
                self.reply_to_cancel(response_sink, CancelInvocationResponse::Done);
            }
            InvocationStatus::Scheduled(scheduled) => {
                self.terminate_scheduled_invocation(
                    TerminationFlavor::Cancel,
                    invocation_id,
                    scheduled,
                )
                .await?;
                self.reply_to_cancel(response_sink, CancelInvocationResponse::Done);
            }
            InvocationStatus::Completed(_) => {
                debug!(
                    "Received cancel command for completed invocation '{invocation_id}'. To cleanup the invocation after it's been completed, use the purge invocation command."
                );
                self.reply_to_cancel(response_sink, CancelInvocationResponse::AlreadyCompleted);
            }
            InvocationStatus::Free => {
                trace!("Received cancel command for unknown invocation with id '{invocation_id}'.");
                // We still try to send the abort signal to the invoker,
                // as it might be the case that previously the user sent an abort signal
                // but some message was still between the invoker/PP queues.
                // This can happen because the invoke/resume and the abort invoker messages end up in different queues,
                // and the abort message can overtake the invoke/resume.
                // Consequently the invoker might have not received the abort and the user tried to send it again.
                self.do_send_abort_invocation_to_invoker(invocation_id);
                self.reply_to_cancel(response_sink, CancelInvocationResponse::NotFound);
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
        S: WriteInvocationStatusTable
            + WriteInboxTable
            + WriteOutboxTable
            + WriteFsmTable
            + WriteJournalTable
            + journal_table_v2::WriteJournalTable
            + ReadVQueueTable
            + WriteVQueueTable
            + WriteJournalEventsTable,
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
                    invocation_target,
                    input,
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
        )?;

        if Configuration::pinned().common.experimental_enable_vqueues {
            let record_unique_ts =
                UniqueTimestamp::from_unix_millis(self.record_created_at).unwrap();
            // Is this an invocation that has a vqueue inbox?
            if !VQueues::end_by_id(
                self.storage,
                self.vqueues_cache,
                self.is_leader.then_some(self.action_collector),
                record_unique_ts,
                EntryKind::Invocation,
                invocation_id.partition_key(),
                &EntryId::from(invocation_id),
            )
            .await?
            {
                // Assuming it's an old-style inbox, fallback to deleting it from inbox since we
                // can't find the entry state.
                self.do_delete_inbox_entry(
                    invocation_target.as_keyed_service_id().expect(
                        "Because the invocation is inboxed, it must have a keyed service id",
                    ),
                    inbox_sequence_number,
                )
                .await?;
            }
        } else {
            // Delete inbox entry and invocation status.
            self.do_delete_inbox_entry(
                invocation_target
                    .as_keyed_service_id()
                    .expect("Because the invocation is inboxed, it must have a keyed service id"),
                inbox_sequence_number,
            )
            .await?;
        }
        self.do_free_invocation(invocation_id)?;

        // If there's a journal, delete journal
        if let PreFlightInvocationArgument::Journal(PreFlightInvocationJournal {
            journal_metadata,
            pinned_deployment,
        }) = &input
        {
            let should_remove_journal_table_v2 =
                pinned_deployment.as_ref().is_some_and(|pinned_deployment| {
                    pinned_deployment.service_protocol_version >= ServiceProtocolVersion::V4
                });
            self.do_drop_journal(
                invocation_id,
                journal_metadata.length,
                should_remove_journal_table_v2,
            )
            .await?;
        }

        self.notify_invocation_result(
            &invocation_id,
            &invocation_target,
            input.span_context(),
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
        S: WriteInvocationStatusTable
            + WriteTimerTable
            + WriteOutboxTable
            + WriteFsmTable
            + WriteJournalTable
            + ReadVQueueTable
            + WriteVQueueTable
            + journal_table_v2::WriteJournalTable
            + WriteJournalEventsTable,
    {
        let error = match termination_flavor {
            TerminationFlavor::Kill => KILLED_INVOCATION_ERROR,
            TerminationFlavor::Cancel => CANCELED_INVOCATION_ERROR,
        };

        let ScheduledInvocation {
            metadata:
                PreFlightInvocationMetadata {
                    response_sinks,
                    input,
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
        )?;

        if Configuration::pinned().common.experimental_enable_vqueues {
            let record_unique_ts =
                UniqueTimestamp::from_unix_millis(self.record_created_at).unwrap();
            // Is this an invocation that has a vqueue inbox?
            if !VQueues::end_by_id(
                self.storage,
                self.vqueues_cache,
                self.is_leader.then_some(self.action_collector),
                record_unique_ts,
                EntryKind::Invocation,
                invocation_id.partition_key(),
                &EntryId::from(invocation_id),
            )
            .await?
            {
                // Assuming it's an old-style inbox, fallback to deleting the timer
                if let Some(execution_time) = execution_time {
                    self.do_delete_timer(TimerKey::neo_invoke(
                        execution_time.as_u64(),
                        invocation_id.invocation_uuid(),
                    ))
                    .await?;
                } else {
                    warn!("Scheduled invocations must always have an execution time.");
                }
            }
        } else {
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
        }

        // Free invocation
        self.do_free_invocation(invocation_id)?;

        // If there's a journal, delete journal
        if let PreFlightInvocationArgument::Journal(PreFlightInvocationJournal {
            journal_metadata,
            pinned_deployment,
        }) = &input
        {
            let should_remove_journal_table_v2 =
                pinned_deployment.as_ref().is_some_and(|pinned_deployment| {
                    pinned_deployment.service_protocol_version >= ServiceProtocolVersion::V4
                });
            self.do_drop_journal(
                invocation_id,
                journal_metadata.length,
                should_remove_journal_table_v2,
            )
            .await?;
        }

        self.notify_invocation_result(
            &invocation_id,
            &invocation_target,
            input.span_context(),
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
        S: WriteInboxTable
            + WriteVirtualObjectStatusTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + WriteVirtualObjectStatusTable
            + ReadStateTable
            + WriteStateTable
            + WriteJournalTable
            + ReadJournalTable
            + WriteOutboxTable
            + WriteFsmTable
            + journal_table_v2::WriteJournalTable
            + journal_table_v2::ReadJournalTable
            + ReadVQueueTable
            + WriteVQueueTable
            + WriteJournalEventsTable,
    {
        self.kill_child_invocations(&invocation_id, metadata.journal_metadata.length, &metadata)
            .await?;

        self.end_invocation(
            invocation_id,
            metadata,
            Some(ResponseResult::Failure(KILLED_INVOCATION_ERROR)),
        )
        .await?;
        self.do_send_abort_invocation_to_invoker(invocation_id);
        Ok(())
    }

    async fn kill_suspended_or_paused_invocation(
        &mut self,
        invocation_id: InvocationId,
        metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error>
    where
        S: WriteInboxTable
            + WriteVirtualObjectStatusTable
            + WriteInvocationStatusTable
            + ReadInvocationStatusTable
            + WriteVirtualObjectStatusTable
            + ReadStateTable
            + WriteStateTable
            + WriteJournalTable
            + ReadJournalTable
            + WriteOutboxTable
            + WriteFsmTable
            + journal_table_v2::WriteJournalTable
            + journal_table_v2::ReadJournalTable
            + ReadVQueueTable
            + WriteVQueueTable
            + WriteJournalEventsTable,
    {
        self.kill_child_invocations(&invocation_id, metadata.journal_metadata.length, &metadata)
            .await?;

        self.end_invocation(
            invocation_id,
            metadata,
            Some(ResponseResult::Failure(KILLED_INVOCATION_ERROR)),
        )
        .await?;
        self.do_send_abort_invocation_to_invoker(invocation_id);
        Ok(())
    }

    async fn kill_child_invocations(
        &mut self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
        metadata: &InFlightInvocationMetadata,
    ) -> Result<(), Error>
    where
        S: WriteOutboxTable + WriteFsmTable + ReadJournalTable + journal_table_v2::ReadJournalTable,
    {
        let invocation_ids_to_kill: Vec<InvocationId> = if metadata
            .pinned_deployment
            .as_ref()
            .is_some_and(|pd| pd.service_protocol_version >= ServiceProtocolVersion::V4)
        {
            journal_table_v2::ReadJournalTable::get_journal(
                self.storage,
                *invocation_id,
                journal_length,
            )?
            .try_filter_map(|(_, journal_entry)| async {
                if let Some(cmd) = journal_entry.inner.try_as_command()
                    && let journal_v2::raw::RawCommandSpecificMetadata::CallOrSend(
                        call_or_send_metadata,
                    ) = cmd.command_specific_metadata()
                {
                    return Ok(Some(call_or_send_metadata.invocation_id));
                }

                Ok(None)
            })
            .try_collect()
            .await?
        } else {
            ReadJournalTable::get_journal(self.storage, invocation_id, journal_length)?
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

        for invocation_id in invocation_ids_to_kill {
            self.handle_outgoing_message(OutboxMessage::InvocationTermination(
                InvocationTermination {
                    invocation_id,
                    flavor: TerminationFlavor::Kill,
                    response_sink: None,
                },
            ))?;
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
        S: ReadJournalTable
            + WriteJournalTable
            + WriteOutboxTable
            + WriteFsmTable
            + WriteTimerTable,
    {
        let journal_entries_to_cancel: Vec<(EntryIndex, EnrichedRawEntry)> = self
            .storage
            .get_journal(&invocation_id, journal_length)?
            .try_filter_map(|(journal_index, journal_entry)| async move {
                if let JournalEntry::Entry(journal_entry) = journal_entry
                    && let Some(is_completed) = journal_entry.header().is_completed()
                    && !is_completed
                {
                    // Every completable journal entry that hasn't been completed yet should be cancelled
                    return Ok(Some((journal_index, journal_entry)));
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
                        InvocationTermination {
                            invocation_id: enrichment_result.invocation_id,
                            flavor: TerminationFlavor::Cancel,
                            response_sink: None,
                        },
                    ))?;
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
        S: ReadJournalTable + WriteJournalTable,
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
            InvocationStatusProjection::Paused => {
                self.handle_completion_for_paused(
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

    async fn on_timer(&mut self, timer_value: TimerKeyValue) -> Result<(), Error>
    where
        S: IdempotencyTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + WriteOutboxTable
            + WriteFsmTable
            + ReadVirtualObjectStatusTable
            + WriteVirtualObjectStatusTable
            + WriteTimerTable
            + WriteInboxTable
            + WriteFsmTable
            + ReadJournalTable
            + WriteJournalTable
            + ReadPromiseTable
            + WritePromiseTable
            + ReadStateTable
            + WriteStateTable
            + WriteVQueueTable
            + ReadVQueueTable
            + journal_table_v2::WriteJournalTable
            + journal_table_v2::ReadJournalTable
            + WriteJournalEventsTable,
    {
        let (key, value) = timer_value.into_inner();
        self.do_delete_timer(key).await?;

        match value {
            Timer::CompleteJournalEntry(invocation_id, entry_index) => {
                let status = self.get_invocation_status(&invocation_id).await?;
                if should_use_journal_table_v2(&status) {
                    // We just apply the journal entry
                    lifecycle::OnNotifySleepCompletionCommand {
                        invocation_id,
                        status,
                        completion_id: entry_index,
                    }
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
                lifecycle::OnPurgeCommand {
                    invocation_id,
                    response_sink: None,
                }
                .apply(self)
                .await?;
                Ok(())
            }
            Timer::NeoInvoke(invocation_id) => self.on_neo_invoke_timer(invocation_id).await,
        }
    }

    async fn on_neo_invoke_timer(&mut self, invocation_id: InvocationId) -> Result<(), Error>
    where
        S: ReadVirtualObjectStatusTable
            + WriteVirtualObjectStatusTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + WriteInboxTable
            + WriteFsmTable
            + WriteJournalTable,
    {
        debug_if_leader!(
            self.is_leader,
            "Handle scheduled invocation timer with invocation id {invocation_id}"
        );
        let invocation_status = self.get_invocation_status(&invocation_id).await?;

        if let InvocationStatus::Free = &invocation_status {
            warn!(
                "Fired a timer for an unknown invocation. The invocation might have been deleted/purged previously."
            );
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
                self.record_created_at,
            );

        self.init_journal_and_invoke(
            invocation_id,
            in_flight_invocation_metadata,
            invocation_input,
        )
    }

    async fn try_invoker_effect(&mut self, invoker_effect: InvokerEffect) -> Result<(), Error>
    where
        S: ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + ReadJournalTable
            + WriteJournalTable
            + ReadStateTable
            + WriteStateTable
            + ReadPromiseTable
            + WritePromiseTable
            + WriteOutboxTable
            + WriteFsmTable
            + WriteTimerTable
            + WriteInboxTable
            + WriteVirtualObjectStatusTable
            + journal_table_v2::WriteJournalTable
            + journal_table_v2::ReadJournalTable
            + ReadVQueueTable
            + WriteVQueueTable
            + WriteJournalEventsTable,
    {
        let status = self
            .get_invocation_status(&invoker_effect.invocation_id)
            .await?;
        self.on_invoker_effect(invoker_effect, status).await?;

        Ok(())
    }

    async fn on_invoker_effect(
        &mut self,
        effect: InvokerEffect,
        invocation_status: InvocationStatus,
    ) -> Result<(), Error>
    where
        S: WriteInvocationStatusTable
            + ReadInvocationStatusTable
            + ReadJournalTable
            + WriteJournalTable
            + ReadStateTable
            + WriteStateTable
            + ReadPromiseTable
            + WritePromiseTable
            + WriteOutboxTable
            + WriteFsmTable
            + WriteTimerTable
            + WriteInboxTable
            + WriteVirtualObjectStatusTable
            + journal_table_v2::WriteJournalTable
            + journal_table_v2::ReadJournalTable
            + WriteJournalEventsTable
            + ReadVQueueTable
            + WriteVQueueTable,
    {
        let is_status_invoked = matches!(invocation_status, InvocationStatus::Invoked(_));

        if !is_status_invoked {
            trace!(
                "Received invoker effect for invocation not in invoked status. Ignoring the effect."
            );
            self.do_send_abort_invocation_to_invoker(effect.invocation_id);
            return Ok(());
        }

        match effect.kind {
            InvokerEffectKind::PinnedDeployment(pinned_deployment) => {
                lifecycle::OnPinnedDeploymentCommand {
                    invocation_id: effect.invocation_id,
                    invocation_status,
                    pinned_deployment,
                }
                .apply(self)
                .await?;
            }
            InvokerEffectKind::JournalEntry { entry_index, entry } => {
                self.handle_journal_entry(
                    effect.invocation_id,
                    entry_index,
                    entry,
                    invocation_status
                        .into_invocation_metadata()
                        .expect("Must be present if status is invoked"),
                )
                .await?;
            }
            InvokerEffectKind::JournalEntryV2 {
                command_index_to_ack,
                entry,
            } => {
                let raw_entry = entry.inner;

                self.on_journal_entry(
                    effect.invocation_id,
                    invocation_status,
                    command_index_to_ack,
                    raw_entry,
                )
                .await?;
            }
            InvokerEffectKind::JournalEntryV2RawEntry {
                command_index_to_ack,
                raw_entry,
            } => {
                self.on_journal_entry(
                    effect.invocation_id,
                    invocation_status,
                    command_index_to_ack,
                    raw_entry,
                )
                .await?;
            }
            InvokerEffectKind::JournalEvent { event } => {
                lifecycle::OnInvokerEventCommand {
                    invocation_id: effect.invocation_id,
                    invocation_status,
                    event,
                }
                .apply(self)
                .await?;
            }
            InvokerEffectKind::Suspended {
                waiting_for_completed_entries,
            } => {
                let invocation_metadata = invocation_status
                    .into_invocation_metadata()
                    .expect("Must be present if status is invoked");
                debug_assert!(
                    !waiting_for_completed_entries.is_empty(),
                    "Expecting at least one entry on which the invocation {} is waiting.",
                    effect.invocation_id
                );
                let mut any_completed = false;
                for entry_index in &waiting_for_completed_entries {
                    if ReadJournalTable::get_journal_entry(
                        self.storage,
                        &effect.invocation_id,
                        *entry_index,
                    )
                    .await?
                    .map(|entry| entry.is_resumable())
                    .unwrap_or_default()
                    {
                        trace!(
                            rpc.service = %invocation_metadata.invocation_target.service_name(),
                            restate.invocation.id = %effect.invocation_id,
                            "Resuming instead of suspending service because an awaited entry is completed/acked.");
                        any_completed = true;
                        break;
                    }
                }
                if any_completed {
                    self.do_resume_service(effect.invocation_id, invocation_metadata)
                        .await?;
                } else {
                    self.do_suspend_service(
                        effect.invocation_id,
                        invocation_metadata,
                        waiting_for_completed_entries,
                    )
                    .await?;
                }
            }
            InvokerEffectKind::SuspendedV2 {
                waiting_for_notifications,
            } => {
                lifecycle::OnSuspendCommand {
                    invocation_id: effect.invocation_id,
                    invocation_status,
                    waiting_for_notifications,
                }
                .apply(self)
                .await?;
            }
            InvokerEffectKind::Paused { paused_event } => {
                lifecycle::OnPausedCommand {
                    invocation_id: effect.invocation_id,
                    paused_event,
                }
                .apply(self)
                .await?;
            }
            InvokerEffectKind::End => {
                self.end_invocation(
                    effect.invocation_id,
                    invocation_status
                        .into_invocation_metadata()
                        .expect("Must be present if status is invoked"),
                    None,
                )
                .await?;
            }
            InvokerEffectKind::Failed(e) => {
                self.end_invocation(
                    effect.invocation_id,
                    invocation_status
                        .into_invocation_metadata()
                        .expect("Must be present if status is invoked"),
                    Some(ResponseResult::Failure(e)),
                )
                .await?;
            }
        }

        Ok(())
    }

    #[inline]
    async fn on_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        invocation_status: InvocationStatus,
        command_index_to_ack: Option<CommandIndex>,
        raw_entry: RawEntry,
    ) -> Result<(), Error>
    where
        S: WriteJournalTable
            + ReadJournalTable
            + journal_table_v2::WriteJournalTable
            + journal_table_v2::ReadJournalTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + WriteTimerTable
            + WriteFsmTable
            + WriteOutboxTable
            + ReadPromiseTable
            + WritePromiseTable
            + ReadStateTable
            + WriteStateTable
            + WriteVQueueTable
            + ReadVQueueTable,
    {
        entries::OnJournalEntryCommand::from_raw_entry(invocation_id, invocation_status, raw_entry)
            .apply(self)
            .await?;

        if let Some(command_index_to_ack) = command_index_to_ack {
            self.action_collector.push(Action::AckStoredCommand {
                invocation_id,
                command_index: command_index_to_ack,
            });
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
        S: WriteInboxTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + WriteVirtualObjectStatusTable
            + WriteJournalTable
            + ReadJournalTable
            + WriteOutboxTable
            + WriteFsmTable
            + ReadStateTable
            + WriteStateTable
            + journal_table_v2::WriteJournalTable
            + journal_table_v2::ReadJournalTable
            + ReadVQueueTable
            + WriteVQueueTable
            + WriteJournalEventsTable,
    {
        let invocation_target = invocation_metadata.invocation_target.clone();
        let journal_length = invocation_metadata.journal_metadata.length;
        let completion_retention = invocation_metadata.completion_retention_duration;
        let journal_retention = invocation_metadata.journal_retention_duration;

        let should_remove_journal_table_v2 = invocation_metadata
            .pinned_deployment
            .as_ref()
            .is_some_and(|pinned_deployment| {
                pinned_deployment.service_protocol_version >= ServiceProtocolVersion::V4
            });

        // If there are any response sinks, or we need to store back the completed status,
        //  we need to find the latest output entry
        if !invocation_metadata.response_sinks.is_empty() || !completion_retention.is_zero() {
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
            )?;

            // Notify invocation result
            self.notify_invocation_result(
                &invocation_id,
                &invocation_metadata.invocation_target,
                &invocation_metadata.journal_metadata.span_context,
                invocation_metadata.timestamps.creation_time(),
                match &response_result {
                    ResponseResult::Success(_) => Ok(()),
                    ResponseResult::Failure(err) => Err((err.code(), err.message().to_owned())),
                },
            );

            // Store the completed status, if needed
            if !completion_retention.is_zero() {
                let completed_invocation = CompletedInvocation::from_in_flight_invocation_metadata(
                    invocation_metadata,
                    if journal_retention.is_zero() {
                        JournalRetentionPolicy::Drop
                    } else {
                        JournalRetentionPolicy::Retain
                    },
                    response_result,
                    self.record_created_at,
                );
                self.do_store_completed_invocation(invocation_id, completed_invocation)?;
            }
        } else {
            // Just notify Ok, no need to read the output entry
            self.notify_invocation_result(
                &invocation_id,
                &invocation_target,
                &invocation_metadata.journal_metadata.span_context,
                invocation_metadata.timestamps.creation_time(),
                Ok(()),
            );
        }

        // If no retention, immediately cleanup the invocation status
        if completion_retention.is_zero() {
            self.do_free_invocation(invocation_id)?;
        }

        if journal_retention.is_zero() {
            self.do_drop_journal(
                invocation_id,
                journal_length,
                should_remove_journal_table_v2,
            )
            .await?;
        }

        if Configuration::pinned().common.experimental_enable_vqueues {
            if invocation_target.invocation_target_ty()
                == InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive)
            {
                let keyed_service_id = invocation_target.as_keyed_service_id().expect(
                    "When the handler type is Exclusive, the invocation target must have a key",
                );
                // We consumed the inbox, nothing else to do here
                self.storage
                    .put_virtual_object_status(&keyed_service_id, &VirtualObjectStatus::Unlocked)
                    .map_err(Error::Storage)?;
            }

            let record_unique_ts =
                UniqueTimestamp::from_unix_millis(self.record_created_at).unwrap();

            // we need to remove the invocation from the running list
            VQueues::end_by_id(
                self.storage,
                self.vqueues_cache,
                self.is_leader.then_some(self.action_collector),
                record_unique_ts,
                EntryKind::Invocation,
                invocation_id.partition_key(),
                &EntryId::from(invocation_id),
            )
            .await?;

            return Ok(());
        } else {
            // Consume inbox and move on
            self.consume_inbox(&invocation_target).await?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn send_response_to_sinks(
        &mut self,
        response_sinks: impl IntoIterator<Item = ServiceInvocationResponseSink>,
        res: impl Into<ResponseResult>,
        invocation_id: Option<InvocationId>,
        completion_expiry_time: Option<MillisSinceEpoch>,
        invocation_target: Option<&InvocationTarget>,
    ) -> Result<(), Error>
    where
        S: WriteOutboxTable + WriteFsmTable,
    {
        let result = res.into();
        for response_sink in response_sinks {
            match response_sink {
                ServiceInvocationResponseSink::PartitionProcessor(target) => self
                    .handle_outgoing_message(OutboxMessage::ServiceResponse(
                        InvocationResponse {
                            target,
                            result: result.clone(),
                        },
                    ))?,
                ServiceInvocationResponseSink::Ingress { request_id } => self
                    .send_ingress_response(
                    request_id,
                    invocation_id,
                    completion_expiry_time,
                    match result.clone() {
                        ResponseResult::Success(res) => InvocationOutputResponse::Success(
                            invocation_target
                                .expect(
                                    "For success responses, there must be an invocation target!",
                                )
                                .clone(),
                            res,
                        ),
                        ResponseResult::Failure(err) => InvocationOutputResponse::Failure(err),
                    },
                ),
            }
        }
        Ok(())
    }

    // [vqueues only]
    async fn attempt_to_run(&mut self, command: vqueues::VQWaitingToRunning) -> Result<(), Error>
    where
        S: WriteInboxTable
            + WriteVirtualObjectStatusTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + WriteVirtualObjectStatusTable
            + ReadVirtualObjectStatusTable
            + WriteJournalTable
            + ReadVQueueTable
            + WriteVQueueTable
            + ReadStateTable
            + WriteStateTable,
    {
        let qid = VQueueId::new(
            VQueueParent::from_raw(command.assignment.parent),
            command.assignment.partition_key,
            VQueueInstance::from_raw(command.assignment.instance),
        );

        let record_unique_ts = UniqueTimestamp::from_unix_millis(self.record_created_at).unwrap();
        let updated_run_token_bucket_zero_time =
            command.meta_updates.updated_token_bucket_zero_time;
        for entry in command.assignment.entries {
            let vqueues::Entry { card, stats } = entry;

            let Some(modified_card) = VQueues::new(
                qid,
                self.storage,
                self.vqueues_cache,
                self.is_leader.then_some(self.action_collector),
            )
            .attempt_to_run(record_unique_ts, &card, updated_run_token_bucket_zero_time)
            .await?
            else {
                // Ignore invocations/mutations that were removed from the vqueue already from the
                // vqueue already.
                debug!(
                    vqueue_id = ?qid,
                    "Not running vqueue entry {card:?} since it was removed from vqueue already!"
                );
                continue;
            };

            match card.kind {
                EntryKind::Unknown => {
                    panic!("Unknown card kind in inbox, cannot proceed");
                }
                EntryKind::StateMutation => {
                    self.vqueue_mutate_state(qid, &modified_card, record_unique_ts)
                        .await?;
                }
                EntryKind::Invocation => {
                    let invocation_id = InvocationId::from_parts(
                        qid.partition_key,
                        InvocationUuid::from_slice(card.id.as_bytes()).unwrap(),
                    );

                    self.run_invocation(
                        qid,
                        // important to pass in the unique hash of the original card to correlate
                        // permits hold by the LeaderState
                        card.unique_hash(),
                        invocation_id,
                        record_unique_ts,
                        stats,
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    // [vqueues only]
    async fn run_invocation(
        &mut self,
        qid: VQueueId,
        item_hash: u64,
        invocation_id: InvocationId,
        at: UniqueTimestamp,
        wait_stats: WaitStats,
    ) -> Result<(), Error>
    where
        S: WriteInboxTable
            + WriteVirtualObjectStatusTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + WriteVirtualObjectStatusTable
            + ReadVirtualObjectStatusTable
            + WriteJournalTable
            + ReadVQueueTable
            + WriteVQueueTable,
    {
        let status = self.get_invocation_status(&invocation_id).await?;
        match status {
            InvocationStatus::Scheduled(ScheduledInvocation { metadata, .. })
            | InvocationStatus::Inboxed(InboxedInvocation { metadata, .. }) => {
                // Validate that if VO, that it's not locked already.
                let invocation_target = &metadata.invocation_target;
                if matches!(
                    invocation_target.invocation_target_ty(),
                    InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive)
                ) {
                    let keyed_service_id = invocation_target.as_keyed_service_id().expect(
                        "When the handler type is Exclusive, the invocation target must have a key",
                    );
                    match self
                        .storage
                        .get_virtual_object_status(&keyed_service_id)
                        .await?
                    {
                        VirtualObjectStatus::Locked(iid) => {
                            panic!(
                                "invariant violated trying to run an invocation {invocation_id} on a VO while another invocation {iid} is holding the lock"
                            );
                        }
                        VirtualObjectStatus::Unlocked => {
                            // Lock the service
                            self.storage
                                .put_virtual_object_status(
                                    &keyed_service_id,
                                    &VirtualObjectStatus::Locked(invocation_id),
                                )
                                .map_err(Error::Storage)?;
                        }
                    }
                }

                let (metadata, invocation_input) =
                    InFlightInvocationMetadata::from_pre_flight_invocation_metadata(
                        metadata,
                        self.record_created_at,
                    );

                info!("Starting invocation {invocation_id}, scheduler stats: {wait_stats:?}");
                self.init_journal_and_vqueue_invoke(
                    qid,
                    item_hash,
                    invocation_id,
                    metadata,
                    invocation_input,
                )?;
            }
            InvocationStatus::Invoked(metadata) if self.is_leader => {
                // just send to invoker
                debug_if_leader!(self.is_leader, "Invoke");
                info!("Resuming invocation {invocation_id}, scheduler stats: {wait_stats:?}");
                self.action_collector.push(Action::VQInvoke {
                    qid,
                    item_hash,
                    invocation_id,
                    invocation_target: metadata.invocation_target,
                    invoke_input_journal: InvokeInputJournal::NoCachedJournal,
                });
            }
            InvocationStatus::Invoked(_) => { /* do nothing when not leader */ }
            // Suspended invocations must first be put back on inbox. On wake-up, they
            // transition back into Invoked state. So seeing a suspended invocation
            // here means that some state transition is missing.
            InvocationStatus::Suspended { .. } | InvocationStatus::Paused(..) => {
                panic!(
                    "Parked invocation {invocation_id} cannot be attempted to run without first being woken up"
                );
            }
            // it's not okay, ignore the attempt, possibly pop the item from running queue
            // and mark completed.
            InvocationStatus::Completed(..) | InvocationStatus::Free => {
                info!(
                    "Will not run invocation {invocation_id} because it has been marked as completed/deleted already!"
                );
                // we delete by id because we are not really sure if the invocation is still in
                // Stage::Inbox or not.
                VQueues::end_by_id(
                    self.storage,
                    self.vqueues_cache,
                    self.is_leader.then_some(self.action_collector),
                    at,
                    EntryKind::Invocation,
                    invocation_id.partition_key(),
                    &EntryId::from(invocation_id),
                )
                .await?;
            }
        }

        Ok(())
    }

    // deprecated: will be replaced by vqueues
    async fn consume_inbox(&mut self, invocation_target: &InvocationTarget) -> Result<(), Error>
    where
        S: WriteInboxTable
            + WriteVirtualObjectStatusTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + WriteVirtualObjectStatusTable
            + ReadStateTable
            + WriteStateTable
            + WriteJournalTable,
    {
        if Configuration::pinned().common.experimental_enable_vqueues {
            return Ok(());
        }

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
                            .map_err(Error::Storage)?;

                        let (in_flight_invocation_meta, invocation_input) =
                            InFlightInvocationMetadata::from_inboxed_invocation(
                                inboxed_invocation,
                                self.record_created_at,
                            );
                        self.init_journal_and_invoke(
                            invocation_id,
                            in_flight_invocation_meta,
                            invocation_input,
                        )?;

                        // Started a new invocation
                        return Ok(());
                    }
                    InboxEntry::StateMutation(state_mutation) => {
                        self.mutate_state(&state_mutation).await?;
                    }
                }
            }

            // We consumed the inbox, nothing else to do here
            self.storage
                .put_virtual_object_status(&keyed_service_id, &VirtualObjectStatus::Unlocked)
                .map_err(Error::Storage)?;
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
        S: ReadStateTable
            + WriteStateTable
            + ReadPromiseTable
            + WritePromiseTable
            + WriteOutboxTable
            + WriteFsmTable
            + WriteTimerTable
            + WriteJournalTable
            + ReadJournalTable
            + WriteInvocationStatusTable,
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
                        .await?;
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
                    self.do_clear_state(service_id, invocation_id, key)?;
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
                    self.do_clear_all_state(service_id, invocation_id)?;
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
                            .get_all_user_states_for_service(&service_id)?
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
                                v.push(JournalCompletionTarget::from_parts(
                                    invocation_id,
                                    entry_index,
                                ));
                                self.do_put_promise(
                                    service_id,
                                    key,
                                    Promise {
                                        state: PromiseState::NotCompleted(v),
                                    },
                                )?;
                            }
                            None => {
                                self.do_put_promise(
                                    service_id,
                                    key,
                                    Promise {
                                        state: PromiseState::NotCompleted(vec![
                                            JournalCompletionTarget::from_parts(
                                                invocation_id,
                                                entry_index,
                                            ),
                                        ]),
                                    },
                                )?;
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
                                )?;
                                CompletionResult::Empty
                            }
                            Some(Promise {
                                state: PromiseState::NotCompleted(listeners),
                            }) => {
                                // Send response to listeners
                                for listener in listeners {
                                    self.handle_outgoing_message(OutboxMessage::ServiceResponse(
                                        InvocationResponse {
                                            target: listener,
                                            result: completion.clone().into(),
                                        },
                                    ))?;
                                }

                                // Now register the promise completion
                                self.do_put_promise(
                                    service_id,
                                    key,
                                    Promise {
                                        state: PromiseState::Completed(completion.into()),
                                    },
                                )?;
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
                )?;
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

                    let service_invocation = Box::new(ServiceInvocation {
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
                        completion_retention_duration: (*completion_retention_time)
                            .unwrap_or_default(),
                        journal_retention_duration: Default::default(),
                        idempotency_key: request.idempotency_key,
                        submit_notification_sink: None,
                        restate_version: RestateVersion::current(),
                    });

                    self.handle_outgoing_message(OutboxMessage::ServiceInvocation(
                        service_invocation,
                    ))?;
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
                    span.add_link(ctx.into(), Vec::default());
                }

                let service_invocation = Box::new(ServiceInvocation {
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
                    completion_retention_duration: (*completion_retention_time).unwrap_or_default(),
                    journal_retention_duration: Default::default(),
                    idempotency_key: request.idempotency_key,
                    submit_notification_sink: None,
                    restate_version: RestateVersion::current(),
                });

                self.handle_outgoing_message(OutboxMessage::ServiceInvocation(service_invocation))?;
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
                    ))?;
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
                                        SignalResult::Failure(journal_v2::Failure {
                                            code,
                                            message,
                                            metadata: vec![],
                                        })
                                    }
                                },
                            ),
                        },
                    ))?;
                } else {
                    warn!(
                        "Invalid awakeable identifier {}. The identifier doesn't start with `awk_1`, neither with `sign_1`",
                        entry.id
                    );
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
                        ))?;
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
                        ))?;
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
        .await?;
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
        S: WriteOutboxTable + WriteFsmTable + ReadJournalTable,
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
                    response_sink: None,
                },
            ))?;
        }
        Ok(())
    }

    async fn get_invocation_query_from_attach_invocation_target(
        &mut self,
        invocation_id: &InvocationId,
        target: AttachInvocationTarget,
    ) -> Result<Option<InvocationQuery>, Error>
    where
        S: ReadJournalTable,
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
        S: ReadJournalTable,
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

    async fn handle_completion(
        &mut self,
        invocation_id: InvocationId,
        status: InvocationStatus,
        completion: Completion,
    ) -> Result<(), Error>
    where
        S: ReadJournalTable
            + WriteJournalTable
            + WriteInvocationStatusTable
            + WriteTimerTable
            + WriteFsmTable
            + WriteOutboxTable
            + WriteVQueueTable
            + ReadVQueueTable,
    {
        match status {
            InvocationStatus::Invoked(_) => {
                self.handle_completion_for_invoked(invocation_id, completion).await?;
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
            self.do_resume_service(invocation_id, metadata).await?;
                }
            }
            _ => {
                debug!(
                    restate.invocation.id = %invocation_id,
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
        S: WriteJournalTable + ReadJournalTable,
    {
        let resume_invocation = waiting_for_completed_entries.contains(&completion.entry_index);
        self.store_completion(invocation_id, completion).await?;

        Ok(resume_invocation)
    }

    async fn handle_completion_for_paused(
        &mut self,
        invocation_id: InvocationId,
        completion: Completion,
    ) -> Result<(), Error>
    where
        S: ReadJournalTable + WriteJournalTable,
    {
        self.store_completion(invocation_id, completion).await?;
        Ok(())
    }

    async fn handle_completion_for_invoked(
        &mut self,
        invocation_id: InvocationId,
        completion: Completion,
    ) -> Result<(), Error>
    where
        S: ReadJournalTable + WriteJournalTable,
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
        S: ReadJournalTable + journal_table_v2::ReadJournalTable,
    {
        if service_protocol_version >= ServiceProtocolVersion::V4 {
            // Find last output entry
            for i in (0..journal_length).rev() {
                let entry = journal_table_v2::ReadJournalTable::get_journal_entry(
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
                    ReadJournalTable::get_journal_entry(self.storage, invocation_id, i)
                        .await?
                        .unwrap_or_else(|| panic!("There should be a journal entry at index {i}"))
                    && e.ty() == EntryType::Output
                {
                    output_entry = Some(e);
                    break;
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
        invocation_id: &InvocationId,
        invocation_target: &InvocationTarget,
        span_context: &ServiceInvocationSpanContext,
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

    fn handle_outgoing_message(&mut self, message: OutboxMessage) -> Result<(), Error>
    where
        S: WriteOutboxTable + WriteFsmTable,
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

        self.do_enqueue_into_outbox(*self.outbox_seq_number, message)?;
        *self.outbox_seq_number += 1;
        Ok(())
    }

    async fn handle_attach_invocation_request(
        &mut self,
        attach_invocation_request: AttachInvocationRequest,
    ) -> Result<(), Error>
    where
        S: ReadOnlyIdempotencyTable
            + ReadInvocationStatusTable
            + WriteInvocationStatusTable
            + ReadVirtualObjectStatusTable
            + WriteOutboxTable
            + WriteFsmTable,
    {
        debug_assert!(
            self.partition_key_range
                .contains(&attach_invocation_request.partition_key()),
            "Attach invocation request with partition key '{}' has been delivered to a partition processor with key range '{:?}'. This indicates a bug.",
            attach_invocation_request.partition_key(),
            self.partition_key_range
        );

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
            InvocationStatus::Free => self.send_response_to_sinks(
                vec![attach_invocation_request.response_sink],
                NOT_FOUND_INVOCATION_ERROR,
                Some(invocation_id),
                None,
                None,
            )?,
            is @ InvocationStatus::Invoked(_)
            | is @ InvocationStatus::Suspended { .. }
            | is @ InvocationStatus::Inboxed(_)
            | is @ InvocationStatus::Paused(_)
            | is @ InvocationStatus::Scheduled(_) => {
                if attach_invocation_request.block_on_inflight {
                    self.do_append_response_sink(
                        invocation_id,
                        is,
                        attach_invocation_request.response_sink,
                    )?;
                } else {
                    self.send_response_to_sinks(
                        vec![attach_invocation_request.response_sink],
                        NOT_READY_INVOCATION_ERROR,
                        Some(invocation_id),
                        None,
                        is.invocation_target(),
                    )?;
                }
            }
            InvocationStatus::Completed(completed) => {
                let completion_expiry_time = completed.completion_expiry_time();
                self.send_response_to_sinks(
                    vec![attach_invocation_request.response_sink],
                    completed.response_result,
                    Some(invocation_id),
                    completion_expiry_time,
                    Some(&completed.invocation_target),
                )?;
            }
        }

        Ok(())
    }

    fn send_ingress_response(
        &mut self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: Option<InvocationId>,
        completion_expiry_time: Option<MillisSinceEpoch>,
        response: InvocationOutputResponse,
    ) {
        match &response {
            InvocationOutputResponse::Success(_, _) => {
                debug_if_leader!(
                    self.is_leader,
                    "Send response to ingress with request id '{:?}': Success",
                    request_id
                )
            }
            InvocationOutputResponse::Failure(e) => {
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

    fn reply_to_cancel(
        &mut self,
        response_sink: Option<InvocationMutationResponseSink>,
        response: CancelInvocationResponse,
    ) {
        if response_sink.is_none() {
            return;
        }
        let InvocationMutationResponseSink::Ingress(IngressInvocationResponseSink { request_id }) =
            response_sink.unwrap();
        debug_if_leader!(
            self.is_leader,
            "Send cancel response to request id '{:?}': {:?}",
            request_id,
            response
        );

        self.action_collector.push(Action::ForwardCancelResponse {
            request_id,
            response,
        });
    }

    fn reply_to_kill(
        &mut self,
        response_sink: Option<InvocationMutationResponseSink>,
        response: KillInvocationResponse,
    ) {
        if response_sink.is_none() {
            return;
        }
        let InvocationMutationResponseSink::Ingress(IngressInvocationResponseSink { request_id }) =
            response_sink.unwrap();
        debug_if_leader!(
            self.is_leader,
            "Send kill response to request id '{:?}': {:?}",
            request_id,
            response
        );

        self.action_collector.push(Action::ForwardKillResponse {
            request_id,
            response,
        });
    }

    fn reply_to_purge_invocation(
        &mut self,
        response_sink: Option<InvocationMutationResponseSink>,
        response: PurgeInvocationResponse,
    ) {
        if response_sink.is_none() {
            return;
        }
        let InvocationMutationResponseSink::Ingress(IngressInvocationResponseSink { request_id }) =
            response_sink.unwrap();
        debug_if_leader!(
            self.is_leader,
            "Send purge response to request id '{:?}': {:?}",
            request_id,
            response
        );

        self.action_collector
            .push(Action::ForwardPurgeInvocationResponse {
                request_id,
                response,
            });
    }

    fn reply_to_purge_journal(
        &mut self,
        response_sink: Option<InvocationMutationResponseSink>,
        response: PurgeInvocationResponse,
    ) {
        if response_sink.is_none() {
            return;
        }
        let InvocationMutationResponseSink::Ingress(IngressInvocationResponseSink { request_id }) =
            response_sink.unwrap();
        debug_if_leader!(
            self.is_leader,
            "Send purge response to request id '{:?}': {:?}",
            request_id,
            response
        );

        self.action_collector
            .push(Action::ForwardPurgeJournalResponse {
                request_id,
                response,
            });
    }

    fn reply_to_resume_invocation(
        &mut self,
        response_sink: Option<InvocationMutationResponseSink>,
        response: ResumeInvocationResponse,
    ) {
        if response_sink.is_none() {
            return;
        }
        let InvocationMutationResponseSink::Ingress(IngressInvocationResponseSink { request_id }) =
            response_sink.unwrap();
        debug_if_leader!(
            self.is_leader,
            "Send resume response to request id '{:?}': {:?}",
            request_id,
            response
        );

        self.action_collector
            .push(Action::ForwardResumeInvocationResponse {
                request_id,
                response,
            });
    }

    fn send_submit_notification_if_needed(
        &mut self,
        invocation_id: InvocationId,
        execution_time: Option<MillisSinceEpoch>,
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
                    execution_time,
                    is_new_invocation,
                });
        }
    }

    // Old code path used by the journal v1 (used by service protocols < v4)
    async fn do_resume_service(
        &mut self,
        invocation_id: InvocationId,
        mut metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error>
    where
        S: WriteInvocationStatusTable + WriteVQueueTable + ReadVQueueTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.journal.length = metadata.journal_metadata.length,
            "Effect: Resume service"
        );

        metadata.timestamps.update(self.record_created_at);
        let invocation_target = metadata.invocation_target.clone();
        self.storage
            .put_invocation_status(&invocation_id, &InvocationStatus::Invoked(metadata))
            .map_err(Error::Storage)?;

        if Configuration::pinned().common.experimental_enable_vqueues {
            self.vqueue_move_invocation_to_inbox_stage(&invocation_id)
                .await?;
        } else {
            self.action_collector.push(Action::Invoke {
                invocation_id,
                invocation_target,
                invoke_input_journal: InvokeInputJournal::NoCachedJournal,
            });
        }

        Ok(())
    }

    // Old code path used by the journal v1 (used by service protocols < v4)
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
    ) -> Result<(), Error>
    where
        S: WriteInvocationStatusTable + WriteVQueueTable + ReadVQueueTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.journal.length = metadata.journal_metadata.length,
            "Effect: Suspend service waiting on entries {:?}",
            waiting_for_completed_entries
        );

        metadata.timestamps.update(self.record_created_at);

        if Configuration::pinned().common.experimental_enable_vqueues {
            self.vqueue_park_invocation(
                &invocation_id,
                &metadata.invocation_target,
                ParkCause::Suspend,
            )
            .await?;
        }

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
            .map_err(Error::Storage)
    }

    fn do_store_completed_invocation(
        &mut self,
        invocation_id: InvocationId,
        completed_invocation: CompletedInvocation,
    ) -> Result<(), Error>
    where
        S: WriteInvocationStatusTable,
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
            .map_err(Error::Storage)
    }

    fn do_free_invocation(&mut self, invocation_id: InvocationId) -> Result<(), Error>
    where
        S: WriteInvocationStatusTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.invocation.id = %invocation_id,
            "Effect: Free invocation"
        );

        self.storage
            .put_invocation_status(&invocation_id, &InvocationStatus::Free)
            .map_err(Error::Storage)
    }

    async fn do_delete_inbox_entry(
        &mut self,
        service_id: ServiceId,
        sequence_number: MessageIndex,
    ) -> Result<(), Error>
    where
        S: WriteInboxTable,
    {
        debug_if_leader!(
            self.is_leader,
            rpc.service = %service_id.service_name,
            restate.inbox.seq = sequence_number,
            "Effect: Delete inbox entry",
        );

        self.storage
            .delete_inbox_entry(&service_id, sequence_number)
            .map_err(Error::Storage)?;

        Ok(())
    }

    fn do_enqueue_into_outbox(
        &mut self,
        seq_number: MessageIndex,
        message: OutboxMessage,
    ) -> Result<(), Error>
    where
        S: WriteOutboxTable + WriteFsmTable,
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
                target,
            }) => {
                debug_if_leader!(
                    self.is_leader,
                    restate.invocation.id = %target.caller_id,
                    restate.outbox.seq = seq_number,
                    "Effect: Send success response to another invocation for completion id {}",
                    target.caller_completion_id
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
                target,
            }) => {
                debug_if_leader!(
                    self.is_leader,
                    restate.invocation.id = %target.caller_id,
                    restate.outbox.seq = seq_number,
                    "Effect: Send failure '{}' response to another invocation for completion id {}",
                    e,
                    target.caller_completion_id
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

        self.storage
            .put_outbox_message(seq_number, &message)
            .map_err(Error::Storage)?;
        // need to store the next outbox sequence number
        self.storage
            .put_outbox_seq_number(seq_number + 1)
            .map_err(Error::Storage)?;

        self.action_collector.push(Action::NewOutboxMessage {
            seq_number,
            message,
        });

        Ok(())
    }

    async fn do_unlock_service(&mut self, service_id: ServiceId) -> Result<(), Error>
    where
        S: WriteVirtualObjectStatusTable,
    {
        debug_if_leader!(
            self.is_leader,
            rpc.service = %service_id.service_name,
            "Effect: Unlock service id",
        );

        self.storage
            .put_virtual_object_status(&service_id, &VirtualObjectStatus::Unlocked)
            .map_err(Error::Storage)?;

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
    ) -> Result<(), Error>
    where
        S: WriteStateTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.state.key = ?key,
            "Effect: Set state"
        );

        self.storage
            .put_user_state(&service_id, key, value)
            .map_err(Error::Storage)
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
    fn do_clear_state(
        &mut self,
        service_id: ServiceId,
        invocation_id: InvocationId,
        key: Bytes,
    ) -> Result<(), Error>
    where
        S: WriteStateTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.state.key = ?key,
            "Effect: Clear state"
        );

        self.storage
            .delete_user_state(&service_id, &key)
            .map_err(Error::Storage)
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
    fn do_clear_all_state(
        &mut self,
        service_id: ServiceId,
        invocation_id: InvocationId,
    ) -> Result<(), Error>
    where
        S: WriteStateTable,
    {
        debug_if_leader!(self.is_leader, "Effect: Clear all state");

        self.storage.delete_all_user_state(&service_id)?;

        Ok(())
    }

    async fn do_delete_timer(&mut self, timer_key: TimerKey) -> Result<(), Error>
    where
        S: WriteTimerTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.timer.key = %TimerKeyDisplay(&timer_key),
            "Effect: Delete timer"
        );

        self.storage
            .delete_timer(&timer_key)
            .map_err(Error::Storage)?;
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
    ) -> Result<(), Error>
    where
        S: WriteJournalTable + WriteInvocationStatusTable,
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
            .map_err(Error::Storage)?;

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
            timestamps.update(self.record_created_at);
        }

        // Store invocation status
        self.storage
            .put_invocation_status(&invocation_id, &previous_invocation_status)
            .map_err(Error::Storage)
    }

    async fn do_drop_journal(
        &mut self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
        should_remove_journal_table_v2: bool,
    ) -> Result<(), Error>
    where
        S: WriteJournalTable + journal_table_v2::WriteJournalTable + WriteJournalEventsTable,
    {
        debug_if_leader!(
            self.is_leader,
            restate.journal.length = journal_length,
            "Effect: Drop journal"
        );

        if should_remove_journal_table_v2 {
            journal_table_v2::WriteJournalTable::delete_journal(
                self.storage,
                invocation_id,
                journal_length,
            )
            .map_err(Error::Storage)?
        } else {
            WriteJournalTable::delete_journal(self.storage, &invocation_id, journal_length)
                .map_err(Error::Storage)?;
        }
        WriteJournalEventsTable::delete_journal_events(self.storage, invocation_id)
            .map_err(Error::Storage)?;
        Ok(())
    }

    async fn do_truncate_outbox(&mut self, range: RangeInclusive<MessageIndex>) -> Result<(), Error>
    where
        S: WriteOutboxTable,
    {
        trace!(
            restate.outbox.seq_from = range.start(),
            restate.outbox.seq_to = range.end(),
            "Effect: Truncate outbox"
        );

        self.storage
            .truncate_outbox(range)
            .map_err(Error::Storage)?;

        Ok(())
    }

    /// Returns the completion if it should be forwarded.
    async fn store_completion(
        &mut self,
        invocation_id: InvocationId,
        mut completion: Completion,
    ) -> Result<Option<Completion>, Error>
    where
        S: ReadJournalTable + WriteJournalTable,
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
                .map_err(Error::Storage)?;
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
                .map_err(Error::Storage)?;
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

    fn do_append_response_sink(
        &mut self,
        invocation_id: InvocationId,
        // We pass around the invocation_status here to avoid an additional read.
        // We could in theory get rid of this here (and in other places, such as StoreDeploymentId),
        // by using a merge operator in rocksdb.
        mut previous_invocation_status: InvocationStatus,
        additional_response_sink: ServiceInvocationResponseSink,
    ) -> Result<(), Error>
    where
        S: WriteInvocationStatusTable,
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
            timestamps.update(self.record_created_at);
        }

        self.storage
            .put_invocation_status(&invocation_id, &previous_invocation_status)
            .map_err(Error::Storage)?;

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
            .await
            .map_err(Error::Storage)?;

        Ok(())
    }

    fn do_send_abort_invocation_to_invoker(&mut self, invocation_id: InvocationId) {
        debug_if_leader!(self.is_leader, restate.invocation.id = %invocation_id, "Send abort command to invoker");

        self.action_collector
            .push(Action::AbortInvocation { invocation_id });
    }

    async fn do_mutate_state(&mut self, state_mutation: &ExternalStateMutation) -> Result<(), Error>
    where
        S: ReadStateTable + WriteStateTable,
    {
        debug_if_leader!(
            self.is_leader,
            "Effect: Mutate state for service id '{:?}'",
            &state_mutation.service_id
        );

        self.mutate_state(state_mutation).await?;

        Ok(())
    }

    fn do_put_promise(
        &mut self,
        service_id: ServiceId,
        key: ByteString,
        promise: Promise,
    ) -> Result<(), Error>
    where
        S: WritePromiseTable,
    {
        debug_if_leader!(self.is_leader, rpc.service = %service_id.service_name, "Effect: Put promise {} in non completed state", key);

        self.storage
            .put_promise(&service_id, &key, &promise)
            .map_err(Error::Storage)
    }

    async fn do_clear_all_promises(&mut self, service_id: ServiceId) -> Result<(), Error>
    where
        S: WritePromiseTable,
    {
        debug_if_leader!(
            self.is_leader,
            rpc.service = %service_id.service_name,
            "Effect: Clear all promises"
        );

        self.storage
            .delete_all_promises(&service_id)
            .map_err(Error::Storage)
    }

    async fn mutate_state(&mut self, state_mutation: &ExternalStateMutation) -> StorageResult<()>
    where
        S: ReadStateTable + WriteStateTable,
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
            .get_all_user_states_for_service(service_id)?
            .try_collect()
            .await?;

        if let Some(expected_version) = version {
            let expected = StateMutationVersion::from_raw(expected_version);
            let actual = StateMutationVersion::from_user_state(&all_user_states);

            if actual != expected {
                debug!(
                    "Ignore state mutation for service id '{:?}' because the expected version '{}' is not matching the actual version '{}'",
                    &service_id, expected, actual
                );
                return Ok(());
            }
        }

        for (key, _) in &all_user_states {
            if !state.contains_key(key) {
                self.storage.delete_user_state(service_id, key)?;
            }
        }

        // overwrite existing key value pairs
        for (key, value) in state {
            self.storage.put_user_state(service_id, key, value)?;
        }

        Ok(())
    }

    // [vqueues only]
    async fn vqueue_park_invocation(
        &mut self,
        invocation_id: &InvocationId,
        invocation_target: &InvocationTarget,
        cause: ParkCause,
    ) -> Result<(), Error>
    where
        S: WriteVQueueTable + ReadVQueueTable,
    {
        let qid = Self::vqueue_id_from_invocation(invocation_id, invocation_target);

        // Not great that we have to look up the entry card here.
        // todo remove once the reworked InvocationStatus can hold the required information
        let Some(entry_state_header) = self
            .storage
            .get_entry_state_header(
                EntryKind::Invocation,
                invocation_id.partition_key(),
                &EntryId::from(invocation_id),
            )
            .await?
        else {
            // todo resolve once we decided on the actual migration strategy
            panic!(
                "Trying to park invocation {invocation_id} which does not exist as a vqueue entry. Have you forgotten to migrate from the old inbox to vqueues?"
            );
        };

        let mut vqueue = VQueues::new(
            qid,
            self.storage,
            self.vqueues_cache,
            self.is_leader.then_some(self.action_collector),
        );

        let now = UniqueTimestamp::from_unix_millis(self.record_created_at).unwrap();

        let should_release_concurrency_token = match cause {
            ParkCause::Suspend => {
                // Always hold on to your concurrency token until the invocation is completed if
                // we are suspending for all types (services, VOs, workflows). This has the benefit
                // of an easy to reason about concurrency model for our users. The downside is that
                // callers might deadlock if they call a limited service which has no more
                // concurrency tokens left and there is a cyclic dependency (e.g. a limited service
                // calling itself).
                false
            }
            ParkCause::Pause => {
                // We release the concurrency token in case we are pausing a service because
                // unpausing requires human intervention, and we don't want to block other service
                // invocations.
                //
                // Note that we don't do this for paused VOs and workflows because they need to
                // ensure that no other instance can run while they hold their lock. Technically,
                // we still have the service_status_table which stores the locking information, and
                // we need to keep things in sync until we decide what to do with this table.
                matches!(invocation_target.service_ty(), ServiceType::Service)
            }
        };

        vqueue
            .park(
                now,
                &entry_state_header.current_entry_card(),
                entry_state_header.stage(),
                should_release_concurrency_token,
            )
            .await?;

        Ok(())
    }

    /// Moves the given invocation to the inbox and making it eligible for scheduling. Depending on its
    /// current [`Stage`], it will either yield the invocation from running, wake it up or be a noop
    /// if the invocation is already in the inbox stage.
    // [vqueues only]
    async fn vqueue_move_invocation_to_inbox_stage(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<(), Error>
    where
        S: WriteVQueueTable + ReadVQueueTable,
    {
        // Not great that we have to look up the entry card here.
        // todo remove once the reworked InvocationStatus can hold the required information
        let Some(entry_state_header) = self
            .storage
            .get_entry_state_header(
                EntryKind::Invocation,
                invocation_id.partition_key(),
                &EntryId::from(invocation_id),
            )
            .await?
        else {
            // todo resolve once we decided on the actual migration strategy
            panic!(
                "Trying to wake up invocation {invocation_id} which does not exist as a vqueue entry. Have you forgotten to migrate from the old inbox to vqueues?"
            );
        };

        let qid = entry_state_header.vqueue_id();

        let mut vqueue = VQueues::new(
            qid,
            self.storage,
            self.vqueues_cache,
            self.is_leader.then_some(self.action_collector),
        );

        let now = UniqueTimestamp::from_unix_millis(self.record_created_at).unwrap();

        match entry_state_header.stage() {
            Stage::Park => {
                vqueue
                    .wake_up(now, &entry_state_header.current_entry_card())
                    .await?;
            }
            Stage::Run => {
                vqueue
                    .yield_running(now, entry_state_header.current_entry_card())
                    .await?;
            }
            Stage::Inbox => {
                // nothing to do if we are already in the inbox
            }
            Stage::Unknown => {
                panic!("Trying to move invocation from unknown stage to inbox is not supported.")
            }
        };

        Ok(())
    }

    // todo placeholder until we have decided on the VQueueParent resolution
    fn vqueue_id_from_invocation(
        invocation_id: &InvocationId,
        invocation_target: &InvocationTarget,
    ) -> VQueueId {
        let (parent, instance) = match invocation_target {
            InvocationTarget::Service { .. } => {
                (VQueueParent::default_unlimited(), VQueueInstance::Default)
            }
            InvocationTarget::VirtualObject {
                handler_ty,
                key,
                name,
                ..
            } => {
                let parent = match handler_ty {
                    VirtualObjectHandlerType::Exclusive => VQueueParent::default_singleton(),
                    VirtualObjectHandlerType::Shared => VQueueParent::default_unlimited(),
                };

                // todo fix once we generate distinct parents for VOs and workflows
                // Temporarily we have to include the virtual object name since VOs with the same
                // key are mapped to the same partition key (see https://github.com/restatedev/restate/blob/786dc7dc6c240ef0a7abd6a48af7463f341bea2f/crates/types/src/identifiers.rs#L151-L150)
                // and different VOs must not fall into the same vqueue.
                (
                    parent,
                    VQueueInstance::infer_from(name.as_bytes(), key.as_bytes()),
                )
            }
            InvocationTarget::Workflow {
                handler_ty,
                key,
                name,
                ..
            } => {
                let parent = match handler_ty {
                    WorkflowHandlerType::Workflow => VQueueParent::default_singleton(),
                    WorkflowHandlerType::Shared => VQueueParent::default_unlimited(),
                };

                // we have to include the virtual object name since VOs with the same key are mapped
                // to the same partition key (see https://github.com/restatedev/restate/blob/786dc7dc6c240ef0a7abd6a48af7463f341bea2f/crates/types/src/identifiers.rs#L151-L150)
                // and different VOs must not fall into the same vqueue.
                (
                    parent,
                    VQueueInstance::infer_from(name.as_bytes(), key.as_bytes()),
                )
            }
        };
        let partition_key = invocation_id.partition_key();

        VQueueId::new(parent, partition_key, instance)
    }

    async fn vqueue_enqueue_state_mutation(
        &mut self,
        state_mutation: ExternalStateMutation,
    ) -> Result<(), Error>
    where
        S: WriteVQueueTable + ReadVQueueTable + WriteFsmTable,
    {
        let now = UniqueTimestamp::from_unix_millis(self.record_created_at).unwrap();
        let visible_at = VisibleAt::Now;

        let service_id = &state_mutation.service_id;
        let parent = VQueueParent::default_singleton();

        let qid = VQueueId::new(
            parent,
            service_id.partition_key(),
            VQueueInstance::infer_from(
                service_id.service_name.as_bytes(),
                service_id.key.as_bytes(),
            ),
        );

        let mut vqueue = VQueues::new(
            qid,
            self.storage,
            self.vqueues_cache,
            self.is_leader.then_some(self.action_collector),
        );
        vqueue
            .enqueue_new(
                now,
                visible_at,
                NewEntryPriority::UserDefault,
                EntryKind::StateMutation,
                // todo revisit entry id generation for state mutations
                EntryId::from(self.record_lsn),
                Some(state_mutation),
            )
            .await?;

        Ok(())
    }

    /// Apply the state mutation identified by the given qid and entry card.
    async fn vqueue_mutate_state(
        &mut self,
        qid: VQueueId,
        card: &EntryCard,
        now: UniqueTimestamp,
    ) -> Result<(), Error>
    where
        S: WriteVQueueTable + ReadVQueueTable + ReadStateTable + WriteStateTable,
    {
        if let Some(state_mutation) = self
            .storage
            .get_item::<ExternalStateMutation>(&qid, card.created_at, card.kind, &card.id)
            .await?
        {
            self.mutate_state(&state_mutation).await?;

            VQueues::new(
                qid,
                self.storage,
                self.vqueues_cache,
                self.is_leader.then_some(self.action_collector),
            )
            .end(now, Stage::Run, card)
            .await?;
        }

        Ok(())
    }
}

/// Cause for parking an invocation
#[derive(Debug)]
enum ParkCause {
    /// The invocation suspends to await completion or signals
    Suspend,
    /// The invocation pauses because it depleted it retries or was manually paused
    Pause,
}

// To write completions in the effects log
struct CompletionResultFmt<'a>(&'a CompletionResult);

impl fmt::Display for CompletionResultFmt<'_> {
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
    Paused,
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
