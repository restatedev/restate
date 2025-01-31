// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::metric_definitions::{PARTITION_ACTUATOR_HANDLED, PARTITION_HANDLE_LEADER_ACTIONS};
use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition::leadership::self_proposer::SelfProposer;
use crate::partition::leadership::{ActionEffect, Error, InvokerStream, TimerService};
use crate::partition::shuffle::HintSender;
use crate::partition::state_machine::Action;
use crate::partition::{respond_to_rpc, shuffle};
use futures::future::OptionFuture;
use futures::stream::FuturesUnordered;
use futures::{stream, FutureExt, StreamExt};
use metrics::{counter, Counter};
use restate_bifrost::CommitToken;
use restate_core::network::Reciprocal;
use restate_core::{TaskCenter, TaskHandle, TaskId};
use restate_partition_store::PartitionStore;
use restate_types::identifiers::{
    InvocationId, LeaderEpoch, PartitionId, PartitionKey, PartitionProcessorRpcRequestId,
    WithPartitionKey,
};
use restate_types::net::partition_processor::{
    InvocationOutput, PartitionProcessorRpcError, PartitionProcessorRpcResponse,
    SubmittedInvocationNotification,
};
use restate_types::time::MillisSinceEpoch;
use restate_wal_protocol::timer::TimerKeyValue;
use restate_wal_protocol::Command;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::future;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::{Duration, SystemTime};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, trace};

const BATCH_READY_UP_TO: usize = 10;

pub struct LeaderState {
    partition_id: PartitionId,
    pub leader_epoch: LeaderEpoch,
    // only needed for proposing TruncateOutbox to ourselves
    own_partition_key: PartitionKey,
    action_effects_counter: Counter,

    pub shuffle_hint_tx: HintSender,
    // It's illegal to await the shuffle task handle once it has
    // been resolved. Hence run() should never be called again if it
    // returns a [`Error:TaskFailed`] error.
    shuffle_task_handle: Option<TaskHandle<anyhow::Result<()>>>,
    pub timer_service: Pin<Box<TimerService>>,
    self_proposer: SelfProposer,

    pub awaiting_rpc_actions: HashMap<
        PartitionProcessorRpcRequestId,
        Reciprocal<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
    >,
    awaiting_rpc_self_propose: FuturesUnordered<SelfAppendFuture>,

    invoker_stream: InvokerStream,
    shuffle_stream: ReceiverStream<shuffle::OutboxTruncation>,
    pub pending_cleanup_timers_to_schedule: VecDeque<(InvocationId, Duration)>,
    cleaner_task_id: TaskId,
}

impl LeaderState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition_id: PartitionId,
        leader_epoch: LeaderEpoch,
        own_partition_key: PartitionKey,
        shuffle_task_handle: TaskHandle<anyhow::Result<()>>,
        cleaner_task_id: TaskId,
        shuffle_hint_tx: HintSender,
        timer_service: TimerService,
        self_proposer: SelfProposer,
        invoker_rx: InvokerStream,
        shuffle_rx: tokio::sync::mpsc::Receiver<shuffle::OutboxTruncation>,
    ) -> Self {
        LeaderState {
            partition_id,
            leader_epoch,
            own_partition_key,
            action_effects_counter: counter!(PARTITION_ACTUATOR_HANDLED),
            shuffle_task_handle: Some(shuffle_task_handle),
            cleaner_task_id,
            shuffle_hint_tx,
            timer_service: Box::pin(timer_service),
            self_proposer,
            awaiting_rpc_actions: Default::default(),
            awaiting_rpc_self_propose: Default::default(),
            invoker_stream: invoker_rx,
            shuffle_stream: ReceiverStream::new(shuffle_rx),
            pending_cleanup_timers_to_schedule: Default::default(),
        }
    }

    /// Runs the leader specific task which is the awaiting of action effects and the monitoring
    /// of unmanaged tasks.
    ///
    /// Important: The future needs to be cancellation safe since it is polled as a tokio::select
    /// arm!
    pub async fn run(&mut self) -> Result<Vec<ActionEffect>, Error> {
        let timer_stream = std::pin::pin!(stream::unfold(
            &mut self.timer_service,
            |timer_service| async {
                let timer_value = timer_service.as_mut().next_timer().await;
                Some((ActionEffect::Timer(timer_value), timer_service))
            }
        ));

        let invoker_stream = (&mut self.invoker_stream).map(ActionEffect::Invoker);
        let shuffle_stream = (&mut self.shuffle_stream).map(ActionEffect::Shuffle);
        let action_effects_stream = stream::unfold(
            &mut self.pending_cleanup_timers_to_schedule,
            |pending_cleanup_timers_to_schedule| {
                let result = pending_cleanup_timers_to_schedule.pop_front();
                future::ready(result.map(|(invocation_id, duration)| {
                    (
                        ActionEffect::ScheduleCleanupTimer(invocation_id, duration),
                        pending_cleanup_timers_to_schedule,
                    )
                }))
            },
        )
        .fuse();
        let awaiting_rpc_self_propose_stream =
            (&mut self.awaiting_rpc_self_propose).map(|_| ActionEffect::AwaitingRpcSelfProposeDone);

        let all_streams = futures::stream_select!(
            invoker_stream,
            shuffle_stream,
            timer_stream,
            action_effects_stream,
            awaiting_rpc_self_propose_stream
        );
        let mut all_streams = all_streams.ready_chunks(BATCH_READY_UP_TO);

        let shuffle_task_handle = self.shuffle_task_handle.as_mut().expect("is set");
        tokio::select! {
            // watch the shuffle task in case it crashed
            result = shuffle_task_handle => {
                // it's not possible to await the shuffler handle
                // if it returns an error. Hence we take it here.
                // run() should then never be called again.
                self.shuffle_task_handle.take();
                match result {
                    Ok(Ok(_)) => Err(Error::task_terminated_unexpectedly("shuffle")),
                    Ok(Err(err)) => Err(Error::task_failed("shuffle", err)),
                    Err(shutdown_error) => Err(Error::Shutdown(shutdown_error))
                }
            }
            Some(action_effects) = all_streams.next() => {
                Ok(action_effects)
            },
            result = self.self_proposer.join_on_err() => {
                Err(result.expect_err("never should never be returned"))
            }
        }
    }

    /// Stops all leader relevant tasks.
    pub async fn stop(
        mut self,
        invoker_handle: &mut impl restate_invoker_api::InvokerHandle<
            InvokerStorageReader<PartitionStore>,
        >,
    ) {
        let shuffle_handle = match self.shuffle_task_handle {
            None => OptionFuture::from(None),
            Some(shuffle_handle) => {
                shuffle_handle.cancel();
                OptionFuture::from(Some(shuffle_handle))
            }
        };

        let cleaner_handle =
            OptionFuture::from(TaskCenter::current().cancel_task(self.cleaner_task_id));

        // It's ok to not check the abort_result because either it succeeded or the invoker
        // is not running. If the invoker is not running, and we are not shutting down, then
        // we will fail the next time we try to invoke.
        let (shuffle_result, cleaner_result, _abort_result) = tokio::join!(
            shuffle_handle,
            cleaner_handle,
            invoker_handle.abort_all_partition((self.partition_id, self.leader_epoch)),
        );

        if let Some(shuffle_result) = shuffle_result {
            let _ = shuffle_result.expect("graceful termination of shuffle task");
        }
        if let Some(cleaner_result) = cleaner_result {
            cleaner_result.expect("graceful termination of cleaner task");
        }

        // Reply to all RPCs with not a leader
        for (request_id, reciprocal) in self.awaiting_rpc_actions.drain() {
            trace!(
                %request_id,
                "Failing rpc because I lost leadership",
            );
            respond_to_rpc(
                reciprocal.prepare(Err(PartitionProcessorRpcError::LostLeadership(
                    self.partition_id,
                ))),
            );
        }
        for fut in self.awaiting_rpc_self_propose.iter_mut() {
            fut.fail_with_lost_leadership(self.partition_id);
        }
    }

    pub async fn handle_action_effects(
        &mut self,
        action_effects: impl IntoIterator<Item = ActionEffect>,
    ) -> Result<(), Error> {
        for effect in action_effects {
            self.action_effects_counter.increment(1);

            match effect {
                ActionEffect::Invoker(invoker_effect) => {
                    self.self_proposer
                        .propose(
                            invoker_effect.invocation_id.partition_key(),
                            Command::InvokerEffect(invoker_effect),
                        )
                        .await?;
                }
                ActionEffect::Shuffle(outbox_truncation) => {
                    // todo: Until we support partition splits we need to get rid of outboxes or introduce partition
                    //  specific destination messages that are identified by a partition_id
                    self.self_proposer
                        .propose(
                            self.own_partition_key,
                            Command::TruncateOutbox(outbox_truncation.index()),
                        )
                        .await?;
                }
                ActionEffect::Timer(timer) => {
                    self.self_proposer
                        .propose(timer.invocation_id().partition_key(), Command::Timer(timer))
                        .await?;
                }
                ActionEffect::ScheduleCleanupTimer(invocation_id, duration) => {
                    self.self_proposer
                        .propose(
                            invocation_id.partition_key(),
                            Command::ScheduleTimer(TimerKeyValue::clean_invocation_status(
                                MillisSinceEpoch::from(SystemTime::now() + duration),
                                invocation_id,
                            )),
                        )
                        .await?;
                }
                ActionEffect::AwaitingRpcSelfProposeDone => {
                    // Nothing to do here
                }
            }
        }

        Ok(())
    }

    pub async fn handle_rpc_proposal_command(
        &mut self,
        request_id: PartitionProcessorRpcRequestId,
        reciprocal: Reciprocal<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
        partition_key: PartitionKey,
        cmd: Command,
    ) {
        match self.awaiting_rpc_actions.entry(request_id) {
            Entry::Occupied(o) => {
                // In this case, someone already proposed this command,
                // let's just replace the reciprocal and fail the old one to avoid keeping it dangling
                let old_reciprocal = o.remove();
                trace!(%request_id, "Replacing rpc with newer request");
                respond_to_rpc(
                    old_reciprocal.prepare(Err(PartitionProcessorRpcError::Internal(
                        "retried".to_string(),
                    ))),
                );
                self.awaiting_rpc_actions.insert(request_id, reciprocal);
            }
            Entry::Vacant(v) => {
                // In this case, no one proposed this command yet, let's try to propose it
                if let Err(e) = self.self_proposer.propose(partition_key, cmd).await {
                    respond_to_rpc(
                        reciprocal
                            .prepare(Err(PartitionProcessorRpcError::Internal(e.to_string()))),
                    );
                } else {
                    v.insert(reciprocal);
                }
            }
        }
    }

    pub async fn self_propose_and_respond_asynchronously(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
        reciprocal: Reciprocal<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
    ) {
        match self
            .self_proposer
            .propose_with_notification(partition_key, cmd)
            .await
        {
            Ok(commit_token) => {
                self.awaiting_rpc_self_propose
                    .push(SelfAppendFuture::new(commit_token, reciprocal));
            }
            Err(e) => {
                respond_to_rpc(
                    reciprocal.prepare(Err(PartitionProcessorRpcError::Internal(e.to_string()))),
                );
            }
        }
    }

    pub async fn handle_actions(
        &mut self,
        invoker_tx: &mut impl restate_invoker_api::InvokerHandle<InvokerStorageReader<PartitionStore>>,
        actions: impl Iterator<Item = Action>,
    ) -> Result<(), Error> {
        for action in actions {
            trace!(?action, "Apply action");
            counter!(PARTITION_HANDLE_LEADER_ACTIONS, "action" =>
                action.name())
            .increment(1);
            self.handle_action(action, invoker_tx).await?;
        }

        Ok(())
    }

    async fn handle_action(
        &mut self,
        action: Action,
        invoker_tx: &mut impl restate_invoker_api::InvokerHandle<InvokerStorageReader<PartitionStore>>,
    ) -> Result<(), Error> {
        let partition_leader_epoch = (self.partition_id, self.leader_epoch);
        match action {
            Action::Invoke {
                invocation_id,
                invocation_target,
                invoke_input_journal,
            } => invoker_tx
                .invoke(
                    partition_leader_epoch,
                    invocation_id,
                    invocation_target,
                    invoke_input_journal,
                )
                .await
                .map_err(Error::Invoker)?,
            Action::NewOutboxMessage {
                seq_number,
                message,
            } => self
                .shuffle_hint_tx
                .send(shuffle::NewOutboxMessage::new(seq_number, message)),
            Action::RegisterTimer { timer_value } => {
                self.timer_service.as_mut().add_timer(timer_value)
            }
            Action::DeleteTimer { timer_key } => {
                self.timer_service.as_mut().remove_timer(timer_key)
            }
            Action::AckStoredCommand {
                invocation_id,
                command_index,
            } => {
                invoker_tx
                    .notify_stored_command_ack(partition_leader_epoch, invocation_id, command_index)
                    .await
                    .map_err(Error::Invoker)?;
            }
            Action::ForwardCompletion {
                invocation_id,
                completion,
            } => invoker_tx
                .notify_completion(partition_leader_epoch, invocation_id, completion)
                .await
                .map_err(Error::Invoker)?,
            Action::AbortInvocation {
                invocation_id,
                acknowledge,
            } => invoker_tx
                .abort_invocation(partition_leader_epoch, invocation_id, acknowledge)
                .await
                .map_err(Error::Invoker)?,
            Action::IngressResponse {
                request_id,
                invocation_id,
                response,
                completion_expiry_time,
                ..
            } => {
                if let Some(response_tx) = self.awaiting_rpc_actions.remove(&request_id) {
                    respond_to_rpc(
                        response_tx.prepare(Ok(PartitionProcessorRpcResponse::Output(
                            InvocationOutput {
                                request_id,
                                invocation_id,
                                completion_expiry_time,
                                response,
                            },
                        ))),
                    );
                } else {
                    debug!(%request_id, "Ignoring sending ingress response because there is no awaiting rpc");
                }
            }
            Action::IngressSubmitNotification {
                request_id,
                execution_time,
                is_new_invocation,
                ..
            } => {
                if let Some(response_tx) = self.awaiting_rpc_actions.remove(&request_id) {
                    respond_to_rpc(response_tx.prepare(Ok(
                        PartitionProcessorRpcResponse::Submitted(SubmittedInvocationNotification {
                            request_id,
                            execution_time,
                            is_new_invocation,
                        }),
                    )));
                }
            }
            Action::ScheduleInvocationStatusCleanup {
                invocation_id,
                retention,
            } => {
                self.pending_cleanup_timers_to_schedule
                    .push_back((invocation_id, retention));
            }
            Action::ForwardNotification {
                invocation_id,
                notification,
            } => {
                invoker_tx
                    .notify_notification(partition_leader_epoch, invocation_id, notification)
                    .await
                    .map_err(Error::Invoker)?;
            }
        }

        Ok(())
    }
}

struct SelfAppendFuture {
    commit_token: CommitToken,
    response: Option<Reciprocal<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>>,
}

impl SelfAppendFuture {
    fn new(
        commit_token: CommitToken,
        response: Reciprocal<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
    ) -> Self {
        Self {
            commit_token,
            response: Some(response),
        }
    }

    fn fail_with_internal(&mut self) {
        if let Some(reciprocal) = self.response.take() {
            respond_to_rpc(reciprocal.prepare(Err(PartitionProcessorRpcError::Internal(
                "error when proposing to bifrost".to_string(),
            ))));
        }
    }

    fn fail_with_lost_leadership(&mut self, this_partition_id: PartitionId) {
        if let Some(reciprocal) = self.response.take() {
            respond_to_rpc(
                reciprocal.prepare(Err(PartitionProcessorRpcError::LostLeadership(
                    this_partition_id,
                ))),
            );
        }
    }

    fn succeed_with_appended(&mut self) {
        if let Some(reciprocal) = self.response.take() {
            respond_to_rpc(reciprocal.prepare(Ok(PartitionProcessorRpcResponse::Appended)));
        }
    }
}

impl Future for SelfAppendFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let append_result = ready!(self.commit_token.poll_unpin(cx));

        if append_result.is_err() {
            self.get_mut().fail_with_internal();
            return Poll::Ready(());
        }
        self.succeed_with_appended();
        Poll::Ready(())
    }
}
