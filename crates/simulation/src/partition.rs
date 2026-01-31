// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Single-partition deterministic simulation.
//!
//! This module provides a closed-loop simulation of a single partition processor,
//! where actions emitted by the state machine (timer registrations, outbox messages)
//! are fed back as commands.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::ops::RangeInclusive;

use bytes::Bytes;
use bytestring::ByteString;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use restate_invoker_api::{Effect, EffectKind, InvokeInputJournal};
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::invocation_status_table::ReadInvocationStatusTable;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::timer_table::TimerKey;
use restate_storage_api::{Storage, Transaction};
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::{DeploymentId, InvocationId, PartitionKey, WithPartitionKey};
use restate_types::invocation::{InvocationTarget, ServiceInvocation, Source};
use restate_types::journal_v2::Entry;
use restate_types::journal_v2::command::{Command as JournalCommand, OutputCommand, OutputResult};
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
use restate_types::time::MillisSinceEpoch;
use restate_types::timer::Timer;
use restate_vqueues::VQueuesMetaMut;
use restate_wal_protocol::Command;
use restate_wal_protocol::timer::TimerKeyValue;
use restate_worker::state_machine::{Action, ActionCollector, StateMachine};

use crate::clock::SimulationClock;

/// Configuration for a partition simulation.
#[derive(Debug, Clone)]
pub struct PartitionSimulationConfig {
    /// Random seed for deterministic execution.
    pub seed: u64,
    /// Maximum number of simulation steps before termination.
    pub max_steps: usize,
    /// Partition key range for this partition.
    pub partition_key_range: RangeInclusive<PartitionKey>,
    /// Whether to check invariants after each step.
    pub check_invariants: bool,
}

impl Default for PartitionSimulationConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            max_steps: 10_000,
            partition_key_range: PartitionKey::MIN..=PartitionKey::MAX,
            check_invariants: true,
        }
    }
}

/// Behavior configuration for the invoker simulator.
#[derive(Debug, Default)]
pub enum InvokerBehavior {
    /// Immediately complete invocations with success.
    #[default]
    ImmediateSuccess,
    /// Immediately fail invocations with an error.
    ImmediateFail { error_code: u16, message: String },
    /// Generate a random sequence of journal entries before completing.
    RandomJournal {
        /// Minimum number of journal entries to generate.
        min_entries: usize,
        /// Maximum number of journal entries to generate.
        max_entries: usize,
    },
    /// Custom behavior provided by the test.
    Custom(Box<dyn InvokerSimulator>),
}

/// Trait for simulating invoker behavior.
///
/// Implementations generate the sequence of `InvokerEffect` commands that would
/// be produced by the invoker when executing an invocation.
pub trait InvokerSimulator: std::fmt::Debug + Send + Sync {
    /// Called when an invocation should be started.
    /// Returns the sequence of commands to apply for this invocation.
    fn on_invoke(
        &mut self,
        invocation_id: InvocationId,
        invocation_target: &InvocationTarget,
        journal: &InvokeInputJournal,
        rng: &mut StdRng,
        clock: &SimulationClock,
    ) -> Vec<Command>;
}

/// Simple invoker simulator that immediately completes invocations.
#[derive(Debug, Default)]
pub struct ImmediateSuccessInvoker;

impl InvokerSimulator for ImmediateSuccessInvoker {
    fn on_invoke(
        &mut self,
        invocation_id: InvocationId,
        _invocation_target: &InvocationTarget,
        _journal: &InvokeInputJournal,
        _rng: &mut StdRng,
        _clock: &SimulationClock,
    ) -> Vec<Command> {
        vec![
            // Pin deployment
            Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                kind: EffectKind::PinnedDeployment(PinnedDeployment {
                    deployment_id: DeploymentId::default(),
                    service_protocol_version: ServiceProtocolVersion::V5,
                }),
            })),
            // Output entry
            Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                kind: EffectKind::JournalEntryV2 {
                    entry: StoredRawEntry::new(
                        StoredRawEntryHeader::new(MillisSinceEpoch::UNIX_EPOCH),
                        Entry::Command(JournalCommand::Output(OutputCommand {
                            result: OutputResult::Success(Bytes::new()),
                            name: ByteString::new(),
                        }))
                        .encode::<ServiceProtocolV4Codec>(),
                    ),
                    command_index_to_ack: None,
                },
            })),
            // End
            Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                kind: EffectKind::End,
            })),
        ]
    }
}

/// Result of a single simulation step.
#[derive(Debug)]
pub struct StepResult {
    /// The command that was applied.
    pub command: Command,
    /// The time at which the command was applied.
    pub time: MillisSinceEpoch,
    /// Actions emitted by the state machine.
    pub actions: Vec<Action>,
}

/// Represents a scheduled event in the simulation.
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum ScheduledEvent {
    /// A timer that should fire at a specific time.
    Timer(TimerKeyValue),
    /// Commands generated by the invoker simulator.
    InvokerCommands(VecDeque<Command>),
}

/// A single-partition deterministic simulation.
///
/// This simulation runs a partition processor state machine in a closed loop,
/// where actions (timer registrations, outbox messages) are converted back
/// into commands that feed back into the state machine.
#[allow(dead_code)]
pub struct PartitionSimulation<S> {
    /// Configuration for this simulation.
    config: PartitionSimulationConfig,
    /// Deterministic random number generator.
    rng: StdRng,
    /// Deterministic clock.
    clock: SimulationClock,
    /// The state machine being simulated.
    state_machine: StateMachine,
    /// Storage backend.
    storage: S,
    /// Invoker simulator for generating invoker effects.
    invoker: Box<dyn InvokerSimulator>,
    /// Queue of pending commands to apply.
    pending_commands: VecDeque<Command>,
    /// Registered timers, indexed by wake time.
    timers: BTreeMap<MillisSinceEpoch, Vec<TimerKeyValue>>,
    /// Set of timer keys that have been deleted (to handle races).
    deleted_timers: HashSet<TimerKey>,
    /// Invocations currently being "executed" by the invoker simulator.
    active_invocations: HashSet<InvocationId>,
    /// Number of steps executed so far.
    steps_executed: usize,
}

#[allow(dead_code)]
impl<S> PartitionSimulation<S>
where
    S: Storage + ReadInvocationStatusTable + Send,
{
    /// Creates a new partition simulation.
    pub fn new(
        config: PartitionSimulationConfig,
        storage: S,
        invoker_behavior: InvokerBehavior,
    ) -> Self {
        let rng = StdRng::seed_from_u64(config.seed);
        let clock = SimulationClock::at_epoch();

        let state_machine = StateMachine::new(
            0,    // inbox_seq_number
            0,    // outbox_seq_number
            None, // outbox_head_seq_number
            config.partition_key_range.clone(),
            restate_types::SemanticRestateVersion::unknown(),
            None,
        );

        let invoker: Box<dyn InvokerSimulator> = match invoker_behavior {
            InvokerBehavior::ImmediateSuccess => Box::new(ImmediateSuccessInvoker),
            InvokerBehavior::ImmediateFail { .. } => {
                // TODO: Implement failure invoker
                Box::new(ImmediateSuccessInvoker)
            }
            InvokerBehavior::RandomJournal { .. } => {
                // TODO: Implement random journal invoker
                Box::new(ImmediateSuccessInvoker)
            }
            InvokerBehavior::Custom(invoker) => invoker,
        };

        Self {
            config,
            rng,
            clock,
            state_machine,
            storage,
            invoker,
            pending_commands: VecDeque::new(),
            timers: BTreeMap::new(),
            deleted_timers: HashSet::new(),
            active_invocations: HashSet::new(),
            steps_executed: 0,
        }
    }

    /// Returns a reference to the simulation clock.
    pub fn clock(&self) -> &SimulationClock {
        &self.clock
    }

    /// Returns a mutable reference to the storage.
    pub fn storage(&mut self) -> &mut S {
        &mut self.storage
    }

    /// Returns the number of steps executed.
    pub fn steps_executed(&self) -> usize {
        self.steps_executed
    }

    /// Enqueues an external command to be processed.
    pub fn enqueue_command(&mut self, command: Command) {
        self.pending_commands.push_back(command);
    }

    /// Enqueues a new invocation.
    pub fn enqueue_invocation(&mut self, invocation: ServiceInvocation) {
        self.pending_commands
            .push_back(Command::Invoke(Box::new(invocation)));
    }

    /// Creates a random invocation targeting this partition.
    pub fn random_invocation(&mut self) -> ServiceInvocation {
        let target = InvocationTarget::virtual_object(
            "TestService",
            format!("key-{}", self.rng.random::<u32>()),
            "handler",
            restate_types::invocation::VirtualObjectHandlerType::Exclusive,
        );
        let invocation_id = InvocationId::generate(&target, None);
        ServiceInvocation::initialize(invocation_id, target, Source::Ingress(Default::default()))
    }

    /// Checks if the simulation should continue.
    pub fn should_continue(&self) -> bool {
        self.steps_executed < self.config.max_steps
            && (!self.pending_commands.is_empty()
                || !self.timers.is_empty()
                || !self.active_invocations.is_empty())
    }

    /// Advances time to fire the next scheduled timer (if any).
    /// Returns true if a timer was fired.
    fn advance_to_next_timer(&mut self) -> bool {
        if let Some((&wake_time, _)) = self.timers.first_key_value() {
            self.clock.advance_to(wake_time);
            if let Some(timers) = self.timers.remove(&wake_time) {
                for timer in timers {
                    let timer_key = timer.timer_key();
                    if !self.deleted_timers.remove(timer_key) {
                        self.pending_commands.push_back(Command::Timer(timer));
                    }
                }
            }
            true
        } else {
            false
        }
    }

    /// Processes actions emitted by the state machine.
    fn process_actions(&mut self, actions: &[Action]) {
        for action in actions {
            match action {
                Action::Invoke {
                    invocation_id,
                    invocation_target,
                    invoke_input_journal,
                } => {
                    self.active_invocations.insert(*invocation_id);
                    let commands = self.invoker.on_invoke(
                        *invocation_id,
                        invocation_target,
                        invoke_input_journal,
                        &mut self.rng,
                        &self.clock,
                    );
                    for cmd in commands {
                        self.pending_commands.push_back(cmd);
                    }
                }
                Action::VQInvoke {
                    invocation_id,
                    invocation_target,
                    invoke_input_journal,
                    ..
                } => {
                    self.active_invocations.insert(*invocation_id);
                    let commands = self.invoker.on_invoke(
                        *invocation_id,
                        invocation_target,
                        invoke_input_journal,
                        &mut self.rng,
                        &self.clock,
                    );
                    for cmd in commands {
                        self.pending_commands.push_back(cmd);
                    }
                }
                Action::RegisterTimer { timer_value } => {
                    let wake_time = timer_value.wake_up_time();
                    self.timers
                        .entry(wake_time)
                        .or_default()
                        .push(timer_value.clone());
                }
                Action::DeleteTimer { timer_key } => {
                    self.deleted_timers.insert(timer_key.clone());
                }
                Action::NewOutboxMessage { message, .. } => {
                    self.process_outbox_message(message);
                }
                Action::AbortInvocation { invocation_id } => {
                    self.active_invocations.remove(invocation_id);
                }
                // These actions don't generate feedback commands in single-partition simulation
                Action::VQEvent(_)
                | Action::AckStoredCommand { .. }
                | Action::ForwardCompletion { .. }
                | Action::ForwardNotification { .. }
                | Action::IngressResponse { .. }
                | Action::IngressSubmitNotification { .. }
                | Action::ForwardKillResponse { .. }
                | Action::ForwardCancelResponse { .. }
                | Action::ForwardPurgeInvocationResponse { .. }
                | Action::ForwardPurgeJournalResponse { .. }
                | Action::ForwardResumeInvocationResponse { .. }
                | Action::ForwardRestartAsNewInvocationResponse { .. } => {}
            }
        }
    }

    /// Processes an outbox message, potentially converting it to a command.
    fn process_outbox_message(&mut self, message: &OutboxMessage) {
        match message {
            OutboxMessage::ServiceInvocation(invocation) => {
                // For single-partition simulation, we handle same-partition invocations
                if self
                    .config
                    .partition_key_range
                    .contains(&invocation.partition_key())
                {
                    self.pending_commands
                        .push_back(Command::Invoke(invocation.clone()));
                }
            }
            OutboxMessage::ServiceResponse(response) => {
                // Responses to invocations on this partition
                if self
                    .config
                    .partition_key_range
                    .contains(&response.partition_key())
                {
                    self.pending_commands
                        .push_back(Command::InvocationResponse(response.clone()));
                }
            }
            OutboxMessage::AttachInvocation(_)
            | OutboxMessage::NotifySignal(_)
            | OutboxMessage::InvocationTermination(_) => {
                // TODO: Handle these in multi-partition simulation
            }
        }
    }

    /// Checks state machine invariants.
    fn check_invariants(&self) -> Result<(), InvariantViolation> {
        // TODO: Implement invariant checks
        // - No duplicate journal entries
        // - Valid state transitions
        // - Proper sequence number ordering
        // - No orphaned invocations
        Ok(())
    }

    /// Executes a single simulation step.
    ///
    /// This takes the next command from the queue (or advances time to fire a timer),
    /// applies it to the state machine, and processes the resulting actions.
    pub async fn step(&mut self) -> Result<StepResult, SimulationError> {
        // If no pending commands, try to advance to the next timer
        if self.pending_commands.is_empty() && !self.advance_to_next_timer() {
            return Err(SimulationError::NoPendingWork);
        }

        // Get the next command
        let command = self
            .pending_commands
            .pop_front()
            .ok_or(SimulationError::NoPendingWork)?;

        let time = self.clock.now();

        // Create a transaction and apply the command
        let mut transaction = self.storage.transaction();
        let mut action_collector = ActionCollector::default();
        let mut vqueues = VQueuesMetaMut::default();

        self.state_machine
            .apply(
                command.clone(),
                time,
                Lsn::OLDEST,
                &mut transaction,
                &mut action_collector,
                &mut vqueues,
                true, // is_leader
            )
            .await?;

        // Commit the transaction
        transaction.commit().await?;

        // Process actions to generate feedback commands
        self.process_actions(&action_collector);

        // Check invariants if enabled
        if self.config.check_invariants {
            self.check_invariants()?;
        }

        self.steps_executed += 1;

        Ok(StepResult {
            command,
            time,
            actions: action_collector,
        })
    }

    /// Runs the simulation until completion or max steps reached.
    ///
    /// Returns the simulation outcome including total steps and any violations.
    pub async fn run(&mut self) -> Result<SimulationOutcome, SimulationError> {
        let mut violations = Vec::new();

        while self.should_continue() {
            match self.step().await {
                Ok(_) => {}
                Err(SimulationError::NoPendingWork) => break,
                Err(SimulationError::Invariant(violation)) => {
                    violations.push(violation);
                    if self.config.check_invariants {
                        // Stop on first invariant violation if checking is enabled
                        break;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Ok(SimulationOutcome {
            steps_executed: self.steps_executed,
            final_time: self.clock.now(),
            success: violations.is_empty(),
            violations,
        })
    }
}

/// Errors that can occur during simulation.
#[derive(Debug, thiserror::Error)]
pub enum SimulationError {
    #[error("State machine error: {0}")]
    StateMachine(#[from] restate_worker::state_machine::Error),
    #[error("Storage error: {0}")]
    Storage(#[from] restate_storage_api::StorageError),
    #[error("Invariant violation: {0}")]
    Invariant(#[from] InvariantViolation),
    #[error("No pending commands and no timers to fire")]
    NoPendingWork,
}

/// An invariant violation detected during simulation.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum InvariantViolation {
    #[error("Duplicate journal entry for invocation {invocation_id} at index {entry_index}")]
    DuplicateJournalEntry {
        invocation_id: InvocationId,
        entry_index: u32,
    },
    #[error("Invalid state transition for invocation {invocation_id}: {details}")]
    InvalidStateTransition {
        invocation_id: InvocationId,
        details: String,
    },
    #[error("Invariant check failed: {0}")]
    Custom(String),
}

/// The outcome of running a simulation.
#[derive(Debug)]
#[allow(dead_code)]
pub struct SimulationOutcome {
    /// Total number of steps executed.
    pub steps_executed: usize,
    /// Final simulation time.
    pub final_time: MillisSinceEpoch,
    /// Whether the simulation completed successfully.
    pub success: bool,
    /// Any invariant violations detected.
    pub violations: Vec<InvariantViolation>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simulation_config_default() {
        let config = PartitionSimulationConfig::default();
        assert_eq!(config.seed, 0);
        assert_eq!(config.max_steps, 10_000);
        assert!(config.check_invariants);
    }
}
