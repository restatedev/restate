// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::log::CommandLog;
use crate::sender::StateMachineSender;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use restate_types::identifiers::{LeaderEpoch, PeerId};
use restate_types::message::PeerTarget;
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::sync::mpsc;
use tracing::{debug, trace};

mod log;
mod sender;

/// Consensus commands that are sent to an associated state machine.
#[derive(Debug, PartialEq)]
pub enum Command<T> {
    /// Apply the given message to the state machine.
    Apply(T),
    CreateSnapshot,
    ApplySnapshot,
    BecomeLeader(LeaderEpoch),
    BecomeFollower,
}

pub type ProposalSender<T> = mpsc::Sender<T>;

/// Component which is responsible for running the consensus algorithm for multiple replicated
/// state machines. Consensus replicates messages of type `Cmd`.
#[derive(Debug)]
pub struct Consensus<Cmd> {
    /// command logs for the associated state machines
    cmd_logs: HashMap<PeerId, CommandLog<Cmd>>,

    /// Receiver of proposals from all associated state machines
    proposal_rx: mpsc::Receiver<PeerTarget<Cmd>>,

    /// Receiver of incoming raft messages
    raft_rx: mpsc::Receiver<PeerTarget<Cmd>>,

    /// Sender for outgoing raft messages
    _raft_tx: mpsc::Sender<PeerTarget<Cmd>>,

    // used to create the ProposalSenders
    proposal_tx: mpsc::Sender<PeerTarget<Cmd>>,
}

impl<Cmd> Consensus<Cmd>
where
    Cmd: Debug + Send + Sync + 'static,
{
    pub fn new(
        raft_rx: mpsc::Receiver<PeerTarget<Cmd>>,
        raft_tx: mpsc::Sender<PeerTarget<Cmd>>,
        proposal_channel_size: usize,
    ) -> Self {
        let (proposal_tx, proposal_rx) = mpsc::channel(proposal_channel_size);

        Self {
            cmd_logs: HashMap::new(),
            proposal_rx,
            raft_rx,
            _raft_tx: raft_tx,
            proposal_tx,
        }
    }

    pub fn create_proposal_sender(&self) -> ProposalSender<PeerTarget<Cmd>> {
        self.proposal_tx.clone()
    }

    pub fn register_state_machines(
        &mut self,
        state_machines: impl IntoIterator<Item = (PeerId, mpsc::Sender<Command<Cmd>>)>,
    ) {
        self.cmd_logs
            .extend(state_machines.into_iter().map(|(peer_id, tx)| {
                (
                    peer_id,
                    CommandLog::new(StateMachineSender::new(peer_id, tx)),
                )
            }));
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let Consensus {
            mut proposal_rx,
            mut cmd_logs,
            mut raft_rx,
            ..
        } = self;

        debug!("Running the consensus driver");

        let mut waiting_for_send_capacity = FuturesUnordered::new();

        for sender in Self::announce_leadership(cmd_logs.values_mut())
            .into_iter()
            .flatten()
        {
            waiting_for_send_capacity.push(sender.reserve_owned())
        }

        loop {
            tokio::select! {
                proposal = proposal_rx.recv() => {
                    trace!(?proposal, "Received proposal");

                    let (target_peer_id, proposal) = proposal.expect("Consensus owns the proposal sender, that's why the receiver should never be closed");

                    let cmd_log = cmd_logs.get_mut(&target_peer_id).expect("Peer id '{target}' is not known. This is a bug.");

                    if let Some(sender) = cmd_log.append_cmd(Command::Apply(proposal)) {
                        waiting_for_send_capacity.push(sender.reserve_owned())
                    }
                },
                raft_msg = raft_rx.recv() => {
                    if let Some((target_peer_id, raft_msg)) = raft_msg {
                        trace!(?raft_msg, "Received raft message");

                        let cmd_log = cmd_logs.get_mut(&target_peer_id).expect("Peer id '{target}' is not known. This is a bug.");

                        if let Some(sender) = cmd_log.append_cmd(Command::Apply(raft_msg)) {
                            waiting_for_send_capacity.push(sender.reserve_owned())
                        }
                    } else {
                        debug!("Shutting consensus down.");
                        break;
                    }
                },
                Some(result) = waiting_for_send_capacity.next() => {
                    let permit = result?;

                    let cmd_log = cmd_logs.get_mut(&permit.peer_id()).expect("Peer id '{target}' is not known. This is a bug.");

                    if let Some(sender) = cmd_log.apply_next_cmd(permit) {
                        waiting_for_send_capacity.push(sender.reserve_owned())
                    }
                }
            }
        }

        Ok(())
    }

    fn announce_leadership<'a>(
        cmd_logs: impl IntoIterator<Item = &'a mut CommandLog<Cmd>> + 'a,
    ) -> impl IntoIterator<Item = Option<StateMachineSender<Command<Cmd>>>> + 'a {
        debug!("Announcing leadership.");

        cmd_logs
            .into_iter()
            .map(|cmd_log| cmd_log.append_cmd(Command::BecomeLeader(1)))
    }
}

#[cfg(test)]
mod tests {
    use crate::{Command, Consensus};
    use restate_test_util::test;
    use tokio::sync::mpsc;

    #[test(tokio::test)]
    async fn unbounded_log() {
        let (raft_in_tx, raft_in_rx) = mpsc::channel(1);
        let (raft_out_tx, _raft_out_rx) = mpsc::channel(1);

        let mut consensus: Consensus<u64> = Consensus::new(raft_in_rx, raft_out_tx, 1);

        let proposal_sender = consensus.create_proposal_sender();

        let (state_machine_tx, mut state_machine_rx) = mpsc::channel::<Command<u64>>(1);
        consensus.register_state_machines(vec![(0, state_machine_tx)]);

        let consensus_handle = tokio::spawn(consensus.run());

        let num_messages = 128;

        for i in 0..num_messages {
            proposal_sender.send((0, i)).await.unwrap();
        }

        assert_eq!(
            state_machine_rx.recv().await.unwrap(),
            Command::BecomeLeader(1)
        );

        for i in 0..num_messages {
            assert_eq!(state_machine_rx.recv().await.unwrap(), Command::Apply(i));
        }

        drop(raft_in_tx);
        consensus_handle.await.unwrap().unwrap()
    }
}
