use common::types::{LeaderEpoch, PeerId, PeerTarget};
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::sync::mpsc;
use tracing::{debug, info};

/// Consensus commands that are sent to an associated state machine.
#[derive(Debug)]
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
    /// Sinks to send replicated commands to the associated state machines
    state_machines: HashMap<PeerId, mpsc::Sender<Command<Cmd>>>,

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
    ) -> Self {
        let (proposal_tx, proposal_rx) = mpsc::channel(64);

        Self {
            state_machines: HashMap::new(),
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
        self.state_machines.extend(state_machines);
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let Consensus {
            mut proposal_rx,
            mut state_machines,
            mut raft_rx,
            ..
        } = self;

        info!("Running the consensus driver.");

        Self::announce_leadership(state_machines.values_mut()).await?;

        loop {
            tokio::select! {
                proposal = proposal_rx.recv() => {
                    debug!(?proposal, "Received proposal");

                    let (target, proposal) = proposal.expect("Consensus owns the proposal sender, that's why the receiver should never be closed");

                    if let Some(state_machine) = state_machines.get_mut(&target) {
                        state_machine.send(Command::Apply(proposal)).await?;
                    }
                },
                raft_msg = raft_rx.recv() => {
                    if let Some((target, raft_msg)) = raft_msg {

                        if let Some(state_machine) = state_machines.get_mut(&target) {
                            state_machine.send(Command::Apply(raft_msg)).await?;
                        }
                    } else {
                        debug!("Shutting consensus down.");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn announce_leadership(
        state_machines: impl IntoIterator<Item = &mut mpsc::Sender<Command<Cmd>>>,
    ) -> anyhow::Result<()> {
        debug!("Announcing leadership.");
        for sink in state_machines {
            sink.send(Command::BecomeLeader(1)).await?
        }

        Ok(())
    }
}
