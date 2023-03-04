use common::types::{LeaderEpoch, PeerId, PeerTarget};
use futures::{SinkExt, Stream, StreamExt};
use futures_sink::Sink;
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
pub struct Consensus<Cmd, SmSink, RaftIn, RaftOut> {
    /// Sinks to send replicated commands to the associated state machines
    state_machines: HashMap<PeerId, SmSink>,

    /// Receiver of proposals from all associated state machines
    proposal_rx: mpsc::Receiver<PeerTarget<Cmd>>,

    /// Receiver of incoming raft messages
    raft_in: RaftIn,

    /// Sender for outgoing raft messages
    _raft_out: RaftOut,

    // used to create the ProposalSenders
    proposal_tx: mpsc::Sender<PeerTarget<Cmd>>,
}

impl<Cmd, SmSink, RaftIn, RaftOut> Consensus<Cmd, SmSink, RaftIn, RaftOut>
where
    Cmd: Send + Debug + 'static,
    SmSink: Sink<Command<Cmd>> + Unpin,
    SmSink::Error: std::error::Error + Send + Sync + 'static,
    RaftIn: Stream<Item = PeerTarget<Cmd>>,
    RaftOut: Sink<PeerTarget<Cmd>>,
{
    pub fn new(raft_in: RaftIn, raft_out: RaftOut) -> Self {
        let (proposal_tx, proposal_rx) = mpsc::channel(64);

        Self {
            state_machines: HashMap::new(),
            proposal_rx,
            raft_in,
            _raft_out: raft_out,
            proposal_tx,
        }
    }

    pub fn create_proposal_sender(&self) -> ProposalSender<PeerTarget<Cmd>> {
        self.proposal_tx.clone()
    }

    pub fn register_state_machines(
        &mut self,
        state_machines: impl IntoIterator<Item = (PeerId, SmSink)>,
    ) {
        self.state_machines.extend(state_machines);
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let Consensus {
            mut proposal_rx,
            mut state_machines,
            raft_in,
            ..
        } = self;

        info!("Running the consensus driver.");

        Self::announce_leadership(&mut state_machines).await?;

        tokio::pin!(raft_in);

        loop {
            tokio::select! {
                proposal = proposal_rx.recv() => {
                    debug!(?proposal, "Received proposal");

                    let (target, proposal) = proposal.expect("Consensus owns the proposal sender, that's why the receiver should never be closed");

                    if let Some(state_machine) = state_machines.get_mut(&target) {
                        state_machine.send(Command::Apply(proposal)).await?;
                    }
                },
                raft_msg = raft_in.next() => {
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
        state_machines: &mut HashMap<PeerId, SmSink>,
    ) -> anyhow::Result<()> {
        debug!("Announcing leadership.");
        for sink in state_machines.values_mut() {
            sink.send(Command::BecomeLeader(1)).await?
        }

        Ok(())
    }
}
