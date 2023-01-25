use common::types::PeerId;
use futures::{SinkExt, Stream, StreamExt};
use futures_sink::Sink;
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::sync::mpsc;
use tokio_util::sync::PollSender;
use tracing::{debug, info};

/// Consensus commands that are sent to an associated state machine.
#[derive(Debug)]
pub enum Command<T> {
    /// Apply the given message to the state machine.
    Apply(T),
    CreateSnapshot,
    ApplySnapshot,
    BecomeLeader,
    BecomeFollower,
}

pub type ProposalSender<T> = PollSender<T>;

/// Wrapper that extends a message with its target peer to which the message should be sent.
pub type Targeted<Msg> = (PeerId, Msg);

/// Component which is responsible for running the consensus algorithm for multiple replicated
/// state machines. Consensus replicates messages of type `Cmd`.
#[derive(Debug)]
pub struct Consensus<Cmd, SmSink, RaftIn, RaftOut> {
    /// Sinks to send replicated commands to the associated state machines
    state_machines: HashMap<PeerId, SmSink>,

    /// Receiver of proposals from all associated state machines
    proposal_rx: mpsc::Receiver<Targeted<Cmd>>,

    /// Receiver of incoming raft messages
    raft_in: RaftIn,

    /// Sender for outgoing raft messages
    _raft_out: RaftOut,

    // used to create the ProposalSenders
    proposal_tx: mpsc::Sender<Targeted<Cmd>>,
}

impl<Cmd, SmSink, RaftIn, RaftOut> Consensus<Cmd, SmSink, RaftIn, RaftOut>
where
    Cmd: Send + Debug + 'static,
    SmSink: Sink<Command<Cmd>> + Unpin,
    SmSink::Error: Debug,
    RaftIn: Stream<Item = Targeted<Cmd>>,
    RaftOut: Sink<Targeted<Cmd>>,
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

    pub fn create_proposal_sender(&self) -> ProposalSender<Targeted<Cmd>> {
        PollSender::new(self.proposal_tx.clone())
    }

    pub fn register_state_machines(
        &mut self,
        state_machines: impl IntoIterator<Item = (PeerId, SmSink)>,
    ) {
        self.state_machines.extend(state_machines);
    }

    pub async fn run(self) {
        let Consensus {
            mut proposal_rx,
            mut state_machines,
            raft_in,
            ..
        } = self;

        info!("Running the consensus driver.");

        tokio::pin!(raft_in);

        loop {
            tokio::select! {
                proposal = proposal_rx.recv() => {
                    debug!(?proposal, "Received proposal");

                    let (target, proposal) = proposal.expect("Consensus owns the proposal sender, that's why the receiver should never be closed.");

                    if let Some(state_machine) = state_machines.get_mut(&target) {
                        state_machine.send(Command::Apply(proposal)).await.expect("The state machine should exist as long as Consensus exists.");
                    }
                },
                raft_msg = raft_in.next() => {
                    if let Some((target, raft_msg)) = raft_msg {

                        if let Some(state_machine) = state_machines.get_mut(&target) {
                            state_machine.send(Command::Apply(raft_msg)).await.expect("The state machine should exist as long as Consensus exists.");
                        }
                    } else {
                        debug!("Shutting consensus down.");
                        break;
                    }
                }
            }
        }
    }
}
