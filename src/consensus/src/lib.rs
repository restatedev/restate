use common::types::PeerId;
use futures::{SinkExt, Stream, StreamExt};
use futures_sink::Sink;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use tokio::sync::mpsc;
use tokio_util::sync::PollSender;
use tracing::{debug, info};

#[derive(Debug)]
pub enum Command<T> {
    Commit(T),
    CreateSnapshot,
    ApplySnapshot,
    Leader,
    Follower,
}

pub type ProposalSender<T> = PollSender<T>;
pub type Targeted<T> = (PeerId, T);

#[derive(Debug)]
pub struct Consensus<FsmCmd, CmdOut, RaftIn, RaftOut>
where
    CmdOut: Sink<Command<FsmCmd>>,
{
    command_senders: HashMap<PeerId, CmdOut>,
    proposal_rx: mpsc::Receiver<Targeted<FsmCmd>>,
    raft_in: RaftIn,
    _raft_out: RaftOut,

    // used to create the ProposalSenders
    proposal_tx: mpsc::Sender<Targeted<FsmCmd>>,

    phantom_data: PhantomData<FsmCmd>,
}

impl<FsmCmd, CmdOut, RaftIn, RaftOut> Consensus<FsmCmd, CmdOut, RaftIn, RaftOut>
where
    CmdOut: Sink<Command<FsmCmd>> + Unpin,
    CmdOut::Error: Debug,
    FsmCmd: Send + Debug + 'static,
    RaftIn: Stream<Item = Targeted<FsmCmd>>,
    RaftOut: Sink<Targeted<FsmCmd>>,
{
    pub fn build(raft_in: RaftIn, raft_out: RaftOut) -> Self {
        let (proposal_tx, proposal_rx) = mpsc::channel(64);

        Self {
            command_senders: HashMap::new(),
            proposal_rx,
            raft_in,
            _raft_out: raft_out,
            proposal_tx,
            phantom_data: PhantomData::default(),
        }
    }

    pub fn create_proposal_sender(&self) -> ProposalSender<Targeted<FsmCmd>> {
        PollSender::new(self.proposal_tx.clone())
    }

    pub fn register_command_senders(
        &mut self,
        command_senders: impl IntoIterator<Item = (PeerId, CmdOut)>,
    ) {
        self.command_senders.extend(command_senders);
    }

    pub async fn run(self) {
        let Consensus {
            mut proposal_rx,
            mut command_senders,
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

                    if let Some(command_sender) = command_senders.get_mut(&target) {
                        command_sender.send(Command::Commit(proposal)).await.expect("The command receiver should exist as long as Consensus exists.");
                    }
                },
                raft_msg = raft_in.next() => {
                    if let Some((target, raft_msg)) = raft_msg {

                        if let Some(command_sender) = command_senders.get_mut(&target) {
                            command_sender.send(Command::Commit(raft_msg)).await.expect("The command receiver should exist as long as Consensus exists.");
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
