use futures::{SinkExt, Stream, StreamExt};
use futures_sink::Sink;
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

#[derive(Debug)]
pub struct Consensus<FsmCmd, CmdOut, NetIn, NetOut>
where
    CmdOut: Sink<Command<FsmCmd>>,
{
    command_senders: Vec<CmdOut>,
    proposal_in_rx: mpsc::Receiver<FsmCmd>,
    raft_in: NetIn,
    _raft_out: NetOut,

    // used to create the ProposalSenders
    proposal_in_tx: mpsc::Sender<FsmCmd>,

    phantom_data: PhantomData<FsmCmd>,
}

impl<FsmCmd, CmdOut, RaftIn, RaftOut> Consensus<FsmCmd, CmdOut, RaftIn, RaftOut>
where
    CmdOut: Sink<Command<FsmCmd>> + Unpin,
    FsmCmd: Send + Debug + 'static,
    RaftIn: Stream<Item = FsmCmd>,
    RaftOut: Sink<FsmCmd>,
{
    pub fn build(raft_in: RaftIn, raft_out: RaftOut) -> Self {
        let (proposal_in_tx, proposal_in_rx) = mpsc::channel(64);

        Self {
            command_senders: Vec::new(),
            proposal_in_rx,
            raft_in,
            _raft_out: raft_out,
            proposal_in_tx,
            phantom_data: PhantomData::default(),
        }
    }

    pub fn create_proposal_sender(&self) -> ProposalSender<FsmCmd> {
        PollSender::new(self.proposal_in_tx.clone())
    }

    pub fn register_command_senders(&mut self, command_senders: Vec<CmdOut>) {
        self.command_senders.extend(command_senders);
    }

    pub async fn run(self) {
        let Consensus {
            mut proposal_in_rx,
            mut command_senders,
            raft_in,
            ..
        } = self;

        info!("Running the consensus driver.");

        tokio::pin!(raft_in);

        loop {
            tokio::select! {
                proposal = proposal_in_rx.recv() => {
                    debug!(?proposal, "Received proposal");

                    // TODO: Introduce safe_unwrap call
                    let _ = command_senders[0].send(Command::Commit(proposal.unwrap())).await;
                },
                raft_msg = raft_in.next() => {
                    if let Some(raft_msg) = raft_msg {
                        // TODO: Introduce safe_unwrap call
                        let _ = command_senders[0].send(Command::Commit(raft_msg)).await;
                    } else {
                        debug!("Shutting consensus down.");
                        break;
                    }
                }
            }
        }
    }
}
