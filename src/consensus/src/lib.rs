use futures::{SinkExt, Stream, StreamExt};
use futures_sink::Sink;
use std::fmt::Debug;
use std::marker::PhantomData;
use tracing::{debug, info};

#[derive(Debug)]
pub enum Command<T> {
    Commit(T),
    CreateSnapshot,
    ApplySnapshot,
    Leader,
    Follower,
}

#[derive(Debug)]
pub struct Consensus<CmdOut, FsmCmd, PropIn, NetIn, NetOut>
where
    CmdOut: Sink<Command<FsmCmd>>,
{
    command_out: CmdOut,
    proposal_in: PropIn,
    raft_in: NetIn,
    _raft_out: NetOut,
    phantom_data: PhantomData<FsmCmd>,
}

impl<CmdOut, FsmCmd, PropIn, RaftIn, RaftOut> Consensus<CmdOut, FsmCmd, PropIn, RaftIn, RaftOut>
where
    CmdOut: Sink<Command<FsmCmd>>,
    PropIn: Stream<Item = FsmCmd>,
    FsmCmd: Debug,
    RaftIn: Stream<Item = FsmCmd>,
    RaftOut: Sink<FsmCmd>,
{
    pub fn build(
        command_out: CmdOut,
        proposal_in: PropIn,
        raft_in: RaftIn,
        raft_out: RaftOut,
    ) -> Self {
        Self {
            command_out,
            proposal_in,
            raft_in,
            _raft_out: raft_out,
            phantom_data: PhantomData::default(),
        }
    }

    pub async fn run(self, drain: drain::Watch) {
        let Consensus {
            proposal_in,
            command_out,
            raft_in,
            ..
        } = self;

        info!("Running the consensus driver.");

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);
        tokio::pin!(proposal_in);
        tokio::pin!(command_out);
        tokio::pin!(raft_in);

        loop {
            tokio::select! {
                proposal = proposal_in.next() => {
                    debug!(?proposal, "Received proposal");

                    if let Some(proposal) = proposal {
                        // TODO: Introduce safe_unwrap call
                        let _ = command_out.send(Command::Commit(proposal)).await;
                    }
                },
                raft_msg = raft_in.next() => {
                    if let Some(raft_msg) = raft_msg {
                        // TODO: Introduce safe_unwrap call
                        let _ = command_out.send(Command::Commit(raft_msg)).await;
                    }
                }
                _ = &mut shutdown => {
                    debug!("Shutting down Consensus.");
                    break;
                }
            }
        }
    }
}
