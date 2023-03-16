use crate::{Input, Output, TimerHandle};
use common::types::PartitionLeaderEpoch;
use std::collections::HashMap;
use std::future;
use tokio::sync::mpsc;
use tracing::debug;

#[derive(Debug)]
pub struct Service {
    input_rx: mpsc::Receiver<Input>,

    // used to create the timer handle
    input_tx: mpsc::Sender<Input>,
}

impl Default for Service {
    fn default() -> Self {
        Self::new()
    }
}

impl Service {
    pub fn new() -> Self {
        let (input_tx, input_rx) = mpsc::channel(64);

        Self { input_rx, input_tx }
    }

    pub fn create_timer_handle(&self) -> TimerHandle {
        TimerHandle::new(self.input_tx.clone())
    }

    pub async fn run(self, drain: drain::Watch) {
        debug!("Running the timer service.");

        let Self { mut input_rx, .. } = self;

        let shutdown_signal = drain.signaled();
        tokio::pin!(shutdown_signal);

        let mut timer_logic = TimerLogic::new();

        loop {
            tokio::select! {
                Some(input) = input_rx.recv() => {
                    match input {
                        Input::Register { partition_leader_epoch, output_tx } => {
                            timer_logic.register_listener(partition_leader_epoch, output_tx);
                        },
                        Input::Unregister(partition_leader_epoch) => {
                            timer_logic.unregister_listener(&partition_leader_epoch);
                        }
                    }
                },
                _ = timer_logic.run() => {},
                _ = &mut shutdown_signal => {
                    break;
                }
            }
        }

        debug!("Shut down the timer service.");
    }
}

struct TimerLogic {
    listeners: HashMap<PartitionLeaderEpoch, mpsc::Sender<Output>>,
}

impl TimerLogic {
    fn new() -> Self {
        Self {
            listeners: Default::default(),
        }
    }

    fn register_listener(
        &mut self,
        partition_leader_epoch: PartitionLeaderEpoch,
        output_tx: mpsc::Sender<Output>,
    ) {
        self.listeners.insert(partition_leader_epoch, output_tx);
    }

    fn unregister_listener(&mut self, partition_leader_epoch: &PartitionLeaderEpoch) {
        self.listeners.remove(partition_leader_epoch);
    }

    async fn run(&mut self) {
        future::pending().await
    }
}
