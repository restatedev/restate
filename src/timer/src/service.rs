use std::collections::HashMap;
use std::time::SystemTime;

use common::types::PartitionLeaderEpoch;
use timer_queue::TimerQueue;
use tokio::sync::mpsc;
use tracing::debug;

use crate::{Input, Output, TimerHandle};

#[derive(Debug)]
pub struct Service<T> {
    input_rx: mpsc::Receiver<Input<T>>,

    // used to create the timer handle
    input_tx: mpsc::Sender<Input<T>>,
}

impl<T> Default for Service<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Service<T> {
    pub fn new() -> Self {
        let (input_tx, input_rx) = mpsc::channel(64);

        Self { input_rx, input_tx }
    }

    pub fn create_timer_handle(&self) -> TimerHandle<T> {
        TimerHandle::new(self.input_tx.clone())
    }

    pub async fn run(self, drain: drain::Watch) {
        debug!("Running the timer service.");

        let Self { mut input_rx, .. } = self;

        let shutdown_signal = drain.signaled();
        tokio::pin!(shutdown_signal);

        let mut timer_logic = TimerLogic::<T>::new();

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
                        Input::Timer { partition_leader_epoch, wake_up_time, payload } => {
                            timer_logic.register_timer(partition_leader_epoch, wake_up_time, payload);
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

struct TimerLogic<T> {
    listeners: HashMap<PartitionLeaderEpoch, mpsc::Sender<Output<T>>>,
    timer_queue: TimerQueue<(PartitionLeaderEpoch, T)>,
}

impl<T> TimerLogic<T> {
    fn new() -> Self {
        Self {
            listeners: Default::default(),
            timer_queue: TimerQueue::new(),
        }
    }

    fn register_listener(
        &mut self,
        partition_leader_epoch: PartitionLeaderEpoch,
        output_tx: mpsc::Sender<Output<T>>,
    ) {
        self.listeners.insert(partition_leader_epoch, output_tx);
    }

    fn unregister_listener(&mut self, partition_leader_epoch: &PartitionLeaderEpoch) {
        self.listeners.remove(partition_leader_epoch);
    }

    fn register_timer(
        &mut self,
        partition_leader_epoch: PartitionLeaderEpoch,
        wake_up_time: SystemTime,
        payload: T,
    ) {
        self.timer_queue
            .sleep_until(wake_up_time, (partition_leader_epoch, payload));
    }

    async fn run(&mut self) {
        let (partition_leader_epoch, payload) = self.timer_queue.await_timer().await.into_inner();

        if let Some(listener) = self.listeners.get(&partition_leader_epoch) {
            // listener might no longer be running, this can happen in case of a leadership change
            let _ = listener.send(Output::TimerFired(payload)).await;
        }
    }
}
