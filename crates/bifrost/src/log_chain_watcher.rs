// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, VecDeque};

use tokio::sync::{mpsc, oneshot};
use tracing::trace;

use restate_core::{Metadata, MetadataKind, TaskCenter, TaskKind, cancellation_watcher};
use restate_types::logs::{
    LogId,
    metadata::{Chain, Logs, SegmentIndex},
};

// TODO: Remove when all the subscriptions are in place
#[allow(dead_code)]
#[derive(Debug)]
pub enum ChainCondition {
    /// Fires whenever the segment with the given index is considered sealed.
    Sealed(SegmentIndex),
    /// Fires whenever there's a writable segment after the given index.
    WritableSegmentAfter(SegmentIndex),
}

#[derive(Debug)]
pub(crate) struct Subscription {
    log_id: LogId,
    condition: ChainCondition,
    sender: oneshot::Sender<()>,
    #[cfg(test)]
    /// A test only notification for when the subscription request is processed by the watcher.
    armed: Option<oneshot::Sender<()>>,
}

impl Subscription {
    /// Fires the test-only notification signaling that the watcher has processed
    /// this subscription request. No-op outside of tests.
    fn notify_armed(&mut self) {
        #[cfg(test)]
        if let Some(tx) = self.armed.take() {
            let _ = tx.send(());
        }
    }

    fn notify(self) {
        let _ = self.sender.send(());
    }
}

enum SubscriptionHandleInner {
    Waiting(oneshot::Receiver<()>),
    Completed,
}

pub struct SubscriptionHandle(SubscriptionHandleInner);
impl SubscriptionHandle {
    fn new(receiver: oneshot::Receiver<()>) -> Self {
        Self(SubscriptionHandleInner::Waiting(receiver))
    }

    /// Waits for the subscription condition to be satisfied.
    ///
    /// This is cancel-safe: if the returned future is dropped before it resolves,
    /// the subscription is left untouched and a subsequent `wait()` will resume
    /// waiting. Once it has resolved, every later call returns immediately.
    pub async fn wait(&mut self) -> Result<(), oneshot::error::RecvError> {
        match &mut self.0 {
            SubscriptionHandleInner::Waiting(receiver) => {
                let result = std::pin::Pin::new(receiver).await;
                self.0 = SubscriptionHandleInner::Completed;
                result
            }
            SubscriptionHandleInner::Completed => Ok(()),
        }
    }

    #[cfg(test)]
    pub fn try_recv(&mut self) -> Result<(), oneshot::error::TryRecvError> {
        match &mut self.0 {
            SubscriptionHandleInner::Waiting(receiver) => receiver.try_recv(),
            SubscriptionHandleInner::Completed => Ok(()),
        }
    }
}

/// The main and only public interface for the log chain watcher. Allows callers to
/// subscribe to chain events for a particular log.
pub struct LogChainWatcherHandle {
    sender: mpsc::UnboundedSender<Subscription>,
}

impl LogChainWatcherHandle {
    pub fn new(sender: mpsc::UnboundedSender<Subscription>) -> Self {
        Self { sender }
    }

    /// Subscribe to chain events for a particular log.
    ///
    /// Note: If the condition is already satisfied by the time it reaches the watcher, the subscription
    /// will fire immediately without observing a new logs config update.
    #[must_use]
    pub fn subscribe(&self, log_id: LogId, condition: ChainCondition) -> SubscriptionHandle {
        let (sender, receiver) = oneshot::channel();
        let _ = self.sender.send(Subscription {
            log_id,
            condition,
            sender,
            #[cfg(test)]
            armed: None,
        });
        SubscriptionHandle::new(receiver)
    }

    /// Same as `subscribe` but waits until the subscription is accepted by the watcher.
    /// This is a test only util.
    #[cfg(test)]
    #[must_use]
    pub async fn subscribe_and_wait_for_armed(
        &self,
        log_id: LogId,
        condition: ChainCondition,
    ) -> SubscriptionHandle {
        let (armed_tx, armed_rx) = oneshot::channel();
        let (sender, receiver) = oneshot::channel();
        let _ = self.sender.send(Subscription {
            log_id,
            condition,
            sender,
            armed: Some(armed_tx),
        });
        armed_rx.await.unwrap();
        SubscriptionHandle::new(receiver)
    }
}

pub(crate) type LogChainWatcherReceiver = mpsc::UnboundedReceiver<Subscription>;

/// LogChainWatcher is a background task that allows users to subscribe only to particular
/// events that they are interested in on the log chain. This amortizes the cost of subscribing
/// to chain events across all the watchers, and reduces the number of irrelevant wakeups on watchers
/// had they subscribed to the entire logs config instead.
pub(crate) struct LogChainWatcher {
    inbound: LogChainWatcherReceiver,
    subscriptions: HashMap<LogId, VecDeque<Subscription>>,
}

impl LogChainWatcher {
    pub fn start(inbound: LogChainWatcherReceiver) -> anyhow::Result<()> {
        let watcher = Self {
            inbound,
            subscriptions: HashMap::default(),
        };

        TaskCenter::spawn(
            TaskKind::BifrostLogChainWatcher,
            "bifrost-logchainwatcher",
            watcher.run(),
        )?;
        Ok(())
    }

    async fn run(mut self) -> anyhow::Result<()> {
        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);
        trace!("Bifrost log chain watcher started");

        let metadata = Metadata::current();
        let mut logs = metadata.updateable_logs_metadata();

        let mut logs_watcher = metadata.watch(MetadataKind::Logs);

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    return Ok(());
                }
                _ = logs_watcher.changed() => {
                    let logs = logs.live_load();
                    self.on_config_change(logs);
                }
                Some(mut sub) = self.inbound.recv() => {
                    // use it as an opportunity to reap potentially dropped subs
                    self.maybe_reap_dropped_subs();

                    let logs = logs.live_load();
                    let chain = logs.chain(&sub.log_id);
                    match chain {
                        // If the subscription condition is satisfied already, notify immediately.
                        Some(chain) if Self::is_condition_satisfied(&sub, chain) => {
                            sub.notify_armed();
                            sub.notify();
                        }
                        // Either the chain is not yet found, or the condition is not satisfied.
                        _ => {
                            self.register_subscription(sub);
                        }
                    }
                }
            }
        }
    }

    fn register_subscription(&mut self, mut sub: Subscription) {
        sub.notify_armed();
        self.subscriptions
            .entry(sub.log_id)
            .or_default()
            .push_back(sub);
    }

    fn on_config_change(&mut self, logs: &Logs) {
        for (log_id, subs) in self.subscriptions.iter_mut() {
            let Some(chain) = logs.chain(log_id) else {
                // reap dropped subs
                Self::reap_dropped_subs_inner(subs);
                continue;
            };

            let mut idx = 0;
            while idx < subs.len() {
                let sub = &subs[idx];
                if sub.sender.is_closed() {
                    subs.swap_remove_back(idx);
                    continue;
                }
                if Self::is_condition_satisfied(sub, chain) {
                    let sub = subs.swap_remove_back(idx).unwrap();
                    sub.notify();
                } else {
                    idx += 1;
                }
            }
        }
    }

    fn is_condition_satisfied(sub: &Subscription, chain: &Chain) -> bool {
        match sub.condition {
            ChainCondition::Sealed(sealed_segment) => {
                let tail_segment = chain.tail();
                // If the tail segment index is greater than the subscribed to segment index,
                // then the subscribed to segment is for sure sealed.
                if tail_segment.index() > sealed_segment {
                    true
                } else if tail_segment.index() == sealed_segment {
                    tail_segment.config.kind.is_seal_marker()
                } else {
                    false
                }
            }
            ChainCondition::WritableSegmentAfter(idx) => {
                let tail_segment = chain.non_special_tail();
                match tail_segment {
                    Some(tail_segment) => {
                        tail_segment.index() > idx && tail_segment.tail_lsn.is_none()
                    }
                    None => false,
                }
            }
        }
    }

    fn maybe_reap_dropped_subs(&mut self) {
        // probabilistically drop subs that are no longer needed.
        // The main full cleanup happens in on_config_change(). So this
        // is just a fallback in case we have a stable logs config for a while.
        // Cleanup will happen with 5% chance.
        if rand::random_bool(0.95) {
            return;
        }
        self.subscriptions.retain(|_, subs| {
            Self::reap_dropped_subs_inner(subs);
            !subs.is_empty()
        });
    }

    fn reap_dropped_subs_inner(subs: &mut VecDeque<Subscription>) {
        let mut idx = 0;
        while idx < subs.len() {
            if subs[idx].sender.is_closed() {
                subs.swap_remove_back(idx);
            } else {
                idx += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use restate_core::{MetadataWriter, TestCoreEnv};
    use restate_types::logs::Lsn;
    use restate_types::logs::builder::LogsBuilder;
    use restate_types::logs::metadata::{LogletParams, ProviderKind, SealMetadata};

    use super::*;

    async fn update_logs_metadata(
        metadata_writer: &mut MetadataWriter,
        f: impl FnOnce(&mut LogsBuilder),
    ) -> anyhow::Result<()> {
        let mut builder = Metadata::current()
            .updateable_logs_metadata()
            .live_load()
            .clone()
            .try_into_builder()?;
        f(&mut builder);
        metadata_writer.update(Arc::new(builder.build())).await?;
        Ok(())
    }

    async fn init() -> anyhow::Result<(LogChainWatcherHandle, MetadataWriter)> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        LogChainWatcher::start(rx)?;

        Ok((LogChainWatcherHandle::new(tx), env.metadata_writer))
    }

    #[restate_core::test]
    async fn seal_subscription() -> anyhow::Result<()> {
        let (handle, mut metadata_writer) = init().await?;

        // Initially, two segments, and segment 1 is opened.
        update_logs_metadata(&mut metadata_writer, |builder| {
            let idx = builder
                .chain(LogId::new(1))
                .unwrap()
                .append_segment(
                    Lsn::from(1),
                    ProviderKind::InMemory,
                    LogletParams::from("test2"),
                )
                .unwrap();
            assert_eq!(idx, SegmentIndex::from(1));
        })
        .await?;

        // Subscribing to seal event should fire when the chain gets sealed.
        {
            let mut sub = handle
                .subscribe_and_wait_for_armed(
                    LogId::new(1),
                    ChainCondition::Sealed(SegmentIndex::from(1)),
                )
                .await;
            assert_eq!(sub.try_recv(), Err(oneshot::error::TryRecvError::Empty));

            update_logs_metadata(&mut metadata_writer, |builder| {
                builder
                    .chain(LogId::new(1))
                    .unwrap()
                    .seal(Lsn::from(15), &SealMetadata::default())
                    .unwrap();
            })
            .await?;

            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), sub.wait()).await,
                Ok(Ok(()))
            );
        }

        // Subscribing to a seal event on an already sealed segment should fire immediately.
        {
            let mut sub =
                handle.subscribe(LogId::new(1), ChainCondition::Sealed(SegmentIndex::from(1)));

            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), sub.wait()).await,
                Ok(Ok(()))
            );
        }

        // A new writable segment should still consider segment 1 sealed.
        {
            update_logs_metadata(&mut metadata_writer, |builder| {
                let idx = builder
                    .chain(LogId::new(1))
                    .unwrap()
                    .append_segment(
                        Lsn::from(15),
                        ProviderKind::InMemory,
                        LogletParams::from("test2"),
                    )
                    .unwrap();
                assert_eq!(idx, SegmentIndex::from(2));
            })
            .await?;

            let mut sub =
                handle.subscribe(LogId::new(1), ChainCondition::Sealed(SegmentIndex::from(1)));

            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), sub.wait()).await,
                Ok(Ok(()))
            );
        }

        Ok(())
    }

    #[restate_core::test]
    async fn writable_segment_after_subscription() -> anyhow::Result<()> {
        let (handle, mut metadata_writer) = init().await?;

        // Create a chain with a single sealed segment.
        update_logs_metadata(&mut metadata_writer, |builder| {
            let idx = builder
                .chain(LogId::new(1))
                .unwrap()
                .append_segment(
                    Lsn::from(1),
                    ProviderKind::InMemory,
                    LogletParams::from("test2"),
                )
                .unwrap();
            assert_eq!(idx, SegmentIndex::from(1));
            builder
                .chain(LogId::new(1))
                .unwrap()
                .seal(Lsn::from(10), &SealMetadata::default())
                .unwrap();
        })
        .await?;

        // Subscribing to a chain writable event should fire when the chain is writable.
        {
            let mut sub = handle
                .subscribe_and_wait_for_armed(
                    LogId::new(1),
                    ChainCondition::WritableSegmentAfter(SegmentIndex::from(1)),
                )
                .await;
            assert_eq!(sub.try_recv(), Err(oneshot::error::TryRecvError::Empty));

            update_logs_metadata(&mut metadata_writer, |builder| {
                let idx = builder
                    .chain(LogId::new(1))
                    .unwrap()
                    .append_segment(
                        Lsn::from(10),
                        ProviderKind::InMemory,
                        LogletParams::from("test2"),
                    )
                    .unwrap();
                assert_eq!(idx, SegmentIndex::from(2));
            })
            .await?;

            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), sub.wait()).await,
                Ok(Ok(()))
            );
        }

        // Subscribing to a chain writable event on an already writable segment should fire immediately.
        {
            let mut sub = handle.subscribe(
                LogId::new(1),
                ChainCondition::WritableSegmentAfter(SegmentIndex::from(1)),
            );

            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), sub.wait()).await,
                Ok(Ok(()))
            );
        }

        Ok(())
    }

    #[restate_core::test]
    async fn non_existent_log_sub() -> anyhow::Result<()> {
        let (handle, mut metadata_writer) = init().await?;

        let mut sub = handle
            .subscribe_and_wait_for_armed(
                LogId::new(1000),
                ChainCondition::WritableSegmentAfter(SegmentIndex::from(0)),
            )
            .await;
        assert_eq!(sub.try_recv(), Err(oneshot::error::TryRecvError::Empty));

        update_logs_metadata(&mut metadata_writer, |builder| {
            builder
                .add_log(
                    LogId::new(1000),
                    Chain::new(ProviderKind::InMemory, LogletParams::from("test2")),
                )
                .unwrap();

            let idx = builder
                .chain(LogId::new(1000))
                .unwrap()
                .append_segment(
                    Lsn::from(1),
                    ProviderKind::InMemory,
                    LogletParams::from("test2"),
                )
                .unwrap();
            assert_eq!(idx, SegmentIndex::from(1));
        })
        .await?;

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), sub.wait()).await,
            Ok(Ok(()))
        );

        Ok(())
    }

    #[restate_core::test]
    async fn dropped_subscriptions() -> anyhow::Result<()> {
        let (handle, mut metadata_writer) = init().await?;

        // Two subscriptions, one is dropped while the other is still active.
        let mut sub1 = handle
            .subscribe_and_wait_for_armed(
                LogId::new(1),
                ChainCondition::Sealed(SegmentIndex::from(1)),
            )
            .await;
        assert_eq!(sub1.try_recv(), Err(oneshot::error::TryRecvError::Empty));

        let mut sub2 = handle
            .subscribe_and_wait_for_armed(
                LogId::new(1),
                ChainCondition::Sealed(SegmentIndex::from(1)),
            )
            .await;
        assert_eq!(sub2.try_recv(), Err(oneshot::error::TryRecvError::Empty));

        // Drop sub1
        drop(sub1);

        update_logs_metadata(&mut metadata_writer, |builder| {
            builder
                .chain(LogId::new(1))
                .unwrap()
                .append_segment(
                    Lsn::from(1),
                    ProviderKind::InMemory,
                    LogletParams::from("test2"),
                )
                .unwrap();

            builder
                .chain(LogId::new(1))
                .unwrap()
                .seal(Lsn::from(10), &SealMetadata::default())
                .unwrap();
        })
        .await?;

        // sub2 should fire as expected, with no problems from the dropped sub1.
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), sub2.wait()).await,
            Ok(Ok(()))
        );

        Ok(())
    }

    #[restate_core::test]
    async fn multiple_subs() -> anyhow::Result<()> {
        let (handle, mut metadata_writer) = init().await?;

        // Initial chain state:
        // log-1 -> 2 segments, segment 1 is open
        // log-2 -> 1 sealed segment
        update_logs_metadata(&mut metadata_writer, |builder| {
            let idx = builder
                .chain(LogId::new(1))
                .unwrap()
                .append_segment(
                    Lsn::from(1),
                    ProviderKind::InMemory,
                    LogletParams::from("test2"),
                )
                .unwrap();
            assert_eq!(idx, SegmentIndex::from(1));
            let idx = builder
                .chain(LogId::new(1))
                .unwrap()
                .append_segment(
                    Lsn::from(1),
                    ProviderKind::InMemory,
                    LogletParams::from("test2"),
                )
                .unwrap();
            assert_eq!(idx, SegmentIndex::from(2));

            builder
                .chain(LogId::new(2))
                .unwrap()
                .seal(Lsn::from(10), &SealMetadata::default())
                .unwrap();
        })
        .await?;

        // Sub 1 is waiting for segment 2 of log-1 to be sealed
        let mut log1_sub = handle
            .subscribe_and_wait_for_armed(
                LogId::new(1),
                ChainCondition::Sealed(SegmentIndex::from(2)),
            )
            .await;

        // Sub 2 is waiting for the chain of log-2 to be writable
        let mut log2_sub = handle
            .subscribe_and_wait_for_armed(
                LogId::new(2),
                ChainCondition::WritableSegmentAfter(SegmentIndex::from(0)),
            )
            .await;

        assert_eq!(
            log1_sub.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        );
        assert_eq!(
            log2_sub.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        );

        // Seal segment 2 of log-1
        update_logs_metadata(&mut metadata_writer, |builder| {
            builder
                .chain(LogId::new(1))
                .unwrap()
                .seal(Lsn::from(10), &SealMetadata::default())
                .unwrap();
        })
        .await?;

        // Sub 1 is notified of the sealed segment
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), log1_sub.wait()).await,
            Ok(Ok(()))
        );
        assert_eq!(
            log2_sub.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        );

        // Add a writable segment to log-2
        update_logs_metadata(&mut metadata_writer, |builder| {
            builder
                .chain(LogId::new(2))
                .unwrap()
                .append_segment(
                    Lsn::from(10),
                    ProviderKind::InMemory,
                    LogletParams::from("test2"),
                )
                .unwrap();
        })
        .await?;

        // Sub 2 is notified of the writable segment
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), log2_sub.wait()).await,
            Ok(Ok(()))
        );

        Ok(())
    }
}
