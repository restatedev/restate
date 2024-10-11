// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use tracing::{debug, info};

use restate_core::network::{Networking, TransportConnect};
use restate_core::task_center;
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::{
    KeyFilter, LogId, LogletOffset, Record, RecordCache, SequenceNumber, TailState,
};
use restate_types::replicated_loglet::ReplicatedLogletParams;

use crate::loglet::util::TailOffsetWatch;
use crate::loglet::{Loglet, LogletCommit, OperationError, SendableLogletReadStream};
use crate::providers::replicated_loglet::replication::spread_selector::SelectorStrategy;
use crate::providers::replicated_loglet::sequencer::Sequencer;
use crate::providers::replicated_loglet::tasks::{FindTailTask, SealTask};

use super::error::ReplicatedLogletError;
use super::log_server_manager::RemoteLogServerManager;
use super::metric_definitions::{BIFROST_RECORDS_ENQUEUED_BYTES, BIFROST_RECORDS_ENQUEUED_TOTAL};
use super::read_path::{ReadStreamTask, ReplicatedLogletReadStream};
use super::remote_sequencer::RemoteSequencer;
use super::rpc_routers::{LogServersRpc, SequencersRpc};
use super::tasks::{CheckSealOutcome, CheckSealTask, FindTailResult};

#[derive(derive_more::Debug)]
pub(super) struct ReplicatedLoglet<T> {
    my_params: ReplicatedLogletParams,
    #[debug(skip)]
    networking: Networking<T>,
    #[debug(skip)]
    logservers_rpc: LogServersRpc,
    #[debug(skip)]
    record_cache: RecordCache,
    /// A shared watch for the last known global tail of the loglet.
    /// Note that this comes with a few caveats:
    /// - On startup, this defaults to `Open(OLDEST)`
    /// - find_tail() should use this value iff we have a local sequencer for all other cases, we
    ///   should run a proper tail search.
    known_global_tail: TailOffsetWatch,
    sequencer: SequencerAccess<T>,
}

impl<T: TransportConnect> ReplicatedLoglet<T> {
    pub fn new(
        log_id: LogId,
        segment_index: SegmentIndex,
        my_params: ReplicatedLogletParams,
        networking: Networking<T>,
        logservers_rpc: LogServersRpc,
        sequencers_rpc: &SequencersRpc,
        record_cache: RecordCache,
    ) -> Self {
        let known_global_tail = TailOffsetWatch::new(TailState::Open(LogletOffset::OLDEST));
        let log_server_manager =
            RemoteLogServerManager::new(my_params.loglet_id, &my_params.nodeset);

        let sequencer = if networking.my_node_id() == my_params.sequencer {
            debug!(
                loglet_id = %my_params.loglet_id,
                "We are the sequencer node for this loglet"
            );
            // todo(asoli): Potentially configurable or controllable in tests either in
            // ReplicatedLogletParams or in the config file.
            let selector_strategy = SelectorStrategy::Flood;

            SequencerAccess::Local {
                handle: Sequencer::new(
                    my_params.clone(),
                    selector_strategy,
                    networking.clone(),
                    logservers_rpc.store.clone(),
                    log_server_manager.clone(),
                    record_cache.clone(),
                    known_global_tail.clone(),
                ),
            }
        } else {
            SequencerAccess::Remote {
                handle: RemoteSequencer::new(
                    log_id,
                    segment_index,
                    my_params.clone(),
                    networking.clone(),
                    known_global_tail.clone(),
                    sequencers_rpc.clone(),
                ),
            }
        };
        Self {
            my_params,
            networking,
            logservers_rpc,
            record_cache,
            known_global_tail,
            sequencer,
        }
    }

    pub(crate) fn params(&self) -> &ReplicatedLogletParams {
        &self.my_params
    }

    pub(crate) fn is_sequencer_local(&self) -> bool {
        matches!(self.sequencer, SequencerAccess::Local { .. })
    }

    pub(crate) fn known_global_tail(&self) -> &TailOffsetWatch {
        &self.known_global_tail
    }
}

#[derive(derive_more::Debug, derive_more::IsVariant)]
pub enum SequencerAccess<T> {
    /// The sequencer is remote (or retired/preempted)
    #[debug("Remote")]
    Remote { handle: RemoteSequencer<T> },
    /// We are the loglet leaders
    #[debug("Local")]
    Local { handle: Sequencer<T> },
}

#[async_trait]
impl<T: TransportConnect> Loglet for ReplicatedLoglet<T> {
    async fn create_read_stream(
        self: Arc<Self>,
        filter: KeyFilter,
        from: LogletOffset,
        to: Option<LogletOffset>,
    ) -> Result<SendableLogletReadStream, OperationError> {
        let cache = self.record_cache.clone();
        let known_global_tail = self.known_global_tail.clone();
        let my_params = self.my_params.clone();
        let networking = self.networking.clone();
        let logservers_rpc = self.logservers_rpc.clone();

        let (rx_stream, reader_task) = ReadStreamTask::start(
            my_params,
            networking,
            logservers_rpc,
            filter,
            from,
            to,
            known_global_tail,
            cache,
            false,
        )
        .await?;
        let read_stream = ReplicatedLogletReadStream::new(from, rx_stream, reader_task);

        Ok(Box::pin(read_stream))
    }

    fn watch_tail(&self) -> BoxStream<'static, TailState<LogletOffset>> {
        // It's acceptable for watch_tail to return an outdated value in the beginning,
        // but if the loglet is unsealed, we need to ensure that we have a mechanism to update
        // this value if we don't have a local sequencer.
        Box::pin(self.known_global_tail.to_stream())
    }

    async fn enqueue_batch(&self, payloads: Arc<[Record]>) -> Result<LogletCommit, OperationError> {
        metrics::counter!(BIFROST_RECORDS_ENQUEUED_TOTAL).increment(payloads.len() as u64);
        metrics::counter!(BIFROST_RECORDS_ENQUEUED_BYTES).increment(
            payloads
                .iter()
                .map(|r| r.estimated_encode_size())
                .sum::<usize>() as u64,
        );

        match self.sequencer {
            SequencerAccess::Local { ref handle } => handle.enqueue_batch(payloads).await,
            SequencerAccess::Remote { ref handle } => handle.append(payloads).await,
        }
    }

    async fn find_tail(&self) -> Result<TailState<LogletOffset>, OperationError> {
        match self.sequencer {
            SequencerAccess::Local { .. } => {
                let latest_tail = *self.known_global_tail.get();
                if latest_tail.is_sealed() {
                    return Ok(latest_tail);
                }
                // We might have been sealed by external node and the sequencer is unaware. In this
                // case, we run the a check seal task to determine if we suspect that sealing is
                // happening.
                let result = CheckSealTask::run(
                    &self.my_params,
                    &self.logservers_rpc.get_loglet_info,
                    &self.known_global_tail,
                    &self.networking,
                )
                .await?;
                if result == CheckSealOutcome::Sealing {
                    // We are likely to be sealing...
                    // let's fire a seal to ensure this seal is complete
                    if self.seal().await.is_ok() {
                        self.known_global_tail.notify_seal();
                    }
                }
                return Ok(*self.known_global_tail.get());
            }
            SequencerAccess::Remote { .. } => {
                let task = FindTailTask::new(
                    task_center(),
                    self.my_params.clone(),
                    self.networking.clone(),
                    self.logservers_rpc.clone(),
                    self.known_global_tail.clone(),
                    self.record_cache.clone(),
                );
                let tail_status = task.run().await;
                match tail_status {
                    FindTailResult::Open { global_tail } => {
                        self.known_global_tail.notify_offset_update(global_tail);
                        Ok(*self.known_global_tail.get())
                    }
                    FindTailResult::Sealed { global_tail } => {
                        self.known_global_tail.notify(true, global_tail);
                        Ok(*self.known_global_tail.get())
                    }
                    FindTailResult::Error(reason) => {
                        Err(ReplicatedLogletError::FindTailFailed(reason).into())
                    }
                }
            }
        }
    }

    async fn get_trim_point(&self) -> Result<Option<LogletOffset>, OperationError> {
        // todo(asoli): Implement trim
        Ok(None)
    }

    /// Trim the log to the minimum of new_trim_point and last_committed_offset
    /// new_trim_point is inclusive (will be trimmed)
    async fn trim(&self, _new_trim_point: LogletOffset) -> Result<(), OperationError> {
        todo!()
    }

    async fn seal(&self) -> Result<(), OperationError> {
        // todo(asoli): If we are the sequencer node, let the sequencer know.
        let _ = SealTask::new(
            task_center(),
            self.my_params.clone(),
            self.logservers_rpc.seal.clone(),
            self.known_global_tail.clone(),
        )
        .run(self.networking.clone())
        .await?;
        info!(loglet_id=%self.my_params.loglet_id, "Loglet has been sealed successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU8;

    use super::*;

    use googletest::prelude::*;
    use test_log::test;

    use restate_core::TestCoreEnvBuilder;
    use restate_log_server::LogServerService;
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::Configuration;
    use restate_types::live::Live;
    use restate_types::logs::Keys;
    use restate_types::replicated_loglet::{NodeSet, ReplicatedLogletId, ReplicationProperty};
    use restate_types::{GenerationalNodeId, PlainNodeId};

    use crate::loglet::{AppendError, Loglet};

    struct TestEnv {
        pub loglet: Arc<dyn Loglet>,
        pub record_cache: RecordCache,
    }

    async fn run_in_test_env<F, O>(
        loglet_params: ReplicatedLogletParams,
        mut future: F,
    ) -> googletest::Result<()>
    where
        F: FnMut(TestEnv) -> O,
        O: std::future::Future<Output = googletest::Result<()>>,
    {
        let config = Live::from_value(Configuration::default());

        let mut node_env =
            TestCoreEnvBuilder::with_incoming_only_connector().add_mock_nodes_config();

        let logserver_rpc = LogServersRpc::new(&mut node_env.router_builder);
        let sequencer_rpc = SequencersRpc::new(&mut node_env.router_builder);
        let record_cache = RecordCache::new(1_000_000);

        let log_server = LogServerService::create(
            config.clone(),
            node_env.tc.clone(),
            node_env.metadata.clone(),
            node_env.metadata_store_client.clone(),
            record_cache.clone(),
            &mut node_env.router_builder,
        )
        .await?;

        let node_env = node_env.build().await;

        node_env
            .tc
            .clone()
            .run_in_scope("test", None, async {
                RocksDbManager::init(config.clone().map(|c| &c.common));

                log_server
                    .start(node_env.metadata_writer.clone())
                    .await
                    .into_test_result()?;

                let loglet = Arc::new(ReplicatedLoglet::new(
                    LogId::new(1),
                    SegmentIndex::from(1),
                    loglet_params,
                    node_env.networking.clone(),
                    logserver_rpc,
                    &sequencer_rpc,
                    record_cache.clone(),
                ));

                let env = TestEnv {
                    loglet,
                    record_cache,
                };

                future(env).await
            })
            .await?;
        node_env.tc.shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    // ** Single-node replicated-loglet smoke tests **
    #[test(tokio::test(start_paused = true))]
    async fn test_append_local_sequencer_single_node() -> Result<()> {
        let loglet_id = ReplicatedLogletId::new(122);
        let params = ReplicatedLogletParams {
            loglet_id,
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(1).unwrap()),
            nodeset: NodeSet::from_single(PlainNodeId::new(1)),
            write_set: None,
        };

        run_in_test_env(params, |env| async move {
            let batch: Arc<[Record]> = vec![
                ("record-1", Keys::Single(1)).into(),
                ("record-2", Keys::Single(2)).into(),
                ("record-3", Keys::Single(3)).into(),
            ]
            .into();
            let offset = env.loglet.enqueue_batch(batch.clone()).await?.await?;
            assert_that!(offset, eq(LogletOffset::new(3)));
            let offset = env.loglet.enqueue_batch(batch.clone()).await?.await?;
            assert_that!(offset, eq(LogletOffset::new(6)));
            let tail = env.loglet.find_tail().await?;
            assert_that!(tail, eq(TailState::Open(LogletOffset::new(7))));

            let cached_record = env.record_cache.get(loglet_id, 1.into());
            assert!(cached_record.is_some());
            assert_that!(
                cached_record.unwrap().keys().clone(),
                matches_pattern!(Keys::Single(eq(1)))
            );

            Ok(())
        })
        .await
    }

    // ** Single-node replicated-loglet seal **
    #[test(tokio::test(start_paused = true))]
    async fn test_seal_local_sequencer_single_node() -> Result<()> {
        let loglet_id = ReplicatedLogletId::new(122);
        let params = ReplicatedLogletParams {
            loglet_id,
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(1).unwrap()),
            nodeset: NodeSet::from_single(PlainNodeId::new(1)),
            write_set: None,
        };

        run_in_test_env(params, |env| async move {
            let batch: Arc<[Record]> = vec![
                ("record-1", Keys::Single(1)).into(),
                ("record-2", Keys::Single(2)).into(),
                ("record-3", Keys::Single(3)).into(),
            ]
            .into();
            let offset = env.loglet.enqueue_batch(batch.clone()).await?.await?;
            assert_that!(offset, eq(LogletOffset::new(3)));
            let offset = env.loglet.enqueue_batch(batch.clone()).await?.await?;
            assert_that!(offset, eq(LogletOffset::new(6)));
            let tail = env.loglet.find_tail().await?;
            assert_that!(tail, eq(TailState::Open(LogletOffset::new(7))));

            env.loglet.seal().await?;
            let batch: Arc<[Record]> = vec![
                ("record-4", Keys::Single(4)).into(),
                ("record-5", Keys::Single(5)).into(),
            ]
            .into();
            let not_appended = env.loglet.enqueue_batch(batch).await?.await;
            assert_that!(not_appended, err(pat!(AppendError::Sealed)));
            let tail = env.loglet.find_tail().await?;
            assert_that!(tail, eq(TailState::Sealed(LogletOffset::new(7))));

            Ok(())
        })
        .await
    }

    // ** Single-node replicated-loglet read-stream **
    #[test(tokio::test(start_paused = true))]
    async fn replicated_loglet_single_loglet_readstream() -> Result<()> {
        let loglet_id = ReplicatedLogletId::new(122);
        let params = ReplicatedLogletParams {
            loglet_id,
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(1).unwrap()),
            nodeset: NodeSet::from_single(PlainNodeId::new(1)),
            write_set: None,
        };
        run_in_test_env(params, |env| {
            crate::loglet::loglet_tests::single_loglet_readstream(env.loglet)
        })
        .await
    }

    #[test(tokio::test(start_paused = true))]
    async fn replicated_loglet_single_append_after_seal() -> Result<()> {
        let loglet_id = ReplicatedLogletId::new(122);
        let params = ReplicatedLogletParams {
            loglet_id,
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(1).unwrap()),
            nodeset: NodeSet::from_single(PlainNodeId::new(1)),
            write_set: None,
        };
        run_in_test_env(params, |env| {
            crate::loglet::loglet_tests::append_after_seal(env.loglet)
        })
        .await
    }

    #[test(tokio::test(start_paused = true))]
    async fn replicated_loglet_single_append_after_seal_concurrent() -> Result<()> {
        let loglet_id = ReplicatedLogletId::new(122);
        let params = ReplicatedLogletParams {
            loglet_id,
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(1).unwrap()),
            nodeset: NodeSet::from_single(PlainNodeId::new(1)),
            write_set: None,
        };
        run_in_test_env(params, |env| {
            crate::loglet::loglet_tests::append_after_seal_concurrent(env.loglet)
        })
        .await
    }

    #[test(tokio::test(start_paused = true))]
    async fn replicated_loglet_single_seal_empty() -> Result<()> {
        let loglet_id = ReplicatedLogletId::new(122);
        let params = ReplicatedLogletParams {
            loglet_id,
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(1).unwrap()),
            nodeset: NodeSet::from_single(PlainNodeId::new(1)),
            write_set: None,
        };
        run_in_test_env(params, |env| {
            crate::loglet::loglet_tests::seal_empty(env.loglet)
        })
        .await
    }
}
