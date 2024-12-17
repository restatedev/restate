// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use tracing::{debug, info, instrument};

use restate_core::network::{Networking, TransportConnect};
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::{
    KeyFilter, LogId, LogletOffset, Record, RecordCache, SequenceNumber, TailState,
};
use restate_types::replicated_loglet::ReplicatedLogletParams;

use crate::loglet::util::TailOffsetWatch;
use crate::loglet::{Loglet, LogletCommit, OperationError, SendableLogletReadStream};
use crate::providers::replicated_loglet::replication::spread_selector::SelectorStrategy;
use crate::providers::replicated_loglet::sequencer::Sequencer;
use crate::providers::replicated_loglet::tasks::{
    FindTailTask, GetTrimPointTask, SealTask, TrimTask,
};

use super::error::ReplicatedLogletError;
use super::metric_definitions::{BIFROST_RECORDS_ENQUEUED_BYTES, BIFROST_RECORDS_ENQUEUED_TOTAL};
use super::read_path::{ReadStreamTask, ReplicatedLogletReadStream};
use super::remote_sequencer::RemoteSequencer;
use super::rpc_routers::{LogServersRpc, SequencersRpc};
use super::tasks::{CheckSealOutcome, CheckSealTask, FindTailResult};

#[derive(derive_more::Debug)]
pub(super) struct ReplicatedLoglet<T> {
    #[debug(skip)]
    log_id: LogId,
    #[debug(skip)]
    segment_index: SegmentIndex,
    my_params: ReplicatedLogletParams,
    #[debug(skip)]
    networking: Networking<T>,
    #[debug(skip)]
    logservers_rpc: LogServersRpc,
    #[debug(skip)]
    sequencers_rpc: SequencersRpc,
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
        sequencers_rpc: SequencersRpc,
        record_cache: RecordCache,
    ) -> Self {
        let known_global_tail = TailOffsetWatch::new(TailState::Open(LogletOffset::OLDEST));
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
            log_id,
            segment_index,
            my_params,
            networking,
            logservers_rpc,
            sequencers_rpc,
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

impl<T: TransportConnect> ReplicatedLoglet<T> {
    pub fn last_known_global_tail(&self) -> TailState<LogletOffset> {
        *self.known_global_tail.get()
    }
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

    #[instrument(
        level="trace",
        skip_all,
        fields(
            otel.name = "replicated_loglet: enqueue_batch",
        )
    )]
    async fn enqueue_batch(&self, payloads: Arc<[Record]>) -> Result<LogletCommit, OperationError> {
        if self.known_global_tail().is_sealed() {
            return Ok(LogletCommit::sealed());
        }
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
                // case, we run a check seal task to determine if we suspect that sealing is
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
                    // let's fire a seal to ensure this seal is complete.
                    self.seal().await?;
                }
                return Ok(*self.known_global_tail.get());
            }
            SequencerAccess::Remote { .. } => {
                let task = FindTailTask::new(
                    self.log_id,
                    self.segment_index,
                    self.my_params.clone(),
                    self.networking.clone(),
                    self.logservers_rpc.clone(),
                    self.sequencers_rpc.clone(),
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

    #[instrument(
        level="error",
        skip_all,
        fields(
            loglet_id = %self.my_params.loglet_id,
            otel.name = "replicated_loglet: get_trim_point",
        )
    )]
    async fn get_trim_point(&self) -> Result<Option<LogletOffset>, OperationError> {
        GetTrimPointTask::new(
            &self.my_params,
            self.logservers_rpc.clone(),
            self.known_global_tail.clone(),
        )
        .run(self.networking.clone())
        .await
        .map_err(Into::into)
    }

    #[instrument(
        level="error",
        skip_all,
        fields(
            loglet_id = %self.my_params.loglet_id,
            new_trim_point,
            otel.name = "replicated_loglet: trim",
        )
    )]
    /// Trim the log to the min(trim_point, last_committed_offset)
    /// trim_point is inclusive (will be trimmed)
    async fn trim(&self, trim_point: LogletOffset) -> Result<(), OperationError> {
        let trim_point = trim_point.min(self.known_global_tail.latest_offset().prev_unchecked());

        TrimTask::new(
            &self.my_params,
            self.logservers_rpc.clone(),
            self.known_global_tail.clone(),
        )
        .run(trim_point, self.networking.clone())
        .await?;
        info!(
            loglet_id=%self.my_params.loglet_id,
            ?trim_point,
            "Loglet has been trimmed successfully"
        );
        Ok(())
    }

    async fn seal(&self) -> Result<(), OperationError> {
        let _ = SealTask::new(
            self.my_params.clone(),
            self.logservers_rpc.seal.clone(),
            self.known_global_tail.clone(),
        )
        .run(self.networking.clone())
        .await?;
        // If we are the sequencer, we need to wait until the sequencer is drained.
        if let SequencerAccess::Local { handle } = &self.sequencer {
            handle.drain().await?;
            self.known_global_tail.notify_seal();
        };
        // On remote sequencer, we only set our global tail to sealed when we call find_tail and it
        // returns Sealed. We should NOT:
        // - Use AppendError::Sealed to mark our sealed global_tail
        // - Mark our global tail as sealed on successful seal() call.
        info!(loglet_id=%self.my_params.loglet_id, "Loglet has been sealed successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::num::{NonZeroU8, NonZeroUsize};

    use googletest::prelude::*;
    use test_log::test;

    use restate_core::network::NetworkServerBuilder;
    use restate_core::{TaskCenter, TestCoreEnvBuilder};
    use restate_log_server::LogServerService;
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::{set_current_config, Configuration};
    use restate_types::health::HealthStatus;
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
        config: Configuration,
        loglet_params: ReplicatedLogletParams,
        record_cache: RecordCache,
        mut future: F,
    ) -> googletest::Result<()>
    where
        F: FnMut(TestEnv) -> O,
        O: std::future::Future<Output = googletest::Result<()>>,
    {
        set_current_config(config.clone());
        let config = Live::from_value(config);

        RocksDbManager::init(config.clone().map(|c| &c.common));

        let mut node_env =
            TestCoreEnvBuilder::with_incoming_only_connector().add_mock_nodes_config();
        let mut server_builder = NetworkServerBuilder::default();

        let logserver_rpc = LogServersRpc::new(&mut node_env.router_builder);
        let sequencer_rpc = SequencersRpc::new(&mut node_env.router_builder);

        let log_server = LogServerService::create(
            HealthStatus::default(),
            config.clone(),
            node_env.metadata.clone(),
            node_env.metadata_store_client.clone(),
            record_cache.clone(),
            &mut node_env.router_builder,
            &mut server_builder,
        )
        .await?;

        let node_env = node_env.build().await;

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
            sequencer_rpc,
            record_cache.clone(),
        ));

        let env = TestEnv {
            loglet,
            record_cache,
        };

        future(env).await?;
        TaskCenter::shutdown_node("test completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    // ** Single-node replicated-loglet smoke tests **
    #[test(restate_core::test(start_paused = true))]
    async fn test_append_local_sequencer_single_node() -> Result<()> {
        let loglet_id = ReplicatedLogletId::new_unchecked(122);
        let params = ReplicatedLogletParams {
            loglet_id,
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(1).unwrap()),
            nodeset: NodeSet::from_single(PlainNodeId::new(1)),
        };
        let record_cache = RecordCache::new(1_000_000);

        run_in_test_env(
            Configuration::default(),
            params,
            record_cache,
            |env| async move {
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
            },
        )
        .await
    }

    // ** Single-node replicated-loglet seal **
    #[test(restate_core::test(start_paused = true))]
    async fn test_seal_local_sequencer_single_node() -> Result<()> {
        let loglet_id = ReplicatedLogletId::new_unchecked(122);
        let params = ReplicatedLogletParams {
            loglet_id,
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(1).unwrap()),
            nodeset: NodeSet::from_single(PlainNodeId::new(1)),
        };

        let record_cache = RecordCache::new(1_000_000);
        run_in_test_env(
            Configuration::default(),
            params,
            record_cache,
            |env| async move {
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
            },
        )
        .await
    }

    // # Loglet Spec Tests On Single Node
    // ** Single-node replicated-loglet **
    #[test(restate_core::test(start_paused = true))]
    async fn single_node_gapless_loglet_smoke_test() -> Result<()> {
        let record_cache = RecordCache::new(1_000_000);
        let loglet_id = ReplicatedLogletId::new_unchecked(122);
        let params = ReplicatedLogletParams {
            loglet_id,
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(1).unwrap()),
            nodeset: NodeSet::from_single(PlainNodeId::new(1)),
        };
        run_in_test_env(Configuration::default(), params, record_cache, |env| {
            crate::loglet::loglet_tests::gapless_loglet_smoke_test(env.loglet)
        })
        .await
    }

    #[test(restate_core::test(start_paused = true))]
    async fn single_node_single_loglet_readstream() -> Result<()> {
        let loglet_id = ReplicatedLogletId::new_unchecked(122);
        let params = ReplicatedLogletParams {
            loglet_id,
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(1).unwrap()),
            nodeset: NodeSet::from_single(PlainNodeId::new(1)),
        };
        let record_cache = RecordCache::new(1_000_000);
        run_in_test_env(Configuration::default(), params, record_cache, |env| {
            crate::loglet::loglet_tests::single_loglet_readstream(env.loglet)
        })
        .await
    }

    #[test(restate_core::test(start_paused = true))]
    async fn single_node_single_loglet_readstream_with_trims() -> Result<()> {
        let loglet_id = ReplicatedLogletId::new_unchecked(122);
        let params = ReplicatedLogletParams {
            loglet_id,
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(1).unwrap()),
            nodeset: NodeSet::from_single(PlainNodeId::new(1)),
        };
        // For this test to work, we need to disable the record cache to ensure we
        // observer the moving trimpoint.
        let mut config = Configuration::default();
        // disable read-ahead to avoid reading records from log-servers before the trim taking
        // place.
        config.bifrost.replicated_loglet.readahead_records = NonZeroUsize::new(1).unwrap();
        config.bifrost.replicated_loglet.readahead_trigger_ratio = 1.0;
        let record_cache = RecordCache::new(0);
        run_in_test_env(config, params, record_cache, |env| {
            crate::loglet::loglet_tests::single_loglet_readstream_with_trims(env.loglet)
        })
        .await
    }

    #[test(restate_core::test(start_paused = true))]
    async fn single_node_append_after_seal() -> Result<()> {
        let loglet_id = ReplicatedLogletId::new_unchecked(122);
        let params = ReplicatedLogletParams {
            loglet_id,
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(1).unwrap()),
            nodeset: NodeSet::from_single(PlainNodeId::new(1)),
        };
        let record_cache = RecordCache::new(1_000_000);
        run_in_test_env(Configuration::default(), params, record_cache, |env| {
            crate::loglet::loglet_tests::append_after_seal(env.loglet)
        })
        .await
    }

    #[test(restate_core::test(start_paused = true))]
    async fn single_node_append_after_seal_concurrent() -> Result<()> {
        let loglet_id = ReplicatedLogletId::new_unchecked(122);
        let params = ReplicatedLogletParams {
            loglet_id,
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(1).unwrap()),
            nodeset: NodeSet::from_single(PlainNodeId::new(1)),
        };

        let record_cache = RecordCache::new(1_000_000);
        run_in_test_env(Configuration::default(), params, record_cache, |env| {
            crate::loglet::loglet_tests::append_after_seal_concurrent(env.loglet)
        })
        .await
    }

    #[test(restate_core::test(start_paused = true))]
    async fn single_node_seal_empty() -> Result<()> {
        let loglet_id = ReplicatedLogletId::new_unchecked(122);
        let params = ReplicatedLogletParams {
            loglet_id,
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(1).unwrap()),
            nodeset: NodeSet::from_single(PlainNodeId::new(1)),
        };
        let record_cache = RecordCache::new(1_000_000);
        run_in_test_env(Configuration::default(), params, record_cache, |env| {
            crate::loglet::loglet_tests::seal_empty(env.loglet)
        })
        .await
    }
}
