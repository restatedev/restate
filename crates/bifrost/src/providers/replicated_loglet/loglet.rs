// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo(asoli): remove once this is fleshed out
#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use tracing::{debug, info};

use restate_core::network::{Networking, TransportConnect};
use restate_core::{task_center, ShutdownError};
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::{KeyFilter, LogId, LogletOffset, Record, SequenceNumber, TailState};
use restate_types::replicated_loglet::ReplicatedLogletParams;

use crate::loglet::util::TailOffsetWatch;
use crate::loglet::{Loglet, LogletCommit, OperationError, SendableLogletReadStream};
use crate::providers::replicated_loglet::replication::spread_selector::SelectorStrategy;
use crate::providers::replicated_loglet::sequencer::Sequencer;
use crate::providers::replicated_loglet::tasks::SealTask;

use super::log_server_manager::RemoteLogServerManager;
use super::record_cache::RecordCache;
use super::remote_sequencer::RemoteSequencer;
use super::rpc_routers::{LogServersRpc, SequencersRpc};

#[derive(derive_more::Debug)]
pub(super) struct ReplicatedLoglet<T> {
    /// This is used only to populate header of outgoing request to a remotely owned sequencer.
    /// Otherwise, it's unused.
    log_id: LogId,
    /// This is used only to populate header of outgoing request to a remotely owned sequencer.
    /// Otherwise, it's unused.
    segment_index: SegmentIndex,
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
    #[debug(skip)]
    log_server_manager: RemoteLogServerManager,
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
    ) -> Result<Self, ShutdownError> {
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
        Ok(Self {
            log_id,
            segment_index,
            my_params,
            networking,
            logservers_rpc,
            record_cache,
            known_global_tail,
            sequencer,
            log_server_manager,
        })
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
        _filter: KeyFilter,
        _from: LogletOffset,
        _to: Option<LogletOffset>,
    ) -> Result<SendableLogletReadStream, OperationError> {
        todo!()
    }

    fn watch_tail(&self) -> BoxStream<'static, TailState<LogletOffset>> {
        // It's acceptable for watch_tail to return an outdated value in the beginning,
        // but if the loglet is unsealed, we need to ensure that we have a mechanism to update
        // this value if we don't have a local sequencer.
        Box::pin(self.known_global_tail.to_stream())
    }

    async fn enqueue_batch(&self, payloads: Arc<[Record]>) -> Result<LogletCommit, OperationError> {
        match self.sequencer {
            SequencerAccess::Local { ref handle } => handle.enqueue_batch(payloads).await,
            SequencerAccess::Remote { ref handle } => handle.append(payloads).await,
        }
    }

    async fn find_tail(&self) -> Result<TailState<LogletOffset>, OperationError> {
        match self.sequencer {
            SequencerAccess::Local { .. } => Ok(*self.known_global_tail.get()),
            SequencerAccess::Remote { ref handle } => handle.find_tail().await,
        }
    }

    async fn get_trim_point(&self) -> Result<Option<LogletOffset>, OperationError> {
        todo!()
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
                )?);

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
}
