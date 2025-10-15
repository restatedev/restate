// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod common;

mod tests {
    use std::{
        collections::BTreeSet,
        num::{NonZeroU8, NonZeroU16},
        sync::Arc,
        time::Duration,
    };

    use futures_util::StreamExt;
    use googletest::prelude::*;

    use crate::common::replicated_loglet::run_in_test_env;
    use restate_bifrost::{
        ErrorRecoveryStrategy,
        loglet::{AppendError, FindTailOptions},
    };
    use restate_core::{Metadata, TaskCenterFutureExt};
    use restate_types::live::{LiveLoad, LiveLoadExt};
    use restate_types::{
        GenerationalNodeId, Version,
        config::Configuration,
        logs::{
            KeyFilter, Keys, LogId, LogletOffset, Lsn, Record, SequenceNumber, TailState,
            metadata::{LogletParams, ProviderKind},
        },
        replicated_loglet::ReplicatedLogletParams,
        replication::ReplicationProperty,
        storage::PolyBytes,
        time::NanosSinceEpoch,
    };
    use test_log::test;
    use tokio::task::{JoinHandle, JoinSet};
    use tokio_util::sync::CancellationToken;
    use tracing::info;

    fn record_from_keys(data: &str, keys: Keys) -> Record {
        Record::from_parts(
            NanosSinceEpoch::now(),
            keys,
            PolyBytes::Typed(Arc::new(data.to_owned())),
        )
    }

    #[test(restate_core::test)]
    async fn test_append_local_sequencer_three_logserver() -> Result<()> {
        run_in_test_env(
            Configuration::new_unix_sockets(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            3,
            "test_sequencer",
            |env| async move {
                let batch: Arc<[Record]> = vec![
                    record_from_keys("record-1", Keys::Single(1)),
                    record_from_keys("record-2", Keys::Single(2)),
                    record_from_keys("record-3", Keys::Single(3)),
                ]
                .into();
                let offset = env.loglet.enqueue_batch(batch.clone()).await?.await?;
                assert_that!(offset, eq(LogletOffset::new(3)));
                let offset = env.loglet.enqueue_batch(batch.clone()).await?.await?;
                assert_that!(offset, eq(LogletOffset::new(6)));
                let tail = env.loglet.find_tail(FindTailOptions::default()).await?;
                assert_that!(tail, eq(TailState::Open(LogletOffset::new(7))));

                Ok(())
            },
        )
        .await
    }

    #[test(restate_core::test)]
    async fn test_seal_local_sequencer_three_logserver() -> Result<()> {
        run_in_test_env(
            Configuration::new_unix_sockets(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            3,
            "test_seal",
            |env| async move {
                let batch: Arc<[Record]> = vec![
                    record_from_keys("record-1", Keys::Single(1)),
                    record_from_keys("record-2", Keys::Single(2)),
                    record_from_keys("record-3", Keys::Single(3)),
                ]
                .into();
                let offset = env.loglet.enqueue_batch(batch.clone()).await?.await?;
                assert_that!(offset, eq(LogletOffset::new(3)));
                let offset = env.loglet.enqueue_batch(batch.clone()).await?.await?;
                assert_that!(offset, eq(LogletOffset::new(6)));
                let tail = env.loglet.find_tail(FindTailOptions::default()).await?;
                assert_that!(tail, eq(TailState::Open(LogletOffset::new(7))));

                env.loglet.seal().await?;
                let batch: Arc<[Record]> = vec![
                    record_from_keys("record-4", Keys::Single(4)),
                    record_from_keys("record-5", Keys::Single(5)),
                ]
                .into();
                let not_appended = env.loglet.enqueue_batch(batch).await?.await;
                assert_that!(not_appended, err(pat!(AppendError::Sealed)));
                let tail = env.loglet.find_tail(FindTailOptions::default()).await?;
                assert_that!(tail, eq(TailState::Sealed(LogletOffset::new(7))));

                Ok(())
            },
        )
        .await
    }

    #[test(restate_core::test)]
    async fn three_logserver_gapless_smoke_test() -> googletest::Result<()> {
        run_in_test_env(
            Configuration::new_unix_sockets(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            3,
            "logserver_smoke",
            |test_env| {
                restate_bifrost::loglet::loglet_tests::gapless_loglet_smoke_test(test_env.loglet)
            },
        )
        .await
    }

    #[test(restate_core::test)]
    async fn three_logserver_readstream() -> googletest::Result<()> {
        run_in_test_env(
            Configuration::new_unix_sockets(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            3,
            "logserver_readstream",
            |test_env| {
                restate_bifrost::loglet::loglet_tests::single_loglet_readstream(test_env.loglet)
            },
        )
        .await
    }

    #[test(restate_core::test)]
    async fn three_logserver_readstream_with_trims() -> googletest::Result<()> {
        // For this test to work, we need to disable the record cache to ensure we
        // observer the moving trimpoint.
        let mut config = Configuration::new_unix_sockets();
        // disable read-ahead to avoid reading records from log-servers before the trim taking
        // place.
        config.bifrost.replicated_loglet.readahead_records = NonZeroU16::new(1).unwrap();
        config.bifrost.replicated_loglet.readahead_trigger_ratio = 1.0;
        config.bifrost.record_cache_memory_size = 0_u64.into();
        run_in_test_env(
            config,
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            3,
            "readstream_trims",
            |test_env| {
                restate_bifrost::loglet::loglet_tests::single_loglet_readstream_with_trims(
                    test_env.loglet,
                )
            },
        )
        .await
    }

    #[test(restate_core::test)]
    async fn three_logserver_append_after_seal() -> googletest::Result<()> {
        run_in_test_env(
            Configuration::new_unix_sockets(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            3,
            "append_after_seal",
            |test_env| restate_bifrost::loglet::loglet_tests::append_after_seal(test_env.loglet),
        )
        .await
    }

    #[test(restate_core::test(flavor = "multi_thread", worker_threads = 4))]
    async fn three_logserver_append_after_seal_concurrent() -> googletest::Result<()> {
        run_in_test_env(
            Configuration::new_unix_sockets(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            3,
            "after_seal_concurrent",
            |test_env| {
                restate_bifrost::loglet::loglet_tests::append_after_seal_concurrent(test_env.loglet)
            },
        )
        .await
    }

    #[test(restate_core::test)]
    async fn three_logserver_seal_empty() -> googletest::Result<()> {
        run_in_test_env(
            Configuration::new_unix_sockets(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            3,
            "seal_empty",
            |test_env| restate_bifrost::loglet::loglet_tests::seal_empty(test_env.loglet),
        )
        .await
    }

    #[test(restate_core::test(flavor = "multi_thread", worker_threads = 4))]
    async fn bifrost_append_and_seal_concurrent() -> googletest::Result<()> {
        const TEST_DURATION: Duration = Duration::from_secs(10);
        const SEAL_PERIOD: Duration = Duration::from_secs(1);
        const CONCURRENT_APPENDERS: usize = 400;

        run_in_test_env(
            Configuration::new_unix_sockets(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            3,
            "seal_concurrent",
            |test_env| async move {
                let log_id = LogId::new(0);

                let metadata = Metadata::current();


                let mut appenders: JoinSet<googletest::Result<_>> = JoinSet::new();
                let stop_signal = CancellationToken::new();

                for appender_id in 0..CONCURRENT_APPENDERS {
                    appenders.spawn({
                        let bifrost = test_env.bifrost.clone();
                        let cancel_appenders = stop_signal.clone();
                        async move {
                            let mut i = 1;
                            let mut committed = Vec::new();
                            while !cancel_appenders.is_cancelled() {
                                let offset = bifrost
                                    .append(
                                        log_id,
                                        ErrorRecoveryStrategy::Wait,
                                        format!("appender-{appender_id}-record{i}"),
                                    )
                                    .await?;
                                i += 1;
                                committed.push(offset);
                            }
                            Ok(committed)
                        }.in_current_tc()
                    });
                }

                let mut sealer_handle: JoinHandle<googletest::Result<()>> = tokio::task::spawn({
                    let bifrost = test_env.bifrost.clone();
                    let stop_signal = stop_signal.clone();
                    async move {

                        let mut chain = metadata.updateable_logs_metadata().map(|logs| logs.chain(&log_id).expect("a chain to exist"));

                        let mut last_loglet_id = None;

                        while !stop_signal.is_cancelled() {
                            tokio::time::sleep(SEAL_PERIOD).await;

                            let mut params = ReplicatedLogletParams::deserialize_from(
                                chain.live_load().tail().config.params.as_ref(),
                            )?;
                            if last_loglet_id == Some(params.loglet_id) {
                                fail!("Could not seal as metadata has not caught up from the last seal (version={})", metadata.logs_version())?;
                            }
                            last_loglet_id = Some(params.loglet_id);
                            info!("Sealing loglet {} and creating new loglet {}", params.loglet_id, params.loglet_id.next());
                            params.loglet_id = params.loglet_id.next();

                            bifrost
                                .admin()
                                .seal_and_extend_chain(
                                    log_id,
                                    None,
                                    Version::MIN,
                                    ProviderKind::Replicated,
                                    LogletParams::from(params.serialize()?),
                                )
                                .await?;
                        }

                        Ok(())
                    }.in_current_tc()
                });

                tokio::select! {
                    res = appenders.join_next() => {
                        fail!("an appender exited early: {res:?}")?;
                    }
                    res = &mut sealer_handle => {
                        fail!("sealer exited early: {res:?}")?;
                    }
                    _ = tokio::time::sleep(TEST_DURATION) => {
                        info!("cancelling appenders and running validation")
                    }
                }

                // stop appending and sealing
                stop_signal.cancel();

                sealer_handle.await??;

                let mut all_committed = BTreeSet::new();
                while let Some(handle) = appenders.join_next().await {
                    let committed = handle??;
                    let committed_len = committed.len();
                    assert_that!(committed_len, ge(0));
                    let tail_record = committed.last().unwrap();
                    info!(
                        "Committed len={committed_len}, last appended={tail_record}"
                    );
                    // ensure that all committed records are unique
                    for offset in committed {
                        if !all_committed.insert(offset) {
                            fail!("Committed duplicate sequence number {}", offset)?
                        }
                    }
                }
                let last_lsn = *all_committed
                    .last()
                    .expect("to have committed some records");

                let mut reader =
                    test_env.bifrost.create_reader(log_id, KeyFilter::Any, Lsn::OLDEST, last_lsn)?;

                let mut records = BTreeSet::new();

                while let Some(record) = reader.next().await {
                    let record = record?;
                    if !records.insert(record.sequence_number()) {
                        fail!("Read duplicate sequence number {}", record.sequence_number())?
                    }
                }

                // every record committed must be observed exactly once in readstream
                assert!(all_committed.eq(&records));
                info!("Validated {} committed records", all_committed.len());

                Ok(())
            },
        )
        .await
    }
}
