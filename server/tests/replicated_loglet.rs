mod common;

#[cfg(feature = "replicated-loglet")]
mod tests {
    use std::{num::NonZeroU8, sync::Arc};

    use googletest::prelude::*;
    use restate_bifrost::loglet::AppendError;
    use test_log::test;

    use restate_types::{
        config::Configuration,
        logs::{Keys, LogletOffset, Record, TailState},
        replicated_loglet::ReplicationProperty,
        storage::PolyBytes,
        time::NanosSinceEpoch,
        GenerationalNodeId,
    };

    use super::common::replicated_loglet::run_in_test_env;

    fn record_from_keys(data: &str, keys: Keys) -> Record {
        Record::from_parts(
            NanosSinceEpoch::now(),
            keys,
            PolyBytes::Typed(Arc::new(data.to_owned())),
        )
    }

    #[test(tokio::test)]
    async fn test_append_local_sequencer_three_logserver() -> Result<()> {
        run_in_test_env(
            Configuration::default(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(3).unwrap()),
            3,
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
                let tail = env.loglet.find_tail().await?;
                assert_that!(tail, eq(TailState::Open(LogletOffset::new(7))));

                Ok(())
            },
        )
        .await
    }

    #[test(tokio::test)]
    async fn test_seal_local_sequencer_three_logserver() -> Result<()> {
        run_in_test_env(
            Configuration::default(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(3).unwrap()),
            3,
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
                let tail = env.loglet.find_tail().await?;
                assert_that!(tail, eq(TailState::Open(LogletOffset::new(7))));

                env.loglet.seal().await?;
                let batch: Arc<[Record]> = vec![
                    record_from_keys("record-4", Keys::Single(4)),
                    record_from_keys("record-5", Keys::Single(5)),
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

    #[test(tokio::test)]
    #[ignore = "requires trim"]
    async fn three_logserver_gapless_smoke_test() -> googletest::Result<()> {
        run_in_test_env(
            Configuration::default(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(3).unwrap()),
            3,
            |test_env| {
                restate_bifrost::loglet::loglet_tests::gapless_loglet_smoke_test(test_env.loglet)
            },
        )
        .await
    }

    #[test(tokio::test)]
    async fn three_logserver_readstream() -> googletest::Result<()> {
        run_in_test_env(
            Configuration::default(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(3).unwrap()),
            3,
            |test_env| {
                restate_bifrost::loglet::loglet_tests::single_loglet_readstream(test_env.loglet)
            },
        )
        .await
    }

    #[test(tokio::test)]
    #[ignore = "requires trim"]
    async fn three_logserver_readstream_with_trims() -> googletest::Result<()> {
        run_in_test_env(
            Configuration::default(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(3).unwrap()),
            3,
            |test_env| {
                restate_bifrost::loglet::loglet_tests::single_loglet_readstream_with_trims(
                    test_env.loglet,
                )
            },
        )
        .await
    }

    #[test(tokio::test)]
    async fn three_logserver_append_after_seal() -> googletest::Result<()> {
        run_in_test_env(
            Configuration::default(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(3).unwrap()),
            3,
            |test_env| restate_bifrost::loglet::loglet_tests::append_after_seal(test_env.loglet),
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_logserver_append_after_seal_concurrent() -> googletest::Result<()> {
        run_in_test_env(
            Configuration::default(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(3).unwrap()),
            3,
            |test_env| {
                restate_bifrost::loglet::loglet_tests::append_after_seal_concurrent(test_env.loglet)
            },
        )
        .await
    }

    #[test(tokio::test)]
    async fn three_logserver_seal_empty() -> googletest::Result<()> {
        run_in_test_env(
            Configuration::default(),
            GenerationalNodeId::new(5, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(3).unwrap()),
            3,
            |test_env| restate_bifrost::loglet::loglet_tests::seal_empty(test_env.loglet),
        )
        .await
    }
}
