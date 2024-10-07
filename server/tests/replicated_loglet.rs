mod common;

#[cfg(feature = "replicated-loglet")]
mod tests {
    use std::num::NonZeroU8;

    use test_log::test;

    use restate_types::{
        config::Configuration, replicated_loglet::ReplicationProperty, GenerationalNodeId,
    };

    use super::common::replicated_loglet::run_in_test_env;

    #[test(tokio::test)]
    async fn three_logserver_readstream() -> googletest::Result<()> {
        run_in_test_env(
            Configuration::default(),
            GenerationalNodeId::new(4, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            3,
            |test_env| {
                restate_bifrost::loglet::loglet_tests::single_loglet_readstream(test_env.loglet)
            },
        )
        .await
    }

    #[test(tokio::test)]
    async fn three_logserver_append_after_seal() -> googletest::Result<()> {
        run_in_test_env(
            Configuration::default(),
            GenerationalNodeId::new(4, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            3,
            |test_env| restate_bifrost::loglet::loglet_tests::append_after_seal(test_env.loglet),
        )
        .await
    }

    #[test(tokio::test)]
    async fn three_logserver_append_after_seal_concurrent() -> googletest::Result<()> {
        run_in_test_env(
            Configuration::default(),
            GenerationalNodeId::new(4, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
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
            GenerationalNodeId::new(4, 1), // local sequencer
            ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            3,
            |test_env| restate_bifrost::loglet::loglet_tests::seal_empty(test_env.loglet),
        )
        .await
    }
}
