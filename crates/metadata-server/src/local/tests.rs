// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytestring::ByteString;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use test_log::test;

use restate_core::network::FailingConnector;
use restate_core::{TaskCenter, TaskKind, TestCoreEnv, TestCoreEnvBuilder};
use restate_rocksdb::RocksDbManager;
use restate_types::config::{
    reset_base_temp_dir_and_retain, Configuration, MetadataServerOptions, RocksDbOptions,
};
use restate_types::health::HealthStatus;
use restate_types::live::{BoxedLiveLoad, Live};
use restate_types::{Version, Versioned};

use crate::local::{data_dir, LocalMetadataServer};
use crate::tests::Value;
use crate::{MetadataStoreClient, Precondition, WriteError};

/// Tests basic operations of the metadata store.
#[test(restate_core::test(flavor = "multi_thread", worker_threads = 2))]
async fn basic_metadata_store_operations() -> anyhow::Result<()> {
    let (client, _env) = create_test_environment(&MetadataServerOptions::default()).await?;

    let key: ByteString = "key".into();
    let value = Value {
        version: Version::MIN,
        value: 1,
    };

    let next_value = Value {
        version: Version::from(2),
        value: 2,
    };

    let other_value = Value {
        version: Version::MIN,
        value: 3,
    };

    // first get should be empty
    assert!(client.get::<Value>(key.clone()).await?.is_none());

    // put initial value
    client.put(key.clone(), &value, Precondition::None).await?;

    assert_eq!(
        client.get_version(key.clone()).await?,
        Some(value.version())
    );
    assert_eq!(client.get(key.clone()).await?, Some(value));

    // fail to overwrite existing value
    assert!(matches!(
        client
            .put(key.clone(), &other_value, Precondition::DoesNotExist)
            .await,
        Err(WriteError::FailedPrecondition(_))
    ));

    // fail to overwrite existing value with wrong version
    assert!(matches!(
        client
            .put(
                key.clone(),
                &other_value,
                Precondition::MatchesVersion(Version::INVALID)
            )
            .await,
        Err(WriteError::FailedPrecondition(_))
    ));

    // overwrite with matching version precondition
    client
        .put(
            key.clone(),
            &next_value,
            Precondition::MatchesVersion(Version::MIN),
        )
        .await?;
    assert_eq!(client.get(key.clone()).await?, Some(next_value));

    // try to delete value with wrong version should fail
    assert!(matches!(
        client
            .delete(key.clone(), Precondition::MatchesVersion(Version::MIN))
            .await,
        Err(WriteError::FailedPrecondition(_))
    ));

    // delete should succeed with the right precondition
    client
        .delete(key.clone(), Precondition::MatchesVersion(Version::from(2)))
        .await?;
    assert!(client.get::<Value>(key.clone()).await?.is_none());

    // unconditional delete
    client
        .put(key.clone(), &other_value, Precondition::None)
        .await?;
    client.delete(key.clone(), Precondition::None).await?;
    assert!(client.get::<Value>(key.clone()).await?.is_none());

    Ok(())
}

/// Tests multiple concurrent operations issued by the same client
#[test(restate_core::test(flavor = "multi_thread", worker_threads = 2))]
async fn concurrent_operations() -> anyhow::Result<()> {
    let (client, _env) = create_test_environment(&MetadataServerOptions::default()).await?;

    let mut concurrent_operations = FuturesUnordered::default();

    for key in 1u32..=10 {
        for _instance in 0..key {
            let client = client.clone();
            let key = ByteString::from(key.to_string());
            concurrent_operations.push(async move {
                loop {
                    let value = client.get::<Value>(key.clone()).await?;

                    let result = if let Some(value) = value {
                        let previous_version = value.version();
                        client
                            .put(
                                key.clone(),
                                &value.next_version(),
                                Precondition::MatchesVersion(previous_version),
                            )
                            .await
                    } else {
                        client
                            .put(key.clone(), &Value::default(), Precondition::DoesNotExist)
                            .await
                    };

                    match result {
                        Ok(()) => return Ok::<(), anyhow::Error>(()),
                        Err(WriteError::FailedPrecondition(_)) => continue,
                        Err(err) => return Err(err.into()),
                    }
                }
            });
        }
    }

    while let Some(result) = concurrent_operations.next().await {
        result?;
    }

    // sanity check
    for key in 1u32..=10 {
        let metadata_key = ByteString::from(key.to_string());
        let value = client
            .get::<Value>(metadata_key)
            .await?
            .map(|v| v.version());

        assert_eq!(value, Some(Version::from(key)));
    }

    Ok(())
}

/// Tests that the metadata store stores values durably so that they can be read after a restart.
#[test(restate_core::test(flavor = "multi_thread", worker_threads = 2))]
async fn durable_storage() -> anyhow::Result<()> {
    // get current base dir and use this for subsequent tests.
    let base_path = reset_base_temp_dir_and_retain();
    let tmp = std::env::temp_dir();
    let opts = MetadataServerOptions::default();
    assert!(base_path.starts_with(tmp));
    assert_eq!(base_path.join("local-metadata-store"), data_dir());

    let (client, _env) = create_test_environment(&opts).await?;

    // write data
    for key in 1u32..=10 {
        let value = key.to_string();
        let metadata_key = ByteString::from(value.clone());
        client
            .put(
                metadata_key,
                &Value {
                    version: Version::from(key),
                    value: key,
                },
                Precondition::DoesNotExist,
            )
            .await?;
    }

    // restart the metadata store
    TaskCenter::cancel_tasks(Some(TaskKind::MetadataServer), None).await;
    // reset RocksDbManager to allow restarting the metadata store
    RocksDbManager::get().reset().await?;

    let metadata_store_opts = opts.clone();
    let metadata_store_opts = Live::from_value(metadata_store_opts);
    let client = start_metadata_server(
        &metadata_store_opts.pinned(),
        metadata_store_opts.map(|c| &c.rocksdb).boxed(),
    )
    .await?;

    // validate data
    for key in 1u32..=10 {
        let value = key.to_string();
        let metadata_key = ByteString::from(value.clone());

        assert_eq!(
            client.get(metadata_key).await?,
            Some(Value {
                version: Version::from(key),
                value: key,
            })
        );
    }

    TaskCenter::shutdown_node("shutdown", 0).await;
    std::fs::remove_dir_all(base_path)?;
    Ok(())
}

/// Creates a test environment with the [`RocksDBMetadataStore`] and a [`GrpcMetadataServerClient`]
/// connected to it.
async fn create_test_environment(
    opts: &MetadataServerOptions,
) -> anyhow::Result<(MetadataStoreClient, TestCoreEnv<FailingConnector>)> {
    // Setup metadata store on unix domain socket.
    let config = Configuration {
        metadata_server: opts.clone(),
        ..Default::default()
    };

    restate_types::config::set_current_config(config.clone());
    let config = Live::from_value(config);
    let env = TestCoreEnvBuilder::with_incoming_only_connector()
        .build()
        .await;

    RocksDbManager::init(config.clone().map(|c| &c.common));

    let client = start_metadata_server(
        &config.pinned().metadata_server,
        config.clone().map(|c| &c.metadata_server.rocksdb).boxed(),
    )
    .await?;

    Ok((client, env))
}

async fn start_metadata_server(
    opts: &MetadataServerOptions,
    updateables_rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
) -> anyhow::Result<MetadataStoreClient> {
    let server =
        LocalMetadataServer::create(opts, updateables_rocksdb_options, HealthStatus::default())
            .await?;

    let client = server.client();

    TaskCenter::spawn(
        TaskKind::MetadataServer,
        "local-metadata-store",
        server.run(),
    )?;

    Ok(client)
}
