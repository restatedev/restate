// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::local::grpc::client::LocalMetadataStoreClient;
use crate::local::service::LocalMetadataStoreService;
use crate::local::store::LocalMetadataStore;
use crate::{MetadataStoreClient, Precondition, WriteError};
use bytestring::ByteString;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use restate_core::{MockNetworkSender, TaskCenter, TaskKind, TestCoreEnv, TestCoreEnvBuilder};
use restate_grpc_util::create_grpc_channel_from_advertised_address;
use restate_rocksdb::RocksDbManager;
use restate_types::arc_util::{Constant, Updateable};
use restate_types::config::{
    reset_base_temp_dir_and_retain, CommonOptions, MetadataStoreOptions, RocksDbOptions,
};
use restate_types::net::{AdvertisedAddress, BindAddress};
use restate_types::retries::RetryPolicy;
use restate_types::{flexbuffers_storage_encode_decode, Version, Versioned};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use test_log::test;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize, Default)]
struct Value {
    version: Version,
    value: String,
}

impl Value {
    fn next_version(mut self) -> Self {
        self.version = self.version.next();
        self
    }
}

impl Versioned for Value {
    fn version(&self) -> Version {
        self.version
    }
}

flexbuffers_storage_encode_decode!(Value);

/// Tests basic operations of the metadata store.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn basic_metadata_store_operations() -> anyhow::Result<()> {
    let (client, env) = create_test_environment(&MetadataStoreOptions::default()).await?;

    env.tc
        .run_in_scope("test", None, async move {
            let key: ByteString = "key".into();
            let value = Value {
                version: Version::MIN,
                value: "test_value".to_owned(),
            };

            let next_value = Value {
                version: Version::from(2),
                value: "next_value".to_owned(),
            };

            let other_value = Value {
                version: Version::MIN,
                value: "other_value".to_owned(),
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

            Ok::<(), anyhow::Error>(())
        })
        .await?;

    env.tc.shutdown_node("shutdown", 0).await;

    Ok(())
}

/// Tests multiple concurrent operations issued by the same client
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn concurrent_operations() -> anyhow::Result<()> {
    let (client, env) = create_test_environment(&MetadataStoreOptions::default()).await?;

    env.tc
        .run_in_scope("test", None, async move {
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
                                        value.next_version(),
                                        Precondition::MatchesVersion(previous_version),
                                    )
                                    .await
                            } else {
                                client
                                    .put(key.clone(), Value::default(), Precondition::DoesNotExist)
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

            Ok::<(), anyhow::Error>(())
        })
        .await?;

    env.tc.shutdown_node("shutdown", 0).await;
    Ok(())
}

/// Tests that the metadata store stores values durably so that they can be read after a restart.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn durable_storage() -> anyhow::Result<()> {
    // get current base dir and use this for subsequent tests.
    let base_path = reset_base_temp_dir_and_retain();
    let tmp = std::env::temp_dir();
    let opts = MetadataStoreOptions::default();
    assert!(base_path.starts_with(tmp));
    assert_eq!(base_path.join("local-metadata-store"), opts.data_dir());

    let (client, env) = create_test_environment(&opts).await?;

    // write data
    env.tc
        .run_in_scope("write-data", None, async move {
            for key in 1u32..=10 {
                let value = key.to_string();
                let metadata_key = ByteString::from(value.clone());
                client
                    .put(
                        metadata_key,
                        Value {
                            version: Version::from(key),
                            value,
                        },
                        Precondition::DoesNotExist,
                    )
                    .await?;
            }

            Ok::<(), anyhow::Error>(())
        })
        .await?;

    // restart the metadata store
    env.tc
        .cancel_tasks(Some(TaskKind::MetadataStore), None)
        .await;
    // reset RocksDbManager to allow restarting the metadata store
    RocksDbManager::get().reset().await?;
    let client = start_metadata_store(&opts, Constant::new(opts.rocksdb.clone()), &env.tc).await?;

    // validate data
    env.tc
        .run_in_scope("validate-data", None, async move {
            for key in 1u32..=10 {
                let value = key.to_string();
                let metadata_key = ByteString::from(value.clone());

                assert_eq!(
                    client.get(metadata_key).await?,
                    Some(Value {
                        version: Version::from(key),
                        value
                    })
                );
            }

            Ok::<(), anyhow::Error>(())
        })
        .await?;

    env.tc.shutdown_node("shutdown", 0).await;
    std::fs::remove_dir_all(base_path)?;
    Ok(())
}

/// Creates a test environment with the [`RocksDBMetadataStore`] and a [`MetadataStoreClient`]
/// connected to it.
async fn create_test_environment(
    opts: &MetadataStoreOptions,
) -> anyhow::Result<(MetadataStoreClient, TestCoreEnv<MockNetworkSender>)> {
    let env = TestCoreEnvBuilder::new_with_mock_network().build().await;

    let task_center = &env.tc;

    task_center.run_in_scope_sync("db-manager-init", None, || {
        RocksDbManager::init(Constant::new(CommonOptions::default()))
    });

    let client =
        start_metadata_store(opts, Constant::new(opts.rocksdb.clone()), task_center).await?;

    Ok((client, env))
}

async fn start_metadata_store(
    opts: &MetadataStoreOptions,
    updateables_rocksdb_options: impl Updateable<RocksDbOptions> + Send + Sync + Clone + 'static,
    task_center: &TaskCenter,
) -> anyhow::Result<MetadataStoreClient> {
    let store = LocalMetadataStore::new(opts, updateables_rocksdb_options)?;

    let uds_path = tempfile::tempdir()?.into_path().join("grpc-server");
    let bind_address = BindAddress::Uds(uds_path.clone());
    let advertised_address = AdvertisedAddress::Uds(uds_path);
    let service = LocalMetadataStoreService::new(store, bind_address);
    let grpc_service_name = service.grpc_service_name().to_owned();

    task_center.spawn(
        TaskKind::MetadataStore,
        "local-metadata-store",
        None,
        async move {
            service.run().await?;
            Ok(())
        },
    )?;

    // await start-up of metadata store
    let health_client = HealthClient::new(create_grpc_channel_from_advertised_address(
        advertised_address.clone(),
    )?);
    let retry_policy = RetryPolicy::exponential(Duration::from_millis(10), 2.0, None, None);

    retry_policy
        .retry(|| async {
            health_client
                .clone()
                .check(HealthCheckRequest {
                    service: grpc_service_name.clone(),
                })
                .await
        })
        .await?;

    let rocksdb_client = LocalMetadataStoreClient::new(advertised_address);
    let client = MetadataStoreClient::new(rocksdb_client);

    Ok(client)
}
