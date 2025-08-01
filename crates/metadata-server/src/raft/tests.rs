// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use bytestring::ByteString;
use futures::TryFutureExt;
use googletest::IntoTestResult;
use rand::RngCore;
use rand::distr::{Alphanumeric, SampleString};

use restate_core::network::NetworkServerBuilder;
use restate_core::{MetadataBuilder, TaskCenter, TaskKind, cancellation_token};
use restate_metadata_providers::create_client;
use restate_metadata_store::serialize_value;
use restate_rocksdb::RocksDbManager;
use restate_types::config::{
    CommonOptions, Configuration, MetadataClientKind, MetadataClientOptions, MetadataServerOptions,
    RaftOptions, set_current_config,
};
use restate_types::health::Health;
use restate_types::live::Constant;
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::net::{AdvertisedAddress, BindAddress};
use restate_types::nodes_config::{MetadataServerState, NodeConfig, NodesConfiguration, Role};
use restate_types::{PlainNodeId, Version};

use crate::raft::RaftMetadataServer;
use crate::tests::Value;

#[test_log::test(restate_core::test)]
async fn migration_local_to_replicated() -> googletest::Result<()> {
    let mut configuration = Configuration::default();
    let raft_options = RaftOptions::default();

    configuration.metadata_server.set_raft_options(raft_options);
    set_current_config(configuration);

    let uds = tempfile::tempdir()?.keep().join("server.sock");
    let bind_address = BindAddress::Uds(uds.clone());
    let advertised_address = AdvertisedAddress::Uds(uds);

    RocksDbManager::init(Constant::new(CommonOptions::default()));

    // initialize the local storage with some data that we can migrate
    let mut local_storage = crate::local::storage::RocksDbStorage::create(Constant::new(
        MetadataServerOptions::default(),
    ))
    .await?;

    let my_generation = 1;
    let zero_plain_node_id = PlainNodeId::from(0);
    let mut nodes_configuration =
        NodesConfiguration::new(Version::MIN, "migration-local-to-replicated".to_owned());
    let my_node_config = NodeConfig::builder()
        .name(Configuration::pinned().common.node_name().to_owned())
        .current_generation(
            // configure a zero node id to test the migration path for zero node ids
            PlainNodeId::from(0).with_generation(my_generation),
        )
        .address(advertised_address.clone())
        .roles(Role::MetadataServer.into())
        .build();
    nodes_configuration.upsert_node(my_node_config);

    let serialized_nodes_configuration = serialize_value(&nodes_configuration)?;
    local_storage
        .put(
            &NODES_CONFIG_KEY,
            &serialized_nodes_configuration,
            Precondition::DoesNotExist,
        )
        .await?;

    let mut rng = rand::rng();
    let number_kv_entries = 100;
    let mut expected_kv_entries: HashMap<_, _> = HashMap::default();

    for _ in 0..number_kv_entries {
        let random_key = Alphanumeric.sample_string(&mut rng, 10);
        let key = ByteString::from(random_key);
        let value = Value {
            value: rng.next_u32(),
            version: Version::from(rng.next_u32().saturating_add(1)),
        };
        let versioned_value = serialize_value(&value)?;

        local_storage
            .put(&key, &versioned_value, Precondition::None)
            .await?;
        expected_kv_entries.insert(key, value);
    }

    drop(local_storage);
    RocksDbManager::get().reset().await.into_test_result()?;

    let metadata_builder = MetadataBuilder::default();
    assert!(TaskCenter::try_set_global_metadata(
        metadata_builder.to_metadata()
    ));

    // start the replicated metadata store and let it migrate from local metadata
    let mut server_builder = NetworkServerBuilder::default();
    let health = Health::default();

    let metadata_server = RaftMetadataServer::create(
        Constant::new(MetadataServerOptions::default()),
        health.metadata_server_status(),
        &mut server_builder,
    )
    .await?;

    TaskCenter::spawn_child(TaskKind::NodeRpcServer, "node-rpc-server", async move {
        server_builder
            .run(health.node_rpc_status(), &bind_address)
            .await
    })?;
    TaskCenter::spawn_child(
        TaskKind::MetadataServer,
        "replicated-metadata-server",
        metadata_server.run(None).map_err(Into::into),
    )?;

    let metadata_client_options = MetadataClientOptions {
        kind: MetadataClientKind::Replicated {
            addresses: vec![advertised_address],
        },
        ..Default::default()
    };
    let metadata_client = create_client(metadata_client_options)
        .await
        .into_test_result()?;

    for (key, expected_value) in expected_kv_entries {
        let actual_value = metadata_client.get::<Value>(key).await?;
        assert_eq!(actual_value, Some(expected_value))
    }

    let actual_nodes_configuration = metadata_client
        .get::<NodesConfiguration>(NODES_CONFIG_KEY.clone())
        .await?
        .expect("NodesConfiguration was present in local metadata");

    assert_eq!(actual_nodes_configuration.len(), 1);

    let my_actual_node_config = actual_nodes_configuration
        .find_node_by_name(Configuration::pinned().common.node_name())
        .expect("my node config should be present");
    // validate that the zero node id was migrated to a non-zero value
    assert_ne!(
        my_actual_node_config.current_generation.as_plain(),
        zero_plain_node_id,
    );
    assert_eq!(
        my_actual_node_config.current_generation.generation(),
        my_generation
    );
    assert_eq!(
        my_actual_node_config
            .metadata_server_config
            .metadata_server_state,
        MetadataServerState::Member
    );

    // shut down all child tasks
    let cancellation_token = cancellation_token();
    cancellation_token.cancel();

    RocksDbManager::get().shutdown().await;
    Ok(())
}
