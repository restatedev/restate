// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::{
    spawn_metadata_manager, MetadataBuilder, MetadataManager, MockNetworkSender, TaskCenter,
    TaskCenterBuilder,
};
use restate_metadata_store::{MetadataStoreClient, Precondition};
use restate_rocksdb::RocksDbManager;
use restate_types::arc_util::Constant;
use restate_types::config::Configuration;
use restate_types::logs::metadata::ProviderKind;
use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;

pub async fn spawn_environment(
    config: Configuration,
    num_logs: u64,
    provider: ProviderKind,
) -> TaskCenter {
    let tc = TaskCenterBuilder::default()
        .options(config.common.clone())
        .build()
        .expect("task_center builds");

    restate_types::config::set_current_config(config.clone());
    let network_sender = MockNetworkSender::default();

    let metadata_store_client = MetadataStoreClient::new_in_memory();
    let metadata_builder = MetadataBuilder::default();
    let metadata = metadata_builder.to_metadata();
    let metadata_manager = MetadataManager::new(
        metadata_builder,
        network_sender.clone(),
        metadata_store_client.clone(),
    );

    let metadata_writer = metadata_manager.writer();
    tc.try_set_global_metadata(metadata.clone());

    tc.run_in_scope_sync("db-manager-init", None, || {
        RocksDbManager::init(Constant::new(config.common))
    });

    let logs = restate_types::logs::metadata::create_static_metadata(provider, num_logs);

    metadata_store_client
        .put(BIFROST_CONFIG_KEY.clone(), logs.clone(), Precondition::None)
        .await
        .expect("to store bifrost config in metadata store");
    metadata_writer.submit(logs);
    spawn_metadata_manager(&tc, metadata_manager).expect("metadata manager starts");
    tc
}
