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

use tracing::warn;

use restate_core::{
    spawn_metadata_manager, task_center, MetadataBuilder, MetadataManager, TaskCenter,
    TaskCenterBuilder, TaskCenterFutureExt,
};
use restate_metadata_store::{MetadataStoreClient, Precondition};
use restate_rocksdb::RocksDbManager;
use restate_types::config::Configuration;
use restate_types::live::Constant;
use restate_types::logs::metadata::ProviderKind;
use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;

pub async fn spawn_environment(
    config: Configuration,
    num_logs: u16,
    provider: ProviderKind,
) -> task_center::Handle {
    if rlimit::increase_nofile_limit(u64::MAX).is_err() {
        warn!("Failed to increase the number of open file descriptors limit.");
    }
    let tc = TaskCenterBuilder::default()
        .options(config.common.clone())
        .build()
        .expect("task_center builds")
        .into_handle();

    async {
        restate_types::config::set_current_config(config.clone());
        let metadata_builder = MetadataBuilder::default();

        let metadata_store_client = MetadataStoreClient::new_in_memory();
        let metadata = metadata_builder.to_metadata();
        let metadata_manager =
            MetadataManager::new(metadata_builder, metadata_store_client.clone());

        let metadata_writer = metadata_manager.writer();
        TaskCenter::try_set_global_metadata(metadata.clone());

        RocksDbManager::init(Constant::new(config.common));

        let logs = restate_types::logs::metadata::bootstrap_logs_metadata(provider, None, num_logs);

        metadata_store_client
            .put(BIFROST_CONFIG_KEY.clone(), &logs, Precondition::None)
            .await
            .expect("to store bifrost config in metadata store");
        metadata_writer.submit(Arc::new(logs));
        spawn_metadata_manager(metadata_manager).expect("metadata manager starts");
    }
    .in_tc(&tc)
    .await;
    tc
}
