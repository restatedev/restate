// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::NetworkServerBuilder;
use restate_rocksdb::RocksError;
use restate_types::config::{MetadataServerOptions, RocksDbOptions};
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::protobuf::common::MetadataServerStatus;

mod store;

use crate::MetadataServerRunner;
pub use store::LocalMetadataServer;

pub(crate) async fn create_server(
    metadata_server_options: &MetadataServerOptions,
    rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    health_status: HealthStatus<MetadataServerStatus>,
    server_builder: &mut NetworkServerBuilder,
) -> Result<MetadataServerRunner<LocalMetadataServer>, RocksError> {
    let store =
        LocalMetadataServer::create(metadata_server_options, rocksdb_options, health_status)
            .await?;
    Ok(MetadataServerRunner::new(store, server_builder))
}

#[cfg(test)]
mod tests;
