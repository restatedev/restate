// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod grpc;
mod store;

mod service;

use restate_core::metadata_store::MetadataStoreClient;
use restate_types::config::MetadataStoreClientOptions;
pub use service::LocalMetadataStoreService;

use crate::local::grpc::client::LocalMetadataStoreClient;

/// Creates a [`MetadataStoreClient`] for the [`LocalMetadataStoreService`].
pub fn create_client(
    metadata_store_client_options: MetadataStoreClientOptions,
) -> MetadataStoreClient {
    MetadataStoreClient::new(
        LocalMetadataStoreClient::new(metadata_store_client_options.metadata_store_address),
        Some(metadata_store_client_options.metadata_store_client_backoff_policy),
    )
}

#[cfg(test)]
mod tests;
