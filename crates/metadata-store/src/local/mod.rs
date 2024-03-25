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

pub use options::Options;
use restate_types::net::AdvertisedAddress;
pub use service::LocalMetadataStoreService;
pub use store::BuildError;

use crate::local::grpc::client::LocalMetadataStoreClient;
use crate::MetadataStoreClient;

/// Creates a [`MetadataStoreClient`] for the [`LocalMetadataStoreService`].
pub fn create_client(advertised_address: AdvertisedAddress) -> MetadataStoreClient {
    MetadataStoreClient::new(LocalMetadataStoreClient::new(advertised_address))
}

mod options;
#[cfg(test)]
mod tests;
