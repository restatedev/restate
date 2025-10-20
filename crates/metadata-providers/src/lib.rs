// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![deny(clippy::perf)]
#![warn(
    clippy::large_futures,
    clippy::large_types_passed_by_value,
    clippy::use_debug,
    clippy::mutex_atomic
)]

#[cfg(feature = "etcd")]
pub mod etcd;
#[cfg(feature = "objstore")]
pub mod objstore;
#[cfg(feature = "replicated")]
pub mod replicated;

use restate_metadata_store::MetadataStoreClient;
use restate_types::config::{MetadataClientKind, MetadataClientOptions};

/// Creates a [`MetadataStoreClient`] for the configured metadata store.
#[allow(unused)]
pub async fn create_client(
    metadata_client_options: MetadataClientOptions,
) -> anyhow::Result<MetadataStoreClient> {
    let backoff_policy = Some(metadata_client_options.backoff_policy.clone());

    match metadata_client_options.kind.clone() {
        MetadataClientKind::Replicated { mut addresses } => {
            #[cfg(feature = "replicated")]
            {
                use std::sync::Arc;

                Ok(replicated::create_replicated_metadata_client(
                    addresses,
                    backoff_policy,
                    Arc::new(metadata_client_options),
                ))
            }
            #[cfg(not(feature = "replicated"))]
            anyhow::bail!("replicated metadata store is not supported in this build")
        }
        MetadataClientKind::Etcd { addresses } => {
            #[cfg(feature = "etcd")]
            {
                let store =
                    etcd::EtcdMetadataStore::new(addresses, &metadata_client_options).await?;
                Ok(MetadataStoreClient::new(store, backoff_policy))
            }
            #[cfg(not(feature = "etcd"))]
            anyhow::bail!("Etcd metadata store is not supported in this build")
        }
        conf @ MetadataClientKind::ObjectStore { .. } => {
            #[cfg(feature = "objstore")]
            {
                let store = objstore::create_object_store_based_meta_store(conf).await?;
                Ok(MetadataStoreClient::new(store, backoff_policy))
            }

            #[cfg(not(feature = "objstore"))]
            anyhow::bail!("object-store metadata store is not supported in this build")
        }
    }
}
