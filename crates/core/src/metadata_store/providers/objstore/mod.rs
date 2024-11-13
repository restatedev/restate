// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::metadata_store::providers::objstore::object_store_version_repository::ObjectStoreVersionRepository;
use crate::metadata_store::providers::objstore::optimistic_store::OptimisticLockingMetadataStoreBuilder;
use crate::metadata_store::providers::objstore::version_repository::VersionRepository;
use crate::metadata_store::MetadataStore;
use crate::{TaskCenter, TaskKind};
use restate_types::config::MetadataStoreClient;
use restate_types::errors::GenericError;

mod glue;
mod object_store_version_repository;
mod optimistic_store;
mod version_repository;

pub async fn create_object_store_based_meta_store(
    configuration: MetadataStoreClient,
) -> Result<impl MetadataStore, GenericError> {
    // obtain an instance of a version repository from the configuration.
    // we use an object_store backed version repository.
    let version_repository = Box::new(ObjectStoreVersionRepository::from_configuration(
        configuration.clone(),
    )?) as Box<dyn VersionRepository>;

    // postpone the building of the store to the background task,
    // the runs at the task center.
    let store_builder = OptimisticLockingMetadataStoreBuilder {
        version_repository,
        configuration,
    };
    //
    // setup all the glue code, the forwarding client and the event loop server.
    //
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let server = glue::Server::new(store_builder, rx);
    TaskCenter::spawn(
        TaskKind::MetadataStore,
        "metadata-store-client",
        server.run(),
    )
    .expect("unable to spawn a task");

    let client = glue::Client::new(tx);
    Ok(client)
}
