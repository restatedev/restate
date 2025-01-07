// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::Metadata;
use restate_metadata_store::{MetadataStoreClient, ReadModifyWriteError};
use restate_types::{
    metadata_store::keys::PARTITION_TABLE_KEY, partition_table::PartitionTableBuilder,
};
use tracing::{debug, info};

pub struct UpdatePlacementTask {
    metadata_store_client: MetadataStoreClient,
}

impl UpdatePlacementTask {
    pub fn new(metadata_store_client: MetadataStoreClient) -> Self {
        Self {
            metadata_store_client,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let result = self
            .metadata_store_client
            .read_modify_write(PARTITION_TABLE_KEY.clone(), |current| {
                debug!("Updating partition placements");
                let Some(table) = current else {
                    return Err(UnmodifiedError);
                };

                let mut builder = PartitionTableBuilder::from(table);

                builder
                    .set_partitions_placements(&Metadata::with_current(|f| f.nodes_config_ref()));

                builder.build_if_modified().ok_or(UnmodifiedError)
            })
            .await;

        match result {
            Ok(_) => {
                info!("Partitions placements updated");
                Ok(())
            }
            Err(ReadModifyWriteError::FailedOperation(UnmodifiedError)) => Ok(()),
            Err(err) => anyhow::bail!(err),
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Partition table was not modified")]
struct UnmodifiedError;
