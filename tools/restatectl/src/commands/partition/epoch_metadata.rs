// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_cli_util::CliContext;
use restate_core::protobuf::cluster_ctrl_svc::{SyncEpochMetadataRequest, new_cluster_ctrl_client};
use restate_metadata_store::MetadataStoreClient;
use restate_metadata_store::protobuf::metadata_proxy_svc::client::MetadataStoreProxy;
use restate_metadata_store::protobuf::metadata_proxy_svc::{
    GetRequest, client::new_metadata_proxy_client,
};
use restate_types::config::MetadataClientOptions;
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::PartitionId;
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::nodes_config::Role;
use restate_types::storage::StorageCodec;

use crate::connection::ConnectionInfo;

pub(super) async fn read_epoch_metadata(
    connection: &ConnectionInfo,
    partition_id: PartitionId,
) -> anyhow::Result<Option<EpochMetadata>> {
    let get_request = GetRequest {
        key: partition_processor_epoch_key(partition_id).to_string(),
    };

    let get_response = connection
        .try_each(None, |channel| async {
            new_metadata_proxy_client(channel, &CliContext::get().network)
                .get(get_request.clone())
                .await
        })
        .await?
        .into_inner();

    get_response
        .value
        .map(|mut value| StorageCodec::decode::<EpochMetadata, _>(&mut value.bytes))
        .transpose()
        .map_err(Into::into)
}

pub(super) async fn update_epoch_metadata<F>(
    connection: &ConnectionInfo,
    partition_id: PartitionId,
    modify: F,
) -> anyhow::Result<()>
where
    F: Fn(Option<EpochMetadata>) -> anyhow::Result<EpochMetadata>,
{
    let backoff_policy = &MetadataClientOptions::default().backoff_policy;

    connection
        .try_each(None, |channel| async {
            let metadata_store_proxy = MetadataStoreProxy::new(channel, &CliContext::get().network);
            let metadata_store_client =
                MetadataStoreClient::new(metadata_store_proxy, Some(backoff_policy.clone()));

            metadata_store_client
                .read_modify_write(partition_processor_epoch_key(partition_id), &modify)
                .await
        })
        .await?;

    Ok(())
}

pub(super) async fn signal_sync_epoch_metadata(
    connection: &ConnectionInfo,
    partition_ids: &[PartitionId],
) -> anyhow::Result<()> {
    let request = SyncEpochMetadataRequest {
        partition_ids: partition_ids.iter().map(|id| u32::from(*id)).collect(),
    };

    connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .sync_epoch_metadata(request.clone())
                .await
        })
        .await?;

    Ok(())
}
