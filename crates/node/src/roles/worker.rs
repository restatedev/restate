// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use codederror::CodedError;
use tracing::info;

use restate_bifrost::Bifrost;
use restate_core::network::MessageRouterBuilder;
use restate_core::{cancellation_watcher, metadata, task_center};
use restate_core::{ShutdownError, TaskKind};
use restate_grpc_util::create_grpc_channel_from_advertised_address;
use restate_metadata_store::MetadataStoreClient;
use restate_network::Networking;
use restate_node_protocol::metadata::MetadataKind;
use restate_node_services::cluster_ctrl::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_node_services::cluster_ctrl::AttachmentRequest;
use restate_schema::UpdateableSchema;
use restate_schema_api::subscription::SubscriptionResolver;
use restate_storage_query_datafusion::context::QueryContext;
use restate_types::config::UpdateableConfiguration;
use restate_types::net::AdvertisedAddress;
use restate_types::retries::RetryPolicy;
use restate_types::Version;
use restate_worker::SubscriptionController;
use restate_worker::{SubscriptionControllerHandle, Worker};

#[derive(Debug, thiserror::Error, CodedError)]
pub enum WorkerRoleError {
    #[error("worker failed: {0}")]
    Worker(
        #[from]
        #[code]
        restate_worker::Error,
    ),
    #[error("invalid cluster controller address: {0}")]
    #[code(unknown)]
    InvalidClusterControllerAddress(http::Error),
    #[error("failed to attach to cluster at '{0}': {1}")]
    #[code(unknown)]
    Attachment(AdvertisedAddress, tonic::Status),
    #[error(transparent)]
    #[code(unknown)]
    Shutdown(#[from] ShutdownError),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum SchemaError {
    #[error("failed to fetch schema updates: {0}")]
    #[code(unknown)]
    Fetch(#[from] tonic::Status),
    #[error("failed updating subscriptions: {0}")]
    #[code(unknown)]
    Subscription(#[from] restate_worker::WorkerHandleError),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum WorkerRoleBuildError {
    #[error("failed creating worker: {0}")]
    Worker(
        #[from]
        #[code]
        restate_worker::BuildError,
    ),
}

pub struct WorkerRole {
    worker: Worker,
}

impl WorkerRole {
    pub fn new(
        updateable_config: UpdateableConfiguration,
        router_builder: &mut MessageRouterBuilder,
        networking: Networking,
        bifrost: Bifrost,
        metadata_store_client: MetadataStoreClient,
        updating_schema_information: UpdateableSchema,
    ) -> Result<Self, WorkerRoleBuildError> {
        let worker = Worker::from_options(
            updateable_config,
            networking,
            bifrost,
            router_builder,
            updating_schema_information,
            metadata_store_client,
        )?;

        Ok(WorkerRole { worker })
    }

    pub fn storage_query_context(&self) -> &QueryContext {
        self.worker.storage_query_context()
    }

    pub fn subscription_controller(&self) -> Option<SubscriptionControllerHandle> {
        Some(self.worker.subscription_controller_handle())
    }

    pub async fn start(self, bifrost: Bifrost) -> anyhow::Result<()> {
        let admin_address = metadata()
            .nodes_config()
            .get_admin_node()
            .expect("at least one admin node")
            .address
            .clone();

        // todo: only run subscriptions on node 0 once being distributed
        task_center().spawn_child(
            TaskKind::MetadataBackgroundSync,
            "subscription_controller",
            None,
            Self::watch_subscriptions(self.worker.subscription_controller_handle()),
        )?;

        task_center().spawn_child(TaskKind::RoleRunner, "worker-service", None, async {
            Self::attach_node(admin_address).await?;
            self.worker.run(bifrost).await
        })?;

        Ok(())
    }

    async fn attach_node(admin_address: AdvertisedAddress) -> Result<(), WorkerRoleError> {
        info!("Worker attaching to admin at '{admin_address}'");

        let channel = create_grpc_channel_from_advertised_address(admin_address.clone())
            .map_err(WorkerRoleError::InvalidClusterControllerAddress)?;

        let cc_client = ClusterCtrlSvcClient::new(channel);

        let _response = RetryPolicy::exponential(Duration::from_millis(50), 2.0, Some(10), None)
            .retry(|| async {
                cc_client
                    .clone()
                    .attach_node(AttachmentRequest {
                        node_id: Some(metadata().my_node_id().into()),
                    })
                    .await
            })
            .await
            .map_err(|err| WorkerRoleError::Attachment(admin_address, err))?
            .into_inner();

        Ok(())
    }

    async fn watch_subscriptions<SC>(subscription_controller: SC) -> anyhow::Result<()>
    where
        SC: SubscriptionController + Clone + Send + Sync,
    {
        let metadata = metadata();
        let schema_view = metadata.schema_updateable();
        let mut next_version = Version::MIN;
        let cancellation_watcher = cancellation_watcher();
        tokio::pin!(cancellation_watcher);

        loop {
            tokio::select! {
                _ = &mut cancellation_watcher => {
                    break;
                },
                version = metadata.wait_for_version(MetadataKind::Schema, next_version) => {
                    next_version = version?.next();

                    // This might return subscriptions belonging to a higher schema version. As a
                    // result we might re-apply the same list of subscriptions. This is not a
                    // problem, since update_subscriptions is idempotent.
                    let subscriptions = schema_view.list_subscriptions(&[]);
                    subscription_controller
                        .update_subscriptions(subscriptions)
                        .await?;
                }
            }
        }

        Ok(())
    }
}
