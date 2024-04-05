// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bincode::error::DecodeError;
use restate_types::config::UpdateableConfiguration;
use std::time::Duration;

use codederror::CodedError;
use restate_core::network::MessageRouterBuilder;
use restate_network::Networking;
use tonic::transport::Channel;
use tracing::subscriber::NoSubscriber;
use tracing::trace;

use restate_bifrost::Bifrost;
use restate_core::{metadata, task_center};
use restate_core::{ShutdownError, TaskKind};
use restate_grpc_util::create_grpc_channel_from_advertised_address;
use restate_metadata_store::MetadataStoreClient;
use restate_node_services::cluster_ctrl::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_node_services::cluster_ctrl::AttachmentRequest;
use restate_node_services::cluster_ctrl::FetchSchemasRequest;
use restate_schema_api::subscription::SubscriptionResolver;
use restate_schema_impl::{Schemas, SchemasUpdateCommand};
use restate_storage_query_datafusion::context::QueryContext;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::net::AdvertisedAddress;
use restate_types::retries::RetryPolicy;
use restate_worker::SubscriptionController;
use restate_worker::{SubscriptionControllerHandle, Worker};
use tracing::info;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum WorkerRoleError {
    #[error("worker failed: {0}")]
    Worker(
        #[from]
        #[code]
        restate_worker::Error,
    ),
    #[error(transparent)]
    Schema(
        #[from]
        #[code]
        SchemaError,
    ),
    #[error("invalid cluster controller address: {0}")]
    #[code(unknown)]
    InvalidClusterControllerAddress(http::Error),
    #[error("failed to attach to cluster at '{0}': {1}")]
    #[code(unknown)]
    Attachment(AdvertisedAddress, tonic::Status),
    #[error("codec error: {0}")]
    #[code(unknown)]
    Codec(#[from] DecodeError),
    #[error(transparent)]
    #[code(unknown)]
    Shutdown(#[from] ShutdownError),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum SchemaError {
    #[error("failed to fetch schema updates: {0}")]
    #[code(unknown)]
    Fetch(#[from] tonic::Status),
    #[error("failed decoding grpc payload: {0}")]
    #[code(unknown)]
    Decode(#[from] bincode::error::DecodeError),
    #[error("failed updating schemas: {0}")]
    Update(
        #[from]
        #[code]
        restate_schema_impl::ErrorKind,
    ),
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
    #[error("failed creating meta: {0}")]
    Meta(
        #[from]
        #[code]
        restate_meta::BuildError,
    ),
}

pub struct WorkerRole {
    schemas: Schemas,
    worker: Worker,
}

impl WorkerRole {
    pub fn new(
        updateable_config: UpdateableConfiguration,
        router_builder: &mut MessageRouterBuilder,
        networking: Networking,
        bifrost: Bifrost,
        metadata_store_client: MetadataStoreClient,
    ) -> Result<Self, WorkerRoleBuildError> {
        let schemas = Schemas::default();
        let worker = Worker::from_options(
            updateable_config,
            networking,
            bifrost,
            router_builder,
            schemas.clone(),
            metadata_store_client,
        )?;

        Ok(WorkerRole { schemas, worker })
    }

    pub fn rocksdb_storage(&self) -> &RocksDBStorage {
        self.worker.rocksdb_storage()
    }

    pub fn storage_query_context(&self) -> &QueryContext {
        self.worker.storage_query_context()
    }

    pub fn schemas(&self) -> Schemas {
        self.schemas.clone()
    }

    pub fn subscription_controller(&self) -> Option<SubscriptionControllerHandle> {
        Some(self.worker.subscription_controller_handle())
    }

    pub async fn start(self, bifrost: Bifrost) -> anyhow::Result<()> {
        // todo: only run subscriptions on node 0 once being distributed
        let subscription_controller = Some(self.worker.subscription_controller_handle());

        let admin_address = metadata()
            .nodes_config()
            .get_admin_node()
            .expect("at least one admin node")
            .address
            .clone();

        let channel = create_grpc_channel_from_advertised_address(admin_address.clone())
            .expect("valid admin address");
        let mut cluster_ctrl_client = ClusterCtrlSvcClient::new(channel);

        // Fetch latest schema information and fail if this is not possible
        Self::fetch_and_update_schemas(
            &self.schemas,
            subscription_controller.as_ref(),
            &mut cluster_ctrl_client,
        )
        .await?;

        // todo: replace by watchdog
        task_center().spawn_child(
            TaskKind::MetadataBackgroundSync,
            "schema-updater",
            None,
            Self::reload_schemas(subscription_controller, self.schemas, cluster_ctrl_client),
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

        let _response = RetryPolicy::exponential(Duration::from_millis(50), 2.0, 10, None)
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

    fn ignore_fetch_error(result: Result<(), SchemaError>) -> Result<(), SchemaError> {
        if let Err(err) = result {
            match err {
                SchemaError::Fetch(err) => {
                    trace!("Failed fetching schema information: {err}. Retrying.");
                }
                SchemaError::Decode(_) | SchemaError::Update(_) | SchemaError::Subscription(_) => {
                    Err(err)?
                }
            }
        }
        Ok(())
    }

    async fn reload_schemas<SC>(
        subscription_controller: Option<SC>,
        schemas: Schemas,
        mut cluster_ctrl_client: ClusterCtrlSvcClient<Channel>,
    ) -> anyhow::Result<()>
    where
        SC: SubscriptionController + Clone + Send + Sync,
    {
        // todo: make this configurable
        let mut fetch_interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            fetch_interval.tick().await;

            trace!("Trying to fetch schema information");

            Self::ignore_fetch_error(
                Self::fetch_and_update_schemas(
                    &schemas,
                    subscription_controller.as_ref(),
                    &mut cluster_ctrl_client,
                )
                .await,
            )?;
        }
    }

    async fn fetch_and_update_schemas<SC>(
        schemas: &Schemas,
        subscription_controller: Option<&SC>,
        cluster_ctrl_client: &mut ClusterCtrlSvcClient<Channel>,
    ) -> Result<(), SchemaError>
    where
        SC: SubscriptionController + Send + Sync,
    {
        let schema_updates = Self::fetch_schemas(cluster_ctrl_client).await?;
        update_schemas(schemas, subscription_controller, schema_updates).await?;

        Ok(())
    }

    async fn fetch_schemas(
        cluster_ctrl_client: &ClusterCtrlSvcClient<Channel>,
    ) -> Result<Vec<SchemasUpdateCommand>, SchemaError> {
        let response = RetryPolicy::exponential(Duration::from_millis(50), 2.0, 10, None)
            .retry(|| {
                let mut cluster_ctrl_client = cluster_ctrl_client.clone();
                async move {
                    cluster_ctrl_client
                        // todo introduce schema version information to avoid fetching and overwriting the schema information
                        //  over and over again
                        .fetch_schemas(FetchSchemasRequest {})
                        .await
                }
            })
            .await?;

        let (schema_updates, _) = bincode::serde::decode_from_slice::<Vec<SchemasUpdateCommand>, _>(
            &response.into_inner().schemas_bin,
            bincode::config::standard(),
        )?;
        Ok(schema_updates)
    }
}

pub async fn update_schemas<SC>(
    schemas: &Schemas,
    subscription_controller: Option<&SC>,
    schema_updates: Vec<SchemasUpdateCommand>,
) -> Result<(), SchemaError>
where
    SC: SubscriptionController + Send + Sync,
{
    // hack to suppress repeated logging of schema registrations
    // todo: Fix it
    tracing::subscriber::with_default(NoSubscriber::new(), || schemas.overwrite(schema_updates));

    if let Some(subscription_controller) = subscription_controller {
        let subscriptions = schemas.list_subscriptions(&[]);
        subscription_controller
            .update_subscriptions(subscriptions)
            .await?;
    }
    Ok(())
}
