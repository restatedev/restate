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
use tonic::transport::Channel;
use tracing::debug;
use tracing::subscriber::NoSubscriber;

use crate::net_utils::NetworkAddressExt;
use restate_bifrost::{Bifrost, BifrostService};
use restate_node_services::metadata::metadata_svc_client::MetadataSvcClient;
use restate_node_services::metadata::FetchSchemasRequest;
use restate_schema_api::subscription::SubscriptionResolver;
use restate_schema_impl::{Schemas, SchemasUpdateCommand};
use restate_storage_query_datafusion::context::QueryContext;
use restate_storage_rocksdb::RocksDBStorage;
use restate_task_center::task_center;
use restate_task_center::TaskKind;
use restate_types::nodes_config::NetworkAddress;
use restate_types::NodeId;
use restate_worker::{SubscriptionControllerHandle, Worker, WorkerCommandSender};
use restate_worker_api::SubscriptionController;

use crate::Options;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum WorkerRoleError {
    #[error("worker failed: {0}")]
    Worker(
        #[from]
        #[code]
        restate_worker::Error,
    ),
    #[error("bifrost failed: {0}")]
    #[code(unknown)]
    Bifrost(#[from] restate_bifrost::Error),
    #[error(transparent)]
    Schema(
        #[from]
        #[code]
        SchemaError,
    ),
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
        restate_schema_impl::SchemasUpdateError,
    ),
    #[error("failed updating subscriptions: {0}")]
    #[code(unknown)]
    Subscription(#[from] restate_worker_api::Error),
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
    bifrost: BifrostService,
}

impl WorkerRole {
    pub fn rocksdb_storage(&self) -> &RocksDBStorage {
        self.worker.rocksdb_storage()
    }

    pub fn bifrost_handle(&self) -> Bifrost {
        self.bifrost.handle()
    }

    pub fn worker_command_tx(&self) -> WorkerCommandSender {
        self.worker.worker_command_tx()
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

    pub async fn start(
        self,
        my_node_id: NodeId,
        admin_address: NetworkAddress,
    ) -> anyhow::Result<()> {
        // todo: only run subscriptions on node 0 once being distributed
        let subscription_controller = Some(self.worker.subscription_controller_handle());

        // Ensures bifrost has initial metadata synced up before starting the worker.
        self.bifrost.start().await?;

        let channel = admin_address.connect_lazy()?;
        let mut metadata_svc_client = MetadataSvcClient::new(channel);

        // Fetch latest schema information and fail if this is not possible
        Self::fetch_and_update_schemas(
            &self.schemas,
            subscription_controller.as_ref(),
            &mut metadata_svc_client,
        )
        .await?;

        // todo: replace by watchdog
        task_center().spawn_child(
            TaskKind::MetadataBackgroundSync,
            "schema-updater",
            None,
            Self::reload_schemas(subscription_controller, self.schemas, metadata_svc_client),
        )?;

        task_center().spawn_child(
            TaskKind::RoleRunner,
            "worker-service",
            None,
            self.worker.run(my_node_id),
        )?;

        Ok(())
    }

    async fn reload_schemas<SC>(
        subscription_controller: Option<SC>,
        schemas: Schemas,
        mut metadata_svc_client: MetadataSvcClient<Channel>,
    ) -> anyhow::Result<()>
    where
        SC: SubscriptionController + Clone + Send + Sync,
    {
        // todo: make this configurable
        let mut fetch_interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            fetch_interval.tick().await;

            debug!("Trying to fetch schema information");

            Self::ignore_fetch_error(
                Self::fetch_and_update_schemas(
                    &schemas,
                    subscription_controller.as_ref(),
                    &mut metadata_svc_client,
                )
                .await,
            )?;
        }
    }

    fn ignore_fetch_error(result: Result<(), SchemaError>) -> Result<(), SchemaError> {
        if let Err(err) = result {
            match err {
                SchemaError::Fetch(err) => {
                    debug!("Failed fetching schema information: {err}. Retrying.");
                }
                SchemaError::Decode(_) | SchemaError::Update(_) | SchemaError::Subscription(_) => {
                    Err(err)?
                }
            }
        }
        Ok(())
    }

    async fn fetch_and_update_schemas<SC>(
        schemas: &Schemas,
        subscription_controller: Option<&SC>,
        metadata_svc_client: &mut MetadataSvcClient<Channel>,
    ) -> Result<(), SchemaError>
    where
        SC: SubscriptionController + Send + Sync,
    {
        let schema_updates = Self::fetch_schemas(metadata_svc_client).await?;
        update_schemas(schemas, subscription_controller, schema_updates).await?;

        Ok(())
    }

    async fn fetch_schemas(
        metadata_svc_client: &mut MetadataSvcClient<Channel>,
    ) -> Result<Vec<SchemasUpdateCommand>, SchemaError> {
        let response = metadata_svc_client
            // todo introduce schema version information to avoid fetching and overwriting the schema information
            //  over and over again
            .fetch_schemas(FetchSchemasRequest {})
            .await?;

        let (schema_updates, _) = bincode::serde::decode_from_slice::<Vec<SchemasUpdateCommand>, _>(
            &response.into_inner().schemas_bin,
            bincode::config::standard(),
        )?;
        Ok(schema_updates)
    }
}

impl TryFrom<Options> for WorkerRole {
    type Error = WorkerRoleBuildError;

    fn try_from(options: Options) -> Result<Self, Self::Error> {
        let bifrost = options.bifrost.build(options.worker.partitions);
        let schemas = Schemas::default();
        let worker = options.worker.build(schemas.clone(), bifrost.handle())?;

        Ok(WorkerRole {
            schemas,
            worker,
            bifrost,
        })
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
    tracing::subscriber::with_default(NoSubscriber::new(), || schemas.overwrite(schema_updates))?;

    if let Some(subscription_controller) = subscription_controller {
        let subscriptions = schemas.list_subscriptions(&[]);
        subscription_controller
            .update_subscriptions(subscriptions)
            .await?;
    }
    Ok(())
}
