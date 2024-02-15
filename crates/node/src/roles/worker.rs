// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Options;
use codederror::CodedError;
use futures::TryFutureExt;
use restate_bifrost::{Bifrost, BifrostService};
use restate_node_services::schema::FetchSchemasRequest;
use restate_schema_api::subscription::SubscriptionResolver;
use restate_schema_impl::{Schemas, SchemasUpdateCommand};
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::NodeId;
use restate_worker::{Worker, WorkerCommandSender};
use restate_worker_api::SubscriptionController;
use std::time::Duration;
use tokio::task::JoinSet;
use tonic::transport::Channel;
use tracing::subscriber::NoSubscriber;
use tracing::{debug, info};

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
    #[error("component panicked: {0}")]
    #[code(unknown)]
    ComponentPanic(tokio::task::JoinError),
    #[error("failed decoding grpc payload: {0}")]
    #[code(unknown)]
    DecodeError(#[from] bincode::error::DecodeError),
    #[error("failed updating schemas: {0}")]
    Schema(
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

    pub async fn run(
        self,
        _node_id: NodeId,
        shutdown_watch: drain::Watch,
    ) -> Result<(), WorkerRoleError> {
        let shutdown_signal = shutdown_watch.signaled();

        let (inner_shutdown_signal, inner_shutdown_watch) = drain::channel();

        // todo: only run subscriptions on node 0 once being distributed
        let subscription_controller = Some(self.worker.subscription_controller_handle());

        let mut component_set = JoinSet::new();

        // Ensures bifrost has initial metadata synced up before starting the worker.
        let mut bifrost_join_handle = self.bifrost.start(inner_shutdown_watch.clone()).await?;

        component_set.spawn(
            self.worker
                .run(inner_shutdown_watch)
                .map_ok(|_| "worker")
                .map_err(WorkerRoleError::Worker),
        );
        component_set.spawn(
            Self::reload_schemas(
                subscription_controller,
                self.schemas,
                // todo: make this configurable
                Channel::builder("http://127.0.0.1:5122/".parse().expect("valid uri"))
                    .connect_lazy(),
            )
            .map_ok(|_| "schema-update"),
        );

        tokio::select! {
            _ = shutdown_signal => {
                info!("Stopping worker role");
                inner_shutdown_signal.drain().await;
                // ignoring result because we are shutting down
                let _ = tokio::join!(component_set.shutdown(), bifrost_join_handle);
            },
            Some(component_result) = component_set.join_next() => {
                let component_name = component_result.map_err(WorkerRoleError::ComponentPanic)??;
                panic!("Unexpected termination of component '{component_name}'");
            }
            bifrost_result = &mut bifrost_join_handle => {
                bifrost_result.map_err(WorkerRoleError::ComponentPanic)??;
                panic!("Unexpected termination of bifrost service");
            }
        }

        Ok(())
    }

    async fn reload_schemas<SC>(
        subscription_controller: Option<SC>,
        schemas: Schemas,
        schema_channel: Channel,
    ) -> Result<(), WorkerRoleError>
    where
        SC: SubscriptionController + Clone + Send + Sync,
    {
        // todo: make this configurable
        let mut fetch_interval = tokio::time::interval(Duration::from_secs(5));
        let mut schema_grpc_client =
            restate_node_services::schema::schema_client::SchemaClient::new(schema_channel);

        loop {
            fetch_interval.tick().await;

            debug!("Trying to fetch schema information");

            let result = schema_grpc_client
                // todo introduce schema version information to avoid fetching and overwriting the schema information
                //  over and over again
                .fetch_schemas(FetchSchemasRequest {})
                .await;
            match result {
                Ok(response) => {
                    let (schema_updates, _) =
                        bincode::serde::decode_from_slice::<Vec<SchemasUpdateCommand>, _>(
                            &response.into_inner().schemas,
                            bincode::config::standard(),
                        )?;

                    // hack to suppress repeated logging of schema registrations
                    // todo: Fix it
                    tracing::subscriber::with_default(NoSubscriber::new(), || {
                        schemas.overwrite(schema_updates)
                    })?;

                    if let Some(subscription_controller) = subscription_controller.as_ref() {
                        Self::update_subscriptions(&schemas, subscription_controller.clone())
                            .await?;
                    }
                }
                Err(err) => {
                    debug!("Failed fetching schema information: {err}")
                }
            };
        }
    }

    async fn update_subscriptions(
        schemas: &Schemas,
        subscription_controller: impl SubscriptionController + Send + Sync,
    ) -> Result<(), WorkerRoleError> {
        let subscriptions = schemas.list_subscriptions(&[]);
        subscription_controller
            .update_subscriptions(subscriptions)
            .await?;

        Ok(())
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
