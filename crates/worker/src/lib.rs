// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

extern crate core;

mod error;
mod handle;
mod ingress_integration;
mod invoker_integration;
mod metric_definitions;
mod partition;
mod partition_processor_manager;
mod subscription_controller;
mod subscription_integration;

use codederror::CodedError;

pub use crate::subscription_controller::SubscriptionController;
pub use crate::subscription_integration::SubscriptionControllerHandle;

use restate_bifrost::Bifrost;
use restate_core::network::MessageRouterBuilder;
use restate_core::network::Networking;
use restate_core::{task_center, Metadata, TaskKind};
use restate_ingress_dispatcher::IngressDispatcher;
use restate_ingress_http::HyperServerIngress;
use restate_ingress_kafka::Service as IngressKafkaService;
use restate_invoker_impl::{
    InvokerHandle as InvokerChannelServiceHandle, Service as InvokerService,
};
use restate_metadata_store::MetadataStoreClient;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_query_datafusion::context::QueryContext;
use restate_storage_query_postgres::service::PostgresQueryService;
use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::schema::UpdateableSchema;

pub use self::error::*;
pub use self::handle::*;
use crate::ingress_integration::InvocationStorageReaderImpl;
use crate::invoker_integration::EntryEnricher;
use crate::partition::storage::invoker::InvokerStorageReader;
use crate::partition_processor_manager::PartitionProcessorManager;

type PartitionProcessor = partition::PartitionProcessor<
    ProtobufRawEntryCodec,
    InvokerChannelServiceHandle<InvokerStorageReader<PartitionStore>>,
>;

type ExternalClientIngress =
    HyperServerIngress<UpdateableSchema, IngressDispatcher, InvocationStorageReaderImpl>;

#[derive(Debug, thiserror::Error, CodedError)]
#[error("failed creating worker: {0}")]
pub enum BuildError {
    Datafusion(
        #[from]
        #[code]
        restate_storage_query_datafusion::BuildError,
    ),
    #[error("failed creating worker: {0}")]
    RocksDB(
        #[from]
        #[code]
        restate_partition_store::BuildError,
    ),
    #[error("failed opening partition store: {0}")]
    RocksDb(
        #[from]
        #[code]
        restate_rocksdb::RocksError,
    ),
    #[code(unknown)]
    Invoker(#[from] restate_invoker_impl::BuildError),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("thread '{thread}' panicked: {cause}")]
    #[code(unknown)]
    ThreadPanic {
        thread: &'static str,
        cause: restate_types::errors::ThreadJoinError,
    },
}

pub struct Worker {
    updateable_config: Live<Configuration>,
    storage_query_context: QueryContext,
    storage_query_postgres: PostgresQueryService,
    #[allow(clippy::type_complexity)]
    invoker: InvokerService<
        InvokerStorageReader<PartitionStore>,
        EntryEnricher<UpdateableSchema, ProtobufRawEntryCodec>,
        UpdateableSchema,
    >,
    external_client_ingress: ExternalClientIngress,
    ingress_kafka: IngressKafkaService,
    subscription_controller_handle: SubscriptionControllerHandle,
    partition_processor_manager: PartitionProcessorManager,
}

impl Worker {
    pub async fn create(
        updateable_config: Live<Configuration>,
        metadata: Metadata,
        networking: Networking,
        bifrost: Bifrost,
        router_builder: &mut MessageRouterBuilder,
        schema_view: UpdateableSchema,
        metadata_store_client: MetadataStoreClient,
    ) -> Result<Self, BuildError> {
        metric_definitions::describe_metrics();

        let ingress_dispatcher = IngressDispatcher::new(bifrost.clone());
        router_builder.add_message_handler(ingress_dispatcher.clone());

        let config = updateable_config.pinned();

        // ingress_kafka
        let ingress_kafka = IngressKafkaService::new(ingress_dispatcher.clone());
        let subscription_controller_handle =
            subscription_integration::SubscriptionControllerHandle::new(
                config.ingress.clone(),
                ingress_kafka.create_command_sender(),
            );

        let partition_store_manager = PartitionStoreManager::create(
            updateable_config.clone().map(|c| &c.worker.storage),
            updateable_config.clone().map(|c| &c.worker.storage.rocksdb),
            &[],
        )
        .await?;

        // http ingress
        let ingress_http = HyperServerIngress::from_options(
            &config.ingress,
            ingress_dispatcher.clone(),
            schema_view.clone(),
            InvocationStorageReaderImpl::new(partition_store_manager.clone()),
        );

        let invoker = InvokerService::from_options(
            &config.common.service_client,
            &config.worker.invoker,
            EntryEnricher::new(schema_view.clone()),
            schema_view.clone(),
        )?;

        let partition_processor_manager = PartitionProcessorManager::new(
            task_center(),
            updateable_config.clone(),
            metadata.clone(),
            metadata_store_client,
            partition_store_manager.clone(),
            router_builder,
            networking,
            bifrost,
            invoker.handle(),
        );

        let storage_query_context = QueryContext::create(
            &config.admin.query_engine,
            partition_processor_manager.handle(),
            partition_store_manager.clone(),
            invoker.status_reader(),
            schema_view.clone(),
        )
        .await?;

        let storage_query_postgres = PostgresQueryService::from_options(
            &config.admin.query_engine,
            storage_query_context.clone(),
        );

        Ok(Self {
            updateable_config,
            storage_query_context,
            storage_query_postgres,
            invoker,
            external_client_ingress: ingress_http,
            ingress_kafka,
            subscription_controller_handle,
            partition_processor_manager,
        })
    }

    pub fn subscription_controller_handle(&self) -> SubscriptionControllerHandle {
        self.subscription_controller_handle.clone()
    }

    pub fn storage_query_context(&self) -> &QueryContext {
        &self.storage_query_context
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let tc = task_center();

        // Ingress RPC server
        tc.spawn_child(
            TaskKind::IngressServer,
            "ingress-rpc-server",
            None,
            self.external_client_ingress.run(),
        )?;

        // Postgres external server
        tc.spawn_child(
            TaskKind::RpcServer,
            "postgres-query-server",
            None,
            self.storage_query_postgres.run(),
        )?;

        // Kafka Ingress
        tc.spawn_child(
            TaskKind::SystemService,
            "kafka-ingress",
            None,
            self.ingress_kafka
                .run(self.updateable_config.clone().map(|c| &c.ingress)),
        )?;

        // Invoker service
        tc.spawn_child(
            TaskKind::SystemService,
            "invoker",
            None,
            self.invoker
                .run(self.updateable_config.clone().map(|c| &c.worker.invoker)),
        )?;

        tc.spawn_child(
            TaskKind::SystemService,
            "partition-processor-manager",
            None,
            self.partition_processor_manager.run(),
        )?;

        Ok(())
    }
}
