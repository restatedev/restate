// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod rest_api;
mod service;
mod storage;

use codederror::CodedError;
use rest_api::MetaRestEndpoint;
use restate_schema_impl::Schemas;
use restate_types::retries::RetryPolicy;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use service::MetaService;
use std::net::SocketAddr;
use std::time::Duration;
use storage::FileMetaStorage;
use tokio::join;
use tracing::{debug, error};

use restate_service_client::{Connector, ServiceClient};
pub use restate_service_client::{
    Options as ServiceClientOptions, OptionsBuilder as ServiceClientOptionsBuilder,
    OptionsBuilderError as LambdaClientOptionsBuilderError,
};

/// # Meta options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "MetaOptions", default))]
#[builder(default)]
pub struct Options {
    /// # Rest endpoint address
    ///
    /// Address to bind for the Meta Operational REST APIs.
    rest_address: SocketAddr,

    /// # Rest concurrency limit
    ///
    /// Concurrency limit for the Meta Operational REST APIs.
    rest_concurrency_limit: usize,

    /// # Storage path
    ///
    /// Root path for Meta storage.
    storage_path: String,

    service_client: ServiceClientOptions,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            rest_address: "0.0.0.0:9070".parse().unwrap(),
            rest_concurrency_limit: 1000,
            storage_path: "target/meta/".to_string(),
            service_client: Default::default(),
        }
    }
}

impl Options {
    pub fn rest_address(&self) -> SocketAddr {
        self.rest_address
    }

    pub fn storage_path(&self) -> &str {
        &self.storage_path
    }

    pub fn build(self) -> Meta {
        let schemas = Schemas::default();

        let client = self.service_client.build();

        let service = MetaService::new(
            schemas.clone(),
            FileMetaStorage::new(self.storage_path.into()),
            // Total duration roughly 66 seconds
            RetryPolicy::exponential(
                Duration::from_millis(100),
                2.0,
                10,
                Some(Duration::from_secs(20)),
            ),
            client,
        );

        Meta {
            schemas,
            rest_endpoint: MetaRestEndpoint::new(self.rest_address, self.rest_concurrency_limit),
            service,
        }
    }
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error(transparent)]
    RestServer(
        #[from]
        #[code]
        rest_api::MetaRestServerError,
    ),
    #[error(transparent)]
    MetaService(
        #[from]
        #[code]
        service::MetaError,
    ),
    #[error("error when reloading the Meta storage: {0}")]
    MetaServiceInit(
        #[source]
        #[code]
        service::MetaError,
    ),
}

pub struct Meta {
    schemas: Schemas,
    rest_endpoint: MetaRestEndpoint,
    service: MetaService<FileMetaStorage, ServiceClient<Connector, hyper::Body>>,
}

impl Meta {
    pub fn schemas(&self) -> Schemas {
        self.schemas.clone()
    }

    pub async fn init(&mut self) -> Result<(), Error> {
        self.service.init().await.map_err(Error::MetaServiceInit)
    }

    pub async fn run(
        self,
        drain: drain::Watch,
        worker_handle: impl restate_worker_api::Handle + Clone + Send + Sync + 'static,
    ) -> Result<(), Error> {
        let (shutdown_signal, shutdown_watch) = drain::channel();

        let meta_handle = self.service.meta_handle();
        let schemas = self.schemas();

        let service_fut = self
            .service
            .run(worker_handle.clone(), shutdown_watch.clone());
        let rest_endpoint_fut =
            self.rest_endpoint
                .run(meta_handle, schemas, worker_handle, shutdown_watch);
        tokio::pin!(service_fut, rest_endpoint_fut);

        let shutdown = drain.signaled();

        tokio::select! {
            _ = shutdown => {
                debug!("Initiating shutdown of meta");

                // ignored because we are shutting down
                let _ = join!(shutdown_signal.drain(), service_fut, rest_endpoint_fut);

                debug!("Completed shutdown of meta");
            },
            result = &mut rest_endpoint_fut => {
                result?;
                panic!("Unexpected termination of the meta rest server. Please contact the Restate developers.");
            },
            result = &mut service_fut => {
                result?;
                panic!("Unexpected termination of the meta service. Please contact the Restate developers.");
            },
        }

        Ok(())
    }
}
