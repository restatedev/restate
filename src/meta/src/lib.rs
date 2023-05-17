mod rest_api;
mod service;
mod storage;

use codederror::CodedError;
use rest_api::MetaRestEndpoint;
use restate_common::retry_policy::RetryPolicy;
use restate_common::worker_command::WorkerCommandSender;
use restate_ingress_grpc::ReflectionRegistry;
use restate_service_key_extractor::KeyExtractorsRegistry;
use restate_service_metadata::{InMemoryMethodDescriptorRegistry, InMemoryServiceEndpointRegistry};
use serde::{Deserialize, Serialize};
use service::MetaService;
use std::net::SocketAddr;
use std::time::Duration;
use storage::FileMetaStorage;
use tokio::join;
use tracing::{debug, error};

/// # Meta options
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "MetaOptions"))]
pub struct Options {
    /// # Rest endpoint address
    ///
    /// Address to bind for the Meta Operational REST APIs.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_rest_address")
    )]
    rest_address: SocketAddr,

    /// # Rest concurrency limit
    ///
    /// Concurrency limit for the Meta Operational REST APIs.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_rest_concurrency_limit")
    )]
    rest_concurrency_limit: usize,

    /// # Storage path
    ///
    /// Root path for Meta storage.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_storage_path")
    )]
    storage_path: String,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            rest_address: Options::default_rest_address(),
            rest_concurrency_limit: Options::default_rest_concurrency_limit(),
            storage_path: Options::default_storage_path(),
        }
    }
}

impl Options {
    fn default_rest_address() -> SocketAddr {
        "0.0.0.0:8081".parse().unwrap()
    }

    fn default_rest_concurrency_limit() -> usize {
        1000
    }

    fn default_storage_path() -> String {
        "target/meta/".to_string()
    }

    pub fn rest_address(&self) -> SocketAddr {
        self.rest_address
    }

    pub fn storage_path(&self) -> &str {
        &self.storage_path
    }

    pub fn build(self) -> Meta {
        let key_extractors_registry = KeyExtractorsRegistry::default();
        let method_descriptors_registry = InMemoryMethodDescriptorRegistry::default();
        let reflections_registry = ReflectionRegistry::default();
        let service_endpoint_registry = InMemoryServiceEndpointRegistry::default();
        let service = MetaService::new(
            key_extractors_registry.clone(),
            method_descriptors_registry.clone(),
            service_endpoint_registry.clone(),
            reflections_registry.clone(),
            FileMetaStorage::new(self.storage_path.into()),
            // Total duration roughly 102 seconds
            RetryPolicy::exponential(Duration::from_millis(100), 2.0, 9, None),
        );

        Meta {
            key_extractors_registry,
            method_descriptors_registry,
            reflections_registry,
            service_endpoint_registry,
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
    #[code(unknown)]
    MetaService(#[from] service::MetaError),
}

pub struct Meta {
    key_extractors_registry: KeyExtractorsRegistry,
    method_descriptors_registry: InMemoryMethodDescriptorRegistry,
    reflections_registry: ReflectionRegistry,
    service_endpoint_registry: InMemoryServiceEndpointRegistry,

    rest_endpoint: MetaRestEndpoint,
    service: MetaService<FileMetaStorage>,
}

impl Meta {
    pub fn key_extractors_registry(&self) -> KeyExtractorsRegistry {
        self.key_extractors_registry.clone()
    }

    pub fn method_descriptor_registry(&self) -> InMemoryMethodDescriptorRegistry {
        self.method_descriptors_registry.clone()
    }

    pub fn reflections_registry(&self) -> ReflectionRegistry {
        self.reflections_registry.clone()
    }

    pub fn service_endpoint_registry(&self) -> InMemoryServiceEndpointRegistry {
        self.service_endpoint_registry.clone()
    }

    pub async fn init(&mut self) -> Result<(), Error> {
        self.service.init().await?;
        Ok(())
    }

    pub async fn run(
        self,
        drain: drain::Watch,
        worker_command_tx: WorkerCommandSender,
    ) -> Result<(), Error> {
        let (shutdown_signal, shutdown_watch) = drain::channel();

        let meta_handle = self.service.meta_handle();
        let service_endpoint_registry = self.service_endpoint_registry();
        let method_descriptor_registry = self.method_descriptor_registry();

        let service_fut = self.service.run(shutdown_watch.clone());
        let rest_endpoint_fut = self.rest_endpoint.run(
            meta_handle,
            service_endpoint_registry,
            method_descriptor_registry,
            worker_command_tx,
            shutdown_watch,
        );
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
