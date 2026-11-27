// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use axum::error_handling::HandleErrorLayer;
use http::{Request, Response, StatusCode};
use tokio::sync::watch;
use tower::ServiceBuilder;
use tower_http::classify::ServerErrorsFailureClass;
use tower_http::compression::CompressionLayer;
use tower_http::trace::TraceLayer;
use tracing::{Span, debug, info, info_span};

use restate_admin_rest_model::version::AdminApiVersion;
use restate_core::network::misdirected_requests::MisdirectedRequestsLayer;
use restate_core::network::{TransportConnect, net_util};
use restate_core::{Metadata, MetadataWriter, TaskCenter, TaskKind};
use restate_ingestion_client::IngestionClient;
use restate_limiter::rule_book::RuleBookObserver;
use restate_metadata_store::MetadataStoreClient;
use restate_service_client::HttpClient;
use restate_service_protocol_v4::discovery::ServiceDiscovery;
use restate_service_protocol_v4::serdes::SerdesClient;
use restate_storage_query_api::QueryEngine;
use restate_types::config::{AdminOptions, Configuration};
use restate_types::invocation::client::InvocationClient;
use restate_types::live::LiveLoad;
use restate_types::net::address::AdminPort;
use restate_types::net::listener::Listeners;
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_types::schema::registry::SchemaRegistry;
use restate_util_time::DurationExt;
use restate_wal_protocol::v2::{Envelope, Raw};

use crate::index_catalog::IndexCatalog;
use crate::metric_definitions::describe_metrics;
use crate::rest_api::{MAX_ADMIN_API_VERSION, MIN_ADMIN_API_VERSION};
use crate::schema_gate::SchemaGate;
use crate::schema_registry_integration::{MetadataService, TelemetryClient};
use crate::{rest_api, state};

#[derive(Debug, thiserror::Error)]
#[error("could not create the service client: {0}")]
pub struct BuildError(#[from] restate_service_client::BuildError);

pub struct AdminService<Metadata, Discovery, Telemetry, Invocations, Transport> {
    listeners: Listeners<AdminPort>,
    ingestion_client: IngestionClient<Transport, Envelope<Raw>>,
    schema_registry: SchemaRegistry<Metadata, Discovery, Telemetry>,
    serdes_client: SerdesClient,
    invocation_client: Invocations,
    query_engine: Arc<QueryEngine>,
    metadata_client: MetadataStoreClient,
    rule_book_observer: Option<Arc<dyn RuleBookObserver>>,
    replica_sets: PartitionReplicaSetStates,
    schema_gate: SchemaGate,
}

impl<Invocations, Transport>
    AdminService<MetadataService, ServiceDiscovery, TelemetryClient, Invocations, Transport>
where
    Invocations: InvocationClient + Send + Sync + Clone + 'static,
    Transport: TransportConnect,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        listeners: Listeners<AdminPort>,
        metadata_writer: MetadataWriter,
        ingestion_client: IngestionClient<Transport, Envelope<Raw>>,
        invocation_client: Invocations,
        serdes_client: SerdesClient,
        service_discovery: ServiceDiscovery,
        telemetry_http_client: Option<HttpClient>,
        query_engine: Arc<QueryEngine>,
        schema_gate: SchemaGate,
    ) -> Self {
        describe_metrics();

        let metadata_client = metadata_writer.raw_metadata_store_client().clone();
        Self {
            listeners,
            ingestion_client,
            schema_registry: SchemaRegistry::new(
                MetadataService(metadata_writer),
                service_discovery,
                TelemetryClient(telemetry_http_client),
            ),
            serdes_client,
            invocation_client,
            query_engine,
            metadata_client,
            rule_book_observer: None,
            replica_sets: PartitionReplicaSetStates::default(),
            schema_gate,
        }
    }

    /// The partitions' replica sets as this node observes them. The index catalog
    /// uses them to find each partition's leader, which statistics reads go to;
    /// without them it finds no evidence that the statistics tables fail.
    pub fn with_replica_sets(self, replica_sets: PartitionReplicaSetStates) -> Self {
        Self {
            replica_sets,
            ..self
        }
    }

    pub fn with_rule_book_observer(self, observer: Arc<dyn RuleBookObserver>) -> Self {
        Self {
            rule_book_observer: Some(observer),
            ..self
        }
    }

    pub async fn run(
        self,
        mut updateable_config: impl LiveLoad<Live = AdminOptions>,
    ) -> anyhow::Result<()> {
        let opts = updateable_config.live_load();
        let misdirected_requests = {
            let config = Configuration::pinned();
            config
                .common
                .reject_misdirected_requests
                .then(|| MisdirectedRequestsLayer::new(config.common.cluster_name()))
        };

        let (index_catalog, catalog) = watch::channel(IndexCatalog::initial());
        TaskCenter::spawn_child(
            TaskKind::Background,
            "index-catalog",
            crate::index_catalog::refresh(
                Arc::clone(&self.query_engine),
                self.replica_sets.clone(),
                index_catalog,
            ),
        )?;

        let mut rest_state = state::AdminServiceState::new(
            self.schema_registry,
            self.serdes_client,
            self.invocation_client,
            self.ingestion_client,
            self.metadata_client.clone(),
            self.query_engine,
            self.rule_book_observer,
        );
        rest_state.index_catalog = catalog;
        rest_state.query_engine_v2_default = Configuration::pinned()
            .common
            .experimental
            .is_query_engine_v2_default_enabled();
        rest_state.query_operator_tables = Configuration::pinned()
            .common
            .experimental
            .is_query_operator_tables_enabled();

        let router = axum::Router::new();

        #[cfg(feature = "metadata-api")]
        let router = router.merge(crate::metadata_api::router(&self.metadata_client));

        // Add now the tracing layer, this makes sure we don't log ui requests
        let router = router.layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<_>| {
                    info_span!(
                        target: "restate_admin::api",
                        "admin-api-request",
                        http.version = ?request.version(),
                        http.request.method = %request.method(),
                        url.path = request.uri().path(),
                        url.query = request.uri().query().unwrap_or_default(),
                        url.scheme = request.uri().scheme_str().unwrap_or("http")
                    )
                })
                // Just log on response
                .on_request(())
                .on_eos(())
                .on_body_chunk(())
                .on_response(
                    move |response: &Response<_>, latency: Duration, span: &Span| {
                        debug!(
                            name: "access-log",
                            target: "restate_admin::api",
                            parent: span,
                            { http.response.status_code = response.status().as_u16(), http.response.latency = %latency.friendly().to_seconds_span() },
                            "Replied"
                        )
                    },
                )
                .on_failure(
                    move |error: ServerErrorsFailureClass, latency: Duration, span: &Span| {
                        match error {
                            ServerErrorsFailureClass::StatusCode(_) => {
                                // No need to log it, on_response will log it already
                            }
                            ServerErrorsFailureClass::Error(error_string) => {
                                debug!(
                                    name: "access-log",
                                    target: "restate_admin::api",
                                    parent: span,
                                    { error.type = error_string, http.response.latency = %latency.friendly().to_seconds_span() },
                                    "Failed processing"
                                )
                            }
                        }
                    },
                ),
        );

        // Merge Web UI router
        #[cfg(feature = "serve-web-ui")]
        let router = if !opts.disable_web_ui {
            router.merge(crate::web_ui::web_ui_router())
        } else {
            router
        };

        // Merge meta API router
        let router = router.merge(rest_api::create_router(rest_state));

        let router = axum::Router::new()
            .merge(with_api_version_middleware(
                router.clone(),
                AdminApiVersion::Unknown,
            ))
            .nest("/v1", unsupported_api_version(AdminApiVersion::V1))
            .nest(
                "/v2",
                with_api_version_middleware(router.clone(), AdminApiVersion::V2),
            )
            .nest(
                "/v3",
                with_api_version_middleware(router.clone(), AdminApiVersion::V3),
            )
            .nest(
                "/v4",
                with_api_version_middleware(router.clone(), AdminApiVersion::V4),
            )
            .nest(
                "/v5",
                with_api_version_middleware(router, AdminApiVersion::V5),
            )
            .layer(CompressionLayer::new())
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_| async {
                        StatusCode::TOO_MANY_REQUESTS
                    }))
                    .layer(tower::load_shed::LoadShedLayer::new())
                    .layer(tower::limit::GlobalConcurrencyLimitLayer::new(
                        opts.concurrent_api_requests_limit(),
                    )),
            );

        let router = ServiceBuilder::new()
            .option_layer(misdirected_requests)
            .service(router.into_service());
        let service = hyper_util::service::TowerToHyperService::new(router);

        // Reads of the node-local schema (deployments, services, `/query`) answer from an empty or
        // stale catalog until this node has caught up with the schema in the metadata store.
        if let Err(err) = self.schema_gate.wait_until_caught_up().await {
            if restate_core::is_cancellation_requested() {
                debug!(%err, "Shutting down before the admin API started");
                return Ok(());
            }
            return Err(anyhow::Error::new(err).context("failed waiting for the schema"));
        }
        let schema_version = Metadata::current().schema_version();

        info!(
            %schema_version,
            "Admin API starting on: {}",
            TaskCenter::with_current(|tc| opts.advertised_address(tc.address_book()))
        );

        net_util::run_hyper_server(self.listeners, service, || (), None)
            .await
            .map_err(Into::into)
    }
}

fn unsupported_api_version(version: AdminApiVersion) -> axum::Router {
    axum::Router::new().fallback((
        StatusCode::BAD_REQUEST,
        format!(
            "Unsupported Admin API version {:?}. This server supports versions between {:?} and {:?}.",
            version, MIN_ADMIN_API_VERSION, MAX_ADMIN_API_VERSION,
        ),
    ))
}

fn with_api_version_middleware(router: axum::Router, version: AdminApiVersion) -> axum::Router {
    router.layer(axum::middleware::from_fn(
        move |mut request: axum::extract::Request, next: axum::middleware::Next| {
            request.extensions_mut().insert(version);
            next.run(request)
        },
    ))
}

#[cfg(test)]
mod tests {
    use std::future::{Future, ready};
    use std::net::SocketAddr;
    use std::num::NonZeroUsize;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use bytestring::ByteString;
    use googletest::prelude::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::watch;

    use restate_core::network::FailingConnector;
    use restate_core::partitions::PartitionRouting;
    use restate_core::{TaskCenter, TaskId, TaskKind, TestCoreEnv};
    use restate_ingestion_client::SessionOptions;
    use restate_metadata_store::{
        MetadataStore, MetadataStoreClient, ProvisionError, ReadError, WriteError,
    };
    use restate_service_client::{AssumeRoleCacheMode, ServiceClient};
    use restate_storage_query_api::QueryEngine;
    use restate_types::Version;
    use restate_types::Versioned;
    use restate_types::config::{
        AdminOptionsBuilder, CommonOptionsBuilder, Configuration, ListenerOptions,
    };
    use restate_types::deployment::{DeploymentAddress, Headers, HttpDeploymentAddress, Metadata};
    use restate_types::endpoint_manifest;
    use restate_types::identifiers::DeploymentId;
    use restate_types::identifiers::PartitionProcessorRpcRequestId;
    use restate_types::live::Live;
    use restate_types::metadata::{Precondition, VersionedValue};
    use restate_types::metadata_store::keys::SCHEMA_INFORMATION_KEY;
    use restate_types::net::address::{AdminPort, ListenerPort};
    use restate_types::net::listener::{AddressBook, Listeners};
    use restate_types::nodes_config::{NodesConfiguration, Role};
    use restate_types::partition_processor::client::{
        PartitionProcessorClient, PartitionProcessorClientError, PartitionProcessorRpc,
    };
    use restate_types::partitions::state::PartitionReplicaSetStates;
    use restate_types::retries::RetryPolicy;
    use restate_types::schema::Schema;
    use restate_types::schema::registry::{
        AllowBreakingChanges, ApplyMode, DiscoveryResponse, Overwrite, RegisterDeploymentRequest,
        SchemaRegistry, mocks::mock_arc_schema,
    };

    use super::*;

    /// The admin API never invokes anything in these tests.
    #[derive(Clone)]
    struct NeverInvocations;

    impl PartitionProcessorClient for NeverInvocations {
        fn send<R: PartitionProcessorRpc>(
            &self,
            _request_id: PartitionProcessorRpcRequestId,
            _request: R,
        ) -> impl Future<Output = std::result::Result<R::Response, PartitionProcessorClientError>> + Send
        {
            ready(Err(PartitionProcessorClientError::new(
                anyhow::anyhow!("unused in this test"),
                false,
            )))
        }
    }

    /// Listen on TCP only: unix sockets are not always available, e.g. in sandboxes.
    fn tcp_listener_options<P>() -> ListenerOptions<P>
    where
        P: ListenerPort + Default + serde::de::DeserializeOwned,
    {
        serde_json::from_str(r#"{"listen-mode": "tcp", "bind-address": "127.0.0.1:0"}"#)
            .expect("valid listener options")
    }

    /// Binds the admin listener on a random localhost port and publishes the address book, so the
    /// service can advertise its address.
    async fn bind_admin_listeners(data_dir: &Path) -> (Listeners<AdminPort>, SocketAddr) {
        let config = Configuration {
            common: CommonOptionsBuilder::default()
                .roles(Role::Admin.into())
                .fabric_listener_options(tcp_listener_options())
                .build()
                .expect("the common options are valid"),
            admin: AdminOptionsBuilder::default()
                .admin_listener_options(tcp_listener_options())
                .build()
                .expect("the admin options are valid"),
            ..Default::default()
        };

        let mut address_book = AddressBook::new(data_dir.to_owned());
        address_book
            .bind_from_config(&config)
            .await
            .expect("the admin listener binds");
        let address = address_book
            .get_bound_addresses::<AdminPort>()
            .expect("the admin listener is bound")
            .tcp_bind_address()
            .expect("the admin listener is on tcp");
        let listeners = address_book.take_listeners::<AdminPort>();
        TaskCenter::try_set_address_book(address_book);

        (listeners, address)
    }

    async fn spawn_admin_api(
        env: &TestCoreEnv<FailingConnector>,
        listeners: Listeners<AdminPort>,
        schema_gate: SchemaGate,
    ) -> TaskId {
        let config = Configuration::pinned();
        let retry_policy = RetryPolicy::exponential(Duration::from_millis(100), 2.0, Some(4), None);
        let service_client = ServiceClient::from_options(
            &config.worker.invoker.service_client,
            AssumeRoleCacheMode::None,
        )
        .expect("the default service client options are valid");

        let ingestion_client = IngestionClient::new(
            env.networking.clone(),
            env.metadata.updateable_partition_table(),
            PartitionRouting::new(PartitionReplicaSetStates::default(), TaskCenter::current()),
            NonZeroUsize::new(1024 * 1024).expect("non-zero"),
            SessionOptions::default(),
        );

        let service = AdminService::new(
            listeners,
            env.metadata_writer.clone(),
            ingestion_client,
            NeverInvocations,
            SerdesClient::new(service_client.clone()),
            ServiceDiscovery::new(retry_policy, service_client),
            None,
            Arc::new(QueryEngine::default()),
            schema_gate,
        );

        TaskCenter::spawn_child(
            TaskKind::AdminApiServer,
            "admin-api",
            service.run(Live::from_value(AdminOptions::default())),
        )
        .expect("the admin API starts")
    }

    fn greeter_service() -> endpoint_manifest::Service {
        endpoint_manifest::Service {
            abort_timeout: None,
            documentation: None,
            ingress_private: None,
            preemptable: None,
            ty: endpoint_manifest::ServiceType::Service,
            name: "greeter.Greeter".parse().expect("a valid service name"),
            retry_policy_exponentiation_factor: None,
            retry_policy_initial_interval: None,
            retry_policy_max_attempts: None,
            retry_policy_max_interval: None,
            handlers: vec![endpoint_manifest::Handler {
                abort_timeout: None,
                documentation: None,
                idempotency_retention: None,
                name: "greet".parse().expect("a valid handler name"),
                ty: None,
                input: None,
                output: None,
                retry_policy_exponentiation_factor: None,
                retry_policy_initial_interval: None,
                retry_policy_max_attempts: None,
                retry_policy_max_interval: None,
                metadata: Default::default(),
                inactivity_timeout: None,
                journal_retention: None,
                workflow_completion_retention: None,
                enable_lazy_state: None,
                eager_state_keys_whitelist: vec![],
                ingress_private: None,
                preemptable: None,
                retry_policy_on_max_attempts: None,
            }],
            idempotency_retention: None,
            inactivity_timeout: None,
            journal_retention: None,
            metadata: Default::default(),
            enable_lazy_state: None,
            eager_state_keys_whitelist: vec![],
            retry_policy_on_max_attempts: None,
        }
    }

    /// The schema a registration writes: the empty initial schema plus one HTTP deployment.
    async fn schema_with_a_deployment() -> (Schema, DeploymentId) {
        let schema = mock_arc_schema();
        schema.store(Arc::new(Schema::initial()));

        let registry = SchemaRegistry::new(
            Arc::clone(&schema),
            DiscoveryResponse::mock(vec![greeter_service()]),
            TelemetryClient(None),
        );
        let (_, deployment, _) = registry
            .register_deployment(RegisterDeploymentRequest {
                deployment_address: DeploymentAddress::Http(HttpDeploymentAddress::new(
                    "http://localhost:9080".parse().expect("a valid uri"),
                )),
                additional_headers: Headers::default(),
                metadata: Metadata::default(),
                use_http_11: false,
                allow_breaking: AllowBreakingChanges::No,
                overwrite: Overwrite::No,
                apply_mode: ApplyMode::Apply,
            })
            .await
            .expect("the deployment registers");
        let schema = schema.load_full().as_ref().clone();

        (schema, deployment.id)
    }

    /// Sends a GET over a fresh connection, returning its status and body, or `None` if no response
    /// arrives in time.
    async fn get(address: SocketAddr, path: &str, timeout: Duration) -> Option<(u16, String)> {
        let mut stream = TcpStream::connect(address)
            .await
            .expect("the listener is bound before the admin API serves");
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .expect("the request is written");

        let mut response = Vec::new();
        let read = tokio::time::timeout(timeout, stream.read_to_end(&mut response)).await;
        if !matches!(read, Ok(Ok(_))) {
            return None;
        }

        let response = String::from_utf8_lossy(&response);
        let (head, body) = response.split_once("\r\n\r\n")?;
        let status = head.split_whitespace().nth(1)?.parse().ok()?;
        Some((status, body.to_owned()))
    }

    /// Waits until the gate reports `expected`. Panics if it reports something else first, so the
    /// tests never proceed on stale state.
    async fn wait_for_gate(mut state: watch::Receiver<Option<Version>>, expected: Option<Version>) {
        loop {
            if *state.borrow_and_update() == expected {
                return;
            }
            state
                .changed()
                .await
                .expect("the gate stopped before reporting the expected version");
        }
    }

    /// Waits until `count` callers are blocked in the gate's wait. The admin API task is the only
    /// caller in these tests, so this observes that it has reached the wait.
    async fn wait_for_waiters(gate: &SchemaGate, count: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while gate.waiters() != count {
            assert!(
                tokio::time::Instant::now() < deadline,
                "expected {count} waiter(s) on the gate, saw {}",
                gate.waiters()
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Cancels the admin API and asserts that it stops without panicking.
    async fn cancel_admin_api(task: TaskId) {
        let handle = TaskCenter::cancel_task(task).expect("the admin API runs");
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("the admin API stops promptly")
            .expect("the admin API stops without panicking");
    }

    #[restate_core::test]
    async fn admin_api_serves_only_once_the_store_schema_is_loaded() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        env.metadata_writer
            .update(Arc::new(Schema::initial()))
            .await
            .expect("the node loads the empty schema");

        // The metadata store holds an empty schema of its own so that the node's own metadata
        // update task never fetches the newer one: the test decides when the node catches up.
        let store = MetadataStoreClient::new_in_memory();
        let (stored, deployment_id) = schema_with_a_deployment().await;
        store
            .put(
                SCHEMA_INFORMATION_KEY.clone(),
                &stored,
                Precondition::DoesNotExist,
            )
            .await
            .expect("the store holds a newer schema");

        let data_dir = restate_test_util::tempdir().expect("a temp dir");
        let (listeners, address) = bind_admin_listeners(data_dir.path()).await;
        let gate = SchemaGate::spawn(store.clone()).expect("the gate starts");
        let task = spawn_admin_api(&env, listeners, gate.clone()).await;

        // The gate is observably waiting for the store's version, which the node does not have,
        // and the admin API task is observably blocked on it.
        wait_for_gate(gate.watch(), Some(stored.version())).await;
        wait_for_waiters(&gate, 1).await;
        assert_that!(env.metadata.schema_version(), eq(Version::MIN));
        assert_that!(
            get(address, "/health", Duration::from_millis(200)).await,
            none()
        );
        assert_that!(
            get(address, "/deployments", Duration::from_millis(100)).await,
            none()
        );

        env.metadata_writer
            .update(Arc::new(stored.clone()))
            .await
            .expect("the node catches up");
        wait_for_gate(gate.watch(), None).await;

        let (status, body) = get(address, "/deployments", Duration::from_secs(5))
            .await
            .expect("the admin API answers once the schema is loaded");
        assert_that!(status, eq(200));
        assert_that!(
            body,
            contains_substring(format!("\"id\":\"{deployment_id}\""))
        );
        assert_that!(
            body,
            contains_substring("\"uri\":\"http://localhost:9080/\"")
        );
        assert_that!(body, contains_substring("greeter.Greeter"));

        cancel_admin_api(task).await;
        Ok(())
    }

    #[restate_core::test]
    async fn shutdown_while_waiting_for_the_schema_returns_promptly() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        env.metadata_writer
            .update(Arc::new(Schema::initial()))
            .await
            .expect("the node loads the empty schema");

        let store = MetadataStoreClient::new_in_memory();
        let (stored, _) = schema_with_a_deployment().await;
        store
            .put(
                SCHEMA_INFORMATION_KEY.clone(),
                &stored,
                Precondition::DoesNotExist,
            )
            .await
            .expect("the store holds a newer schema");

        let data_dir = restate_test_util::tempdir().expect("a temp dir");
        let (listeners, _) = bind_admin_listeners(data_dir.path()).await;
        let gate = SchemaGate::spawn(store.clone()).expect("the gate starts");
        let task = spawn_admin_api(&env, listeners, gate.clone()).await;

        // Cancel only once the gate is observably waiting for a version the node does not have and
        // the admin API task is blocked on it.
        wait_for_gate(gate.watch(), Some(stored.version())).await;
        wait_for_waiters(&gate, 1).await;
        cancel_admin_api(task).await;

        Ok(())
    }

    /// A metadata store that fails every operation until it is marked available.
    #[derive(Default)]
    struct UnavailableStore {
        available: Arc<AtomicBool>,
        schema_version: Option<Version>,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("the metadata store is unavailable")]
    struct StoreUnavailable;

    #[async_trait::async_trait]
    impl MetadataStore for UnavailableStore {
        async fn get(
            &self,
            _key: ByteString,
        ) -> std::result::Result<Option<VersionedValue>, ReadError> {
            Err(ReadError::terminal(StoreUnavailable))
        }

        async fn get_version(
            &self,
            _key: ByteString,
        ) -> std::result::Result<Option<Version>, ReadError> {
            if !self.available.load(Ordering::SeqCst) {
                return Err(ReadError::terminal(StoreUnavailable));
            }
            Ok(self.schema_version)
        }

        async fn put(
            &self,
            _key: ByteString,
            _value: VersionedValue,
            _precondition: Precondition,
        ) -> std::result::Result<(), WriteError> {
            Err(WriteError::terminal(StoreUnavailable))
        }

        async fn delete(
            &self,
            _key: ByteString,
            _precondition: Precondition,
        ) -> std::result::Result<(), WriteError> {
            Err(WriteError::terminal(StoreUnavailable))
        }

        async fn provision(
            &self,
            _nodes_configuration: &NodesConfiguration,
        ) -> std::result::Result<bool, ProvisionError> {
            Err(ProvisionError::terminal(StoreUnavailable))
        }
    }

    #[restate_core::test]
    async fn gate_opens_once_the_metadata_store_answers() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        env.metadata_writer
            .update(Arc::new(Schema::initial()))
            .await
            .expect("the node loads the empty schema");

        let available = Arc::new(AtomicBool::new(false));
        let store = UnavailableStore {
            available: Arc::clone(&available),
            schema_version: Some(Version::MIN),
        };
        let gate =
            SchemaGate::spawn(MetadataStoreClient::new(store, None)).expect("the gate starts");

        // The node has the empty schema locally, but the gate cannot know it is up to date while
        // the store keeps failing: terminal errors do not end the retries.
        assert!(
            tokio::time::timeout(Duration::from_millis(300), gate.wait_until_caught_up())
                .await
                .is_err(),
            "the gate cannot open while the metadata store keeps failing"
        );

        available.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(5), gate.wait_until_caught_up())
            .await
            .expect("the gate answers once the store does")
            .expect("the gate opened");

        Ok(())
    }
}
