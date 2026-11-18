// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, http};
use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::json::writer::JsonArray;
use datafusion::common::DataFusionError;
use futures::{StreamExt, TryStreamExt};
use http::{HeaderMap, HeaderValue};
use http_body::Frame;
use http_body_util::StreamBody;
use parking_lot::Mutex;
use serde::Serialize;

use restate_admin_rest_model::query::QueryRequest;
use restate_core::network::TransportConnect;
use restate_storage_query_api::errors::{QueryExecutionError, SessionError};
use restate_storage_query_api::{
    QueryEngine, QueryEngineTable, QueryEngineVersion, QueryOptions, QuerySession,
    RecordBatchWriter, SessionOptions, WriteRecordBatchStream,
};
use restate_storage_query_datafusion::config::ConfigTable;
use restate_types::invocation::client::InvocationClient;
use restate_types::schema::registry::{DiscoveryClient, MetadataService, TelemetryClient};

use crate::query_context::{
    QUERY_ENGINE_HEADER, collect_query_headers, engine_version_from_headers,
};
use crate::state::AdminServiceState;

const RETRY_AFTER_HEADER: &str = "Retry-After";
const QUERY_SESSION_HEADER: &str = "x-restate-query-session-id";
const QUERY_LEVEL_HEADER: &str = "x-restate-query-level";

fn operator_level_from_headers(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(QUERY_LEVEL_HEADER).iter();
    matches!(values.next().map(HeaderValue::as_bytes), Some(b"operator")) && values.next().is_none()
}

/// Error response for query endpoint.
#[derive(Debug, Serialize, utoipa::ToSchema)]
struct QueryErrorBody {
    message: String,
}

/// Errors that can occur when executing a query.
#[derive(Debug, thiserror::Error)]
pub(crate) enum QueryError {
    #[error("X-Restate-Query-Engine must contain a single value: v1 or v2")]
    InvalidEngineSelection,
    #[error("Query engine {0} is not enabled on this node")]
    VersionUnavailable(QueryEngineVersion),
    #[error(transparent)]
    Datafusion(#[from] datafusion::error::DataFusionError),
    #[error("Query service not available")]
    Unavailable,
    #[error("Rate limited")]
    RateLimited(#[from] gardal::RateLimited),
    #[error("Session initialization error: {0}")]
    SessionInitialization(datafusion::error::DataFusionError),
}

impl From<QueryExecutionError> for QueryError {
    fn from(err: QueryExecutionError) -> Self {
        match err {
            QueryExecutionError::DataFusion(e) => Self::Datafusion(e),
        }
    }
}

impl From<SessionError> for QueryError {
    fn from(err: SessionError) -> Self {
        match err {
            SessionError::EngineDisabled => Self::Unavailable,
            SessionError::VersionUnavailable(version) => Self::VersionUnavailable(version),
            SessionError::RateLimited(e) => Self::RateLimited(e),
            SessionError::DataFusion(e) => Self::SessionInitialization(e),
        }
    }
}

impl IntoResponse for QueryError {
    fn into_response(self) -> Response {
        let mut headers = http::HeaderMap::new();
        let status_code = match &self {
            QueryError::Datafusion(datafusion::error::DataFusionError::Plan(_))
            | QueryError::Datafusion(datafusion::error::DataFusionError::SchemaError(_, _))
            | QueryError::Datafusion(datafusion::error::DataFusionError::SQL(_, _)) => {
                StatusCode::BAD_REQUEST
            }
            QueryError::Datafusion(_) | QueryError::SessionInitialization(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            QueryError::InvalidEngineSelection => StatusCode::BAD_REQUEST,
            QueryError::Unavailable | QueryError::VersionUnavailable(_) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            QueryError::RateLimited(e) => {
                headers.insert(
                    RETRY_AFTER_HEADER,
                    HeaderValue::from(std::cmp::max(e.earliest_retry_after().as_secs(), 1)),
                );
                StatusCode::TOO_MANY_REQUESTS
            }
        };

        (
            status_code,
            headers,
            Json(QueryErrorBody {
                message: self.to_string(),
            }),
        )
            .into_response()
    }
}

/// Query the system and service state by using SQL.
#[utoipa::path(
    post,
    path = "/query",
    operation_id = "query",
    tag = "introspection",
    params(
        ("X-Restate-Query-Engine" = Option<String>, Header, description = "Query engine to run the query on: v1, or v2 (experimental, distributed). Without it, the node's default engine runs the query"),
        ("X-Restate-Query-Level" = Option<String>, Header, description = "`operator` exposes the cluster tables, except `config`, when `experimental-enable-query-operator-tables` is set; ignored otherwise")
    ),
    responses(
        (status = 200, description = "Query results",
            content (
                ("application/vnd.apache.arrow.stream"),
                ("application/json", example = json!({"rows": []}))
            ),
            headers(
                ("X-Restate-Query-Session-Id" = String, description = "Server-generated query session ID"),
                ("X-Restate-Query-Engine" = String, description = "Selected query engine: v1 or v2 (distributed)"),
                ("Server-Timing" = String, description = "Query planning duration in milliseconds")
            )),
        (status = 400, description = "Invalid query or engine selection", body = QueryErrorBody),
        (status = 500, description = "Internal query error", body = QueryErrorBody),
        (status = 503, description = "Query service not available", body = QueryErrorBody),
    )
)]
pub(crate) async fn query<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    headers: HeaderMap,
    Json(payload): Json<QueryRequest>,
) -> Result<Response, QueryError>
where
    Metadata: MetadataService + Send + Sync + Clone + 'static,
    Discovery: DiscoveryClient + Send + Sync + Clone + 'static,
    Telemetry: TelemetryClient + Send + Sync + Clone + 'static,
    Invocations: InvocationClient + Send + Sync + Clone + 'static,
    Transport: TransportConnect,
{
    let default = if state.query_engine_v2_default {
        QueryEngineVersion::V2
    } else {
        QueryEngineVersion::V1
    };
    query_with_engine(
        state.query_engine.as_ref(),
        default,
        state.query_operator_tables,
        &headers,
        payload,
    )
    .await
}

async fn query_with_engine(
    engine: &QueryEngine,
    default: QueryEngineVersion,
    operator_tables_enabled: bool,
    headers: &HeaderMap,
    payload: QueryRequest,
) -> Result<Response, QueryError> {
    let version =
        engine_version_from_headers(headers, default).ok_or(QueryError::InvalidEngineSelection)?;
    let mut options = SessionOptions {
        engine_version: Some(version),
        headers: collect_query_headers(headers),
        excluded_tables: vec![ConfigTable::identity()],
        ..Default::default()
    };
    if operator_tables_enabled && operator_level_from_headers(headers) {
        options.schemas.push("cluster".into());
    }
    let mut response = async {
        let session = engine.create_session(options)?;
        tracing::info!(target: "query_engine", session = %session.session_id(),
            engine = version.as_str(), "Selected query engine");
        query_in_session(session.as_ref(), headers, payload).await
    }
    .await
    .into_response();
    response.headers_mut().insert(
        QUERY_ENGINE_HEADER,
        HeaderValue::from_static(version.as_str()),
    );
    Ok(response)
}

async fn query_in_session(
    session: &dyn QuerySession,
    headers: &HeaderMap,
    payload: QueryRequest,
) -> Result<Response, QueryError> {
    let session_id = HeaderValue::from_str(session.session_id())
        .map_err(|err| DataFusionError::Internal(format!("Invalid query session ID: {err}")))?;
    // Preserve correlation on planning and first-batch errors as well as success.
    let mut response = execute_query(session, headers, payload)
        .await
        .into_response();
    response
        .headers_mut()
        .insert(QUERY_SESSION_HEADER, session_id);
    Ok(response)
}

async fn execute_query(
    session: &dyn QuerySession,
    headers: &HeaderMap,
    payload: QueryRequest,
) -> Result<Response, QueryError> {
    let query_result = session.execute(&payload.query, QueryOptions {}).await?;
    let server_timing = format!(
        "planning;dur={:.3}",
        query_result.metadata.planning_duration.as_secs_f64() * 1000.0
    );

    let (result_stream, content_type) = match headers.get(http::header::ACCEPT) {
        Some(v) if v == HeaderValue::from_static("application/json") => (
            WriteRecordBatchStream::<JsonWriter>::new(query_result.stream, query_result.metadata)?
                .map_ok(Frame::data)
                .left_stream(),
            "application/json",
        ),
        _ => (
            WriteRecordBatchStream::<StreamWriter<Vec<u8>>>::new(
                query_result.stream,
                query_result.metadata,
            )?
            .map_ok(Frame::data)
            .right_stream(),
            "application/vnd.apache.arrow.stream",
        ),
    };

    let mut result_stream = result_stream.peekable();

    // return an error (instead of just closing the stream) if there is a error getting the first record batch (eg, out of memory)
    if let Some(Err(_)) = futures::stream::Peekable::peek(Pin::new(&mut result_stream)).await {
        let err = result_stream.next().await.unwrap().unwrap_err();
        return Err(err.into());
    }

    Ok(Response::builder()
        .header(http::header::CONTENT_TYPE, content_type)
        .header("server-timing", server_timing)
        .body(StreamBody::new(result_stream))
        .expect("content-type header is correct")
        .into_response())
}

#[derive(Clone)]
// unfortunately the json writer doesnt give a way to get a mutable reference to the underlying writer, so we need another pointer in to its buffer
// we use a lock here to help make the writer send/sync, despite it being totally uncontended :(
struct LockWriter(Arc<Mutex<Vec<u8>>>);

impl LockWriter {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn take(&self) -> Vec<u8> {
        let mut vec = self.0.lock();
        let new_vec = Vec::with_capacity(vec.capacity());
        std::mem::replace(&mut vec, new_vec)
    }
}

impl Write for LockWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.lock().flush()
    }
}

pub(crate) struct JsonWriter {
    json_writer: datafusion::arrow::json::Writer<LockWriter, JsonArray>,
    lock_writer: LockWriter,
    finished: bool,
}

impl RecordBatchWriter for JsonWriter {
    fn new(_schema: &Schema) -> Result<Self, DataFusionError> {
        let mut lock_writer = LockWriter::new();
        // we write out under 'rows' key so that we may add extra keys later (eg 'schema')
        lock_writer.write_all(br#"{"rows":"#)?;
        Ok(Self {
            json_writer: datafusion::arrow::json::Writer::new(lock_writer.clone()),
            lock_writer,
            finished: false,
        })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<Bytes, DataFusionError> {
        self.json_writer.write(batch)?;
        Ok(Bytes::from(self.lock_writer.take()))
    }

    fn finish(&mut self) -> Result<Bytes, DataFusionError> {
        if !self.finished {
            self.finished = true;

            self.json_writer.finish()?;
            self.lock_writer.write_all(b"}")?;
        }
        Ok(Bytes::from(self.lock_writer.take()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use http_body_util::BodyExt;

    use restate_storage_query_datafusion::DataFusionEnv;
    use restate_storage_query_datafusion::context::DataFusionQueryEngine;

    use super::*;

    #[tokio::test]
    async fn engine_selection_is_explicit_and_preserves_responses() {
        let engine = |version, sessions: Arc<AtomicUsize>| {
            let env = DataFusionEnv::new(10 * 1024 * 1024, None, None, &HashMap::new())
                .unwrap()
                .with_mock_clock(restate_types::clock::MockClock::new())
                .unwrap();
            let inner = DataFusionQueryEngine::from_env(env, None);
            QueryEngine::new(version, move |mut opts| {
                assert!(!opts.headers.contains_key(QUERY_ENGINE_HEADER));
                sessions.fetch_add(1, Ordering::Relaxed);
                // Both counters use v1 to exercise the transport independently of execution.
                opts.engine_version = None;
                inner.create_session(opts)
            })
        };
        let legacy = Arc::new(AtomicUsize::new(0));
        let distributed = Arc::new(AtomicUsize::new(0));
        let shared = engine(QueryEngineVersion::V1, Arc::clone(&legacy))
            .with_engine(engine(QueryEngineVersion::V2, Arc::clone(&distributed)));
        let v1_only = engine(QueryEngineVersion::V1, Arc::clone(&legacy));
        let payload = || QueryRequest {
            query: "SELECT 42 AS answer".into(),
        };
        let mut headers = HeaderMap::new();
        for default in [QueryEngineVersion::V1, QueryEngineVersion::V2] {
            headers.remove(QUERY_ENGINE_HEADER);
            for selection in [None, Some("v1"), Some("v2")] {
                for accept in ["application/json", "application/vnd.apache.arrow.stream"] {
                    headers.insert(http::header::ACCEPT, HeaderValue::from_static(accept));
                    if let Some(selection) = selection {
                        headers.insert(QUERY_ENGINE_HEADER, HeaderValue::from_static(selection));
                    }
                    let response = query_with_engine(&shared, default, false, &headers, payload())
                        .await
                        .unwrap();
                    assert_eq!(response.status(), StatusCode::OK);
                    assert_eq!(
                        response.headers()[QUERY_ENGINE_HEADER],
                        selection.unwrap_or(default.as_str())
                    );
                    assert!(response.headers().contains_key(QUERY_SESSION_HEADER));
                    assert!(
                        response.headers()["server-timing"]
                            .to_str()
                            .unwrap()
                            .starts_with("planning;dur=")
                    );
                    assert_eq!(response.headers()[http::header::CONTENT_TYPE], accept);
                    let bytes = response.into_body().collect().await.unwrap().to_bytes();
                    if accept == "application/json" {
                        assert_eq!(
                            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
                            serde_json::json!({"rows": [{"answer": 42}]})
                        );
                    } else {
                        let batches = datafusion::arrow::ipc::reader::StreamReader::try_new(
                            std::io::Cursor::new(bytes),
                            None,
                        )
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                        assert_eq!(
                            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                            1
                        );
                    }
                }
            }
        }
        assert_eq!(legacy.load(Ordering::Relaxed), 6);
        assert_eq!(distributed.load(Ordering::Relaxed), 6);

        let response =
            query_with_engine(&v1_only, QueryEngineVersion::V1, false, &headers, payload())
                .await
                .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[QUERY_ENGINE_HEADER], "v2");
        assert!(!response.headers().contains_key(QUERY_SESSION_HEADER));
        let response = query_with_engine(
            &shared,
            QueryEngineVersion::V1,
            false,
            &headers,
            QueryRequest {
                query: "SELECT (".into(),
            },
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response.headers()[QUERY_ENGINE_HEADER], "v2");
        assert!(response.headers().contains_key(QUERY_SESSION_HEADER));

        headers.append(QUERY_ENGINE_HEADER, HeaderValue::from_static("v1"));
        assert_eq!(
            query_with_engine(&shared, QueryEngineVersion::V2, false, &headers, payload())
                .await
                .into_response()
                .status(),
            StatusCode::BAD_REQUEST
        );
        for invalid in [
            HeaderValue::from_static("unknown"),
            HeaderValue::from_bytes(b"\xff").unwrap(),
        ] {
            headers.insert(QUERY_ENGINE_HEADER, invalid);
            assert_eq!(
                query_with_engine(&shared, QueryEngineVersion::V2, false, &headers, payload())
                    .await
                    .into_response()
                    .status(),
                StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(
            legacy.load(Ordering::Relaxed),
            6,
            "selection failures must not fall back"
        );
        assert_eq!(distributed.load(Ordering::Relaxed), 7);

        headers.remove(QUERY_ENGINE_HEADER);
        let response =
            query_with_engine(&v1_only, QueryEngineVersion::V2, false, &headers, payload())
                .await
                .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[QUERY_ENGINE_HEADER], "v2");
        assert_eq!(legacy.load(Ordering::Relaxed), 6);

        headers.insert(QUERY_ENGINE_HEADER, HeaderValue::from_static("v1"));
        let response =
            query_with_engine(&v1_only, QueryEngineVersion::V2, false, &headers, payload())
                .await
                .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[QUERY_ENGINE_HEADER], "v1");
        response.into_body().collect().await.unwrap();
        assert_eq!(legacy.load(Ordering::Relaxed), 7);
    }

    #[restate_core::test]
    async fn operator_level_exposes_cluster_tables_except_config() {
        use futures::FutureExt;

        use restate_core::partitions::PartitionRouting;
        use restate_core::test_env::TestCoreEnvBuilder;
        use restate_core::{TaskCenter, TaskKind};
        use restate_distributed_datafusion as v2;
        use restate_storage_query_datafusion::context::SelectPartitionsFromMetadata;
        use restate_storage_query_datafusion::remote_query_scanner_client::create_remote_scanner_service;
        use restate_storage_query_datafusion::remote_query_scanner_manager::{
            RemoteScannerManager, create_partition_locator,
        };
        use restate_storage_query_datafusion::remote_query_scanner_server::RemoteQueryScannerServer;
        use restate_storage_query_datafusion::{ClusterTables, UserTables};
        use restate_types::cluster_state::NodeState;
        use restate_types::config::{Configuration, Experimental};

        #[derive(Debug)]
        struct EmptyBifrostScanner(Arc<AtomicUsize>);

        impl v2::Scan for EmptyBifrostScanner {
            fn scan(
                &self,
                projection: datafusion::arrow::datatypes::SchemaRef,
                _: &[datafusion::logical_expr::Expr],
                _: usize,
                _: Option<usize>,
            ) -> datafusion::execution::SendableRecordBatchStream {
                self.0.fetch_add(1, Ordering::Relaxed);
                Box::pin(
                    datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                        projection,
                        futures::stream::empty(),
                    ),
                )
            }
        }

        let mut core = TestCoreEnvBuilder::with_incoming_only_connector().add_mock_nodes_config();
        let env = DataFusionEnv::new(
            64 * 1024 * 1024,
            None,
            None,
            &HashMap::from([("datafusion.execution.target_partitions".into(), "1".into())]),
        )
        .unwrap()
        .with_mock_clock(restate_types::clock::MockClock::new())
        .unwrap();
        let scanners = RemoteScannerManager::new(
            create_remote_scanner_service(core.networking.clone()),
            create_partition_locator(
                PartitionRouting::new(Default::default(), TaskCenter::current()),
                core.metadata.clone(),
            ),
            core.metadata.clone(),
        );
        let distributed_env =
            v2::DataFusionEnv::new(64 * 1024 * 1024, None, Some(1), &HashMap::new())
                .unwrap()
                .with_mock_clock(restate_types::clock::MockClock::new())
                .unwrap()
                .with_distributed_execution(core.networking.clone());
        let distributed_scanners =
            v2::source_registry::SourceRegistry::local_only(core.metadata.clone());
        let scanner = restate_storage_query_datafusion::config::create_scanner(
            core.metadata.clone(),
            Configuration::live(),
        );
        scanners.register_node_scanner::<ConfigTable>(Arc::clone(&scanner));
        distributed_scanners
            .register_node_scanner::<v2::tables::config::ConfigTable>(Arc::clone(&scanner));
        let node_reads = Arc::new(AtomicUsize::new(0));
        distributed_scanners
            .register_node_scanner::<v2::tables::bifrost_read_stream::BifrostReadStreamsTable>(
                Arc::new(EmptyBifrostScanner(Arc::clone(&node_reads))),
            );
        let legacy_server =
            RemoteQueryScannerServer::new(env.clone(), scanners.clone(), &mut core.router_builder);
        let distributed_server = v2::distributed::DistributedQueryServer::new(
            distributed_env.clone(),
            distributed_scanners.clone(),
            &mut core.router_builder,
        );
        let core = core.build().await;
        TaskCenter::current()
            .cluster_state()
            .clone()
            .updater()
            .upsert_node_state(core.metadata.my_node_id(), NodeState::Alive);
        // Start both receivers before issuing loopback RPCs through the real transports.
        let mut legacy_task = Box::pin(legacy_server.run());
        assert!(legacy_task.as_mut().now_or_never().is_none());
        TaskCenter::spawn(TaskKind::SystemService, "query-test-v1", legacy_task).unwrap();
        let mut distributed_task = Box::pin(distributed_server.run());
        assert!(distributed_task.as_mut().now_or_never().is_none());
        TaskCenter::spawn(TaskKind::SystemService, "query-test-v2", distributed_task).unwrap();
        env.register_tables(ClusterTables::new(
            Default::default(),
            None,
            scanners.clone(),
        ))
        .await
        .unwrap();
        env.register_tables(UserTables::new(SelectPartitionsFromMetadata, scanners))
            .await
            .unwrap();
        assert!(env.table_provider(&ConfigTable::identity()).is_some());
        distributed_env
            .register_tables(v2::ClusterTables::new(
                distributed_scanners.clone(),
                Default::default(),
            ))
            .await
            .unwrap();
        distributed_env
            .register_tables(v2::UserTables::new(
                v2::context::SelectPartitionsFromMetadata,
                distributed_scanners,
            ))
            .await
            .unwrap();
        let engine = DataFusionQueryEngine::from_env(env, None).with_engine(
            v2::context::DataFusionQueryEngine::from_env(distributed_env, None),
        );

        async fn request(
            engine: &QueryEngine,
            experimental: &Experimental,
            headers: &HeaderMap,
            sql: &str,
        ) -> (StatusCode, serde_json::Value) {
            let response = query_with_engine(
                engine,
                QueryEngineVersion::V1,
                experimental.is_query_operator_tables_enabled(),
                headers,
                QueryRequest { query: sql.into() },
            )
            .await
            .into_response();
            assert_eq!(
                response.headers()[QUERY_ENGINE_HEADER],
                headers[QUERY_ENGINE_HEADER]
            );
            let status = response.status();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            (status, serde_json::from_slice(&bytes).unwrap())
        }

        for version in [QueryEngineVersion::V1, QueryEngineVersion::V2] {
            // Exercise each engine's node transport before applying HTTP exposure
            // rules, in a `restatectl sql` (gRPC) session, which serves `config`.
            let mut metadata = tonic::metadata::MetadataMap::new();
            metadata.insert(
                QUERY_ENGINE_HEADER,
                tonic::metadata::MetadataValue::from_static(version.as_str()),
            );
            let (_, options) = crate::cluster_controller::grpc_svc_handler::cluster_query_options(
                &metadata, false,
            )
            .unwrap();
            let session = engine.create_session(options).unwrap();
            let mut queries = vec![("SELECT * FROM config LIMIT 1", 1)];
            if version == QueryEngineVersion::V2 {
                queries.push(("SELECT * FROM cluster.bifrost_read_streams", 0));
            }
            for (sql, expected_rows) in queries {
                let result = session.execute(sql, QueryOptions {}).await.unwrap();
                let batches: Vec<_> = result.stream.try_collect().await.unwrap();
                assert!(
                    result.diagnostics.warnings().is_empty(),
                    "{version}: {:?}",
                    result.diagnostics.warnings()
                );
                assert_eq!(
                    batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                    expected_rows,
                    "{version}: {sql}"
                );
            }
            if version == QueryEngineVersion::V2 {
                assert!(
                    node_reads.load(Ordering::Relaxed) > 0,
                    "V2 must execute the node source"
                );
            }

            let mut headers = HeaderMap::new();
            headers.insert(
                QUERY_ENGINE_HEADER,
                HeaderValue::from_static(version.as_str()),
            );
            headers.insert(
                http::header::ACCEPT,
                HeaderValue::from_static("application/json"),
            );
            let catalog_sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'cluster' ORDER BY table_name";
            let empty_catalog = (StatusCode::OK, serde_json::json!({"rows": []}));
            let mut experimental = Experimental::default();
            assert!(!experimental.is_query_operator_tables_enabled());
            assert_eq!(
                request(&engine, &experimental, &headers, catalog_sql).await,
                empty_catalog
            );
            headers.insert(QUERY_LEVEL_HEADER, HeaderValue::from_static("operator"));
            assert_eq!(
                request(&engine, &experimental, &headers, catalog_sql).await,
                empty_catalog
            );
            let (status, body) = request(
                &engine,
                &experimental,
                &headers,
                "SELECT * FROM cluster.nodes",
            )
            .await;
            assert!(!status.is_success(), "{body}");
            assert!(
                body["message"].as_str().unwrap().contains("not found"),
                "{body}"
            );

            experimental = serde_json::from_value(serde_json::json!({
                "experimental-enable-query-operator-tables": true
            }))
            .unwrap();
            let expected_catalog = match version {
                QueryEngineVersion::V1 => serde_json::json!({"rows": [
                    {"table_name": "bifrost_read_streams"},
                    {"table_name": "loglet_workers"},
                    {"table_name": "logs"},
                    {"table_name": "logs_tail_segments"},
                    {"table_name": "migration_jobs"},
                    {"table_name": "nodes"},
                    {"table_name": "partition_replica_set"},
                    {"table_name": "partition_state"},
                    {"table_name": "partitions"},
                    {"table_name": "registry"}
                ]}),
                QueryEngineVersion::V2 => {
                    serde_json::json!({"rows": [
                        {"table_name": "bifrost_read_streams"},
                        {"table_name": "loglet_workers"},
                        {"table_name": "logs"},
                        {"table_name": "logs_tail_segments"},
                        {"table_name": "migration_jobs"},
                        {"table_name": "nodes"},
                        {"table_name": "partition_replica_set"},
                        {"table_name": "partition_state"},
                        {"table_name": "partitions"},
                        {"table_name": "registry"}
                    ]})
                }
            };
            assert_eq!(
                request(&engine, &experimental, &headers, catalog_sql).await,
                (StatusCode::OK, expected_catalog)
            );
            let cluster_count = match version {
                QueryEngineVersion::V1 => ("SELECT COUNT(*) AS n FROM cluster.nodes", 1),
                QueryEngineVersion::V2 => {
                    ("SELECT COUNT(*) AS n FROM cluster.bifrost_read_streams", 0)
                }
            };
            for (sql, expected) in [
                cluster_count,
                (
                    "SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sys_vqueue_entry_status'",
                    1,
                ),
            ] {
                assert_eq!(
                    request(&engine, &experimental, &headers, sql).await,
                    (
                        StatusCode::OK,
                        serde_json::json!({"rows": [{"n": expected}]})
                    )
                );
            }
            // Both engines register `config`, yet `/query` never shows it, not even at
            // the operator level.
            for table in ["config", "cluster.config", "restate.cluster.config"] {
                let (status, body) = request(
                    &engine,
                    &experimental,
                    &headers,
                    &format!("SELECT * FROM {table}"),
                )
                .await;
                assert!(!status.is_success(), "{body}");
                assert!(
                    body["message"].as_str().unwrap().contains("not found"),
                    "{body}"
                );
            }

            headers.append(QUERY_LEVEL_HEADER, HeaderValue::from_static("operator"));
            assert_eq!(
                request(&engine, &experimental, &headers, catalog_sql).await,
                empty_catalog
            );
            for invalid in [
                HeaderValue::from_static("unknown"),
                HeaderValue::from_bytes(b"\xff").unwrap(),
            ] {
                headers.insert(QUERY_LEVEL_HEADER, invalid);
                assert_eq!(
                    request(&engine, &experimental, &headers, catalog_sql).await,
                    empty_catalog
                );
            }
            headers.remove(QUERY_LEVEL_HEADER);
            assert_eq!(
                request(&engine, &experimental, &headers, catalog_sql).await,
                empty_catalog
            );
            experimental.set_query_operator_tables(false);
            headers.insert(QUERY_LEVEL_HEADER, HeaderValue::from_static("operator"));
            assert_eq!(
                request(&engine, &experimental, &headers, catalog_sql).await,
                empty_catalog
            );
        }
    }

    #[tokio::test]
    async fn query_context_and_session_header_preserve_streaming_responses() {
        let mut headers = HeaderMap::new();
        assert!(collect_query_headers(&headers).is_empty());
        for (name, value) in [
            ("X-Restate-Query-Client", "ui"),
            ("X-Restate-Query-Origin", "built-in"),
            ("X-RestateCloud-User-id", "user-1"),
            ("X-RestateCloud-Environment-id", "env-1"),
            ("X-RestateCloud-Caller-Principal", "principal-1"),
            ("Authorization", "Bearer private-token"),
            ("Cookie", "private-cookie"),
            ("X-Unlisted-Header", "ignored"),
        ] {
            headers.insert(
                name.parse::<http::header::HeaderName>().unwrap(),
                HeaderValue::from_static(value),
            );
        }
        headers.append(
            "x-restate-query-client",
            HeaderValue::from_static("second-client"),
        );
        let collected = collect_query_headers(&headers);
        assert_eq!(collected.len(), 6);
        assert_eq!(collected["x-restate-query-client"], "ui");
        assert_eq!(
            collected.get_all("x-restate-query-client").iter().count(),
            2
        );
        assert_eq!(collected["x-restate-query-origin"], "built-in");
        assert_eq!(collected["x-restatecloud-user-id"], "user-1");
        assert_eq!(collected["x-restatecloud-environment-id"], "env-1");
        assert_eq!(collected["x-restatecloud-caller-principal"], "principal-1");
        assert!(!collected.contains_key("authorization"));
        assert!(!collected.contains_key("cookie"));
        assert!(!collected.contains_key("x-unlisted-header"));
        let grpc_metadata = tonic::metadata::MetadataMap::from_headers(headers.clone());
        assert_eq!(collect_query_headers(grpc_metadata.as_ref()), collected);

        let env = DataFusionEnv::new(10 * 1024 * 1024, None, None, &HashMap::new())
            .unwrap()
            .with_mock_clock(restate_types::clock::MockClock::new())
            .unwrap();
        let engine = DataFusionQueryEngine::from_env(env, None);
        let session = engine
            .create_session(SessionOptions {
                headers: collected.clone(),
                ..Default::default()
            })
            .unwrap();
        let result = session.execute("SELECT 42", QueryOptions {}).await.unwrap();
        assert_eq!(result.metadata.headers, collected);
        drop(result);
        for accept in ["application/json", "application/vnd.apache.arrow.stream"] {
            headers.insert(http::header::ACCEPT, HeaderValue::from_static(accept));
            let response = query_in_session(
                session.as_ref(),
                &headers,
                QueryRequest {
                    query: "SELECT 42 AS answer".into(),
                },
            )
            .await
            .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers()[QUERY_SESSION_HEADER],
                session.session_id()
            );
            assert_eq!(response.headers()[http::header::CONTENT_TYPE], accept);
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            if accept == "application/json" {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
                    serde_json::json!({"rows": [{"answer": 42}]})
                );
            } else {
                let batches = datafusion::arrow::ipc::reader::StreamReader::try_new(
                    std::io::Cursor::new(bytes),
                    None,
                )
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
                assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
            }
        }
        let response = query_in_session(
            session.as_ref(),
            &headers,
            QueryRequest {
                query: "SELECT (".into(),
            },
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers()[QUERY_SESSION_HEADER],
            session.session_id()
        );

        headers.insert(
            "x-restate-query-client",
            HeaderValue::from_bytes(b"\xff").unwrap(),
        );
        assert_eq!(
            collect_query_headers(&headers)["x-restate-query-client"].as_bytes(),
            b"\xff"
        );
    }
}
