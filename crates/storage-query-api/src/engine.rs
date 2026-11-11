// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::TableReference;
use datafusion::execution::SendableRecordBatchStream;

use restate_platform::sync::Mutex;
use restate_util_string::ReString;

use crate::QueryEngineTable;
use crate::errors::{QueryExecutionError, SessionError};

#[derive(Debug, Clone)]
pub struct SessionOptions {
    /// Implementation selected by the shared query engine. `None` uses its default version.
    pub engine_version: Option<QueryEngineVersion>,
    /// Apply the engine's configured session-admission limiter. Defaults to `true`.
    /// Disabling it bypasses admission without changing the shared rate-limit budget.
    pub apply_rate_limit: bool,
    /// Allows SQL statements such as `SET` in this session. Defaults to `false`.
    pub allow_statements: bool,
    /// Schemas to expose from the shared inventory. Defaults to `public`.
    /// Only registered tables are included. Ignored when `tables` is supplied.
    /// An empty list exposes no application tables.
    pub schemas: Vec<ReString>,
    /// Overrides the default schema used to resolve unqualified SQL names.
    /// `None` inherits the environment's default. This does not change which tables are exposed.
    pub default_schema: Option<ReString>,
    /// SQL names to expose from the shared inventory. `None` selects `schemas`;
    /// an empty list exposes no application tables. Missing inventory entries are omitted.
    /// Exposing a prebound view exposes its result without exposing its base tables.
    pub tables: Option<Vec<SessionTable>>,
    /// Inventory identities to omit from SQL exposure, including explicitly aliased tables.
    /// This does not remove dependencies retained by prebound views.
    pub excluded_tables: Vec<ReString>,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            engine_version: None,
            apply_rate_limit: true,
            allow_statements: false,
            schemas: vec![ReString::from_static("public")],
            default_schema: None,
            tables: None,
            excluded_tables: Vec::new(),
        }
    }
}

/// Maps a stable inventory identity to a session-local SQL name.
#[derive(Debug, Clone)]
pub struct SessionTable {
    pub identity: ReString,
    pub name: TableReference,
}

impl SessionTable {
    pub fn new(identity: impl Into<ReString>, name: impl Into<TableReference>) -> Self {
        Self {
            identity: identity.into(),
            name: name.into(),
        }
    }

    pub fn for_table<T: QueryEngineTable>(name: impl Into<TableReference>) -> Self {
        Self::new(T::identity(), name)
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryOptions {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueryEngineVersion {
    #[default]
    V1,
    V2,
}

impl QueryEngineVersion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }
}

impl fmt::Display for QueryEngineVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

type SessionFactory =
    dyn Fn(SessionOptions) -> Result<Arc<dyn QuerySession>, SessionError> + Send + Sync;

/// Shared query engine. Session options select the implementation, SQL exposure, and admission.
/// An engine with no registered implementations is disabled.
#[derive(Default)]
pub struct QueryEngine {
    default_version: QueryEngineVersion,
    v1: Option<Box<SessionFactory>>,
    v2: Option<Box<SessionFactory>>,
}

impl QueryEngine {
    pub fn new(
        version: QueryEngineVersion,
        create_session: impl Fn(SessionOptions) -> Result<Arc<dyn QuerySession>, SessionError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        let mut engine = Self {
            default_version: version,
            ..Default::default()
        };
        let factory = match version {
            QueryEngineVersion::V1 => &mut engine.v1,
            QueryEngineVersion::V2 => &mut engine.v2,
        };
        *factory = Some(Box::new(create_session));
        engine
    }

    /// Combines implementations at startup, retaining this engine's default version.
    /// Each version must be registered only once.
    pub fn with_engine(mut self, other: Self) -> Self {
        assert!(
            self.v1.is_none() || other.v1.is_none(),
            "duplicate query engine v1"
        );
        assert!(
            self.v2.is_none() || other.v2.is_none(),
            "duplicate query engine v2"
        );
        self.v1 = self.v1.or(other.v1);
        self.v2 = self.v2.or(other.v2);
        self
    }

    /// Creates an independent session, without falling back if the requested version is unavailable.
    pub fn create_session(
        &self,
        mut opts: SessionOptions,
    ) -> Result<Arc<dyn QuerySession>, SessionError> {
        if self.v1.is_none() && self.v2.is_none() {
            return Err(SessionError::EngineDisabled);
        }
        let version = opts.engine_version.unwrap_or(self.default_version);
        let create_session = match version {
            QueryEngineVersion::V1 => &self.v1,
            QueryEngineVersion::V2 => &self.v2,
        }
        .as_ref()
        .ok_or(SessionError::VersionUnavailable(version))?;
        opts.engine_version = Some(version);
        create_session(opts)
    }
}

#[async_trait]
pub trait QuerySession: Send + Sync {
    /// Executes in a caller-owned session, applying SQL restrictions.
    ///
    /// Use a session from [`QueryEngine::create_session`] to retain settings across executions or attach
    /// request metadata before planning. The returned stream can outlive the session handle.
    async fn execute(
        &self,
        query: &str,
        opts: QueryOptions,
    ) -> Result<QueryResult, QueryExecutionError>;
}

/// Result of a SQL query execution, containing the record batch stream
/// and any per-node warning collectors from fan-out execution plans.
pub struct QueryResult {
    pub stream: SendableRecordBatchStream,
    pub node_warnings: Vec<NodeWarnings>,
}

/// A warning collected from a node that failed during query execution.
#[derive(Debug, Clone)]
pub struct NodeWarning {
    pub node_id: ReString,
    pub message: ReString,
}

/// Shared collection of per-node warnings accumulated during fan-out execution.
///
/// Each partition (node) stream that encounters an error will push a warning
/// here instead of propagating the error through DataFusion.
pub type NodeWarnings = Arc<Mutex<Vec<NodeWarning>>>;
