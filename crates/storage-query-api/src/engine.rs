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

use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;

use restate_platform::sync::Mutex;
use restate_util_string::ReString;

use crate::errors::{QueryExecutionError, SessionError};

#[derive(Debug, Clone, Default)]
pub struct SessionOptions {
    /// Allows SQL statements such as `SET` in this session. Defaults to `false`.
    pub allow_statements: bool,
}

#[derive(Debug, Clone, Default)]
pub struct QueryOptions {}

pub trait QueryEngine<T>: Send + Sync {
    /// Creates an independent session with the configured tables, functions, and defaults.
    ///
    /// Sessions share the runtime (including its memory pool) and catalog providers. Session
    /// settings and prepared statements are private to the returned session.
    fn create_session(
        &self,
        opts: SessionOptions,
    ) -> Result<Arc<dyn QuerySession<T>>, SessionError>;
}

#[async_trait]
pub trait QuerySession<T>: Send + Sync {
    /// Executes in a caller-owned session, applying the shared rate limit and SQL restrictions.
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

pub struct NoOpQueryEngine;

impl<T> QueryEngine<T> for NoOpQueryEngine {
    fn create_session(
        &self,
        _opts: SessionOptions,
    ) -> Result<Arc<dyn QuerySession<T>>, SessionError> {
        Err(SessionError::EngineDisabled)
    }
}
