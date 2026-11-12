// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::common::DataFusionError;

#[derive(thiserror::Error, Debug)]
pub enum SessionError {
    #[error("query engine is disabled")]
    EngineDisabled,
    #[error("query engine {0} is not enabled on this node")]
    VersionUnavailable(crate::QueryEngineVersion),
    #[error("rate limited")]
    RateLimited(#[from] gardal::RateLimited),
    #[error("session initialization error: {0}")]
    DataFusion(#[from] DataFusionError),
}

#[derive(thiserror::Error, Debug)]
pub enum QueryExecutionError {
    #[error("execution error: {0}")]
    DataFusion(#[from] DataFusionError),
}

/// The kind of `error`, named without its message, which can contain literal values from the
/// query. Logs that must not include query data report this instead of the error.
pub fn error_kind(error: &DataFusionError) -> &'static str {
    match error.find_root() {
        DataFusionError::ArrowError(..) => "arrow",
        DataFusionError::IoError(_) => "io",
        DataFusionError::NotImplemented(_) => "not-implemented",
        DataFusionError::Internal(_) => "internal",
        DataFusionError::Plan(_) => "plan",
        DataFusionError::Configuration(_) => "configuration",
        DataFusionError::SchemaError(..) => "schema",
        DataFusionError::Execution(_) => "execution",
        DataFusionError::ExecutionJoin(_) => "execution-join",
        DataFusionError::ResourcesExhausted(_) => "resources-exhausted",
        DataFusionError::External(_) => "external",
        _ => "other",
    }
}
