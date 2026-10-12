// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[derive(thiserror::Error, Debug)]
pub enum SessionError {
    #[error("query engine is disabled")]
    EngineDisabled,
    #[error("rate limited")]
    RateLimited(#[from] gardal::RateLimited),
    #[error("session initialization error: {0}")]
    DataFusion(#[from] datafusion::common::DataFusionError),
}

#[derive(thiserror::Error, Debug)]
pub enum QueryExecutionError {
    #[error("execution error: {0}")]
    DataFusion(#[from] datafusion::common::DataFusionError),
}
