// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use codederror::CodedError;

use restate_core::ShutdownError;

/// Result type for log-server operations.
pub type Result<T, E = LogServerError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum LogServerBuildError {
    #[error("unknown")]
    #[code(unknown)]
    Unknown,
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum LogServerError {
    #[error("operation failed due to an ongoing shutdown")]
    Shutdown(#[from] ShutdownError),
}
