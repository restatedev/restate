// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use codederror::CodedError;

use restate_types::errors::GenericError;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum LogServerBuildError {
    #[error("unknown")]
    #[code(unknown)]
    Unknown,
    #[error("failed building log server: {0}")]
    #[code(unknown)]
    Other(GenericError),
}

impl LogServerBuildError {
    pub fn other(err: impl Into<GenericError>) -> Self {
        LogServerBuildError::Other(err.into())
    }
}
