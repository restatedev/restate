// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Common types used by the journal data model

use crate::errors::{InvocationError, InvocationErrorCode};
use bytestring::ByteString;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct Failure {
    #[bilrost(1)]
    pub code: InvocationErrorCode,
    #[bilrost(2)]
    pub message: ByteString,
}

impl From<InvocationError> for Failure {
    fn from(value: InvocationError) -> Self {
        Failure {
            code: value.code(),
            message: value.message().into(),
        }
    }
}

impl From<Failure> for InvocationError {
    fn from(value: Failure) -> Self {
        InvocationError::new(value.code, value.message)
    }
}
