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
pub struct FailureMetadata {
    #[bilrost(1)]
    pub key: ByteString,
    #[bilrost(2)]
    pub value: ByteString,
}

impl From<(String, String)> for FailureMetadata {
    fn from((key, value): (String, String)) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl From<(ByteString, ByteString)> for FailureMetadata {
    fn from((key, value): (ByteString, ByteString)) -> Self {
        Self { key, value }
    }
}

impl From<FailureMetadata> for (ByteString, ByteString) {
    fn from(metadata: FailureMetadata) -> Self {
        (metadata.key, metadata.value)
    }
}

impl From<FailureMetadata> for (String, String) {
    fn from(metadata: FailureMetadata) -> Self {
        (metadata.key.into(), metadata.value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct Failure {
    #[bilrost(1)]
    pub code: InvocationErrorCode,
    #[bilrost(2)]
    pub message: ByteString,
    #[bilrost(3)]
    pub metadata: Vec<FailureMetadata>,
}

impl From<InvocationError> for Failure {
    fn from(value: InvocationError) -> Self {
        Failure {
            code: value.code,
            message: value.message().into(),
            metadata: value
                .metadata
                .into_iter()
                .map(|(key, value)| FailureMetadata {
                    key: key.into(),
                    value: value.into(),
                })
                .collect(),
        }
    }
}

impl From<Failure> for InvocationError {
    fn from(value: Failure) -> Self {
        InvocationError::new(value.code, value.message).with_metadata_vec(
            value
                .metadata
                .into_iter()
                .map(|failure_metadata| {
                    (failure_metadata.key.into(), failure_metadata.value.into())
                })
                .collect(),
        )
    }
}
