// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::Uri;

use restate_core::metadata_store::ReadModifyWriteError;
use restate_core::ShutdownError;
use restate_types::endpoint_manifest;
use restate_types::errors::GenericError;
use restate_types::identifiers::DeploymentId;
use restate_types::invocation::ServiceType;
use restate_types::schema::invocation_target::BadInputContentType;

use crate::schema_registry::ServiceName;

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum SchemaRegistryError {
    #[error(transparent)]
    Schema(
        #[from]
        #[code]
        SchemaError,
    ),
    #[error(transparent)]
    Discovery(
        #[from]
        #[code]
        restate_service_protocol::discovery::DiscoveryError,
    ),
    #[error("internal error: {0}")]
    #[code(unknown)]
    Internal(String),
    #[error(transparent)]
    #[code(unknown)]
    Shutdown(#[from] ShutdownError),
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum SchemaError {
    // Those are generic and used by all schema resources
    #[error("not found in the schema registry: {0}")]
    #[code(unknown)]
    NotFound(String),
    #[error("already exists in the schema registry: {0}")]
    #[code(unknown)]
    Override(String),

    // Specific resources errors
    #[error(transparent)]
    Service(
        #[from]
        #[code]
        ServiceError,
    ),
    #[error(transparent)]
    Deployment(
        #[from]
        #[code]
        DeploymentError,
    ),
    #[error(transparent)]
    Subscription(
        #[from]
        #[code]
        SubscriptionError,
    ),
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum ServiceError {
    #[error("cannot insert/modify service '{0}' as it contains a reserved name")]
    #[code(restate_errors::META0005)]
    ReservedName(String),
    #[error("detected a new service '{0}' revision with a service type different from the previous revision. Service type cannot be changed across revisions")]
    #[code(restate_errors::META0006)]
    DifferentType(ServiceName),
    #[error("the service '{0}' already exists but the new revision removed the handlers {1:?}")]
    #[code(restate_errors::META0006)]
    RemovedHandlers(ServiceName, Vec<String>),
    #[error("the handler '{0}' input content-type is not valid: {1}")]
    #[code(unknown)]
    BadInputContentType(String, BadInputContentType),
    #[error("the handler '{0}' output content-type is not valid: {1}")]
    #[code(unknown)]
    BadOutputContentType(String, http_1::header::InvalidHeaderValue),
    #[error("invalid combination of service type and handler type '({0}, {1:?})'")]
    #[code(unknown)]
    BadServiceAndHandlerType(ServiceType, Option<endpoint_manifest::HandlerType>),
    #[error("modifying retention time for service type {0} is unsupported")]
    #[code(unknown)]
    CannotModifyRetentionTime(ServiceType),
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
#[code(restate_errors::META0009)]
pub enum SubscriptionError {
    #[error(
        "invalid source URI '{0}': must have a scheme segment, with supported schemes: [kafka]."
    )]
    InvalidSourceScheme(Uri),
    #[error("invalid source URI '{0}': source URI of Kafka type must have a authority segment containing the cluster name.")]
    InvalidKafkaSourceAuthority(Uri),

    #[error(
        "invalid sink URI '{0}': must have a scheme segment, with supported schemes: [service]."
    )]
    InvalidSinkScheme(Uri),
    #[error("invalid sink URI '{0}': sink URI of service type must have a authority segment containing the service name.")]
    InvalidServiceSinkAuthority(Uri),
    #[error("invalid sink URI '{0}': cannot find service/handler specified in the sink URI.")]
    SinkServiceNotFound(Uri),
    #[error("invalid sink URI '{0}': shared handlers cannot be used as sinks.")]
    InvalidSinkSharedHandler(Uri),

    #[error(transparent)]
    #[code(unknown)]
    Validation(GenericError),
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum DeploymentError {
    #[error("existing deployment id is different from requested (requested = {requested}, existing = {existing})")]
    #[code(restate_errors::META0004)]
    IncorrectId {
        requested: DeploymentId,
        existing: DeploymentId,
    },
}

impl From<ReadModifyWriteError<SchemaError>> for SchemaRegistryError {
    fn from(value: ReadModifyWriteError<SchemaError>) -> Self {
        match value {
            ReadModifyWriteError::FailedOperation(err) => SchemaRegistryError::Schema(err),
            err => SchemaRegistryError::Internal(err.to_string()),
        }
    }
}
