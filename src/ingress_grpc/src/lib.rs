// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod handler;
mod options;
mod protocol;
mod reflection;
mod server;

pub use options::{Options, OptionsBuilder, OptionsBuilderError};
pub use server::{HyperServerIngress, IngressServerError, StartSignal};

use bytes::Bytes;
use opentelemetry::Context;
use restate_types::identifiers::FullInvocationId;
use tonic::Status;

// --- Data model used by handlers and protocol

#[derive(Debug, Clone)]
struct IngressRequestHeaders {
    service_name: String,
    method_name: String,
    tracing_context: Context,
}

impl IngressRequestHeaders {
    pub fn new(service_name: String, method_name: String, tracing_context: Context) -> Self {
        Self {
            service_name,
            method_name,
            tracing_context,
        }
    }
}
type HandlerRequest = (IngressRequestHeaders, Bytes);
type HandlerResult = Result<Bytes, Status>;

// Contains some mocks we use in unit tests in this crate
#[cfg(test)]
mod mocks {
    use restate_schema_api::endpoint::{DeliveryOptions, EndpointMetadata, ProtocolType};
    use restate_schema_api::key::ServiceInstanceType;
    use restate_schema_impl::{Schemas, ServiceRegistrationRequest};

    pub(super) fn test_schemas() -> Schemas {
        let schemas = Schemas::default();

        schemas
            .apply_updates(
                schemas
                    .compute_new_endpoint_updates(
                        EndpointMetadata::new(
                            "http://localhost:9080".parse().unwrap(),
                            ProtocolType::BidiStream,
                            DeliveryOptions::default(),
                        ),
                        vec![ServiceRegistrationRequest::new(
                            "greeter.Greeter".to_string(),
                            ServiceInstanceType::Singleton,
                        )],
                        restate_pb::mocks::DESCRIPTOR_POOL.clone(),
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();

        schemas
    }
}
