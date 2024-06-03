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
mod layers;
mod metric_definitions;
mod server;

pub use server::{HyperServerIngress, IngressServerError, StartSignal};

use bytes::Bytes;
use restate_types::ingress::InvocationResponse;
use restate_types::invocation::InvocationQuery;
use std::net::{IpAddr, SocketAddr};

/// Client connection information for a given RPC request
#[derive(Clone, Copy, Debug)]
pub(crate) struct ConnectInfo {
    remote: SocketAddr,
}

impl ConnectInfo {
    fn new(remote: SocketAddr) -> Self {
        Self { remote }
    }
    fn address(&self) -> IpAddr {
        self.remote.ip()
    }
    fn port(&self) -> u16 {
        self.remote.port()
    }
}

pub enum GetOutputResult {
    NotFound,
    NotReady,
    NotSupported,
    Ready(InvocationResponse),
}

pub trait InvocationStorageReader {
    fn get_output(
        &self,
        query: InvocationQuery,
    ) -> impl std::future::Future<Output = Result<GetOutputResult, anyhow::Error>> + Send;
}

// Contains some mocks we use in unit tests in this crate
#[cfg(test)]
mod mocks {
    use super::*;

    use anyhow::Error;
    use restate_schema_api::invocation_target::mocks::MockInvocationTargetResolver;
    use restate_schema_api::invocation_target::{
        InvocationTargetMetadata, InvocationTargetResolver, DEFAULT_IDEMPOTENCY_RETENTION,
    };
    use restate_schema_api::service::mocks::MockServiceMetadataResolver;
    use restate_schema_api::service::{HandlerMetadata, ServiceMetadata, ServiceMetadataResolver};
    use restate_types::identifiers::DeploymentId;
    use restate_types::ingress::InvocationResponse;
    use restate_types::invocation::{
        InvocationQuery, InvocationTargetType, ServiceType, VirtualObjectHandlerType,
    };
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub(super) struct GreetingRequest {
        pub person: String,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub(super) struct GreetingResponse {
        pub greeting: String,
    }

    #[derive(Debug, Clone, Default)]
    pub(crate) struct MockSchemas(
        pub(crate) MockServiceMetadataResolver,
        pub(crate) MockInvocationTargetResolver,
    );

    impl MockSchemas {
        pub fn add_service_and_target(
            &mut self,
            service_name: &str,
            handler_name: &str,
            invocation_target_metadata: InvocationTargetMetadata,
        ) {
            self.0.add(ServiceMetadata {
                name: service_name.to_string(),
                handlers: vec![HandlerMetadata {
                    name: handler_name.to_string(),
                    ty: invocation_target_metadata.target_ty.into(),
                    input_description: "any".to_string(),
                    output_description: "any".to_string(),
                }],
                ty: invocation_target_metadata.target_ty.into(),
                deployment_id: DeploymentId::default(),
                revision: 0,
                public: invocation_target_metadata.public,
                idempotency_retention: DEFAULT_IDEMPOTENCY_RETENTION.into(),
                workflow_completion_retention: None,
            });
            self.1
                .add(service_name, [(handler_name, invocation_target_metadata)]);
        }

        pub fn with_service_and_target(
            mut self,
            service_name: &str,
            handler_name: &str,
            invocation_target_metadata: InvocationTargetMetadata,
        ) -> Self {
            self.add_service_and_target(service_name, handler_name, invocation_target_metadata);
            self
        }
    }

    impl ServiceMetadataResolver for MockSchemas {
        fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
            self.0.resolve_latest_service(service_name)
        }

        fn resolve_latest_service_type(
            &self,
            service_name: impl AsRef<str>,
        ) -> Option<ServiceType> {
            self.0.resolve_latest_service_type(service_name)
        }

        fn list_services(&self) -> Vec<ServiceMetadata> {
            self.0.list_services()
        }
    }

    impl InvocationTargetResolver for MockSchemas {
        fn resolve_latest_invocation_target(
            &self,
            service_name: impl AsRef<str>,
            handler_name: impl AsRef<str>,
        ) -> Option<InvocationTargetMetadata> {
            self.1
                .resolve_latest_invocation_target(service_name, handler_name)
        }
    }

    pub(super) fn mock_schemas() -> MockSchemas {
        let mut mock_schemas = MockSchemas::default();

        mock_schemas.add_service_and_target(
            "greeter.Greeter",
            "greet",
            InvocationTargetMetadata::mock(InvocationTargetType::Service),
        );
        mock_schemas.add_service_and_target(
            "greeter.GreeterObject",
            "greet",
            InvocationTargetMetadata::mock(InvocationTargetType::VirtualObject(
                VirtualObjectHandlerType::Exclusive,
            )),
        );

        mock_schemas
    }

    #[derive(Debug, Clone, Default)]
    pub(crate) struct MockStorageReader(pub(crate) HashMap<InvocationQuery, InvocationResponse>);

    impl InvocationStorageReader for MockStorageReader {
        async fn get_output(&self, query: InvocationQuery) -> Result<GetOutputResult, Error> {
            Ok(self
                .0
                .get(&query)
                .cloned()
                .map(GetOutputResult::Ready)
                .unwrap_or(GetOutputResult::NotFound))
        }
    }
}
