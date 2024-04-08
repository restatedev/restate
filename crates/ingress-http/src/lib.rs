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

// Contains some mocks we use in unit tests in this crate
#[cfg(test)]
mod mocks {
    use restate_schema_api::component::mocks::MockComponentMetadataResolver;
    use restate_schema_api::component::{
        ComponentMetadata, ComponentMetadataResolver, ComponentType, HandlerMetadata, HandlerType,
    };
    use restate_schema_api::invocation_target::mocks::MockInvocationTargetResolver;
    use restate_schema_api::invocation_target::{
        InvocationTargetMetadata, InvocationTargetResolver, DEFAULT_IDEMPOTENCY_RETENTION,
    };
    use restate_types::identifiers::DeploymentId;
    use serde::{Deserialize, Serialize};

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
        pub(crate) MockComponentMetadataResolver,
        pub(crate) MockInvocationTargetResolver,
    );

    impl MockSchemas {
        pub fn add_component_and_target(
            &mut self,
            component_name: &str,
            handler_name: &str,
            invocation_target_metadata: InvocationTargetMetadata,
        ) {
            self.0.add(ComponentMetadata {
                name: component_name.to_string(),
                handlers: vec![HandlerMetadata {
                    name: handler_name.to_string(),
                    ty: invocation_target_metadata.handler_ty,
                    input_description: "any".to_string(),
                    output_description: "any".to_string(),
                }],
                ty: invocation_target_metadata.component_ty,
                deployment_id: DeploymentId::default(),
                revision: 0,
                public: invocation_target_metadata.public,
                idempotency_retention: DEFAULT_IDEMPOTENCY_RETENTION.into(),
            });
            self.1
                .add(component_name, [(handler_name, invocation_target_metadata)]);
        }

        pub fn with_component_and_target(
            mut self,
            component_name: &str,
            handler_name: &str,
            invocation_target_metadata: InvocationTargetMetadata,
        ) -> Self {
            self.add_component_and_target(component_name, handler_name, invocation_target_metadata);
            self
        }
    }

    impl ComponentMetadataResolver for MockSchemas {
        fn resolve_latest_component(
            &self,
            component_name: impl AsRef<str>,
        ) -> Option<ComponentMetadata> {
            self.0.resolve_latest_component(component_name)
        }

        fn resolve_latest_component_type(
            &self,
            component_name: impl AsRef<str>,
        ) -> Option<ComponentType> {
            self.0.resolve_latest_component_type(component_name)
        }

        fn list_components(&self) -> Vec<ComponentMetadata> {
            self.0.list_components()
        }
    }

    impl InvocationTargetResolver for MockSchemas {
        fn resolve_latest_invocation_target(
            &self,
            component_name: impl AsRef<str>,
            handler_name: impl AsRef<str>,
        ) -> Option<InvocationTargetMetadata> {
            self.1
                .resolve_latest_invocation_target(component_name, handler_name)
        }
    }

    pub(super) fn mock_schemas() -> MockSchemas {
        let mut mock_schemas = MockSchemas::default();

        mock_schemas.add_component_and_target(
            "greeter.Greeter",
            "greet",
            InvocationTargetMetadata::mock(ComponentType::Service, HandlerType::Shared),
        );
        mock_schemas.add_component_and_target(
            "greeter.GreeterObject",
            "greet",
            InvocationTargetMetadata::mock(ComponentType::VirtualObject, HandlerType::Exclusive),
        );

        mock_schemas
    }
}
