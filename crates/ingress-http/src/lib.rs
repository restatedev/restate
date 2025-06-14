// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
mod rpc_request_dispatcher;
mod server;

pub use rpc_request_dispatcher::InvocationClientRequestDispatcher;
pub use server::{HyperServerIngress, IngressServerError, StartSignal};

use bytes::Bytes;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use restate_types::identifiers::InvocationId;
use restate_types::invocation::client::{
    AttachInvocationResponse, GetInvocationOutputResponse, InvocationOutput,
    SubmittedInvocationNotification,
};
use restate_types::invocation::{InvocationQuery, InvocationRequest, InvocationResponse};
use restate_types::journal_v2::Signal;

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

#[derive(Debug, thiserror::Error)]
pub enum RequestDispatcherError {
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// Trait used by the invoker to dispatch requests to target partition processors.
#[cfg_attr(test, mockall::automock)]
pub trait RequestDispatcher {
    /// Send: append invocation and wait for the [`SubmittedInvocationNotification`]
    fn send(
        &self,
        invocation_request: Arc<InvocationRequest>,
    ) -> impl Future<Output = Result<SubmittedInvocationNotification, RequestDispatcherError>> + Send;

    /// Call: append invocation and wait for its response
    fn call(
        &self,
        invocation_request: Arc<InvocationRequest>,
    ) -> impl Future<Output = Result<InvocationOutput, RequestDispatcherError>> + Send;

    /// Attach to an invocation using the given query
    fn attach_invocation(
        &self,
        invocation_query: InvocationQuery,
    ) -> impl Future<Output = Result<AttachInvocationResponse, RequestDispatcherError>> + Send;

    /// Get invocation output, without blocking when it's still running.
    fn get_invocation_output(
        &self,
        invocation_query: InvocationQuery,
    ) -> impl Future<Output = Result<GetInvocationOutputResponse, RequestDispatcherError>> + Send;

    /// Send invocation response (for awakeables).
    /// **NOTE:** This works only for targeting invocations using Journal Table V1/Service Protocol <= V3.
    fn send_invocation_response(
        &self,
        invocation_response: InvocationResponse,
    ) -> impl Future<Output = Result<(), RequestDispatcherError>> + Send;

    /// Send signal to invocation.
    /// **NOTE:** This works only for targeting invocations using Journal Table V2/Service Protocol >= V4.
    fn send_signal(
        &self,
        target_invocation: InvocationId,
        signal: Signal,
    ) -> impl Future<Output = Result<(), RequestDispatcherError>> + Send;
}

// Contains some mocks we use in unit tests in this crate
#[cfg(test)]
mod mocks {
    use restate_types::identifiers::DeploymentId;
    use restate_types::invocation::{
        InvocationQuery, InvocationTargetType, ServiceType, VirtualObjectHandlerType,
    };
    use restate_types::schema::invocation_target::test_util::MockInvocationTargetResolver;
    use restate_types::schema::invocation_target::{
        InvocationTargetMetadata, InvocationTargetResolver,
    };
    use restate_types::schema::service::test_util::MockServiceMetadataResolver;
    use restate_types::schema::service::{
        HandlerMetadata, InvocationAttemptOptions, ServiceMetadata, ServiceMetadataResolver,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;

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
                handlers: HashMap::from([(
                    handler_name.to_string(),
                    HandlerMetadata {
                        name: handler_name.to_string(),
                        ty: invocation_target_metadata.target_ty.into(),
                        documentation: None,
                        metadata: Default::default(),
                        idempotency_retention: None,
                        workflow_completion_retention: None,
                        journal_retention: None,
                        inactivity_timeout: None,
                        abort_timeout: None,
                        enable_lazy_state: None,
                        public: true,
                        input_description: "any".to_string(),
                        output_description: "any".to_string(),
                        input_json_schema: None,
                        output_json_schema: None,
                    },
                )]),
                ty: invocation_target_metadata.target_ty.into(),
                documentation: None,
                metadata: Default::default(),
                deployment_id: DeploymentId::default(),
                revision: 0,
                public: invocation_target_metadata.public,
                idempotency_retention: None,
                workflow_completion_retention: None,
                journal_retention: None,
                inactivity_timeout: None,
                abort_timeout: None,
                enable_lazy_state: None,
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

        fn resolve_invocation_attempt_options(
            &self,
            _deployment_id: &DeploymentId,
            _service_name: impl AsRef<str>,
            _handler_name: impl AsRef<str>,
        ) -> Option<InvocationAttemptOptions> {
            todo!()
        }

        fn resolve_latest_service_openapi(&self, _: impl AsRef<str>) -> Option<Value> {
            todo!()
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

    impl RequestDispatcher for Arc<MockRequestDispatcher> {
        fn send(
            &self,
            invocation_request: Arc<InvocationRequest>,
        ) -> impl Future<Output = Result<SubmittedInvocationNotification, RequestDispatcherError>> + Send
        {
            MockRequestDispatcher::send(self, invocation_request)
        }

        fn call(
            &self,
            invocation_request: Arc<InvocationRequest>,
        ) -> impl Future<Output = Result<InvocationOutput, RequestDispatcherError>> + Send {
            MockRequestDispatcher::call(self, invocation_request)
        }

        fn attach_invocation(
            &self,
            invocation_query: InvocationQuery,
        ) -> impl Future<Output = Result<AttachInvocationResponse, RequestDispatcherError>> + Send
        {
            MockRequestDispatcher::attach_invocation(self, invocation_query)
        }

        fn get_invocation_output(
            &self,
            invocation_query: InvocationQuery,
        ) -> impl Future<Output = Result<GetInvocationOutputResponse, RequestDispatcherError>> + Send
        {
            MockRequestDispatcher::get_invocation_output(self, invocation_query)
        }

        fn send_invocation_response(
            &self,
            invocation_response: InvocationResponse,
        ) -> impl Future<Output = Result<(), RequestDispatcherError>> + Send {
            MockRequestDispatcher::send_invocation_response(self, invocation_response)
        }

        fn send_signal(
            &self,
            target_invocation: InvocationId,
            signal: Signal,
        ) -> impl Future<Output = Result<(), RequestDispatcherError>> + Send {
            MockRequestDispatcher::send_signal(self, target_invocation, signal)
        }
    }
}
