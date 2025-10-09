// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::AdminClient;
use super::admin_client::Envelope;
use http::{Uri, Version};
use std::collections::HashMap;

use restate_admin_rest_model::deployments::*;
use restate_admin_rest_model::invocations::RestartAsNewInvocationResponse;
use restate_admin_rest_model::services::*;
use restate_admin_rest_model::version::VersionInformation;
use restate_serde_util::SerdeableHeaderHashMap;
use restate_types::identifiers::{DeploymentId, LambdaARN};
use restate_types::schema::deployment::ProtocolType;
use restate_types::schema::service::ServiceMetadata;

pub trait AdminClientInterface {
    /// Check if the admin service is healthy by invoking /health
    async fn health(&self) -> reqwest::Result<Envelope<()>>;
    async fn get_services(&self) -> reqwest::Result<Envelope<ListServicesResponse>>;
    async fn get_service(&self, name: &str) -> reqwest::Result<Envelope<ServiceMetadata>>;
    async fn patch_service(
        &self,
        name: &str,
        modify_service_request: ModifyServiceRequest,
    ) -> reqwest::Result<Envelope<ServiceMetadata>>;
    async fn get_deployments(&self) -> reqwest::Result<Envelope<ListDeploymentsResponse>>;
    async fn get_deployment<D: AsRef<str>>(
        &self,
        id: D,
    ) -> reqwest::Result<Envelope<DetailedDeploymentResponse>>;
    async fn remove_deployment(&self, id: &str, force: bool) -> reqwest::Result<Envelope<()>>;

    async fn discover_deployment(
        &self,
        body: RegisterDeploymentRequest,
    ) -> reqwest::Result<Envelope<RegisterDeploymentResponse>>;

    async fn cancel_invocation(&self, id: &str) -> reqwest::Result<Envelope<()>>;

    async fn kill_invocation(&self, id: &str) -> reqwest::Result<Envelope<()>>;

    async fn purge_invocation(&self, id: &str) -> reqwest::Result<Envelope<()>>;

    async fn restart_invocation(
        &self,
        id: &str,
    ) -> reqwest::Result<Envelope<RestartAsNewInvocationResponse>>;

    async fn resume_invocation(&self, id: &str) -> reqwest::Result<Envelope<()>>;

    async fn pause_invocation(&self, id: &str) -> reqwest::Result<Envelope<()>>;

    async fn patch_state(
        &self,
        service: &str,
        req: ModifyServiceStateRequest,
    ) -> reqwest::Result<Envelope<()>>;

    async fn version(&self) -> reqwest::Result<Envelope<VersionInformation>>;
}

impl AdminClientInterface for AdminClient {
    async fn health(&self) -> reqwest::Result<Envelope<()>> {
        let url = self.versioned_url(["health"]);
        self.run(reqwest::Method::GET, url).await
    }

    async fn get_services(&self) -> reqwest::Result<Envelope<ListServicesResponse>> {
        let url = self.versioned_url(["services"]);
        self.run(reqwest::Method::GET, url).await
    }

    async fn get_service(&self, name: &str) -> reqwest::Result<Envelope<ServiceMetadata>> {
        let url = self.versioned_url(["services", name]);
        self.run(reqwest::Method::GET, url).await
    }

    async fn patch_service(
        &self,
        name: &str,
        modify_service_request: ModifyServiceRequest,
    ) -> reqwest::Result<Envelope<ServiceMetadata>> {
        let url = self.versioned_url(["services", name]);
        self.run_with_body(reqwest::Method::PATCH, url, modify_service_request)
            .await
    }

    async fn get_deployments(&self) -> reqwest::Result<Envelope<ListDeploymentsResponse>> {
        let url = self.versioned_url(["deployments"]);
        self.run(reqwest::Method::GET, url).await
    }

    async fn get_deployment<D: AsRef<str>>(
        &self,
        id: D,
    ) -> reqwest::Result<Envelope<DetailedDeploymentResponse>> {
        let url = self.versioned_url(["deployments", id.as_ref()]);
        self.run(reqwest::Method::GET, url).await
    }

    async fn remove_deployment(&self, id: &str, force: bool) -> reqwest::Result<Envelope<()>> {
        let mut url = self.versioned_url(["deployments", id]);
        url.set_query(Some(&format!("force={force}")));

        self.run(reqwest::Method::DELETE, url).await
    }

    async fn discover_deployment(
        &self,
        body: RegisterDeploymentRequest,
    ) -> reqwest::Result<Envelope<RegisterDeploymentResponse>> {
        let url = self.versioned_url(["deployments"]);
        self.run_with_body(reqwest::Method::POST, url, body).await
    }

    async fn cancel_invocation(&self, id: &str) -> reqwest::Result<Envelope<()>> {
        self.run(
            reqwest::Method::PATCH,
            self.versioned_url(["invocations", id, "cancel"]),
        )
        .await
    }

    async fn kill_invocation(&self, id: &str) -> reqwest::Result<Envelope<()>> {
        self.run(
            reqwest::Method::PATCH,
            self.versioned_url(["invocations", id, "kill"]),
        )
        .await
    }

    async fn purge_invocation(&self, id: &str) -> reqwest::Result<Envelope<()>> {
        self.run(
            reqwest::Method::PATCH,
            self.versioned_url(["invocations", id, "purge"]),
        )
        .await
    }

    async fn restart_invocation(
        &self,
        id: &str,
    ) -> reqwest::Result<Envelope<RestartAsNewInvocationResponse>> {
        let url = self.versioned_url(["invocations", id, "restart-as-new"]);
        self.run(reqwest::Method::PATCH, url).await
    }

    async fn resume_invocation(&self, id: &str) -> reqwest::Result<Envelope<()>> {
        let url = self.versioned_url(["invocations", id, "resume"]);
        self.run(reqwest::Method::PATCH, url).await
    }

    async fn pause_invocation(&self, id: &str) -> reqwest::Result<Envelope<()>> {
        let url = self.versioned_url(["invocations", id, "pause"]);
        self.run(reqwest::Method::PATCH, url).await
    }

    async fn patch_state(
        &self,
        service: &str,
        req: ModifyServiceStateRequest,
    ) -> reqwest::Result<Envelope<()>> {
        let url = self.versioned_url(["services", service, "state"]);
        self.run_with_body(reqwest::Method::POST, url, req).await
    }

    async fn version(&self) -> reqwest::Result<Envelope<VersionInformation>> {
        let url = self.versioned_url(["version"]);
        self.run(reqwest::Method::GET, url).await
    }
}

// Helper type used within the cli crate
#[derive(Clone, Debug)]
pub enum Deployment {
    Http {
        uri: Uri,
        protocol_type: ProtocolType,
        http_version: Version,
        additional_headers: SerdeableHeaderHashMap,
        created_at: humantime::Timestamp,
        min_protocol_version: i32,
        max_protocol_version: i32,
        metadata: HashMap<String, String>,
    },
    Lambda {
        arn: LambdaARN,
        assume_role_arn: Option<String>,
        additional_headers: SerdeableHeaderHashMap,
        created_at: humantime::Timestamp,
        min_protocol_version: i32,
        max_protocol_version: i32,
        metadata: HashMap<String, String>,
    },
}

impl Deployment {
    pub fn from_deployment_response(
        deployment_response: DeploymentResponse,
    ) -> (DeploymentId, Self, Vec<ServiceNameRevPair>) {
        match deployment_response {
            DeploymentResponse::Http {
                id,
                uri,
                protocol_type,
                http_version,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                services,
                metadata,
                ..
            } => (
                id,
                Deployment::Http {
                    uri,
                    protocol_type,
                    http_version,
                    additional_headers,
                    created_at,
                    min_protocol_version,
                    max_protocol_version,
                    metadata,
                },
                services,
            ),
            DeploymentResponse::Lambda {
                id,
                arn,
                assume_role_arn,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                services,
                metadata,
                ..
            } => (
                id,
                Deployment::Lambda {
                    arn,
                    assume_role_arn,
                    additional_headers,
                    created_at,
                    min_protocol_version,
                    max_protocol_version,
                    metadata,
                },
                services,
            ),
        }
    }

    pub fn from_detailed_deployment_response(
        detailed_deployment_response: DetailedDeploymentResponse,
    ) -> (DeploymentId, Self, Vec<ServiceMetadata>) {
        match detailed_deployment_response {
            DetailedDeploymentResponse::Http {
                id,
                uri,
                protocol_type,
                http_version,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                services,
                metadata,
                ..
            } => (
                id,
                Deployment::Http {
                    uri,
                    protocol_type,
                    http_version,
                    additional_headers,
                    created_at,
                    min_protocol_version,
                    max_protocol_version,
                    metadata,
                },
                services,
            ),
            DetailedDeploymentResponse::Lambda {
                id,
                arn,
                assume_role_arn,
                additional_headers,
                created_at,
                min_protocol_version,
                max_protocol_version,
                services,
                metadata,
                ..
            } => (
                id,
                Deployment::Lambda {
                    arn,
                    assume_role_arn,
                    additional_headers,
                    created_at,
                    min_protocol_version,
                    max_protocol_version,
                    metadata,
                },
                services,
            ),
        }
    }
}
