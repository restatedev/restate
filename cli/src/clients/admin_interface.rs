// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use futures::StreamExt;
use futures::stream;
use http::{Uri, Version};
use indicatif::ProgressBar;
use restate_admin_rest_model::deployments::*;
use restate_admin_rest_model::invocations::RestartAsNewInvocationResponse;
use restate_admin_rest_model::services::*;
use restate_admin_rest_model::version::VersionInformation;
use restate_futures_util::streams::StreamExt as RestateStreamExt;
use restate_serde_util::SerdeableHeaderHashMap;
use restate_types::identifiers::{DeploymentId, LambdaARN};
use restate_types::schema::deployment::ProtocolType;
use restate_types::schema::service::ServiceMetadata;
use std::collections::HashMap;

const MAX_PARALLEL_REQUESTS: usize = 500;

pub trait AdminClientInterface {
    /// Check if the admin service is healthy by invoking /health
    fn health(&self) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static;
    fn get_services(
        &self,
    ) -> impl Future<Output = reqwest::Result<Envelope<ListServicesResponse>>> + Send + 'static;
    fn get_service(
        &self,
        name: &str,
    ) -> impl Future<Output = reqwest::Result<Envelope<ServiceMetadata>>> + Send + 'static;
    fn patch_service(
        &self,
        name: &str,
        modify_service_request: ModifyServiceRequest,
    ) -> impl Future<Output = reqwest::Result<Envelope<ServiceMetadata>>> + Send + 'static;
    fn get_deployments(
        &self,
    ) -> impl Future<Output = reqwest::Result<Envelope<ListDeploymentsResponse>>> + Send + 'static;
    fn get_deployment<D: AsRef<str>>(
        &self,
        id: D,
    ) -> impl Future<Output = reqwest::Result<Envelope<DetailedDeploymentResponse>>> + Send + 'static;
    fn remove_deployment(
        &self,
        id: &str,
        force: bool,
    ) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static;

    fn discover_deployment(
        &self,
        body: RegisterDeploymentRequest,
    ) -> impl Future<Output = reqwest::Result<Envelope<RegisterDeploymentResponse>>> + Send + 'static;

    fn cancel_invocation(
        &self,
        id: &str,
    ) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static;

    fn resume_invocation(
        &self,
        id: &str,
        deployment: Option<&str>,
    ) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static;

    fn kill_invocation(
        &self,
        id: &str,
    ) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static;

    fn purge_invocation(
        &self,
        id: &str,
    ) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static;

    fn restart_invocation(
        &self,
        id: &str,
    ) -> impl Future<Output = reqwest::Result<Envelope<RestartAsNewInvocationResponse>>> + Send + 'static;

    fn pause_invocation(
        &self,
        id: &str,
    ) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static;

    fn patch_state(
        &self,
        service: &str,
        req: ModifyServiceStateRequest,
    ) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static;

    fn version(
        &self,
    ) -> impl Future<Output = reqwest::Result<Envelope<VersionInformation>>> + Send + 'static;
}

impl AdminClientInterface for AdminClient {
    fn health(&self) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static {
        let url = self.versioned_url(["health"]);
        self.run(reqwest::Method::GET, url)
    }

    fn get_services(
        &self,
    ) -> impl Future<Output = reqwest::Result<Envelope<ListServicesResponse>>> + Send + 'static
    {
        let url = self.versioned_url(["services"]);
        self.run(reqwest::Method::GET, url)
    }

    fn get_service(
        &self,
        name: &str,
    ) -> impl Future<Output = reqwest::Result<Envelope<ServiceMetadata>>> + Send + 'static {
        let url = self.versioned_url(["services", name]);
        self.run(reqwest::Method::GET, url)
    }

    fn patch_service(
        &self,
        name: &str,
        modify_service_request: ModifyServiceRequest,
    ) -> impl Future<Output = reqwest::Result<Envelope<ServiceMetadata>>> + Send + 'static {
        let url = self.versioned_url(["services", name]);
        self.run_with_body(reqwest::Method::PATCH, url, modify_service_request)
    }

    fn get_deployments(
        &self,
    ) -> impl Future<Output = reqwest::Result<Envelope<ListDeploymentsResponse>>> + Send + 'static
    {
        let url = self.versioned_url(["deployments"]);
        self.run(reqwest::Method::GET, url)
    }

    fn get_deployment<D: AsRef<str>>(
        &self,
        id: D,
    ) -> impl Future<Output = reqwest::Result<Envelope<DetailedDeploymentResponse>>> + Send + 'static
    {
        let url = self.versioned_url(["deployments", id.as_ref()]);
        self.run(reqwest::Method::GET, url)
    }

    fn remove_deployment(
        &self,
        id: &str,
        force: bool,
    ) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static {
        let mut url = self.versioned_url(["deployments", id]);
        url.set_query(Some(&format!("force={force}")));

        self.run(reqwest::Method::DELETE, url)
    }

    fn discover_deployment(
        &self,
        body: RegisterDeploymentRequest,
    ) -> impl Future<Output = reqwest::Result<Envelope<RegisterDeploymentResponse>>> + Send + 'static
    {
        let url = self.versioned_url(["deployments"]);
        self.run_with_body(reqwest::Method::POST, url, body)
    }

    fn cancel_invocation(
        &self,
        id: &str,
    ) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static {
        self.run(
            reqwest::Method::PATCH,
            self.versioned_url(["invocations", id, "cancel"]),
        )
    }

    fn resume_invocation(
        &self,
        id: &str,
        deployment: Option<&str>,
    ) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static {
        let mut url = self.versioned_url(["invocations", id, "resume"]);
        if let Some(deployment) = deployment {
            url.set_query(Some(&format!("deployment={deployment}")));
        }
        self.run(reqwest::Method::PATCH, url)
    }

    fn kill_invocation(
        &self,
        id: &str,
    ) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static {
        self.run(
            reqwest::Method::PATCH,
            self.versioned_url(["invocations", id, "kill"]),
        )
    }

    fn purge_invocation(
        &self,
        id: &str,
    ) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static {
        self.run(
            reqwest::Method::PATCH,
            self.versioned_url(["invocations", id, "purge"]),
        )
    }

    fn restart_invocation(
        &self,
        id: &str,
    ) -> impl Future<Output = reqwest::Result<Envelope<RestartAsNewInvocationResponse>>> + Send + 'static
    {
        let url = self.versioned_url(["invocations", id, "restart-as-new"]);
        self.run(reqwest::Method::PATCH, url)
    }

    fn pause_invocation(
        &self,
        id: &str,
    ) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static {
        let url = self.versioned_url(["invocations", id, "pause"]);
        self.run(reqwest::Method::PATCH, url)
    }

    fn patch_state(
        &self,
        service: &str,
        req: ModifyServiceStateRequest,
    ) -> impl Future<Output = reqwest::Result<Envelope<()>>> + Send + 'static {
        let url = self.versioned_url(["services", service, "state"]);
        self.run_with_body(reqwest::Method::POST, url, req)
    }

    fn version(
        &self,
    ) -> impl Future<Output = reqwest::Result<Envelope<VersionInformation>>> + Send + 'static {
        let url = self.versioned_url(["version"]);
        self.run(reqwest::Method::GET, url)
    }
}

pub async fn batch_execute<
    In: Clone + Send + 'static,
    Out: Send + 'static,
    Fun: Fn(AdminClient, In) -> Fut + Copy + Send + 'static,
    Fut: Future<Output = Result<Out, anyhow::Error>> + Send,
>(
    client: AdminClient,
    input: Vec<In>,
    fun: Fun,
) -> (Vec<(In, Out)>, Vec<(In, anyhow::Error)>) {
    let progress = ProgressBar::new(input.len() as u64);
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    let results = stream::iter(input)
        .concurrent_map_unordered(
            MAX_PARALLEL_REQUESTS,
            client,
            move |client, input| async move {
                fun(client, input.clone())
                    .await
                    .map(|out| (input.clone(), out))
                    .map_err(|e| (input.clone(), e))
            },
        )
        .fold((vec![], vec![]), |(mut done, mut failed), result| {
            progress.inc(1);
            async {
                match result {
                    Ok(ok) => done.push(ok),
                    Err(err) => failed.push(err),
                }
                (done, failed)
            }
        })
        .await;

    progress.finish_and_clear();

    results
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
        sdk_version: Option<String>,
    },
    Lambda {
        arn: LambdaARN,
        assume_role_arn: Option<String>,
        additional_headers: SerdeableHeaderHashMap,
        created_at: humantime::Timestamp,
        min_protocol_version: i32,
        max_protocol_version: i32,
        metadata: HashMap<String, String>,
        sdk_version: Option<String>,
    },
}

impl Deployment {
    pub fn created_at(&self) -> humantime::Timestamp {
        match self {
            Self::Http { created_at, .. } => *created_at,
            Self::Lambda { created_at, .. } => *created_at,
        }
    }

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
                sdk_version,
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
                    sdk_version,
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
                sdk_version,
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
                    sdk_version,
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
                sdk_version,
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
                    sdk_version,
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
                sdk_version,
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
                    sdk_version,
                },
                services,
            ),
        }
    }
}
