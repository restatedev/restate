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

use restate_admin_rest_model::deployments::*;
use restate_admin_rest_model::invocations::RestartAsNewInvocationResponse;
use restate_admin_rest_model::services::*;
use restate_admin_rest_model::version::VersionInformation;
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

    async fn purge_invocation(&self, id: &str) -> reqwest::Result<Envelope<()>>;

    async fn restart_invocation(
        &self,
        id: &str,
    ) -> reqwest::Result<Envelope<RestartAsNewInvocationResponse>>;

    async fn cancel_invocation(&self, id: &str, kill: bool) -> reqwest::Result<Envelope<()>>;

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

    async fn purge_invocation(&self, id: &str) -> reqwest::Result<Envelope<()>> {
        let mut url = self.versioned_url(["invocations", id]);
        url.set_query(Some("mode=purge"));

        self.run(reqwest::Method::DELETE, url).await
    }

    async fn restart_invocation(
        &self,
        id: &str,
    ) -> reqwest::Result<Envelope<RestartAsNewInvocationResponse>> {
        let url = self.versioned_url(["invocations", id, "restart-as-new"]);
        self.run(reqwest::Method::PATCH, url).await
    }

    async fn cancel_invocation(&self, id: &str, kill: bool) -> reqwest::Result<Envelope<()>> {
        let mut url = self.versioned_url(["invocations", id]);
        url.set_query(Some(&format!(
            "mode={}",
            if kill { "kill" } else { "cancel" }
        )));

        self.run(reqwest::Method::DELETE, url).await
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
