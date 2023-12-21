// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::metas_client::Envelope;
use super::MetasClient;

use restate_meta_rest_model::deployments::*;
use restate_meta_rest_model::services::*;

use async_trait::async_trait;

#[async_trait]
pub trait MetaClientInterface {
    /// Check if the meta service is healthy by invoking /health
    async fn health(&self) -> reqwest::Result<Envelope<()>>;
    async fn get_services(&self) -> reqwest::Result<Envelope<ListServicesResponse>>;
    async fn get_service(&self, name: &str) -> reqwest::Result<Envelope<ServiceMetadata>>;
    async fn get_deployments(&self) -> reqwest::Result<Envelope<ListDeploymentsResponse>>;
    async fn get_deployment(
        &self,
        id: &str,
    ) -> reqwest::Result<Envelope<DetailedDeploymentResponse>>;
    async fn remove_deployment(&self, id: &str, force: bool) -> reqwest::Result<Envelope<()>>;

    async fn discover_deployment(
        &self,
        body: RegisterDeploymentRequest,
    ) -> reqwest::Result<Envelope<RegisterDeploymentResponse>>;

    async fn cancel_invocation(&self, id: &str, kill: bool) -> reqwest::Result<Envelope<()>>;
}

#[async_trait]
impl MetaClientInterface for MetasClient {
    async fn health(&self) -> reqwest::Result<Envelope<()>> {
        let url = self.base_url.join("/health").expect("Bad url!");
        Ok(self.run(reqwest::Method::GET, url).await?)
    }

    async fn get_services(&self) -> reqwest::Result<Envelope<ListServicesResponse>> {
        let url = self.base_url.join("/services").expect("Bad url!");
        Ok(self.run(reqwest::Method::GET, url).await?)
    }

    async fn get_service(&self, name: &str) -> reqwest::Result<Envelope<ServiceMetadata>> {
        let url = self
            .base_url
            .join(&format!("/services/{}", name))
            .expect("Bad url!");

        Ok(self.run(reqwest::Method::GET, url).await?)
    }

    async fn get_deployments(&self) -> reqwest::Result<Envelope<ListDeploymentsResponse>> {
        let url = self.base_url.join("/deployments").expect("Bad url!");
        Ok(self.run(reqwest::Method::GET, url).await?)
    }

    async fn get_deployment(
        &self,
        id: &str,
    ) -> reqwest::Result<Envelope<DetailedDeploymentResponse>> {
        let url = self
            .base_url
            .join(&format!("/deployments/{}", id))
            .expect("Bad url!");
        Ok(self.run(reqwest::Method::GET, url).await?)
    }

    async fn discover_deployment(
        &self,
        body: RegisterDeploymentRequest,
    ) -> reqwest::Result<Envelope<RegisterDeploymentResponse>> {
        let url = self.base_url.join("/deployments").expect("Bad url!");
        Ok(self.run_with_body(reqwest::Method::POST, url, body).await?)
    }

    async fn remove_deployment(&self, id: &str, force: bool) -> reqwest::Result<Envelope<()>> {
        let mut url = self
            .base_url
            .join(&format!("/deployments/{}", id))
            .expect("Bad url!");

        url.set_query(Some(&format!("force={}", force)));

        Ok(self.run(reqwest::Method::DELETE, url).await?)
    }

    async fn cancel_invocation(&self, id: &str, kill: bool) -> reqwest::Result<Envelope<()>> {
        let mut url = self
            .base_url
            .join(&format!("/invocations/{}", id))
            .expect("Bad url!");

        url.set_query(Some(&format!(
            "mode={}",
            if kill { "kill" } else { "cancel" }
        )));

        Ok(self.run(reqwest::Method::DELETE, url).await?)
    }
}
