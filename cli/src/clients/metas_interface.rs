// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use std::fmt::Display;

use super::metas_client::Envelope;
use super::MetasClient;

use restate_meta_rest_model::components::*;
use restate_meta_rest_model::deployments::*;

pub trait MetaClientInterface {
    /// Check if the meta service is healthy by invoking /health
    async fn health(&self) -> reqwest::Result<Envelope<()>>;
    async fn get_components(&self) -> reqwest::Result<Envelope<ListComponentsResponse>>;
    async fn get_component(&self, name: &str) -> reqwest::Result<Envelope<ComponentMetadata>>;
    async fn get_deployments(&self) -> reqwest::Result<Envelope<ListDeploymentsResponse>>;
    async fn get_deployment<D: Display>(
        &self,
        id: D,
    ) -> reqwest::Result<Envelope<DetailedDeploymentResponse>>;
    async fn remove_deployment(&self, id: &str, force: bool) -> reqwest::Result<Envelope<()>>;

    async fn discover_deployment(
        &self,
        body: RegisterDeploymentRequest,
    ) -> reqwest::Result<Envelope<RegisterDeploymentResponse>>;

    async fn cancel_invocation(&self, id: &str, kill: bool) -> reqwest::Result<Envelope<()>>;

    async fn patch_state(
        &self,
        service: &str,
        req: ModifyComponentStateRequest,
    ) -> reqwest::Result<Envelope<()>>;
}

impl MetaClientInterface for MetasClient {
    async fn health(&self) -> reqwest::Result<Envelope<()>> {
        let url = self.base_url.join("/health").expect("Bad url!");
        self.run(reqwest::Method::GET, url).await
    }

    async fn get_components(&self) -> reqwest::Result<Envelope<ListComponentsResponse>> {
        let url = self.base_url.join("/components").expect("Bad url!");
        self.run(reqwest::Method::GET, url).await
    }

    async fn get_component(&self, name: &str) -> reqwest::Result<Envelope<ComponentMetadata>> {
        let url = self
            .base_url
            .join(&format!("/components/{}", name))
            .expect("Bad url!");

        self.run(reqwest::Method::GET, url).await
    }

    async fn get_deployments(&self) -> reqwest::Result<Envelope<ListDeploymentsResponse>> {
        let url = self.base_url.join("/deployments").expect("Bad url!");
        self.run(reqwest::Method::GET, url).await
    }

    async fn get_deployment<D: Display>(
        &self,
        id: D,
    ) -> reqwest::Result<Envelope<DetailedDeploymentResponse>> {
        let url = self
            .base_url
            .join(&format!("/deployments/{}", id))
            .expect("Bad url!");
        self.run(reqwest::Method::GET, url).await
    }

    async fn remove_deployment(&self, id: &str, force: bool) -> reqwest::Result<Envelope<()>> {
        let mut url = self
            .base_url
            .join(&format!("/deployments/{}", id))
            .expect("Bad url!");

        url.set_query(Some(&format!("force={}", force)));

        self.run(reqwest::Method::DELETE, url).await
    }

    async fn discover_deployment(
        &self,
        body: RegisterDeploymentRequest,
    ) -> reqwest::Result<Envelope<RegisterDeploymentResponse>> {
        let url = self.base_url.join("/deployments").expect("Bad url!");
        self.run_with_body(reqwest::Method::POST, url, body).await
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

        self.run(reqwest::Method::DELETE, url).await
    }

    async fn patch_state(
        &self,
        service: &str,
        req: ModifyComponentStateRequest,
    ) -> reqwest::Result<Envelope<()>> {
        let url = self
            .base_url
            .join(&format!("/components/{service}/state"))
            .expect("Bad url!");

        self.run_with_body(reqwest::Method::POST, url, req).await
    }
}
