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

use restate_meta_rest_model::endpoints::*;
use restate_meta_rest_model::services::*;

use async_trait::async_trait;

#[async_trait]
pub trait MetaClientInterface {
    /// Check if the meta service is healthy by invoking /health
    async fn health(&self) -> reqwest::Result<Envelope<()>>;
    async fn get_services(&self) -> reqwest::Result<Envelope<ListServicesResponse>>;
    async fn get_service(&self, name: &str) -> reqwest::Result<Envelope<ServiceMetadata>>;
    async fn get_endpoints(&self) -> reqwest::Result<Envelope<ListServiceEndpointsResponse>>;
    async fn get_endpoint(&self, id: &str) -> reqwest::Result<Envelope<ServiceEndpointResponse>>;

    async fn discover_endpoint(
        &self,
        body: RegisterServiceEndpointRequest,
    ) -> reqwest::Result<Envelope<RegisterServiceEndpointResponse>>;
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

    async fn get_endpoints(&self) -> reqwest::Result<Envelope<ListServiceEndpointsResponse>> {
        let url = self.base_url.join("/endpoints").expect("Bad url!");
        Ok(self.run(reqwest::Method::GET, url).await?)
    }

    async fn get_endpoint(&self, id: &str) -> reqwest::Result<Envelope<ServiceEndpointResponse>> {
        let url = self
            .base_url
            .join(&format!("/endpoints/{}", id))
            .expect("Bad url!");
        Ok(self.run(reqwest::Method::GET, url).await?)
    }

    async fn discover_endpoint(
        &self,
        body: RegisterServiceEndpointRequest,
    ) -> reqwest::Result<Envelope<RegisterServiceEndpointResponse>> {
        let url = self.base_url.join("/endpoints").expect("Bad url!");
        Ok(self.run_with_body(reqwest::Method::POST, url, body).await?)
    }
}
