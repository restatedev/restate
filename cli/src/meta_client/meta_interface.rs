// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use restate_meta::rest_api::endpoints::ListServiceEndpointsResponse;
use restate_meta::rest_api::endpoints::ServiceEndpointResponse;

use restate_meta::rest_api::services::ListServicesResponse;
use restate_schema_api::service::ServiceMetadata;

use super::client::Envelope;
use super::MetaClient;

#[async_trait]
pub trait MetaClientInterface {
    /// Check if the meta service is healthy by invoking /health
    async fn health(&self) -> reqwest::Result<Envelope<()>>;
    async fn get_services(&self) -> reqwest::Result<Envelope<ListServicesResponse>>;
    async fn get_service(&self, name: &str) -> reqwest::Result<Envelope<ServiceMetadata>>;
    async fn get_endpoints(&self) -> reqwest::Result<Envelope<ListServiceEndpointsResponse>>;
    async fn get_endpoint(&self, name: &str) -> reqwest::Result<Envelope<ServiceEndpointResponse>>;
}

#[async_trait]
impl MetaClientInterface for MetaClient {
    async fn health(&self) -> reqwest::Result<Envelope<()>> {
        let url = self.base_url.join("/health").expect("Bad url!");
        Ok(self.run(reqwest::Method::GET, url).await?)
    }

    async fn get_services(&self) -> reqwest::Result<Envelope<ListServicesResponse>> {
        let url = self.base_url.join("/services").expect("Bad url!");
        Ok(self.run(reqwest::Method::GET, url).await?)
    }

    async fn get_service(&self, name: &str) -> reqwest::Result<Envelope<ServiceMetadata>> {
        // TODO: FIX ME
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

    async fn get_endpoint(&self, name: &str) -> reqwest::Result<Envelope<ServiceEndpointResponse>> {
        // TODO: FIX ME
        let url = self
            .base_url
            .join(&format!("/endpoints/{}", name))
            .expect("Bad url!");
        Ok(self.run(reqwest::Method::GET, url).await?)
    }
}
