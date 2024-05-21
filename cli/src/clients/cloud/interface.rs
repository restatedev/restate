// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::client::Envelope;

use super::{generated::*, CloudClient};

pub trait CloudClientInterface {
    async fn list_accounts(&self) -> reqwest::Result<Envelope<ListAccountsResponse>>;
    async fn list_environments(
        &self,
        account_id: &str,
    ) -> reqwest::Result<Envelope<ListEnvironmentsResponse>>;
    async fn describe_environment(
        &self,
        account_id: &str,
        environment_id: &str,
    ) -> reqwest::Result<Envelope<DescribeEnvironmentResponse>>;
}

impl CloudClientInterface for CloudClient {
    async fn list_accounts(&self) -> reqwest::Result<Envelope<ListAccountsResponse>> {
        let url = self.base_url.join("/cloud/ListAccounts").expect("Bad url!");

        self.run(reqwest::Method::POST, url).await
    }

    async fn list_environments(
        &self,
        account_id: &str,
    ) -> reqwest::Result<Envelope<ListEnvironmentsResponse>> {
        let url = self
            .base_url
            .join(&format!("/cloud/{account_id}/ListEnvironments"))
            .expect("Bad url!");

        self.run(reqwest::Method::POST, url).await
    }

    async fn describe_environment(
        &self,
        account_id: &str,
        environment_id: &str,
    ) -> reqwest::Result<Envelope<DescribeEnvironmentResponse>> {
        let url = self
            .base_url
            .join(&format!("/cloud/{account_id}/DescribeEnvironment"))
            .expect("Bad url!");

        self.run_with_body(
            reqwest::Method::POST,
            url,
            DescribeEnvironmentRequest {
                environment_id: environment_id.into(),
            },
        )
        .await
    }
}
