// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::ops::RangeInclusive;

use codederror::CodedError;
use http::{HeaderName, HeaderValue};

use crate::deployment::DeploymentAddress;
use crate::endpoint_manifest;
use crate::schema::deployment::DeploymentType;

#[derive(Debug)]
pub struct DiscoveryRequest {
    pub address: DeploymentAddress,
    pub use_http_11: bool,
    pub additional_headers: HashMap<HeaderName, HeaderValue>,
}

#[derive(Debug)]
pub struct DiscoveryResponse {
    pub deployment_type: DeploymentType,
    pub headers: HashMap<HeaderName, HeaderValue>,
    pub services: Vec<endpoint_manifest::Service>,
    // type is i32 because the generated ServiceProtocolVersion enum uses this as its representation
    // and we need to represent unknown later versions
    pub supported_protocol_versions: RangeInclusive<i32>,
    pub sdk_version: Option<String>,
}

pub trait DiscoveryClient {
    type Error: CodedError + Send + Sync + 'static;

    fn discover(
        &self,
        req: crate::schema::registry::DiscoveryRequest,
    ) -> impl Future<Output = Result<crate::schema::registry::DiscoveryResponse, Self::Error>> + Send;
}
