// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::HyperServerIngress;

use restate_ingress_dispatcher::IngressDispatcher;
use restate_schema_api::component::ComponentMetadataResolver;
use restate_schema_api::invocation_target::InvocationTargetResolver;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// # Ingress options
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "IngressOptions"))]
#[cfg_attr(feature = "options_schema", schemars(default))]
#[builder(default)]
pub struct Options {
    /// # Bind address
    ///
    /// The address to bind for the ingress.
    bind_address: SocketAddr,

    /// # Concurrency limit
    ///
    /// Local concurrency limit to use to limit the amount of concurrent requests. If exceeded, the ingress will reply immediately with an appropriate status code.
    concurrency_limit: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".parse().unwrap(),
            concurrency_limit: 10_000_000,
        }
    }
}

impl Options {
    pub fn build<Schemas>(
        self,
        dispatcher: IngressDispatcher,
        schemas: Schemas,
    ) -> HyperServerIngress<Schemas, IngressDispatcher>
    where
        Schemas:
            ComponentMetadataResolver + InvocationTargetResolver + Clone + Send + Sync + 'static,
    {
        let Options {
            bind_address,
            concurrency_limit,
        } = self;

        crate::metric_definitions::describe_metrics();
        let (hyper_ingress_server, _) =
            HyperServerIngress::new(bind_address, concurrency_limit, schemas, dispatcher);

        hyper_ingress_server
    }
}
