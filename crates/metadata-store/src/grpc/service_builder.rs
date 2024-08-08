// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::{Request, Response};
use std::convert::Infallible;
use tonic::body::BoxBody;
use tonic::server::NamedService;
use tonic::service::{Routes, RoutesBuilder};
use tonic_health::ServingStatus;
use tower::Service;

#[derive(Debug)]
pub struct GrpcServiceBuilder<'a> {
    reflection_service_builder: Option<tonic_reflection::server::Builder<'a>>,
    routes_builder: RoutesBuilder,
    svc_names: Vec<&'static str>,
}

impl<'a> Default for GrpcServiceBuilder<'a> {
    fn default() -> Self {
        let routes_builder = RoutesBuilder::default();

        Self {
            reflection_service_builder: Some(tonic_reflection::server::Builder::configure()),
            routes_builder,
            svc_names: Vec::default(),
        }
    }
}

impl<'a> GrpcServiceBuilder<'a> {
    pub fn add_service<S>(&mut self, svc: S)
    where
        S: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
    {
        self.svc_names.push(S::NAME);
        self.routes_builder.add_service(svc);
    }

    pub fn register_file_descriptor_set_for_reflection<'b: 'a>(
        &mut self,
        encoded_file_descriptor_set: &'b [u8],
    ) {
        self.reflection_service_builder = Some(
            self.reflection_service_builder
                .take()
                .expect("be present")
                .register_encoded_file_descriptor_set(encoded_file_descriptor_set),
        );
    }

    pub async fn build(mut self) -> Result<Routes, tonic_reflection::server::Error> {
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

        for svc_name in self.svc_names {
            health_reporter
                .set_service_status(svc_name, ServingStatus::Serving)
                .await;
        }

        self.routes_builder.add_service(health_service);
        self.routes_builder.add_service(
            self.reflection_service_builder
                .expect("be present")
                .build()?,
        );
        Ok(self.routes_builder.routes())
    }
}
