// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains Restate public protobuf definitions

use once_cell::sync::Lazy;
use prost_reflect::{DescriptorPool, ServiceDescriptor};
use std::convert::AsRef;

pub mod grpc {
    pub mod health {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(env!("OUT_DIR"), "/grpc.health.v1.rs"));
    }
    pub mod reflection {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(env!("OUT_DIR"), "/grpc.reflection.v1alpha.rs"));
    }
}
pub mod restate {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unknown_lints)]
    include!(concat!(env!("OUT_DIR"), "/dev.restate.rs"));
}

pub static DESCRIPTOR_POOL: Lazy<DescriptorPool> = Lazy::new(|| {
    DescriptorPool::decode(
        include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin")).as_ref(),
    )
    .expect("The built-in descriptor pool should be valid")
});

pub fn get_service(svc_name: &str) -> ServiceDescriptor {
    DESCRIPTOR_POOL
        .get_service_by_name(svc_name)
        .unwrap_or_else(|| {
            panic!(
                "The built-in descriptor pool should contain the {} service",
                svc_name
            )
        })
}

pub const INGRESS_SERVICE_NAME: &str = "dev.restate.Ingress";
pub const AWAKEABLES_SERVICE_NAME: &str = "dev.restate.Awakeables";
pub const REFLECTION_SERVICE_NAME: &str = "grpc.reflection.v1alpha.ServerReflection";
pub const HEALTH_SERVICE_NAME: &str = "grpc.health.v1.Health";

#[cfg(feature = "mocks")]
pub mod mocks;
