//! This module contains Restate public protobuf definitions

use once_cell::sync::Lazy;
use prost_reflect::DescriptorPool;
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
    pub mod services {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(env!("OUT_DIR"), "/dev.restate.rs"));
    }
}

pub static DESCRIPTOR_POOL: Lazy<DescriptorPool> = Lazy::new(|| {
    DescriptorPool::decode(
        include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin")).as_ref(),
    )
    .expect("The built-in descriptor pool should be valid")
});

pub const INGRESS_SERVICE_NAME: &str = "dev.restate.Ingress";
pub const REFLECTION_SERVICE_NAME: &str = "grpc.reflection.v1alpha.ServerReflection";
pub const HEALTH_SERVICE_NAME: &str = "grpc.health.v1.Health";

#[cfg(feature = "mocks")]
pub mod mocks;
