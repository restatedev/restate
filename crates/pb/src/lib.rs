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
        pub mod v1 {
            #![allow(warnings)]
            #![allow(clippy::all)]
            #![allow(unknown_lints)]
            include!(concat!(env!("OUT_DIR"), "/grpc.reflection.v1.rs"));
        }

        pub mod v1alpha {
            #![allow(warnings)]
            #![allow(clippy::all)]
            #![allow(unknown_lints)]
            include!(concat!(env!("OUT_DIR"), "/grpc.reflection.v1alpha.rs"));
        }
    }
}
pub mod restate {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unknown_lints)]
    include!(concat!(env!("OUT_DIR"), "/dev.restate.rs"));

    pub mod internal {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]

        include!(concat!(env!("OUT_DIR"), "/dev.restate.internal.rs"));

        #[cfg(feature = "restate-types")]
        mod conversions {
            use super::*;

            impl From<InvocationFailure> for restate_types::errors::InvocationError {
                fn from(value: InvocationFailure) -> Self {
                    restate_types::errors::InvocationError::new(value.code, value.message)
                }
            }

            impl From<restate_types::errors::InvocationError> for InvocationFailure {
                fn from(value: restate_types::errors::InvocationError) -> Self {
                    Self {
                        code: value.code().into(),
                        message: value.message().to_string(),
                    }
                }
            }
        }
    }
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
pub const REFLECTION_SERVICE_NAME: &str = "grpc.reflection.v1.ServerReflection";
pub const REFLECTION_SERVICE_NAME_V1ALPHA: &str = "grpc.reflection.v1alpha.ServerReflection";
pub const HEALTH_SERVICE_NAME: &str = "grpc.health.v1.Health";
pub const PROXY_SERVICE_NAME: &str = "restate_internal_pp_proxy";
pub const PROXY_PROXY_THROUGH_METHOD_NAME: &str = "ProxyThrough";
pub const REMOTE_CONTEXT_SERVICE_NAME: &str = "restate_remote_context";
pub const REMOTE_CONTEXT_INTERNAL_ON_COMPLETION_METHOD_NAME: &str = "InternalOnCompletion";
pub const REMOTE_CONTEXT_INTERNAL_ON_KILL_METHOD_NAME: &str = "InternalOnKill";
pub const IDEMPOTENT_INVOKER_SERVICE_NAME: &str = "dev.restate.internal.IdempotentInvoker";
pub const IDEMPOTENT_INVOKER_INVOKE_METHOD_NAME: &str = "Invoke";
pub const IDEMPOTENT_INVOKER_INTERNAL_ON_RESPONSE_METHOD_NAME: &str = "InternalOnResponse";
pub const IDEMPOTENT_INVOKER_INTERNAL_ON_TIMER_METHOD_NAME: &str = "InternalOnTimer";

#[cfg(feature = "builtin-service")]
pub mod builtin_service {
    use prost::bytes::Bytes;
    use restate_types::errors::InvocationError;
    use restate_types::invocation::ResponseResult;
    use std::future::Future;
    use std::marker::PhantomData;

    pub trait BuiltInService {
        fn invoke_builtin<'a>(
            &'a mut self,
            method: &'a str,
            input: Bytes,
        ) -> impl Future<Output = Result<Bytes, InvocationError>> + Send + '_;
    }

    pub trait ManualResponseBuiltInService {
        fn invoke_builtin<'a>(
            &'a mut self,
            method: &'a str,
            input: Bytes,
        ) -> impl Future<Output = Result<(), InvocationError>> + Send + '_;
    }

    #[derive(Default)]
    pub struct ResponseSerializer<T>(PhantomData<T>);

    impl<T: prost::Message> ResponseSerializer<T> {
        pub fn serialize_success(&self, t: T) -> ResponseResult {
            ResponseResult::Success(t.encode_to_vec().into())
        }

        pub fn serialize_failure(&self, err: InvocationError) -> ResponseResult {
            ResponseResult::Failure(err.code().into(), err.message().into())
        }
    }

    impl TryFrom<crate::restate::internal::ServiceInvocationSinkRequest> for ResponseResult {
        type Error = &'static str;

        fn try_from(
            value: crate::restate::internal::ServiceInvocationSinkRequest,
        ) -> Result<Self, Self::Error> {
            match value.response {
                Some(
                    crate::restate::internal::service_invocation_sink_request::Response::Success(s),
                ) => Ok(ResponseResult::Success(s)),
                Some(
                    crate::restate::internal::service_invocation_sink_request::Response::Failure(e),
                ) => Ok(ResponseResult::Failure(e.code.into(), e.message.into())),
                None => Err("response_result field must be set"),
            }
        }
    }

    impl From<ResponseResult> for crate::restate::internal::service_invocation_sink_request::Response {
        fn from(value: ResponseResult) -> Self {
            match value {
                ResponseResult::Success(s) => {
                    crate::restate::internal::service_invocation_sink_request::Response::Success(s)
                }

                ResponseResult::Failure(code, message) => {
                    crate::restate::internal::service_invocation_sink_request::Response::Failure(
                        crate::restate::internal::InvocationFailure {
                            code: code.into(),
                            message: message.to_string(),
                        },
                    )
                }
            }
        }
    }
}

#[cfg(feature = "mocks")]
pub mod mocks;
