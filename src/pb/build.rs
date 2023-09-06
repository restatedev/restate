// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use prost_build::Service;
use std::env;
use std::path::PathBuf;

struct RestateBuiltInServiceGen;

impl prost_build::ServiceGenerator for RestateBuiltInServiceGen {
    fn generate(&mut self, service: Service, buf: &mut String) {
        if service.package != "dev.restate" {
            // Skip non-restate packages
            return;
        }

        // We generate two things:
        // * Interface to implement looking like the service definition, named [SvcName]BuiltInService
        // * Implementation of BuiltInService for [SvcName]BuiltInService that performs the routing

        // Everything is hidden behind the feature flag "builtin-service"

        let svc_interface_name = format!("{}BuiltInService", service.name);
        let svc_interface_method_signatures: String = service.methods.iter().map(|m| format!("async fn {}(&mut self, input: {}) -> Result<{}, restate_types::errors::InvocationError>;\n", m.name, m.input_type, m.output_type)).collect();

        let interface_def = format!(
            r#"
            #[cfg(feature = "builtin-service")]
            #[async_trait::async_trait]
            pub trait {svc_interface_name} {{
                {svc_interface_method_signatures}
            }}

        "#
        );

        buf.push_str(interface_def.as_str());

        let impl_built_in_service_match_arms: String = service.methods.iter().map(|m| format!(r#""{}" => {{
            use prost::Message;

            let mut input_t = {}::decode(&mut input).map_err(|e| restate_types::errors::InvocationError::new(restate_types::errors::UserErrorCode::InvalidArgument, e.to_string()))?;
            let output_t = T::{}(&mut self.0, input_t).await?;
            Ok(output_t.encode_to_vec().into())
        }},"#, m.proto_name, m.input_type, m.name)).collect();
        let invoker_name = format!("{}Invoker", service.name);
        let invoker = format!(
            r#"
            #[cfg(feature = "builtin-service")]
            #[derive(Default)]
            pub struct {invoker_name}<T>(pub T);

            #[cfg(feature = "builtin-service")]
            #[async_trait::async_trait]
            impl<T: {svc_interface_name} + Send> crate::builtin_service::BuiltInService for {invoker_name}<T> {{
                async fn invoke_builtin(&mut self, method: &str, mut input: prost::bytes::Bytes) -> Result<prost::bytes::Bytes, restate_types::errors::InvocationError> {{
                    match method {{
                        {impl_built_in_service_match_arms}
                        _ => Err(restate_types::errors::InvocationError::new(restate_types::errors::UserErrorCode::NotFound, format!("{{}} not found", method)))
                    }}
                }}
            }}

        "#
        );
        buf.push_str(invoker.as_str());
    }
}

fn main() -> std::io::Result<()> {
    prost_build::Config::new()
        .bytes(["."])
        .file_descriptor_set_path(
            PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
                .join("file_descriptor_set.bin"),
        )
        .service_generator(
            tonic_build::configure()
                .build_client(false)
                .build_transport(false)
                .service_generator(),
        )
        .compile_protos(
            &[
                "proto/grpc/health/v1/health.proto",
                "proto/grpc/reflection/v1alpha/reflection.proto",
                "proto/dev/restate/services.proto",
            ],
            &[
                "proto/grpc/health/v1",
                "proto/grpc/reflection/v1alpha",
                "proto/dev/restate",
            ],
        )?;

    prost_build::Config::new()
        .bytes(["."])
        .service_generator(Box::new(RestateBuiltInServiceGen))
        .compile_protos(
            &["proto/dev/restate/services.proto"],
            &["proto/dev/restate"],
        )?;

    prost_build::Config::new()
        .file_descriptor_set_path(
            PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
                .join("file_descriptor_set_test.bin"),
        )
        .bytes(["."])
        .service_generator(tonic_build::configure().service_generator())
        .compile_protos(
            &["tests/proto/test.proto", "tests/proto/greeter.proto"],
            &["tests/proto"],
        )?;

    Ok(())
}
