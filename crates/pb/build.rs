// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::built_in_service_gen::RestateBuiltInServiceGen;
use crate::manual_response_built_in_service_gen::ManualResponseRestateBuiltInServiceGen;
use crate::multi_service_generator::MultiServiceGenerator;
use std::env;
use std::path::PathBuf;

mod multi_service_generator {
    use prost_build::{Service, ServiceGenerator};
    use std::collections::HashMap;

    pub struct MultiServiceGenerator {
        services: HashMap<&'static str, Box<dyn ServiceGenerator>>,
        fallback: Option<Box<dyn ServiceGenerator>>,
    }

    impl MultiServiceGenerator {
        pub fn new() -> Self {
            MultiServiceGenerator {
                services: Default::default(),
                fallback: None,
            }
        }

        pub fn with_svc(
            mut self,
            svc_fully_qualified_name: &'static str,
            svc_gen: Box<dyn ServiceGenerator>,
        ) -> Self {
            self.services.insert(svc_fully_qualified_name, svc_gen);
            self
        }

        pub fn with_fallback(mut self, svc_gen: Box<dyn ServiceGenerator>) -> Self {
            self.fallback = Some(svc_gen);
            self
        }
    }

    impl ServiceGenerator for MultiServiceGenerator {
        fn generate(&mut self, service: Service, buf: &mut String) {
            println!("Known services: {:#?}", self.services.keys());
            println!("{}.{}, {:#?}", service.package, service.proto_name, service);
            if let Some(svc_gen) = self
                .services
                .get_mut(format!("{}.{}", service.package, service.proto_name).as_str())
                .or(self.fallback.as_mut())
            {
                svc_gen.generate(service, buf)
            }
        }

        fn finalize(&mut self, buf: &mut String) {
            for (_, svc_gen) in self.services.iter_mut() {
                svc_gen.finalize(buf)
            }
            if let Some(svc_gen) = self.fallback.as_mut() {
                svc_gen.finalize(buf)
            }
        }

        fn finalize_package(&mut self, package: &str, buf: &mut String) {
            for (_, svc_gen) in self.services.iter_mut() {
                svc_gen.finalize_package(package, buf)
            }
            if let Some(svc_gen) = self.fallback.as_mut() {
                svc_gen.finalize_package(package, buf)
            }
        }
    }
}

mod built_in_service_gen {
    use prost_build::Service;
    use std::fmt::Write;

    pub struct RestateBuiltInServiceGen;

    impl prost_build::ServiceGenerator for RestateBuiltInServiceGen {
        fn generate(&mut self, service: Service, buf: &mut String) {
            // We generate two things:
            // * Interface to implement looking like the service definition, named [SvcName]BuiltInService
            // * Implementation of BuiltInService for [SvcName]BuiltInService that performs the routing

            // Everything is hidden behind the feature flag "builtin-service"

            let service_name = service.name;
            let svc_interface_name = format!("{}BuiltInService", service_name);
            let svc_interface_method_signatures: String = service.methods
                .iter()
                .fold(String::new(), |mut output, m| {
                    let _ = writeln!(output, "fn {}(&mut self, input: {}) -> impl std::future::Future<Output = Result<{}, restate_types::errors::InvocationError>> + Send + '_;", m.name, m.input_type, m.output_type);
                    output
                });

            let interface_def = format!(
                r#"
            #[cfg(feature = "builtin-service")]
            pub trait {svc_interface_name} {{
                {svc_interface_method_signatures}
            }}

            "#
            );

            buf.push_str(interface_def.as_str());

            let impl_built_in_service_match_arms: String = service.methods
                .iter()
                .fold(String::new(), |mut output, m| {
                    let _ = write!(
                        output,
                        r#""{}" => {{
                            use prost::Message;

                            let mut input_t = {}::decode(&mut input).map_err(|e| restate_types::errors::InvocationError::new(restate_types::errors::UserErrorCode::InvalidArgument, e.to_string()))?;
                            let output_t = T::{}(&mut self.0, input_t).await?;
                            Ok(output_t.encode_to_vec().into())
                        }},"#,
                        m.proto_name, m.input_type, m.name
                    );
                    output
                });

            let invoker_name = format!("{}Invoker", service_name);
            let invoker = format!(
                r#"
            #[cfg(feature = "builtin-service")]
            #[derive(Default)]
            pub struct {invoker_name}<T>(pub T);

            #[cfg(feature = "builtin-service")]
            impl<T: {svc_interface_name} + Send> crate::builtin_service::BuiltInService for {invoker_name}<T> {{
                fn invoke_builtin<'a>(&'a mut self, method: &'a str, mut input: prost::bytes::Bytes) -> impl std::future::Future<Output = Result<prost::bytes::Bytes, restate_types::errors::InvocationError>> + Send + '_ {{
                    async move {{
                        match method {{
                            {impl_built_in_service_match_arms}
                            _ => Err(restate_types::errors::InvocationError::service_method_not_found("{service_name}", method))
                        }}
                    }}
                }}
            }}

           "#
            );
            buf.push_str(invoker.as_str());
        }
    }
}

mod manual_response_built_in_service_gen {
    use prost_build::{Method, Service};
    use std::fmt::Write;

    #[derive(Default)]
    pub struct ManualResponseRestateBuiltInServiceGen {
        additional_methods: Vec<Method>,
    }

    impl ManualResponseRestateBuiltInServiceGen {
        pub fn with_additional_method(
            mut self,
            proto_name: &str,
            rust_input_type: &str,
            rust_output_type: &str,
        ) -> Self {
            use convert_case::{Case, Casing};

            self.additional_methods.push(Method {
                name: proto_name.from_case(Case::UpperCamel).to_case(Case::Snake),
                proto_name: proto_name.to_string(),
                comments: Default::default(),
                input_type: rust_input_type.to_string(),
                output_type: rust_output_type.to_string(),
                input_proto_type: "".to_string(),
                output_proto_type: "".to_string(),
                options: Default::default(),
                client_streaming: false,
                server_streaming: false,
            });

            self
        }
    }

    impl prost_build::ServiceGenerator for ManualResponseRestateBuiltInServiceGen {
        fn generate(&mut self, service: Service, buf: &mut String) {
            // We generate two things:
            // * Interface to implement looking like the service definition, named [SvcName]BuiltInService
            // * Implementation of BuiltInService for [SvcName]BuiltInService that performs the routing

            // Everything is hidden behind the feature flag "builtin-service"

            // --- Generate interface [SvcName]BuiltInService to implement

            let mut methods_to_generate = service.methods.clone();
            methods_to_generate.extend(self.additional_methods.clone());

            let service_name = service.name;
            let svc_interface_name = format!("{}BuiltInService", service_name);
            let svc_interface_method_signatures: String = methods_to_generate
                .iter()
                .fold(String::new(), |mut output, m| {
                    let _ = writeln!(
                        output,
                        "fn {}(&mut self, request: {}, response_serializer: crate::builtin_service::ResponseSerializer<{}>) -> impl std::future::Future<Output = Result<(), restate_types::errors::InvocationError>> + Send;",
                        m.name,
                        m.input_type,
                        m.output_type
                    );
                    output
                });

            let interface_def = format!(
                r#"
            #[cfg(feature = "builtin-service")]
            pub trait {svc_interface_name} {{
                {svc_interface_method_signatures}
            }}

            "#
            );
            buf.push_str(interface_def.as_str());

            // --- Generate invoker [SvcName]Invoker to route invocations through service methods

            let invoker_name = format!("{}Invoker", service_name);
            let impl_built_in_service_match_arms: String = methods_to_generate
                .iter()
                .fold(String::new(), |mut output, m| {
                    let _ = write!(
                        output,
                        r#""{}" => {{
                            use prost::Message;

                            let mut input_t = <{}>::decode(&mut input).map_err(|e| restate_types::errors::InvocationError::new(restate_types::errors::UserErrorCode::InvalidArgument, e.to_string()))?;
                            T::{}(&mut self.0, input_t, crate::builtin_service::ResponseSerializer::default()).await?;
                            Ok(())
                        }},"#,
                        m.proto_name,
                        m.input_type,
                        m.name
                    );
                    output
                });

            let invoker = format!(
                r#"
            #[cfg(feature = "builtin-service")]
            #[derive(Default)]
            pub struct {invoker_name}<T>(pub T);

            #[cfg(feature = "builtin-service")]
            impl<T: {svc_interface_name} + Send> crate::builtin_service::ManualResponseBuiltInService for {invoker_name}<T> {{
                fn invoke_builtin<'a>(&'a mut self, method: &'a str, mut input: prost::bytes::Bytes) -> impl std::future::Future<Output = Result<(), restate_types::errors::InvocationError>> + Send + '_ {{
                    async move {{
                        match method {{
                            {impl_built_in_service_match_arms}
                            _ => Err(restate_types::errors::InvocationError::service_method_not_found("{service_name}", method))
                        }}
                    }}
                }}
            }}

            "#
            );
            buf.push_str(invoker.as_str());
        }
    }
}

fn main() -> std::io::Result<()> {
    prost_build::Config::new()
        .bytes(["."])
        .service_generator(Box::new(
            MultiServiceGenerator::new()
                .with_svc("dev.restate.Awakeables", Box::new(RestateBuiltInServiceGen))
                .with_svc(
                    "dev.restate.internal.Proxy",
                    Box::new(RestateBuiltInServiceGen),
                )
                .with_svc(
                    "dev.restate.internal.RemoteContext",
                    Box::new(
                        ManualResponseRestateBuiltInServiceGen::default()
                            .with_additional_method(
                                "InternalOnCompletion",
                                "crate::restate::internal::JournalCompletionNotificationRequest",
                                "()",
                            )
                            .with_additional_method(
                                "InternalOnInactivityTimer",
                                "InactivityTimeoutTimerRequest",
                                "()",
                            )
                            .with_additional_method(
                                "InternalOnKill",
                                "crate::restate::internal::KillNotificationRequest",
                                "()",
                            ),
                    ),
                )
                .with_svc(
                    "dev.restate.internal.IdempotentInvoker",
                    Box::new(
                        ManualResponseRestateBuiltInServiceGen::default()
                            .with_additional_method(
                                "InternalOnResponse",
                                "crate::restate::internal::ServiceInvocationSinkRequest",
                                "()",
                            )
                            .with_additional_method("InternalOnTimer", "()", "()"),
                    ),
                )
                .with_fallback(
                    tonic_build::configure()
                        .build_client(false)
                        .build_transport(false)
                        .service_generator(),
                ),
        ))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &[
                "proto/dev/restate/ext.proto",
                "proto/dev/restate/services.proto",
                "proto/dev/restate/internal/services.proto",
                "proto/dev/restate/events.proto",
                "proto/dev/restate/internal/messages.proto",
            ],
            &["proto"],
        )?;

    prost_build::Config::new()
        .file_descriptor_set_path(
            PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
                .join("file_descriptor_set_test.bin"),
        )
        .bytes(["."])
        .service_generator(tonic_build::configure().service_generator())
        .extern_path(".dev.restate", "crate::restate")
        .compile_protos(
            &[
                "tests/proto/test.proto",
                "tests/proto/greeter.proto",
                "tests/proto/event_handler.proto",
            ],
            &["proto", "tests/proto"],
        )?;

    Ok(())
}
