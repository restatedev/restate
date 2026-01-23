// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::Handler;

use crate::identifiers::ServiceRevision;
use crate::invocation::{InvocationTargetType, ServiceType, WorkflowHandlerType};
use crate::net::address::{AdvertisedAddress, HttpIngressPort, PeerNetAddress};
use crate::schema::invocation_target::{InputValidationRule, OutputContentTypeRule};
use restate_utoipa::openapi::extensions::Extensions;
use restate_utoipa::openapi::path::{Operation, Parameter, ParameterIn};
use restate_utoipa::openapi::request_body::RequestBody;
use restate_utoipa::openapi::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ServiceOpenAPI {
    rpc_paths: Paths,
    send_paths: Paths,
    attach_paths: Paths,
    get_output_paths: Paths,
    extensions: Option<Extensions>,
    components: Components,
}

impl ServiceOpenAPI {
    pub(super) fn infer(
        service_name: &str,
        service_type: ServiceType,
        service_metadata: &HashMap<String, String>,
        handlers: &HashMap<String, Handler>,
    ) -> Self {
        let mut schemas_collector = Vec::new();

        let root_path = if service_type.is_keyed() {
            format!("/{service_name}/{{key}}/")
        } else {
            format!("/{service_name}/")
        };

        let mut call_parameters: Vec<RefOr<Parameter>> = vec![];
        let mut attach_get_output_parameters: Vec<RefOr<Parameter>> = vec![];
        if service_type.is_keyed() {
            call_parameters.push(parameters_ref(KEY_PARAMETER_REF_NAME).into());
            attach_get_output_parameters.push(parameters_ref(KEY_PARAMETER_REF_NAME).into());
        }
        if service_type != ServiceType::Workflow {
            call_parameters.push(parameters_ref(IDEMPOTENCY_KEY_HEADER_PARAMETER_REF_NAME).into());
            attach_get_output_parameters
                .push(parameters_ref(IDEMPOTENCY_KEY_PATH_PARAMETER_REF_NAME).into());
        }

        let mut rpc_paths = Paths::builder();
        let mut send_paths = Paths::builder();
        let mut attach_paths = Paths::builder();
        let mut get_output_paths = Paths::builder();
        for (handler_name, handler_schemas) in handlers {
            let operation_id = handler_name;
            let summary = Some(handler_name.clone());

            if !handler_schemas.public.unwrap_or(true) {
                // We don't generate the OpenAPI route for that.
                continue;
            }

            let request_body =
                infer_handler_request_body(operation_id, handler_schemas, &mut schemas_collector);
            let response =
                infer_handler_response(operation_id, handler_schemas, &mut schemas_collector);

            let extensions = if !handler_schemas.metadata.is_empty() {
                Some(
                    Extensions::builder()
                        .add(
                            "x-restate-extensions",
                            serde_json::to_value(&handler_schemas.metadata)
                                .expect("Serialization of map<string, string> should never fail!"),
                        )
                        .build(),
                )
            } else {
                None
            };

            let call_item = PathItem::builder()
                .operation(
                    HttpMethod::Post,
                    Operation::builder()
                        .summary(summary.clone())
                        .operation_id(Some(operation_id.clone()))
                        .description(Some(
                            handler_schemas
                                .documentation
                                .as_ref()
                                .cloned()
                                .unwrap_or_else(|| {
                                    format!(
                                        "Call {service_name} handler {handler_name} and wait for response"
                                    )
                                }),
                        ))
                        .parameters(Some(call_parameters.clone()))
                        .request_body(request_body.clone())
                        .response("200", response.clone())
                        .response("default", responses_ref(GENERIC_ERROR_RESPONSE_REF_NAME))
                        .extensions(extensions.clone())
                        .build(),
                )
                .build();
            rpc_paths = rpc_paths.path(format!("{root_path}{handler_name}"), call_item);

            let send_item = PathItem::builder()
                .operation(
                    HttpMethod::Post,
                    Operation::builder()
                        .summary(summary.clone())
                        .operation_id(Some(format!("{operation_id}Send")))
                        .description(Some(
                            handler_schemas
                                .documentation
                                .as_ref()
                                .cloned()
                                .unwrap_or_else(|| {
                                    format!("Send request to {service_name} handler {handler_name}")
                                }),
                        ))
                        .parameters(Some(call_parameters.clone()))
                        .parameter(parameters_ref(DELAY_PARAMETER_REF_NAME))
                        .tag(SEND_TAG_NAME.to_string())
                        .request_body(request_body)
                        .response("200", responses_ref(SEND_RESPONSE_REF_NAME))
                        .response("202", responses_ref(SEND_RESPONSE_REF_NAME))
                        .response("default", responses_ref(GENERIC_ERROR_RESPONSE_REF_NAME))
                        .extensions(extensions.clone())
                        .build(),
                )
                .build();
            send_paths = send_paths.path(format!("{root_path}{handler_name}/send"), send_item);

            if service_type == ServiceType::Workflow {
                // We add attach/get output only for workflow run
                if handler_schemas.target_ty
                    == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
                {
                    let attach_item = PathItem::builder()
                        .operation(
                            HttpMethod::Get,
                            Operation::builder()
                                .summary(summary.clone())
                                .operation_id(Some(format!("{operation_id}Attach")))
                                .description(Some(
                                    handler_schemas
                                        .documentation
                                        .as_ref()
                                        .cloned()
                                        .unwrap_or_else(|| {
                                            format!(
                                                "Attach to the running instance of {service_name}/{handler_name} workflow and wait to finish"
                                            )
                                        }),
                                ))
                                .parameters(Some(attach_get_output_parameters.clone()))
                                .tag(ATTACH_TAG_NAME.to_string())
                                .response("200", response.clone())
                                .response("404", responses_ref(INVOCATION_NOT_FOUND_ERROR_RESPONSE_REF_NAME))
                                .response("default", responses_ref(GENERIC_ERROR_RESPONSE_REF_NAME))
                                .extensions(extensions.clone())
                                .build(),
                        )
                        .build();
                    attach_paths = attach_paths.path(
                        format!("/restate/workflow/{service_name}/{{key}}/attach"),
                        attach_item,
                    );

                    let output_item = PathItem::builder()
                        .operation(
                            HttpMethod::Get,
                            Operation::builder()
                                .summary(summary.clone())
                                .operation_id(Some(format!("{operation_id}Output")))
                                .description(Some(
                                    handler_schemas
                                        .documentation
                                        .as_ref()
                                        .cloned()
                                        .unwrap_or_else(|| {
                                            format!(
                                                "Get output of the {service_name}/{handler_name} workflow"
                                            )
                                        }),
                                ))
                                .parameters(Some(attach_get_output_parameters.clone()))
                                .tag(GET_OUTPUT_TAG_NAME.to_string())
                                .response("200", response)
                                .response("404", responses_ref(INVOCATION_NOT_FOUND_ERROR_RESPONSE_REF_NAME))
                                .response("470", responses_ref(INVOCATION_NOT_READY_RESPONSE_REF_NAME))
                                .response("default", responses_ref(GENERIC_ERROR_RESPONSE_REF_NAME))
                                .extensions(extensions)
                                .build(),
                        )
                        .build();
                    get_output_paths = get_output_paths.path(
                        format!("/restate/workflow/{service_name}/{{key}}/output"),
                        output_item,
                    );
                }
            } else {
                // We add attach/get output for each individual handler with idempotency key
                let attach_item = PathItem::builder()
                    .operation(
                        HttpMethod::Get,
                        Operation::builder()
                            .summary(summary.clone())
                            .operation_id(Some(format!("{operation_id}Attach")))
                            .description(Some(
                                handler_schemas
                                    .documentation
                                    .as_ref()
                                    .cloned()
                                    .unwrap_or_else(|| {
                                        format!(
                                            "Attach to a running instance of {service_name}/{handler_name} using the idempotency key and wait to finish"
                                        )
                                    }),
                            ))
                            .parameters(Some(attach_get_output_parameters.clone()))
                            .tag(ATTACH_TAG_NAME.to_string())
                            .response("200", response.clone())
                            .response("404", responses_ref(INVOCATION_NOT_FOUND_ERROR_RESPONSE_REF_NAME))
                            .response("default", responses_ref(GENERIC_ERROR_RESPONSE_REF_NAME))
                            .extensions(extensions.clone())
                            .build(),
                    )
                    .build();
                attach_paths = attach_paths.path(
                    format!(
                        "/restate/invocation{root_path}{handler_name}/{{idempotencyKey}}/attach"
                    ),
                    attach_item,
                );

                let output_item = PathItem::builder()
                    .operation(
                        HttpMethod::Get,
                        Operation::builder()
                            .summary(summary.clone())
                            .operation_id(Some(format!("{operation_id}Output")))
                            .description(Some(
                                handler_schemas
                                    .documentation
                                    .as_ref()
                                    .cloned()
                                    .unwrap_or_else(|| {
                                        format!(
                                            "Get output of a running instance of {service_name}/{handler_name} using idempotency key"
                                        )
                                    }),
                            ))
                            .parameters(Some(attach_get_output_parameters.clone()))
                            .tag(GET_OUTPUT_TAG_NAME.to_string())
                            .response("200", response)
                            .response("404", responses_ref(INVOCATION_NOT_FOUND_ERROR_RESPONSE_REF_NAME))
                            .response("470", responses_ref(INVOCATION_NOT_READY_RESPONSE_REF_NAME))
                            .response("default", responses_ref(GENERIC_ERROR_RESPONSE_REF_NAME))
                            .extensions(extensions)
                            .build(),
                    )
                    .build();
                get_output_paths = get_output_paths.path(
                    format!(
                        "/restate/invocation{root_path}{handler_name}/{{idempotencyKey}}/output"
                    ),
                    output_item,
                );
            }
        }

        ServiceOpenAPI {
            rpc_paths: rpc_paths.build(),
            send_paths: send_paths.build(),
            attach_paths: attach_paths.build(),
            get_output_paths: get_output_paths.build(),
            extensions: if !service_metadata.is_empty() {
                Some(
                    Extensions::builder()
                        .add(
                            "x-restate-extensions",
                            serde_json::to_value(service_metadata)
                                .expect("Serialization of map<string, string> should never fail!"),
                        )
                        .build(),
                )
            } else {
                None
            },
            components: Components::builder()
                .schemas_from_iter(schemas_collector.into_iter().map(|(schema_name, schema)| {
                    let ref_refix = format!("#/components/schemas/{schema_name}");
                    (schema_name, normalize_schema_refs(&ref_refix, schema))
                }))
                .build(),
        }
    }

    /// Returns the OpenAPI contract of this individual service
    pub(super) fn to_openapi_contract(
        &self,
        service_name: &str,
        ingress_url: AdvertisedAddress<HttpIngressPort>,
        documentation: Option<&str>,
        revision: ServiceRevision,
    ) -> Value {
        let mut components = restate_components();

        // Make sure Restate components appear last in the list
        let mut restate_components_schemas = components.schemas.clone();
        components.schemas.clear();
        components
            .schemas
            .append(&mut self.components.schemas.clone());
        components.schemas.append(&mut restate_components_schemas);

        let mut paths = self.rpc_paths.clone();
        paths.merge(self.send_paths.clone());
        paths.merge(self.attach_paths.clone());
        paths.merge(self.get_output_paths.clone());

        let servers = match ingress_url.into_address().unwrap() {
            // we are not going to expose the unix-socket address via openapi
            PeerNetAddress::Uds(_) => None,
            PeerNetAddress::Http(uri) => Some(vec![
                Server::builder()
                    .description(Some("Restate Ingress URL"))
                    .url(uri.to_string())
                    .build(),
            ]),
        };

        serde_json::to_value(
            OpenApi::builder()
                .info(
                    Info::builder()
                        .title(service_name.to_owned())
                        .version(revision.to_string())
                        .description(documentation)
                        .extensions(self.extensions.clone())
                        .build(),
                )
                .servers(servers)
                .paths(paths)
                .tags(Some(restate_tags()))
                .components(Some(components))
                .build(),
        )
        .expect("Mapping OpenAPI to JSON should never fail")
    }
}

fn request_schema_name(operation_id: &str) -> String {
    format!("{operation_id}Request")
}

fn request_schema_ref(operation_id: &str) -> Ref {
    Ref::new(format!(
        "#/components/schemas/{}",
        request_schema_name(operation_id)
    ))
}

fn response_schema_name(operation_id: &str) -> String {
    format!("{operation_id}Response")
}

fn response_schema_ref(operation_id: &str) -> Ref {
    Ref::new(format!(
        "#/components/schemas/{}",
        response_schema_name(operation_id)
    ))
}

fn infer_handler_request_body(
    operation_id: &str,
    handler_schemas: &Handler,
    schemas_collector: &mut Vec<(String, Schema)>,
) -> Option<RequestBody> {
    let mut is_required = true;
    if handler_schemas
        .input_rules
        .input_validation_rules
        .contains(&InputValidationRule::NoBodyAndContentType)
    {
        is_required = false;
    }

    // This whole thing is a byproduct of how we store
    let content_type_and_schema = if let Some(r) = handler_schemas
        .input_rules
        .input_validation_rules
        .iter()
        .find(|rule| matches!(rule, InputValidationRule::ContentType { .. }))
    {
        match r {
            InputValidationRule::ContentType { content_type } => {
                Some((content_type.to_string(), Content::new::<Schema>(None)))
            }
            _ => unreachable!(),
        }
    } else if let Some(r) = handler_schemas
        .input_rules
        .input_validation_rules
        .iter()
        .find(|rule| matches!(rule, InputValidationRule::JsonValue { .. }))
    {
        match r {
            InputValidationRule::JsonValue {
                content_type,
                schema,
            } => {
                let mut schema = Schema::new(schema.clone().unwrap_or(Value::Bool(true)));
                if schema.0 == Value::Bool(true) {
                    // Even though the boolean schema true is valid Json Schema,
                    // tools generally like more the {} schema more.
                    schema.0 = Value::Object(Default::default());
                }

                schemas_collector.push((request_schema_name(operation_id), schema));

                Some((
                    content_type.to_string(),
                    Content::new(Some(request_schema_ref(operation_id))),
                ))
            }
            _ => unreachable!(),
        }
    } else {
        None
    };

    if let Some((content_type, content)) = content_type_and_schema {
        Some(
            RequestBody::builder()
                .required(Some(if is_required {
                    Required::True
                } else {
                    Required::False
                }))
                .content(content_type, content)
                .build(),
        )
    } else {
        None
    }
}

fn infer_handler_response(
    operation_id: &str,
    handler_schemas: &Handler,
    schemas_collector: &mut Vec<(String, Schema)>,
) -> Response {
    match (
        &handler_schemas.output_rules.json_schema,
        &handler_schemas.output_rules.content_type_rule,
    ) {
        (_, OutputContentTypeRule::None) => Response::builder().description("Empty").build(),
        (None, OutputContentTypeRule::Set { content_type, .. }) => Response::builder()
            .content(
                content_type
                    .to_str()
                    .expect("content_type should have been checked before during registration"),
                Content::builder().build(),
            )
            .build(),
        (Some(schema), OutputContentTypeRule::Set { content_type, .. }) => {
            let schema = Schema::new(schema.clone());
            schemas_collector.push((response_schema_name(operation_id), schema));

            Response::builder()
                .content(
                    content_type
                        .to_str()
                        .expect("content_type should have been checked before during registration"),
                    Content::new(Some(response_schema_ref(operation_id))),
                )
                .build()
        }
    }
}

fn restate_components() -> Components {
    Components::builder()
        .parameter(DELAY_PARAMETER_REF_NAME, delay_parameter())
        .parameter(KEY_PARAMETER_REF_NAME, key_parameter())
        .parameter(
            IDEMPOTENCY_KEY_HEADER_PARAMETER_REF_NAME,
            idempotency_key_header_parameter(),
        )
        .parameter(
            IDEMPOTENCY_KEY_PATH_PARAMETER_REF_NAME,
            idempotency_key_path_parameter(),
        )
        .response(GENERIC_ERROR_RESPONSE_REF_NAME, generic_error_response())
        .response(
            INVOCATION_NOT_FOUND_ERROR_RESPONSE_REF_NAME,
            invocation_not_found_error_response(),
        )
        .response(
            INVOCATION_NOT_READY_RESPONSE_REF_NAME,
            invocation_not_ready_error_response(),
        )
        .response(SEND_RESPONSE_REF_NAME, send_response())
        .schema(
            RESTATE_ERROR_SCHEMA_REF_NAME,
            Schema::new(restate_error_json_schema()),
        )
        .build()
}

fn parameters_ref(name: &str) -> Ref {
    Ref::new(format!("#/components/parameters/{name}"))
}

fn schemas_ref(name: &str) -> Ref {
    Ref::new(format!("#/components/schemas/{name}"))
}

fn responses_ref(name: &str) -> Ref {
    Ref::new(format!("#/components/responses/{name}"))
}

const DELAY_PARAMETER_REF_NAME: &str = "delay";

fn delay_parameter() -> Parameter {
    Parameter::builder()
        .name("delay")
        .parameter_in(ParameterIn::Query)
        .schema(Some(
            string_json_schema()
        ))
        .example(Some(Value::String("10s".to_string())))
        .required(Required::False)
        .description(Some("Specify the delay to execute the operation, for more info check the [delay documentation](https://docs.restate.dev/invoke/http#sending-a-delayed-message-over-http)"))
        .build()
}

const KEY_PARAMETER_REF_NAME: &str = "key";

fn key_parameter() -> Parameter {
    Parameter::builder()
        .name("key")
        .parameter_in(ParameterIn::Path)
        .schema(Some(string_json_schema()))
        .required(Required::True)
        .build()
}

const IDEMPOTENCY_KEY_HEADER_PARAMETER_REF_NAME: &str = "idempotencyKeyHeader";

fn idempotency_key_header_parameter() -> Parameter {
    Parameter::builder()
        .name("idempotency-key")
        .parameter_in(ParameterIn::Header)
        .schema(Some(
            string_json_schema()
        ))
        .required(Required::False)
        .description(Some("Idempotency key to execute the request, for more details checkout the [idempotency key documentation](https://docs.restate.dev/invoke/http#invoke-a-handler-idempotently)."))
        .build()
}

const IDEMPOTENCY_KEY_PATH_PARAMETER_REF_NAME: &str = "idempotencyKeyPath";

fn idempotency_key_path_parameter() -> Parameter {
    Parameter::builder()
        .name("idempotencyKey")
        .parameter_in(ParameterIn::Path)
        .schema(Some(
            string_json_schema()
        ))
        .required(Required::True)
        .description(Some("Idempotency key used to execute the original request, for more details checkout the [idempotency key documentation](https://docs.restate.dev/invoke/http#invoke-a-handler-idempotently)."))
        .build()
}

const GENERIC_ERROR_RESPONSE_REF_NAME: &str = "GenericError";

fn generic_error_response() -> Response {
    Response::builder()
        .description("Error response")
        .content(
            "application/json",
            ContentBuilder::new()
                .schema(Some(schemas_ref(RESTATE_ERROR_SCHEMA_REF_NAME)))
                .example(Some(error_response_example()))
                .build(),
        )
        .build()
}

// Ideally we code generate this
fn error_response_example() -> Value {
    json!({
        "code": 500,
        "message": "Internal server error",
        "description": "Very bad error happened"
    })
}

const INVOCATION_NOT_FOUND_ERROR_RESPONSE_REF_NAME: &str = "InvocationNotFoundError";

fn invocation_not_found_error_response() -> Response {
    Response::builder()
        .description("Invocation not found")
        .content(
            "application/json",
            ContentBuilder::new()
                .schema(Some(schemas_ref(RESTATE_ERROR_SCHEMA_REF_NAME)))
                .example(Some(invocation_not_found_error_response_example()))
                .build(),
        )
        .build()
}

fn invocation_not_found_error_response_example() -> Value {
    json!({
        "code": 404,
        "message": "Not found"
    })
}

const INVOCATION_NOT_READY_RESPONSE_REF_NAME: &str = "InvocationNotReadyError";

fn invocation_not_ready_error_response() -> Response {
    Response::builder()
        .description("Invocation still running, output is not ready yet")
        .content(
            "application/json",
            ContentBuilder::new()
                .schema(Some(schemas_ref(RESTATE_ERROR_SCHEMA_REF_NAME)))
                .example(Some(invocation_not_ready_error_response_example()))
                .build(),
        )
        .build()
}

fn invocation_not_ready_error_response_example() -> Value {
    json!({
        "code": 470,
        "message": "Not ready"
    })
}

const SEND_RESPONSE_REF_NAME: &str = "Send";

fn send_response() -> Response {
    Response::builder()
        .description("Send response")
        .content(
            "application/json",
            ContentBuilder::new()
                .schema(Some(Schema::new(send_response_json_schema())))
                .example(Some(send_response_example()))
                .build(),
        )
        .build()
}

// Ideally we code generate this
fn send_response_json_schema() -> Value {
    json!({
        "type": "object",
        "title": "RestateSendResponse",
        "properties": {
            "invocationId": {
                "type": "string"
            },
            "status": {
                "type": "string",
                "enum": ["Accepted", "PreviouslyAccepted"]
            },
            "executionTime": {
                "type": "string",
                "format": "date-time",
                "description": "Time when the invocation will be executed, in case 'delay' is used"
            }
        },
        "required": ["invocationId", "status"],
        "additionalProperties": false
    })
}

// Ideally we code generate this
fn send_response_example() -> Value {
    json!({
        "invocationId": "inv_1gdJBtdVEcM942bjcDmb1c1khoaJe11Hbz",
        "status": "Accepted",
    })
}

fn string_json_schema() -> Schema {
    Schema::new(json!({"type": "string"}))
}

const RESTATE_ERROR_SCHEMA_REF_NAME: &str = "RestateError";

// Ideally we code generate this
fn restate_error_json_schema() -> Value {
    json!({
        "type": "object",
        "title": "RestateError",
        "description": "Generic error used by Restate to propagate Terminal errors/exceptions back to callers",
        "properties": {
            "code": {
                "type": "number",
                "title": "error code"
            },
            "message": {
                "type": "string",
                "title": "Error message"
            },
            "stacktrace": {
                "type": "string",
                "title": "Stacktrace of the error"
            },
            "metadata": {
                "type": "object",
                "title": "Additional metadata map",
                "additionalProperties": {
                    "type": "string"
                }
            }
        },
        "required": ["message"],
        "additionalProperties": false
    })
}

const REQUEST_RESPONSE_TAG_NAME: &str = "Request Response";
const SEND_TAG_NAME: &str = "Send";
const ATTACH_TAG_NAME: &str = "Attach";
const GET_OUTPUT_TAG_NAME: &str = "Get Output";

fn restate_tags() -> Vec<Tag> {
    vec![
        Tag::builder()
            .name(REQUEST_RESPONSE_TAG_NAME)
            .description(Some("Request response to handler".to_string()))
            .external_docs(Some(ExternalDocs::new(
                "https://docs.restate.dev/invoke/http",
            )))
            .build(),
        Tag::builder()
            .name(SEND_TAG_NAME)
            .description(Some("Send to handler".to_string()))
            .external_docs(Some(ExternalDocs::new(
                "https://docs.restate.dev/invoke/http",
            )))
            .build(),
        Tag::builder()
            .name(ATTACH_TAG_NAME)
            .description(Some("Attach to invocation".to_string()))
            .external_docs(Some(ExternalDocs::new(
                "https://docs.restate.dev/invoke/http",
            )))
            .build(),
        Tag::builder()
            .name(GET_OUTPUT_TAG_NAME)
            .description(Some("Get invocation output".to_string()))
            .external_docs(Some(ExternalDocs::new(
                "https://docs.restate.dev/invoke/http",
            )))
            .build(),
    ]
}

// We need to normalize all the $refs in the schema to append the ref_prefix
fn normalize_schema_refs(ref_prefix: &str, mut schema: Schema) -> Schema {
    normalize_schema_refs_inner(ref_prefix, &mut schema.0);
    schema
}

fn normalize_schema_refs_inner(ref_prefix: &str, schema: &mut Value) {
    match schema {
        Value::Array(array_value) => {
            for val in array_value.iter_mut() {
                normalize_schema_refs_inner(ref_prefix, val)
            }
        }
        Value::Object(obj_value) => {
            // Replace $ref attribute, if existing and starts with #
            if let Some(ref_value) = obj_value.get_mut("$ref")
                && let Some(str_ref_value) = ref_value.as_str()
                && str_ref_value.starts_with('#')
            {
                *ref_value = Value::String(format!(
                    "{ref_prefix}{}",
                    str_ref_value.trim_start_matches('#')
                ));
            }

            for val in obj_value.values_mut() {
                normalize_schema_refs_inner(ref_prefix, val)
            }
        }
        _ => {}
    }
}
