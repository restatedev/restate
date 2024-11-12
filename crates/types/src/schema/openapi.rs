use crate::identifiers::ServiceRevision;
use crate::invocation::ServiceType;
use crate::schema::invocation_target::{InputValidationRule, OutputContentTypeRule};
use crate::schema::service::HandlerSchemas;
use restate_utoipa::openapi::path::{Operation, Parameter, ParameterIn};
use restate_utoipa::openapi::request_body::RequestBody;
use restate_utoipa::openapi::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceOpenAPI {
    paths: Paths,
}

impl ServiceOpenAPI {
    pub fn infer(
        service_name: &str,
        service_type: ServiceType,
        handlers: &HashMap<String, HandlerSchemas>,
    ) -> Self {
        let root_path = if service_type.is_keyed() {
            format!("/{service_name}/{{key}}/")
        } else {
            format!("/{service_name}/")
        };

        let mut parameters: Vec<RefOr<Parameter>> = vec![];
        if service_type.is_keyed() {
            parameters.push(parameters_ref(KEY_PARAMETER_REF_NAME).into());
        }
        if service_type != ServiceType::Workflow {
            parameters.push(parameters_ref(IDEMPOTENCY_KEY_PARAMETER_REF_NAME).into());
        }

        let mut paths = Paths::builder();
        for (handler_name, handler_schemas) in handlers {
            let operation_id = format!("{service_name}-{handler_name}");

            if !handler_schemas.target_meta.public {
                // We don't generate the OpenAPI route for that.
                continue;
            }

            let request_body = infer_handler_request_body(handler_schemas);
            let response = infer_handler_response(handler_schemas);

            let call_item = PathItem::builder()
                .summary(Some(format!("Call {service_name}/{handler_name}")))
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
                .operation(
                    HttpMethod::Post,
                    Operation::builder()
                        .operation_id(Some(operation_id.clone()))
                        .parameters(Some(parameters.clone()))
                        .tag(service_name.to_string())
                        .request_body(request_body.clone())
                        .response("200", response)
                        .response("default", responses_ref(ERROR_RESPONSE_REF_NAME))
                        .build(),
                )
                .build();
            paths = paths.path(format!("{root_path}{handler_name}"), call_item);

            let send_item = PathItem::builder()
                .summary(Some(format!("Send to {service_name}/{handler_name}")))
                .description(Some(
                    handler_schemas
                        .documentation
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| {
                            format!("Send request to {service_name} handler {handler_name}")
                        }),
                ))
                .operation(
                    HttpMethod::Post,
                    Operation::builder()
                        .operation_id(Some(format!("{operation_id}-send")))
                        .parameters(Some(parameters.clone()))
                        .parameter(parameters_ref(DELAY_PARAMETER_REF_NAME))
                        .tag(service_name.to_string())
                        .request_body(request_body)
                        .response("200", responses_ref(SEND_RESPONSE_REF_NAME))
                        .response("202", responses_ref(SEND_RESPONSE_REF_NAME))
                        .response("default", responses_ref(ERROR_RESPONSE_REF_NAME))
                        .build(),
                )
                .build();
            paths = paths.path(format!("{root_path}{handler_name}/send"), send_item);
        }

        ServiceOpenAPI {
            paths: paths.build(),
        }
    }

    /// Returns the OpenAPI contract of this individual service
    pub(crate) fn to_openapi_contract(
        &self,
        service_name: &str,
        documentation: Option<&str>,
        revision: ServiceRevision,
    ) -> serde_json::Value {
        // TODO how to add servers?! :(
        serde_json::to_value(
            OpenApi::builder()
                .info(
                    Info::builder()
                        .title(service_name.to_owned())
                        .version(revision.to_string())
                        .description(documentation)
                        .build(),
                )
                .paths(self.paths.clone())
                .components(Some(restate_components()))
                .build(),
        )
        .expect("Mapping OpenAPI to JSON should never fail")
    }

    // We need these for back-compat

    pub fn empty() -> Self {
        Self {
            paths: Default::default(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.paths.paths.is_empty()
    }
}

fn infer_handler_request_body(handler_schemas: &HandlerSchemas) -> Option<RequestBody> {
    let mut is_required = true;
    if handler_schemas
        .target_meta
        .input_rules
        .input_validation_rules
        .contains(&InputValidationRule::NoBodyAndContentType)
    {
        is_required = false;
    }

    // This whole thing is a byproduct of how we store
    let content_type_and_schema = if let Some(r) = handler_schemas
        .target_meta
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
        .target_meta
        .input_rules
        .input_validation_rules
        .iter()
        .find(|rule| matches!(rule, InputValidationRule::JsonValue { .. }))
    {
        match r {
            InputValidationRule::JsonValue {
                content_type,
                schema,
            } => Some((
                content_type.to_string(),
                Content::new(Some(Schema::new(schema.clone()))),
            )),
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

fn infer_handler_response(handler_schemas: &HandlerSchemas) -> Response {
    match (
        &handler_schemas.target_meta.output_rules.json_schema,
        &handler_schemas.target_meta.output_rules.content_type_rule,
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
        (Some(schema), OutputContentTypeRule::Set { content_type, .. }) => Response::builder()
            .content(
                content_type
                    .to_str()
                    .expect("content_type should have been checked before during registration"),
                Content::new(Some(Schema::new(schema.clone()))),
            )
            .build(),
    }
}

fn restate_components() -> Components {
    Components::builder()
        .parameter(DELAY_PARAMETER_REF_NAME, delay_parameter())
        .parameter(KEY_PARAMETER_REF_NAME, key_parameter())
        .parameter(
            IDEMPOTENCY_KEY_PARAMETER_REF_NAME,
            idempotency_key_parameter(),
        )
        .response(ERROR_RESPONSE_REF_NAME, error_response())
        .response(SEND_RESPONSE_REF_NAME, send_response())
        .build()
}

fn parameters_ref(name: &str) -> Ref {
    Ref::new(format!("#/components/parameters/{name}"))
}

const DELAY_PARAMETER_REF_NAME: &str = "delay";

fn delay_parameter() -> Parameter {
    Parameter::builder()
        .name("delay")
        .parameter_in(ParameterIn::Query)
        .schema(Some(
            string_json_schema()
        ))
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

const IDEMPOTENCY_KEY_PARAMETER_REF_NAME: &str = "idempotencyKey";

fn idempotency_key_parameter() -> Parameter {
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

fn responses_ref(name: &str) -> Ref {
    Ref::new(format!("#/components/responses/{name}"))
}

const ERROR_RESPONSE_REF_NAME: &str = "Error";

fn error_response() -> Response {
    Response::builder()
        .description("Error response")
        .content("application/json", Content::new(Some(Schema::new(json!({
            "type": "object",
            "title": "Error",
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
                "description": {
                    "type": "string",
                    "title": "Verbose error description"
                }
            },
            "required": ["message"],
            "additionalProperties": false
        })))))
        .build()
}

const SEND_RESPONSE_REF_NAME: &str = "Send";

fn send_response() -> Response {
    Response::builder()
        .description("Send response")
        .content(
            "application/json",
            Content::new(Some(Schema::new(json!({
                "type": "object",
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
                        "format": "date-time"
                    }
                },
                "required": ["invocationId", "status"],
                "additionalProperties": false
            })))),
        )
        .build()
}

fn string_json_schema() -> Schema {
    Schema::new(json!({"type": "string"}))
}
