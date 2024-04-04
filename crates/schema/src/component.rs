// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use restate_schema_api::component::ComponentMetadataResolver;
use restate_schema_api::invocation_target::InvocationTargetMetadata;

pub(crate) fn check_reserved_name(name: &str) -> Result<(), ComponentError> {
    if name.to_lowercase().starts_with("restate")
        || name.to_lowercase().eq_ignore_ascii_case("openapi")
    {
        return Err(ComponentError::ReservedName(name.to_string()));
    }
    Ok(())
}

pub(super) fn to_component_type(ty: schema::ComponentType) -> ComponentType {
    match ty {
        schema::ComponentType::VirtualObject => ComponentType::VirtualObject,
        schema::ComponentType::Service => ComponentType::Service,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredHandlerMetadata {
    name: String,
    ty: HandlerType,
    input: InputRules,
    output: OutputRules,
}

impl DiscoveredHandlerMetadata {
    pub(crate) fn from_schema(
        component_type: ComponentType,
        handler: schema::Handler,
    ) -> Result<Self, ComponentError> {
        let handler_type = match handler.handler_type {
            None => HandlerType::default_for_component_type(component_type),
            Some(schema::HandlerType::Exclusive) => HandlerType::Exclusive,
            Some(schema::HandlerType::Shared) => HandlerType::Shared,
        };

        Ok(Self {
            name: handler.name.to_string(),
            ty: handler_type,
            input: handler
                .input
                .map(|s| DiscoveredHandlerMetadata::input_rules_from_schema(&handler.name, s))
                .transpose()?
                .unwrap_or_default(),
            output: handler
                .output
                .map(DiscoveredHandlerMetadata::output_rules_from_schema)
                .transpose()?
                .unwrap_or_default(),
        })
    }

    fn input_rules_from_schema(
        handler_name: &str,
        schema: schema::InputPayload,
    ) -> Result<InputRules, ComponentError> {
        let required = schema.required.unwrap_or(false);

        let mut input_validation_rules = vec![];

        // Add rule for empty if input not required
        if !required {
            input_validation_rules.push(InputValidationRule::NoBodyAndContentType);
        }

        // Add content-type validation rule
        let content_type = schema
            .content_type
            .map(|s| {
                s.parse()
                    .map_err(|e| ComponentError::BadInputContentType(handler_name.to_owned(), e))
            })
            .transpose()?
            .unwrap_or_default();
        if schema.json_schema.is_some() {
            input_validation_rules.push(InputValidationRule::JsonValue { content_type });
        } else {
            input_validation_rules.push(InputValidationRule::ContentType { content_type });
        }

        Ok(InputRules {
            input_validation_rules,
        })
    }

    fn output_rules_from_schema(
        schema: schema::OutputPayload,
    ) -> Result<OutputRules, ComponentError> {
        Ok(if let Some(ct) = schema.content_type {
            OutputRules {
                content_type_rule: OutputContentTypeRule::Set {
                    content_type: HeaderValue::from_str(&ct)
                        .map_err(|e| ComponentError::BadOutputContentType(ct, e))?,
                    set_content_type_if_empty: schema.set_content_type_if_empty.unwrap_or(false),
                    has_json_schema: schema.json_schema.is_some(),
                },
            }
        } else {
            OutputRules {
                content_type_rule: OutputContentTypeRule::None,
            }
        })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct HandlerSchemas {
    pub(crate) target_meta: InvocationTargetMetadata,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ComponentSchemas {
    pub(crate) revision: ComponentRevision,
    pub(crate) handlers: HashMap<String, HandlerSchemas>,
    pub(crate) ty: ComponentType,
    pub(crate) location: ComponentLocation,
}

impl ComponentSchemas {
    pub(crate) fn compute_handlers(
        component_ty: ComponentType,
        handlers: Vec<DiscoveredHandlerMetadata>,
    ) -> HashMap<String, HandlerSchemas> {
        handlers
            .into_iter()
            .map(|handler| {
                (
                    handler.name,
                    HandlerSchemas {
                        target_meta: InvocationTargetMetadata {
                            public: true,
                            component_ty,
                            handler_ty: handler.ty,
                            input_rules: handler.input,
                            output_rules: handler.output,
                        },
                    },
                )
            })
            .collect()
    }

    pub(crate) fn as_component_metadata(&self, name: String) -> ComponentMetadata {
        ComponentMetadata {
            name,
            handlers: self
                .handlers
                .iter()
                .map(|(h_name, h_schemas)| HandlerMetadata {
                    name: h_name.clone(),
                    ty: h_schemas.target_meta.handler_ty,
                    input_description: h_schemas.target_meta.input_rules.to_string(),
                    output_description: h_schemas.target_meta.output_rules.to_string(),
                })
                .collect(),
            ty: self.ty,
            deployment_id: self.location.latest_deployment,
            revision: self.revision,
            public: self.location.public,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ComponentLocation {
    pub(crate) latest_deployment: DeploymentId,
    pub(crate) public: bool,
}

impl ComponentMetadataResolver for SchemaRegistry {
    fn resolve_latest_component(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<ComponentMetadata> {
        let name = component_name.as_ref();
        self.use_component_schema(name, |component_schemas| {
            component_schemas.as_component_metadata(name.to_owned())
        })
    }

    fn resolve_latest_component_type(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<ComponentType> {
        self.use_component_schema(component_name.as_ref(), |component_schemas| {
            component_schemas.ty
        })
    }

    fn list_components(&self) -> Vec<ComponentMetadata> {
        self.components
            .iter()
            .map(|(component_name, component_schemas)| {
                component_schemas.as_component_metadata(component_name.clone())
            })
            .collect()
    }
}

impl ComponentMetadataResolver for SchemaView {
    fn resolve_latest_component(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<ComponentMetadata> {
        self.0.load().resolve_latest_component(component_name)
    }

    fn resolve_latest_component_type(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<ComponentType> {
        self.0.load().resolve_latest_component_type(component_name)
    }

    fn list_components(&self) -> Vec<ComponentMetadata> {
        self.0.load().list_components()
    }
}
