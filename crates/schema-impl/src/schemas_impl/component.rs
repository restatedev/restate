use super::*;

use http::header::InvalidHeaderValue;
use http::HeaderValue;
use restate_schema_api::invocation_target::{
    BadInputContentType, InputValidationRule, OutputContentTypeRule,
};

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum ComponentError {
    #[error("cannot insert/modify component '{0}' as it contains a reserved name")]
    #[code(restate_errors::META0005)]
    ReservedName(String),
    #[error("detected a new component '{0}' revision with a component type different from the previous revision. Component type cannot be changed across revisions")]
    #[code(restate_errors::META0006)]
    DifferentType(String),
    #[error("the component '{0}' already exists but the new revision removed the handlers {1:?}")]
    #[code(restate_errors::META0006)]
    RemovedHandlers(String, Vec<String>),
    #[error("the handler '{0}' input content-type is not valid: {1}")]
    #[code(unknown)]
    BadInputContentType(String, BadInputContentType),
    #[error("the handler '{0}' output content-type is not valid: {1}")]
    #[code(unknown)]
    BadOutputContentType(String, InvalidHeaderValue),
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

impl SchemasInner {
    pub(crate) fn compute_modify_component_updates(
        &self,
        name: String,
        public: bool,
    ) -> Result<SchemasUpdateCommand, ErrorKind> {
        check_reserved_name(&name)?;
        if !self.components.contains_key(&name) {
            return Err(ErrorKind::NotFound);
        }

        Ok(SchemasUpdateCommand::ModifyComponent { name, public })
    }

    pub(crate) fn apply_insert_component(
        &mut self,
        name: String,
        revision: ComponentRevision,
        ty: ComponentType,
        deployment_id: DeploymentId,
        handlers: Vec<DiscoveredHandlerMetadata>,
    ) {
        info!(rpc.service = name, "Registering component");

        if tracing::enabled!(tracing::Level::DEBUG) {
            handlers.iter().for_each(|handler| {
                debug!(
                    rpc.service = name,
                    rpc.method = handler.name.as_str(),
                    "Registering handler"
                )
            });
        }

        // We need to retain the `public` field from previous registrations
        let component_schemas = self
            .components
            .entry(name.clone())
            .and_modify(|component_schemas| {
                info!(rpc.service = name, "Overwriting existing component schemas");

                component_schemas.revision = revision;
                component_schemas.ty = ty;
                component_schemas.handlers =
                    ComponentSchemas::compute_handlers(ty, handlers.clone());
                component_schemas.location.latest_deployment = deployment_id;
            })
            .or_insert_with(|| ComponentSchemas {
                revision,
                handlers: ComponentSchemas::compute_handlers(ty, handlers),
                ty,
                location: ComponentLocation {
                    latest_deployment: deployment_id,
                    public: true,
                },
            });

        // Make sure to register it in the deployment
        self.deployments
            .get_mut(&deployment_id)
            .expect("Deployment must be present at this point")
            .components
            .push(component_schemas.as_component_metadata(name));
    }

    pub(crate) fn apply_modify_component(&mut self, name: String, new_public_value: bool) {
        if let Some(schemas) = self.components.get_mut(&name) {
            // Update the public field
            schemas.location.public = new_public_value;
            for h in schemas.handlers.values_mut() {
                h.target_meta.public = new_public_value;
            }
        }
    }

    pub(crate) fn apply_remove_component(&mut self, name: String, revision: ComponentRevision) {
        let entry = self.components.entry(name);
        match entry {
            Entry::Occupied(e) if e.get().revision == revision => {
                e.remove();
            }
            _ => {}
        }
    }
}

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
