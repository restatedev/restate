use super::*;

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
                component_schemas.handlers = ComponentSchemas::compute_handlers(handlers.clone());
                if let ComponentLocation::Deployment {
                    latest_deployment, ..
                } = &mut component_schemas.location
                {
                    *latest_deployment = deployment_id;
                }
            })
            .or_insert_with(|| ComponentSchemas {
                revision,
                handlers: ComponentSchemas::compute_handlers(handlers),
                ty,
                location: ComponentLocation::Deployment {
                    latest_deployment: deployment_id,
                    public: true,
                },
            });

        // Make sure to register it in the deployment
        self.deployments
            .get_mut(&deployment_id)
            .expect("Deployment must be present at this point")
            .components
            .push(
                component_schemas
                    .as_component_metadata(name)
                    .expect("Should not be a built-in service"),
            );
    }

    pub(crate) fn apply_modify_component(&mut self, name: String, new_public_value: bool) {
        if let Some(schemas) = self.components.get_mut(&name) {
            // Update the public field
            if let ComponentLocation::Deployment {
                public: old_public_value,
                ..
            } = &mut schemas.location
            {
                *old_public_value = new_public_value;
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
