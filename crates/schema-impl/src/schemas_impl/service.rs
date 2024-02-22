use super::*;

impl SchemasInner {
    pub(crate) fn compute_modify_service_updates(
        &self,
        name: String,
        public: bool,
    ) -> Result<SchemasUpdateCommand, SchemasUpdateError> {
        check_service_name_reserved(&name)?;
        if !self.services.contains_key(&name) {
            return Err(SchemasUpdateError::UnknownService(name));
        }

        Ok(SchemasUpdateCommand::ModifyService { name, public })
    }

    pub(crate) fn apply_modify_service(
        &mut self,
        name: String,
        new_public_value: bool,
    ) -> Result<(), SchemasUpdateError> {
        let schemas = self
            .services
            .get_mut(&name)
            .ok_or_else(|| SchemasUpdateError::UnknownService(name.clone()))?;

        // Update proto_symbols
        if let ServiceLocation::Deployment {
            latest_deployment, ..
        } = &schemas.location
        {
            if new_public_value {
                self.proto_symbols
                    .add_service(latest_deployment, schemas.service_descriptor());
            } else {
                self.proto_symbols
                    .remove_service(schemas.service_descriptor());
            }
        }

        // Update the public field
        if let ServiceLocation::Deployment {
            public: old_public_value,
            ..
        } = &mut schemas.location
        {
            *old_public_value = new_public_value;
        }

        Ok(())
    }

    pub(crate) fn apply_remove_service(
        &mut self,
        name: String,
        revision: ServiceRevision,
    ) -> Result<(), SchemasUpdateError> {
        let entry = self.services.entry(name);
        match entry {
            Entry::Occupied(e) if e.get().revision == revision => {
                let schemas = e.remove();
                self.proto_symbols
                    .remove_service(schemas.service_descriptor());
            }
            _ => {}
        }

        Ok(())
    }
}

const RESTATE_SERVICE_NAME_PREFIX: &str = "dev.restate.";
const GRPC_SERVICE_NAME_PREFIX: &str = "grpc.";

pub fn check_service_name_reserved(svc_name: &str) -> Result<(), SchemasUpdateError> {
    if svc_name.starts_with(GRPC_SERVICE_NAME_PREFIX)
        || svc_name.starts_with(RESTATE_SERVICE_NAME_PREFIX)
    {
        return Err(SchemasUpdateError::ModifyInternalService(
            svc_name.to_string(),
        ));
    }
    Ok(())
}
