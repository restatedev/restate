use super::*;

impl SchemasInner {
    pub(crate) fn compute_modify_service_updates(
        &self,
        name: String,
        public: bool,
    ) -> Result<SchemasUpdateCommand, RegistrationError> {
        check_service_name_reserved(&name)?;
        if !self.services.contains_key(&name) {
            return Err(RegistrationError::UnknownService(name));
        }

        Ok(SchemasUpdateCommand::ModifyService { name, public })
    }

    pub(crate) fn apply_modify_service(
        &mut self,
        name: String,
        new_public_value: bool,
    ) -> Result<(), RegistrationError> {
        let schemas = self
            .services
            .get_mut(&name)
            .ok_or_else(|| RegistrationError::UnknownService(name.clone()))?;

        // Update proto_symbols
        if let ServiceLocation::ServiceEndpoint {
            public: old_public_value,
            latest_endpoint,
        } = &schemas.location
        {
            match (*old_public_value, new_public_value) {
                (true, false) => {
                    self.proto_symbols
                        .remove_service(schemas.service_descriptor());
                }
                (false, true) => {
                    self.proto_symbols
                        .add_service(latest_endpoint, schemas.service_descriptor());
                }
                _ => {}
            }
        }

        // Update the public field
        if let ServiceLocation::ServiceEndpoint {
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
    ) -> Result<(), RegistrationError> {
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

pub fn check_service_name_reserved(svc_name: &str) -> Result<(), RegistrationError> {
    if svc_name.starts_with(GRPC_SERVICE_NAME_PREFIX)
        || svc_name.starts_with(RESTATE_SERVICE_NAME_PREFIX)
    {
        return Err(RegistrationError::ModifyInternalService(
            svc_name.to_string(),
        ));
    }
    Ok(())
}
