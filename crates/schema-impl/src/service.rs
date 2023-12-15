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

use crate::schemas_impl::ServiceLocation;
use bytes::Bytes;
use restate_schema_api::proto_symbol::ProtoSymbolResolver;
use restate_schema_api::service::{MethodMetadata, ServiceMetadata, ServiceMetadataResolver};

impl ServiceMetadataResolver for Schemas {
    fn resolve_latest_service_metadata(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<ServiceMetadata> {
        self.use_service_schema(service_name.as_ref(), |service_schemas| {
            map_to_service_metadata(service_name.as_ref(), service_schemas)
        })
        .flatten()
    }

    fn descriptors(&self, service_name: impl AsRef<str>) -> Option<Vec<Bytes>> {
        self.get_file_descriptors_by_symbol_name(service_name.as_ref())
    }

    fn list_services(&self) -> Vec<ServiceMetadata> {
        let schemas = self.0.load();
        schemas
            .services
            .iter()
            .filter_map(|(service_name, service_schemas)| {
                map_to_service_metadata(service_name, service_schemas)
            })
            .collect()
    }

    fn is_service_public(&self, service_name: impl AsRef<str>) -> Option<bool> {
        self.use_service_schema(service_name.as_ref(), |service_schemas| {
            service_schemas.location.is_ingress_available()
        })
    }
}

pub(crate) fn map_to_service_metadata(
    service_name: &str,
    service_schemas: &ServiceSchemas,
) -> Option<ServiceMetadata> {
    match &service_schemas.location {
        ServiceLocation::BuiltIn { .. } => None, // We filter out from this interface ingress only services
        ServiceLocation::Deployment {
            latest_deployment,
            public,
        } => Some(ServiceMetadata {
            name: service_name.to_string(),
            methods: service_schemas
                .methods
                .values()
                .map(|method_desc| MethodMetadata {
                    name: method_desc.descriptor().name().to_string(),
                    input_type: method_desc.descriptor().input().full_name().to_string(),
                    output_type: method_desc.descriptor().output().full_name().to_string(),
                    key_field_number: match &service_schemas.instance_type {
                        InstanceTypeMetadata::Keyed { .. } => Some(
                            method_desc
                                .input_field_annotated(FieldAnnotation::Key)
                                .expect("Method must exist in the parsed service methods"),
                        ),
                        _ => None,
                    },
                })
                .collect(),
            instance_type: (&service_schemas.instance_type)
                .try_into()
                .expect("Checked in the line above whether this is a built-in service or not"),
            deployment_id: latest_deployment.clone(),
            revision: service_schemas.revision,
            public: *public,
        }),
    }
}
