// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Handler JSON schemas stored outside of the [`Schema`].
//!
//! The JSON schemas are only needed by the admin API, but dominate the schema's size. With
//! `json_schemas_split` enabled, the admin moves them into one [`DeploymentJsonSchemas`] object
//! per newly discovered deployment (see [`DeploymentJsonSchemas::from_discovery`]), stored under
//! [`deployment_json_schemas_key`](crate::metadata_store::keys::deployment_json_schemas_key) and
//! referenced by the deployment. Objects are immutable and their ids are never reused.
//!
//! The [`Schema`] never carries the JSON schemas of such deployments. The admin API fetches the
//! object when needed, and either fills it into the resolved service metadata
//! ([`DeploymentJsonSchemas::hydrate`]) or builds the OpenAPI document from it
//! ([`Schema::resolve_latest_service_openapi_with`]).
//!
//! Every id is reserved in the [`JsonSchemasIndex`] before its object is written, so that objects
//! which end up unreferenced (crashes, aborted updates, removed deployments) can be found and
//! deleted.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use restate_encoding::RestateEncoding;

use super::*;

use crate::endpoint_manifest;
use crate::identifiers::JsonSchemasId;
use crate::schema::invocation_target::{InputValidationRule, json_schema_description};
use crate::schema::metadata::updater::SchemaError;

/// The JSON schemas of all handlers of a deployment.
#[derive(Debug, Clone, PartialEq, bilrost::Message)]
pub struct DeploymentJsonSchemas {
    #[bilrost(tag(1))]
    version: Version,
    #[bilrost(tag(2))]
    services: HashMap<String, ServiceJsonSchemas>,
}

#[derive(Debug, Clone, Default, PartialEq, bilrost::Message)]
pub(super) struct ServiceJsonSchemas {
    #[bilrost(tag(1))]
    pub(super) handlers: HashMap<String, HandlerJsonSchemas>,
}

#[derive(Debug, Clone, Default, PartialEq, bilrost::Message)]
pub(super) struct HandlerJsonSchemas {
    #[bilrost(tag(1), encoding(RestateEncoding))]
    pub(super) input: Option<Value>,
    #[bilrost(tag(2), encoding(RestateEncoding))]
    pub(super) output: Option<Value>,
}

impl Versioned for DeploymentJsonSchemas {
    fn version(&self) -> Version {
        self.version
    }
}

impl HandlerJsonSchemas {
    /// Moves the inline JSON schemas out of the handler.
    fn take(handler: &mut Handler) -> Option<Self> {
        let input = if has_input_json_schema(&handler.input_rules) {
            Arc::make_mut(&mut handler.input_rules)
                .input_validation_rules
                .iter_mut()
                .find_map(|rule| match rule {
                    InputValidationRule::JsonValue { schema, .. } => schema.take(),
                    _ => None,
                })
        } else {
            None
        };
        let output = if handler.output_rules.json_schema.is_some() {
            Arc::make_mut(&mut handler.output_rules).json_schema.take()
        } else {
            None
        };

        (input.is_some() || output.is_some()).then_some(Self { input, output })
    }

    /// Fills the JSON schemas into the handler metadata, as if they were stored inline.
    fn hydrate(&self, handler: &mut service::HandlerMetadata) {
        if let Some(input) = &self.input {
            if let Some(description) = json_schema_description(input) {
                handler.input_description = description;
            }
            handler.input_json_schema = Some(input.clone());
        }
        if let Some(output) = &self.output {
            if let Some(description) = json_schema_description(output) {
                handler.output_description = description;
            }
            handler.output_json_schema = Some(output.clone());
        }
    }
}

impl DeploymentJsonSchemas {
    /// The JSON schemas of the given discovered services, as they would be stored inline by the
    /// schema updater. Returns `None` if none of the handlers has a JSON schema.
    pub(in crate::schema) fn from_discovery(
        services: &[endpoint_manifest::Service],
    ) -> Result<Option<Self>, SchemaError> {
        let mut json_schemas = HashMap::new();
        for service in services {
            let service_type = ServiceType::from(service.ty);
            let mut handlers = HashMap::new();
            for handler in &service.handlers {
                let mut handler = Handler::from_schema(
                    service.name.as_ref(),
                    service_type,
                    true,
                    handler.clone(),
                )?;
                if let Some(schemas) = HandlerJsonSchemas::take(&mut handler) {
                    handlers.insert(handler.name, schemas);
                }
            }
            if !handlers.is_empty() {
                json_schemas.insert(service.name.to_string(), ServiceJsonSchemas { handlers });
            }
        }

        Ok((!json_schemas.is_empty()).then_some(Self {
            version: Version::MIN,
            services: json_schemas,
        }))
    }

    /// Moves the inline JSON schemas out of the deployment, if it has any.
    fn take(deployment: &mut Deployment) -> Option<Self> {
        let mut services = HashMap::new();
        for (service_name, service) in &mut deployment.services {
            if !service.handlers.values().any(Handler::has_json_schemas) {
                continue;
            }

            let service = Arc::make_mut(service);
            // The cached OpenAPI document was inferred with the JSON schemas
            service.service_openapi_cache = default_empty_openapi_cache();
            let handlers: HashMap<_, _> = service
                .handlers
                .iter_mut()
                .filter_map(|(name, handler)| {
                    HandlerJsonSchemas::take(handler).map(|schemas| (name.clone(), schemas))
                })
                .collect();
            services.insert(service_name.clone(), ServiceJsonSchemas { handlers });
        }

        (!services.is_empty()).then_some(Self {
            version: Version::MIN,
            services,
        })
    }

    pub(super) fn service(&self, service_name: &str) -> Option<&ServiceJsonSchemas> {
        self.services.get(service_name)
    }

    /// Fills the JSON schemas into the service metadata resolved from a deployment that
    /// references this object, as if they were stored inline.
    pub fn hydrate(&self, service: &mut service::ServiceMetadata) {
        let Some(service_schemas) = self.services.get(&service.name) else {
            return;
        };
        for (handler_name, handler) in &mut service.handlers {
            if let Some(handler_schemas) = service_schemas.handlers.get(handler_name) {
                handler_schemas.hydrate(handler);
            }
        }
    }
}

impl Deployment {
    /// Removes the inline JSON schemas and references the stored `json_schemas` object `id`
    /// instead. Returns the removed JSON schemas.
    pub(super) fn detach_json_schemas(
        &mut self,
        id: JsonSchemasId,
    ) -> Option<DeploymentJsonSchemas> {
        self.json_schemas = Some(id);
        DeploymentJsonSchemas::take(self)
    }
}

impl Handler {
    fn has_json_schemas(&self) -> bool {
        has_input_json_schema(&self.input_rules) || self.output_rules.json_schema.is_some()
    }
}

/// Like `InputRules::json_schema().is_some()`, without cloning the schema.
fn has_input_json_schema(input_rules: &InputRules) -> bool {
    input_rules.input_validation_rules.iter().any(|rule| {
        matches!(
            rule,
            InputValidationRule::JsonValue {
                schema: Some(_),
                ..
            }
        )
    })
}

impl Schema {
    /// The JSON schemas object referenced by the given deployment, if any.
    pub fn json_schemas_ref(&self, deployment_id: &DeploymentId) -> Option<JsonSchemasId> {
        self.deployments
            .get(deployment_id)
            .and_then(|deployment| deployment.json_schemas)
    }

    /// All JSON schemas objects referenced by this schema.
    pub fn json_schemas_refs(&self) -> HashSet<JsonSchemasId> {
        self.deployments
            .values()
            .filter_map(|deployment| deployment.json_schemas)
            .collect()
    }

    /// The inline JSON schemas of all deployments that have any.
    pub fn inline_json_schemas(&self) -> Vec<(DeploymentId, DeploymentJsonSchemas)> {
        self.deployments
            .iter()
            .filter_map(|(deployment_id, deployment)| {
                let json_schemas = DeploymentJsonSchemas::take(&mut deployment.clone())?;
                Some((*deployment_id, json_schemas))
            })
            .collect()
    }

    /// Makes the deployment reference the stored `json_schemas` object `id` instead of keeping
    /// its JSON schemas inline. Only applies if the deployment's inline JSON schemas are equal to
    /// `json_schemas`, returns whether it did.
    ///
    /// Doesn't bump the schema version.
    pub fn attach_json_schemas(
        &mut self,
        deployment_id: DeploymentId,
        id: JsonSchemasId,
        json_schemas: &DeploymentJsonSchemas,
    ) -> bool {
        let Some(deployment) = self.unindexed.deployments.get(&deployment_id) else {
            return false;
        };
        // Detach from a copy, as detaching modifies the deployment before we know whether its
        // JSON schemas match, and a mismatch must leave it untouched. The copy is shallow (the
        // services are shared until a handler's JSON schemas are taken out).
        let mut detached = deployment.clone();
        if detached.detach_json_schemas(id).as_ref() != Some(json_schemas) {
            return false;
        }
        self.unindexed.deployments.insert(deployment_id, detached);
        self.active_service_revisions =
            ActiveServiceRevision::create_index(self.unindexed.deployments.values());
        true
    }
}

/// Index of all [`DeploymentJsonSchemas`] objects in the metadata store.
#[derive(Debug, Clone, bilrost::Message)]
pub struct JsonSchemasIndex {
    #[bilrost(tag(1))]
    version: Version,
    #[bilrost(tag(2))]
    entries: Vec<JsonSchemasIndexEntry>,
}

#[derive(Debug, Clone, bilrost::Message)]
struct JsonSchemasIndexEntry {
    #[bilrost(tag(1))]
    id: JsonSchemasId,
    #[bilrost(tag(2))]
    reserved_at: MillisSinceEpoch,
}

crate::bilrost_storage_encode_decode!(JsonSchemasIndex);

impl Versioned for JsonSchemasIndex {
    fn version(&self) -> Version {
        self.version
    }
}

impl JsonSchemasIndex {
    /// Returns the index with the given ids added.
    pub fn reserve(
        index: Option<Self>,
        ids: impl IntoIterator<Item = JsonSchemasId>,
        reserved_at: MillisSinceEpoch,
    ) -> Self {
        let mut index = index.map_or_else(
            || Self {
                version: Version::MIN,
                entries: vec![],
            },
            |mut index| {
                index.version = index.version.next();
                index
            },
        );
        index.entries.extend(
            ids.into_iter()
                .map(|id| JsonSchemasIndexEntry { id, reserved_at }),
        );
        index
    }

    /// Ids that are not referenced by `schema` and were reserved before `reserved_before`.
    pub fn unreferenced(
        &self,
        schema: &Schema,
        reserved_before: MillisSinceEpoch,
    ) -> Vec<JsonSchemasId> {
        let referenced = schema.json_schemas_refs();
        self.entries
            .iter()
            .filter(|entry| entry.reserved_at < reserved_before && !referenced.contains(&entry.id))
            .map(|entry| entry.id)
            .collect()
    }

    /// Removes the given ids. Returns `None` if none of them is in the index.
    pub fn remove(mut self, ids: &HashSet<JsonSchemasId>) -> Option<Self> {
        let len = self.entries.len();
        self.entries.retain(|entry| !ids.contains(&entry.id));
        (self.entries.len() != len).then(|| {
            self.version = self.version.next();
            self
        })
    }

    pub fn ids(&self) -> impl Iterator<Item = JsonSchemasId> + '_ {
        self.entries.iter().map(|entry| entry.id)
    }
}

mod storage {
    use bytes::BytesMut;

    use restate_platform::storage::{
        StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError,
    };

    use super::DeploymentJsonSchemas;
    use crate::storage;

    impl StorageEncode for DeploymentJsonSchemas {
        fn default_codec(&self) -> StorageCodecKind {
            StorageCodecKind::ZstdBilrostDefault
        }

        fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
            storage::encode::encode_bilrost_zstd(self, buf)
        }
    }

    impl StorageDecode for DeploymentJsonSchemas {
        fn decode<B: bytes::Buf>(buf: B, kind: StorageCodecKind) -> Result<Self, StorageDecodeError>
        where
            Self: Sized,
        {
            match kind {
                StorageCodecKind::ZstdBilrostDefault => storage::decode::decode_bilrost_zstd(buf),
                _ => Err(StorageDecodeError::UnsupportedCodecKind(kind)),
            }
        }
    }
}
