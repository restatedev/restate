// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use restate_schema_api::component::{ComponentMetadata, HandlerMetadata};
use restate_schema_api::deployment::DeploymentType;
use restate_schema_api::subscription::Subscription;
use restate_types::identifiers::{ComponentRevision, DeploymentId, SubscriptionId};
use serde_with::serde_as;
use std::collections::HashMap;
use std::sync::Arc;

pub mod component;
pub mod deployment;
mod invocation_target;
mod subscriptions;

use crate::component::ComponentSchemas;
use crate::deployment::DeploymentSchemas;
use restate_types::{Version, Versioned};

/// Schema information which automatically loads the latest version when accessing it.
///
/// Temporary bridge until users are migrated to directly using the metadata
/// provided schema information.
#[derive(Debug, Default, Clone, derive_more::From)]
pub struct UpdateableSchema(Arc<ArcSwap<Schema>>);

/// The schema information
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Schema {
    pub version: Version,
    pub components: HashMap<String, ComponentSchemas>,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    pub deployments: HashMap<DeploymentId, DeploymentSchemas>,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    pub subscriptions: HashMap<SubscriptionId, Subscription>,
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            components: HashMap::default(),
            deployments: HashMap::default(),
            subscriptions: HashMap::default(),
        }
    }
}

impl Schema {
    pub fn increment_version(&mut self) {
        self.version = self.version.next();
    }

    /// Find existing deployment that knows about a particular endpoint
    pub fn find_existing_deployment_by_endpoint(
        &self,
        endpoint: &DeploymentType,
    ) -> Option<(&DeploymentId, &DeploymentSchemas)> {
        self.deployments.iter().find(|(_, schemas)| {
            schemas.metadata.ty.protocol_type() == endpoint.protocol_type()
                && schemas.metadata.ty.normalized_address() == endpoint.normalized_address()
        })
    }

    pub fn find_existing_deployment_by_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(&DeploymentId, &DeploymentSchemas)> {
        self.deployments.iter().find(|(id, _)| deployment_id == *id)
    }

    pub(crate) fn use_component_schema<F, R>(
        &self,
        component_name: impl AsRef<str>,
        f: F,
    ) -> Option<R>
    where
        F: FnOnce(&ComponentSchemas) -> R,
    {
        self.components.get(component_name.as_ref()).map(f)
    }
}

impl Versioned for Schema {
    fn version(&self) -> Version {
        self.version
    }
}

pub mod storage {
    use crate::Schema;
    use restate_types::flexbuffers_storage_encode_decode;

    flexbuffers_storage_encode_decode!(Schema);
}

#[cfg(feature = "test-util")]
mod test_util {
    use super::*;

    use restate_schema_api::component::ComponentMetadataResolver;
    use restate_schema_api::invocation_target::InvocationTargetResolver;
    use restate_test_util::{assert, assert_eq};

    impl Schema {
        #[track_caller]
        pub fn assert_component_handler(&self, component_name: &str, handler_name: &str) {
            assert!(self
                .resolve_latest_invocation_target(component_name, handler_name)
                .is_some());
        }

        #[track_caller]
        pub fn assert_component_revision(&self, component_name: &str, revision: ComponentRevision) {
            assert_eq!(
                self.resolve_latest_component(component_name)
                    .unwrap()
                    .revision,
                revision
            );
        }

        #[track_caller]
        pub fn assert_component_deployment(
            &self,
            component_name: &str,
            deployment_id: DeploymentId,
        ) {
            assert_eq!(
                self.resolve_latest_component(component_name)
                    .unwrap()
                    .deployment_id,
                deployment_id
            );
        }

        #[track_caller]
        pub fn assert_component(&self, component_name: &str) -> ComponentMetadata {
            self.resolve_latest_component(component_name).unwrap()
        }
    }
}
