// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod deployment;
pub mod invocation_target;
pub mod openapi;
pub mod service;
pub mod subscriptions;
mod v2;

use std::collections::HashMap;
use std::sync::Arc;

use serde_with::serde_as;

use self::deployment::DeploymentSchemas;
use self::deployment::DeploymentType;
use self::service::ServiceSchemas;
use self::subscriptions::Subscription;
use crate::Version;
use crate::Versioned;
use crate::identifiers::{DeploymentId, SubscriptionId};
use crate::metadata::GlobalMetadata;
use crate::net::metadata::MetadataContainer;
use crate::net::metadata::MetadataKind;

/// The schema information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(from = "serde_hacks::Schema", into = "serde_hacks::Schema")]
pub struct Schema {
    /// This gets bumped on each update.
    pub version: Version,

    // TODO(slinkydeveloper) replace these with the new data model,
    //  See https://github.com/restatedev/restate/issues/3303
    pub services: HashMap<String, ServiceSchemas>,
    pub deployments: HashMap<DeploymentId, DeploymentSchemas>,

    pub subscriptions: HashMap<SubscriptionId, Subscription>,
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            services: HashMap::default(),
            deployments: HashMap::default(),
            subscriptions: HashMap::default(),
        }
    }
}

impl GlobalMetadata for Schema {
    const KEY: &'static str = "schema_registry";

    const KIND: MetadataKind = MetadataKind::Schema;

    fn into_container(self: Arc<Self>) -> MetadataContainer {
        MetadataContainer::Schema(self)
    }
}

impl Schema {
    pub fn increment_version(&mut self) {
        self.version = self.version.next();
    }

    /// Find existing deployments that know about a particular endpoint
    pub fn find_existing_deployments_by_endpoint<'a>(
        &'a self,
        endpoint: &'a DeploymentType,
    ) -> impl Iterator<Item = (&'a DeploymentId, &'a DeploymentSchemas)> {
        self.deployments.iter().filter(|(_, schemas)| {
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

    pub(crate) fn use_service_schema<F, R>(&self, service_name: impl AsRef<str>, f: F) -> Option<R>
    where
        F: FnOnce(&ServiceSchemas) -> R,
    {
        self.services.get(service_name.as_ref()).map(f)
    }
}

impl Versioned for Schema {
    fn version(&self) -> Version {
        self.version
    }
}

pub mod storage {
    use crate::flexbuffers_storage_encode_decode;

    use super::Schema;

    flexbuffers_storage_encode_decode!(Schema);
}

mod serde_hacks {
    use super::*;

    /// The schema information
    #[serde_as]
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct Schema {
        // --- Old data structure
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub services: Option<HashMap<String, ServiceSchemas>>,
        // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
        #[serde_as(as = "Option<serde_with::Seq<(_, _)>>")]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub deployments: Option<HashMap<DeploymentId, DeploymentSchemas>>,

        // --- New data structure

        // Registered deployments
        #[serde_as(as = "Option<restate_serde_util::MapAsVec>")]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub deployments_v2: Option<HashMap<DeploymentId, v2::Deployment>>,

        // --- Same in old and new schema data structure
        /// This gets bumped on each update.
        pub version: Version,
        // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
        #[serde_as(as = "serde_with::Seq<(_, _)>")]
        pub subscriptions: HashMap<SubscriptionId, Subscription>,
    }

    impl From<super::Schema> for Schema {
        fn from(
            super::Schema {
                version,
                services,
                deployments,
                subscriptions,
            }: super::Schema,
        ) -> Self {
            // TODO(slinkydeveloper) Switch the default to write the new data structure in 1.5
            Self {
                services: Some(services),
                deployments: Some(deployments),
                deployments_v2: None,
                version,
                subscriptions,
            }
        }
    }

    impl From<Schema> for super::Schema {
        fn from(
            Schema {
                services,
                deployments,
                deployments_v2,
                version,
                subscriptions,
            }: Schema,
        ) -> Self {
            if let Some(deployments_v2) = deployments_v2 {
                let v2::conversions::V1Schemas {
                    services,
                    deployments,
                } = v2::conversions::V2Schemas {
                    deployments: deployments_v2,
                }
                .into_v1();
                Self {
                    version,
                    services,
                    deployments,
                    subscriptions,
                }
            } else if let (Some(services), Some(deployments)) = (services, deployments) {
                Self {
                    version,
                    services,
                    deployments,
                    subscriptions,
                }
            } else {
                panic!(
                    "Unexpected situation where neither v1 data structure nor v2 data structure is used!"
                )
            }
        }
    }
}

#[cfg(feature = "test-util")]
mod test_util {

    use super::*;

    use super::invocation_target::InvocationTargetResolver;
    use super::service::ServiceMetadata;
    use super::service::ServiceMetadataResolver;
    use crate::identifiers::ServiceRevision;
    use invocation_target::InvocationTargetMetadata;
    use restate_test_util::assert_eq;

    impl Schema {
        #[track_caller]
        pub fn assert_service_handler(
            &self,
            service_name: &str,
            handler_name: &str,
        ) -> InvocationTargetMetadata {
            self.resolve_latest_invocation_target(service_name, handler_name)
                .unwrap_or_else(|| {
                    panic!(
                        "Invocation target for {service_name}/{handler_name} must exists"
                    )
                })
        }

        #[track_caller]
        pub fn assert_service_revision(&self, service_name: &str, revision: ServiceRevision) {
            assert_eq!(
                self.resolve_latest_service(service_name).unwrap().revision,
                revision
            );
        }

        #[track_caller]
        pub fn assert_service_deployment(&self, service_name: &str, deployment_id: DeploymentId) {
            assert_eq!(
                self.resolve_latest_service(service_name)
                    .unwrap()
                    .deployment_id,
                deployment_id
            );
        }

        #[track_caller]
        pub fn assert_service(&self, service_name: &str) -> ServiceMetadata {
            self.resolve_latest_service(service_name).unwrap()
        }
    }
}
