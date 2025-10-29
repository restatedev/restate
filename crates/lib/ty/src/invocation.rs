// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains all the core types representing a service invocation.

use std::fmt;
use std::hash::Hash;

use bytestring::ByteString;

use crate::partitions::{PartitionKey, WithPartitionKey, partitioner};

/// Incremental id defining the service revision.
pub type ServiceRevision = u32;

/// The invocation epoch represents the restarts count of the invocation, as seen from the Partition processor.
pub type InvocationEpoch = u32;

/// Id of a keyed service instance.
///
/// Services are isolated by key. This means that there cannot be two concurrent
/// invocations for the same service instance (service name, key).
#[derive(
    Eq, Hash, PartialEq, PartialOrd, Ord, Clone, Debug, serde::Serialize, serde::Deserialize,
)]
pub struct ServiceId {
    // TODO rename this to KeyedServiceId. This type can be used only by keyed service types (virtual objects and workflows)
    /// Identifies the grpc service
    pub service_name: ByteString,
    /// Identifies the service instance for the given service name
    pub key: ByteString,

    partition_key: PartitionKey,
}

impl ServiceId {
    pub fn new(service_name: impl Into<ByteString>, key: impl Into<ByteString>) -> Self {
        let key = key.into();
        let partition_key = partitioner::HashPartitioner::compute_partition_key(&key);
        Self::with_partition_key(partition_key, service_name, key)
    }

    /// # Important
    /// The `partition_key` must be hash of the `key` computed via [`HashPartitioner`].
    pub fn with_partition_key(
        partition_key: PartitionKey,
        service_name: impl Into<ByteString>,
        key: impl Into<ByteString>,
    ) -> Self {
        Self::from_parts(partition_key, service_name.into(), key.into())
    }

    /// # Important
    /// The `partition_key` must be hash of the `key` computed via [`HashPartitioner`].
    pub const fn from_parts(
        partition_key: PartitionKey,
        service_name: ByteString,
        key: ByteString,
    ) -> Self {
        Self {
            service_name,
            key,
            partition_key,
        }
    }
}

impl WithPartitionKey for ServiceId {
    fn partition_key(&self) -> PartitionKey {
        self.partition_key
    }
}

impl fmt::Display for ServiceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.service_name, self.key)
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum ServiceType {
    Service,
    VirtualObject,
    Workflow,
}

impl ServiceType {
    pub fn is_keyed(&self) -> bool {
        matches!(self, ServiceType::VirtualObject | ServiceType::Workflow)
    }

    pub fn has_state(&self) -> bool {
        self.is_keyed()
    }
}

impl fmt::Display for ServiceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(
    Eq, Hash, PartialEq, Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum VirtualObjectHandlerType {
    #[default]
    Exclusive,
    Shared,
}

impl fmt::Display for VirtualObjectHandlerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(
    Eq, Hash, PartialEq, Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum WorkflowHandlerType {
    #[default]
    Workflow,
    Shared,
}

impl fmt::Display for WorkflowHandlerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum InvocationTargetType {
    Service,
    VirtualObject(VirtualObjectHandlerType),
    Workflow(WorkflowHandlerType),
}

impl InvocationTargetType {
    pub fn is_keyed(&self) -> bool {
        matches!(
            self,
            InvocationTargetType::VirtualObject(_) | InvocationTargetType::Workflow(_)
        )
    }

    pub fn can_read_state(&self) -> bool {
        self.is_keyed()
    }

    pub fn can_write_state(&self) -> bool {
        matches!(
            self,
            InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive)
                | InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
        )
    }
}

impl fmt::Display for InvocationTargetType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl From<InvocationTargetType> for ServiceType {
    fn from(value: InvocationTargetType) -> Self {
        match value {
            InvocationTargetType::Service => ServiceType::Service,
            InvocationTargetType::VirtualObject(_) => ServiceType::VirtualObject,
            InvocationTargetType::Workflow(_) => ServiceType::Workflow,
        }
    }
}

#[derive(Debug, derive_more::Display)]
/// Short is used to create a short [`Display`] implementation
/// for InvocationTarget. it's mainly use for tracing purposes
pub enum Short<'a> {
    #[display("{name}/{{key}}/{handler}")]
    Keyed { name: &'a str, handler: &'a str },
    #[display("{name}/{handler}")]
    UnKeyed { name: &'a str, handler: &'a str },
}

#[derive(Eq, Hash, PartialEq, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum InvocationTarget {
    Service {
        name: ByteString,
        handler: ByteString,
    },
    VirtualObject {
        name: ByteString,
        key: ByteString,
        handler: ByteString,
        handler_ty: VirtualObjectHandlerType,
    },
    Workflow {
        name: ByteString,
        key: ByteString,
        handler: ByteString,
        handler_ty: WorkflowHandlerType,
    },
}

impl InvocationTarget {
    pub fn service(name: impl Into<ByteString>, handler: impl Into<ByteString>) -> Self {
        Self::Service {
            name: name.into(),
            handler: handler.into(),
        }
    }

    pub fn short(&self) -> Short<'_> {
        match self {
            Self::Service { name, handler } => Short::UnKeyed { name, handler },
            Self::VirtualObject { name, handler, .. } | Self::Workflow { name, handler, .. } => {
                Short::Keyed { name, handler }
            }
        }
    }

    pub fn virtual_object(
        name: impl Into<ByteString>,
        key: impl Into<ByteString>,
        handler: impl Into<ByteString>,
        handler_ty: VirtualObjectHandlerType,
    ) -> Self {
        Self::VirtualObject {
            name: name.into(),
            key: key.into(),
            handler: handler.into(),
            handler_ty,
        }
    }

    pub fn workflow(
        name: impl Into<ByteString>,
        key: impl Into<ByteString>,
        handler: impl Into<ByteString>,
        handler_ty: WorkflowHandlerType,
    ) -> Self {
        Self::Workflow {
            name: name.into(),
            key: key.into(),
            handler: handler.into(),
            handler_ty,
        }
    }

    pub fn service_name(&self) -> &ByteString {
        match self {
            InvocationTarget::Service { name, .. } => name,
            InvocationTarget::VirtualObject { name, .. } => name,
            InvocationTarget::Workflow { name, .. } => name,
        }
    }

    pub fn key(&self) -> Option<&ByteString> {
        match self {
            InvocationTarget::Service { .. } => None,
            InvocationTarget::VirtualObject { key, .. } => Some(key),
            InvocationTarget::Workflow { key, .. } => Some(key),
        }
    }

    pub fn handler_name(&self) -> &ByteString {
        match self {
            InvocationTarget::Service { handler, .. } => handler,
            InvocationTarget::VirtualObject { handler, .. } => handler,
            InvocationTarget::Workflow { handler, .. } => handler,
        }
    }

    pub fn as_keyed_service_id(&self) -> Option<ServiceId> {
        match self {
            InvocationTarget::Service { .. } => None,
            InvocationTarget::VirtualObject { name, key, .. } => {
                Some(ServiceId::new(name.clone(), key.clone()))
            }
            InvocationTarget::Workflow { name, key, .. } => {
                Some(ServiceId::new(name.clone(), key.clone()))
            }
        }
    }

    pub fn service_ty(&self) -> ServiceType {
        match self {
            InvocationTarget::Service { .. } => ServiceType::Service,
            InvocationTarget::VirtualObject { .. } => ServiceType::VirtualObject,
            InvocationTarget::Workflow { .. } => ServiceType::Workflow,
        }
    }

    pub fn invocation_target_ty(&self) -> InvocationTargetType {
        match self {
            InvocationTarget::Service { .. } => InvocationTargetType::Service,
            InvocationTarget::VirtualObject { handler_ty, .. } => {
                InvocationTargetType::VirtualObject(*handler_ty)
            }
            InvocationTarget::Workflow { handler_ty, .. } => {
                InvocationTargetType::Workflow(*handler_ty)
            }
        }
    }
}

impl fmt::Display for InvocationTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/", self.service_name())?;
        if let Some(key) = self.key() {
            write!(f, "{key}/")?;
        }
        write!(f, "{}", self.handler_name())?;
        Ok(())
    }
}

#[cfg(any(test, feature = "test-util"))]
mod mocks {
    use super::*;

    use rand::distr::{Alphanumeric, SampleString};

    fn generate_string() -> ByteString {
        Alphanumeric.sample_string(&mut rand::rng(), 8).into()
    }

    impl ServiceId {
        pub fn mock_random() -> Self {
            Self::new(
                Alphanumeric.sample_string(&mut rand::rng(), 8),
                Alphanumeric.sample_string(&mut rand::rng(), 16),
            )
        }

        pub const fn from_static(
            partition_key: PartitionKey,
            service_name: &'static str,
            service_key: &'static str,
        ) -> Self {
            Self {
                service_name: ByteString::from_static(service_name),
                key: ByteString::from_static(service_key),
                partition_key,
            }
        }
    }

    impl InvocationTarget {
        pub fn mock_service() -> Self {
            InvocationTarget::service(generate_string(), generate_string())
        }

        pub fn mock_virtual_object() -> Self {
            InvocationTarget::virtual_object(
                generate_string(),
                generate_string(),
                generate_string(),
                VirtualObjectHandlerType::Exclusive,
            )
        }

        pub fn mock_workflow() -> Self {
            InvocationTarget::workflow(
                generate_string(),
                generate_string(),
                generate_string(),
                WorkflowHandlerType::Workflow,
            )
        }

        pub fn mock_from_service_id(service_id: ServiceId) -> Self {
            InvocationTarget::virtual_object(
                service_id.service_name,
                service_id.key,
                "MyMethod",
                VirtualObjectHandlerType::Exclusive,
            )
        }
    }
}
